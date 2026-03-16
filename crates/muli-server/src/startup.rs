// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server initialization and startup orchestration.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use muli_engine::docker::cleanup::CleanupService;
use muli_engine::docker::client::DockerClient;
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_engine::resource_manager::ResourceManager;
use muli_queue::{ConcurrencyLimiter, PriorityQueue, Scheduler};

use crate::config::ServerConfig;
use crate::metrics::metrics_router;
use crate::start_grpc::start_grpc;
use crate::stores::init_stores;
use crate::{cleanup, embedded_agent, execution, recovery};

/// Run the server with the given configuration.
pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    info!(?config, "Server configuration loaded");

    let shutdown_timeout = Duration::from_secs(config.shutdown_timeout_seconds);

    let stores = init_stores(&config).await?;

    let docker = DockerClient::new().context("Failed to connect to Docker")?;
    docker
        .check_connection()
        .await
        .context("Docker daemon is not reachable")?;
    info!("Docker daemon reachable");

    let resource_manager = Arc::new(ResourceManager::new(
        config.total_cpu_millicores,
        config.total_memory_bytes,
        config.max_concurrent_jobs,
    ));

    let executor = Arc::new(DockerExecutor::new(
        docker.clone(),
        resource_manager.clone(),
    ));

    let log_collectors: Arc<DashMap<String, Arc<LogCollector>>> = Arc::new(DashMap::new());
    let cancel = CancellationToken::new();

    // Scheduler
    let notify = Arc::new(Notify::new());
    let queue = Arc::new(PriorityQueue::new(notify.clone()));
    let limiter = Arc::new(ConcurrencyLimiter::new(
        config.max_concurrent_jobs,
        config.max_jobs_per_tenant,
    ));
    let scheduler = Arc::new(Scheduler::new(queue, limiter, notify));

    recovery::recover_jobs(&stores.job_store, &scheduler).await;

    let scheduler_handle = {
        let scheduler = scheduler.clone();
        let store = stores.job_store.clone();
        let executor = executor.clone();
        let log_collectors = log_collectors.clone();
        let ls = stores.job_log_store.clone();
        let cancel = cancel.clone();

        tokio::spawn(async move {
            scheduler
                .run(cancel, move |job_id, _tenant_id| {
                    let store = store.clone();
                    let executor = executor.clone();
                    let log_collectors = log_collectors.clone();
                    let ls = ls.clone();
                    async move {
                        execution::execute_job(job_id, store, executor, log_collectors, ls).await;
                    }
                })
                .await;
        })
    };

    // Docker cleanup
    let cleanup_service = CleanupService::new(
        docker.clone(),
        Duration::from_secs(config.cleanup_interval_seconds),
        Duration::from_secs(config.cleanup_max_age_seconds),
    );
    let _cleanup_handle = cleanup_service.run();

    // HTTP metrics
    start_metrics(&config, &cancel).await?;

    // Registry
    if config.registry_enabled {
        start_registry(
            &config,
            &stores.registry_token_store,
            &stores.tenant_quota_store,
            &cancel,
        )
        .await?;
    }

    // Git
    if config.git_enabled {
        match muli_git::storage::check_git_available().await {
            Ok(version) => info!("git binary found: {}", version),
            Err(e) => {
                anyhow::bail!(
                    "Cannot start git service: {e}. \
                     Install git or set MULI_GIT_ENABLED=false."
                );
            }
        }
    }

    let git_root = config.effective_git_root();
    let git_storage = Arc::new(
        muli_git::storage::FilesystemStorage::new(git_root.to_str().unwrap_or_default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create git storage: {e}"))?,
    );

    // Job submitter for pipeline steps
    let pipeline_job_submitter: Arc<dyn muli_pipeline::JobSubmitter> =
        Arc::new(crate::pipeline_job_submitter::SchedulerJobSubmitter {
            job_store: stores.job_store.clone(),
            scheduler: scheduler.clone(),
        });

    // Pipeline trigger
    let pipeline_trigger: Option<Arc<dyn muli_git::api::PipelineTriggerHook>> =
        if config.pipeline_enabled && config.git_enabled {
            info!("Pipeline triggering enabled");
            Some(Arc::new(crate::pipeline_trigger::PipelineTriggerImpl::new(
                git_storage.clone(),
                stores.repo_store.clone(),
                stores.pr_store.clone(),
                stores.pipeline_store.clone(),
                stores.pipeline_run_store.clone(),
                stores.step_run_store.clone(),
                stores.job_store.clone(),
                pipeline_job_submitter.clone(),
            )))
        } else {
            None
        };

    let git_stores = GitStores {
        storage: git_storage.clone(),
        repo_store: stores.repo_store.clone(),
        token_store: stores.git_token_store.clone(),
        webhook_store: stores.webhook_store.clone(),
        ssh_key_store: stores.ssh_key_store.clone(),
        pr_store: stores.pr_store.clone(),
        pr_comment_store: stores.pr_comment_store.clone(),
        cache_store: stores.tree_commit_cache.clone(),
        pipeline_trigger,
        org_store: stores.org_store.clone(),
        org_member_store: stores.org_member_store.clone(),
    };
    if config.git_enabled {
        start_git_http(&config, &git_stores, &cancel).await?;
    }
    if config.git_ssh_enabled {
        start_git_ssh(
            &config,
            &stores.ssh_key_store,
            &stores.repo_store,
            &git_storage,
            &stores.org_store,
            &stores.org_member_store,
            &stores.collaborator_store,
            &cancel,
        )
        .await?;
    }

    if config.embedded_agent {
        embedded_agent::spawn(&config, cancel.clone()).await?;
    }

    // Background cleanup tasks
    cleanup::spawn_registry_token_cleanup(stores.registry_token_store.clone(), cancel.clone());
    cleanup::spawn_git_token_cleanup(stores.git_token_store.clone(), cancel.clone());
    cleanup::spawn_job_cleanup(
        stores.job_store.clone(),
        cancel.clone(),
        Duration::from_secs(config.cleanup_interval_seconds),
        Duration::from_secs(config.cleanup_max_age_seconds),
    );

    // gRPC server (blocks until shutdown)
    start_grpc(
        &config,
        stores,
        docker.clone(),
        scheduler.clone(),
        executor.clone(),
        log_collectors,
        git_storage,
        pipeline_job_submitter,
        cancel,
    )
    .await?;

    // Graceful shutdown
    info!(
        timeout_secs = shutdown_timeout.as_secs(),
        "Waiting for scheduler to drain"
    );
    match tokio::time::timeout(shutdown_timeout, scheduler_handle).await {
        Ok(Ok(())) => info!("Scheduler stopped cleanly"),
        Ok(Err(e)) => tracing::warn!(error = %e, "Scheduler task panicked"),
        Err(_) => tracing::warn!("Scheduler did not stop within timeout, forcing shutdown"),
    }

    info!("Server shut down gracefully");
    Ok(())
}

// ── Helper startup functions ────────────────────────────────────────────

async fn start_metrics(
    config: &ServerConfig,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let http_addr = format!("0.0.0.0:{}", config.metrics_port);
    let http_listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .with_context(|| format!("Failed to bind HTTP metrics listener on {http_addr}"))?;
    info!(addr = %http_addr, "HTTP metrics server listening");

    let cancel_http = cancel.clone();
    tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, metrics_router())
            .with_graceful_shutdown(cancel_http.cancelled_owned())
            .await
        {
            error!(error = %e, "HTTP metrics server error");
        }
    });
    Ok(())
}

async fn start_registry(
    config: &ServerConfig,
    registry_token_store: &Arc<dyn muli_core::traits::RegistryTokenStore>,
    tenant_quota_store: &Arc<dyn muli_core::traits::TenantQuotaStore>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let max_blob_size_bytes = config.registry_max_blob_size_mb * 1024 * 1024;
    let registry_storage = Arc::new(
        muli_registry::storage::FilesystemStorage::with_max_blob_size(
            config.effective_registry_root(),
            max_blob_size_bytes,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create registry storage: {e}"))?,
    );
    let mut tenant_config = muli_registry::TenantConfig::new(&config.registry_domain);
    if let Some(ref dt) = config.default_tenant_id {
        tenant_config = tenant_config.with_default_tenant(dt.as_str());
    }
    let registry_auth = muli_registry::RegistryAuth::new(registry_token_store.clone());
    let registry_config = muli_registry::api::RegistryConfig {
        npm_enabled: config.npm_enabled,
        cargo_enabled: config.cargo_enabled,
        maven_enabled: config.maven_enabled,
    };
    let registry_router = muli_registry::registry_router(
        registry_storage,
        Some(registry_auth),
        tenant_config,
        Some(tenant_quota_store.clone()),
        registry_config,
    );

    let registry_addr = format!("0.0.0.0:{}", config.registry_port);

    if let (Some(cert_path), Some(key_path)) = (
        &config.registry_tls_cert_path,
        &config.registry_tls_key_path,
    ) {
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
                .await
                .context("Failed to load registry TLS certificate/key")?;
        info!(addr = %registry_addr, "Registry listening with TLS");

        let cancel_registry = cancel.clone();
        tokio::spawn(async move {
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                cancel_registry.cancelled().await;
                shutdown_handle.graceful_shutdown(None);
            });
            if let Err(e) = axum_server::bind_rustls(
                registry_addr.parse().expect("Invalid registry address"),
                rustls_config,
            )
            .handle(handle)
            .serve(registry_router.into_make_service())
            .await
            {
                error!(error = %e, "Registry TLS server error");
            }
        });
    } else {
        let registry_listener = tokio::net::TcpListener::bind(&registry_addr)
            .await
            .with_context(|| format!("Failed to bind registry listener on {registry_addr}"))?;
        info!(addr = %registry_addr, "Registry listening on port {}", config.registry_port);

        let cancel_registry = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = axum::serve(registry_listener, registry_router)
                .with_graceful_shutdown(cancel_registry.cancelled_owned())
                .await
            {
                error!(error = %e, "Registry server error");
            }
        });
    }

    Ok(())
}

/// Bundled git store dependencies for passing to helper functions.
pub(crate) struct GitStores {
    pub storage: Arc<muli_git::storage::FilesystemStorage>,
    pub repo_store: Arc<dyn muli_core::traits::RepositoryStore>,
    pub token_store: Arc<dyn muli_core::traits::GitTokenStore>,
    pub webhook_store: Arc<dyn muli_core::traits::WebhookStore>,
    pub ssh_key_store: Arc<dyn muli_core::traits::SshKeyStore>,
    pub pr_store: Arc<dyn muli_core::traits::PullRequestStore>,
    pub pr_comment_store: Arc<dyn muli_core::traits::PrCommentStore>,
    pub cache_store: Arc<dyn muli_core::traits::TreeCommitCacheStore>,
    pub pipeline_trigger: Option<Arc<dyn muli_git::api::PipelineTriggerHook>>,
    pub org_store: Arc<dyn muli_core::traits::OrgStore>,
    pub org_member_store: Arc<dyn muli_core::traits::OrgMemberStore>,
}

async fn start_git_http(
    config: &ServerConfig,
    git: &GitStores,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let mut git_tenant_config = muli_git::TenantConfig::new(&config.git_domain);
    if let Some(ref dt) = config.default_tenant_id {
        git_tenant_config = git_tenant_config.with_default_tenant(dt.as_str());
    }
    let git_auth = muli_git::GitAuth::new(git.token_store.clone())
        .with_org_stores(git.org_store.clone(), git.org_member_store.clone());
    let lfs_storage: Option<Arc<dyn muli_git::lfs::storage::LfsStorage>> = {
        let git_root = config.effective_git_root();
        match muli_git::lfs::storage::filesystem::FilesystemLfsStorage::new(
            &git_root,
            config.lfs_max_object_size_mb * 1024 * 1024,
        )
        .await
        {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                warn!(error = %e, "Failed to initialize LFS storage, LFS disabled");
                None
            }
        }
    };

    let git_app = muli_git::git_router(muli_git::GitRouterConfig {
        storage: git.storage.clone(),
        repo_store: git.repo_store.clone(),
        token_store: git.token_store.clone(),
        webhook_store: git.webhook_store.clone(),
        ssh_key_store: git.ssh_key_store.clone(),
        pr_store: git.pr_store.clone(),
        pr_comment_store: git.pr_comment_store.clone(),
        auth: Some(git_auth),
        tenant_config: git_tenant_config,
        cache_store: Some(git.cache_store.clone()),
        allow_localhost_webhooks: config.git_allow_localhost_webhooks,
        lfs_storage,
        pipeline_trigger: git.pipeline_trigger.clone(),
    });

    let git_addr = format!("0.0.0.0:{}", config.git_port);
    let git_listener = tokio::net::TcpListener::bind(&git_addr)
        .await
        .with_context(|| format!("Failed to bind git listener on {git_addr}"))?;
    info!(addr = %git_addr, "Git service listening on port {}", config.git_port);

    let cancel_git = cancel.clone();
    tokio::spawn(async move {
        if let Err(e) = axum::serve(git_listener, git_app)
            .with_graceful_shutdown(cancel_git.cancelled_owned())
            .await
        {
            error!(error = %e, "Git server error");
        }
    });

    Ok(())
}

async fn start_git_ssh(
    config: &ServerConfig,
    ssh_key_store: &Arc<dyn muli_core::traits::SshKeyStore>,
    repo_store: &Arc<dyn muli_core::traits::RepositoryStore>,
    git_storage: &Arc<muli_git::storage::FilesystemStorage>,
    org_store: &Arc<dyn muli_core::traits::OrgStore>,
    org_member_store: &Arc<dyn muli_core::traits::OrgMemberStore>,
    collaborator_store: &Arc<dyn muli_core::traits::CollaboratorStore>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let host_key_path = config
        .git_ssh_host_key_path
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| config.effective_git_root().join("ssh_host_ed25519_key"));

    let host_key = muli_git::ssh::load_or_generate_host_key(&host_key_path)
        .await
        .context("Failed to load or generate SSH host key")?;

    let ssh_server = muli_git::SshServer {
        ssh_key_store: ssh_key_store.clone(),
        repo_store: repo_store.clone(),
        storage: git_storage.clone(),
        default_tenant_id: config.default_tenant_id.clone(),
        org_store: org_store.clone(),
        org_member_store: org_member_store.clone(),
        collaborator_store: collaborator_store.clone(),
        token_store: None,
        git_domain: Some(config.git_domain.clone()),
    };

    let ssh_addr = format!("0.0.0.0:{}", config.git_ssh_port);
    let ssh_listener = tokio::net::TcpListener::bind(&ssh_addr)
        .await
        .with_context(|| format!("Failed to bind SSH git listener on {ssh_addr}"))?;
    info!(addr = %ssh_addr, "Git SSH service listening on port {}", config.git_ssh_port);

    let cancel_ssh = cancel.clone();
    tokio::spawn(async move {
        if let Err(e) = ssh_server.run_on(ssh_listener, host_key, cancel_ssh).await {
            error!(error = %e, "Git SSH server error");
        }
    });

    Ok(())
}
