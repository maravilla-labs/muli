// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC server construction and startup.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use muli_engine::docker::client::DockerClient;
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_queue::Scheduler;

use muli_proto::agent_service_server::AgentServiceServer;
use muli_proto::git_service_server::GitServiceServer;
use muli_proto::health_service_server::HealthServiceServer;
use muli_proto::job_service_server::JobServiceServer;
use muli_proto::log_service_server::LogServiceServer;
use muli_proto::org_service_server::OrgServiceServer;
use muli_proto::pipeline_service_server::PipelineServiceServer;
use muli_proto::registry_service_server::RegistryServiceServer;
use muli_proto::tenant_service_server::TenantServiceServer;
use muli_proto::user_service_server::UserServiceServer;

use crate::config::ServerConfig;
use crate::grpc::{
    AgentServiceImpl, AuthInterceptor, GitServiceImpl, HealthServiceImpl, JobServiceImpl,
    LogServiceImpl, OrgServiceImpl, PipelineServiceImpl, RegistryServiceImpl, TenantServiceImpl,
    UserServiceImpl,
};
use crate::shutdown::shutdown_signal;
use crate::stores::Stores;

/// Build and start the gRPC server with all services.
pub(crate) async fn start_grpc(
    config: &ServerConfig,
    stores: Stores,
    docker: DockerClient,
    scheduler: Arc<Scheduler>,
    executor: Arc<DockerExecutor>,
    log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    git_storage: Arc<muli_git::storage::FilesystemStorage>,
    pipeline_job_submitter: Arc<dyn muli_pipeline::JobSubmitter>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let job_service = JobServiceImpl {
        store: stores.job_store.clone(),
        scheduler: scheduler.clone(),
        executor: executor.clone(),
        log_collectors: log_collectors.clone(),
    };

    let log_service = LogServiceImpl {
        log_collectors: log_collectors.clone(),
        max_log_lines: config.max_log_lines,
        job_log_store: stores.job_log_store.clone(),
        job_store: stores.job_store.clone(),
    };

    let agent_service = AgentServiceImpl {
        agent_registry: stores.agent_registry,
        job_store: stores.job_store.clone(),
        queue: scheduler.queue().clone(),
        log_collectors: log_collectors.clone(),
        job_log_store: stores.job_log_store.clone(),
    };

    let health_service = HealthServiceImpl::new(docker);

    let registry_service = RegistryServiceImpl {
        token_store: stores.registry_token_store,
        quota_store: stores.tenant_quota_store,
    };

    let git_service = GitServiceImpl {
        repo_store: stores.repo_store,
        token_store: stores.git_token_store,
        ssh_key_store: stores.ssh_key_store,
        webhook_store: stores.webhook_store,
        collaborator_store: stores.collaborator_store,
        git_storage,
        allow_localhost_webhooks: config.git_allow_localhost_webhooks,
    };

    let user_service = UserServiceImpl {
        user_store: stores.user_store,
    };

    let org_service = OrgServiceImpl {
        org_store: stores.org_store,
        member_store: stores.org_member_store,
    };

    let tenant_service = TenantServiceImpl {
        tenant_store: stores.tenant_store,
    };

    let pipeline_service = PipelineServiceImpl {
        pipeline_store: stores.pipeline_store,
        run_store: stores.pipeline_run_store,
        step_store: stores.step_run_store,
        artifact_store: stores.artifact_store,
        cache_store: stores.pipeline_cache_store,
        secret_store: stores.pipeline_secret_store,
        job_store: stores.job_store.clone(),
        job_log_store: stores.job_log_store.clone(),
        job_submitter: pipeline_job_submitter,
    };

    // Auth interceptor (no-op when MULI_API_KEY is unset)
    let auth = AuthInterceptor::new(config.api_key.clone(), config.default_tenant_id.clone());
    if config.api_key.is_some() {
        info!("gRPC authentication enabled (MULI_API_KEY is set)");
    } else {
        info!("gRPC authentication disabled (MULI_API_KEY not set)");
    }

    if config.require_auth && config.api_key.is_none() {
        anyhow::bail!(
            "MULI_REQUIRE_AUTH=true but MULI_API_KEY is not set. \
             Set MULI_API_KEY to a strong random value or disable MULI_REQUIRE_AUTH."
        );
    }

    let grpc_addr = format!("0.0.0.0:{}", config.grpc_port)
        .parse()
        .context("Invalid gRPC address")?;

    info!(addr = %grpc_addr, "gRPC server listening");

    let cancel_grpc = cancel;
    let mut server_builder = tonic::transport::Server::builder();

    if let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path) {
        let cert =
            std::fs::read_to_string(cert_path).context("Failed to read TLS certificate file")?;
        let key = std::fs::read_to_string(key_path).context("Failed to read TLS key file")?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        server_builder = server_builder
            .tls_config(tls_config)
            .context("Invalid TLS configuration")?;
        info!("gRPC TLS enabled");
    } else {
        info!("gRPC TLS disabled (MULI_TLS_CERT_PATH/MULI_TLS_KEY_PATH not set)");
    }

    server_builder
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .http2_adaptive_window(Some(true))
        .initial_connection_window_size(Some(1024 * 1024))
        .initial_stream_window_size(Some(1024 * 1024))
        .max_frame_size(Some(32768))
        .concurrency_limit_per_connection(100)
        .timeout(Duration::from_secs(300))
        .add_service(JobServiceServer::with_interceptor(
            job_service,
            auth.clone(),
        ))
        .add_service(LogServiceServer::with_interceptor(
            log_service,
            auth.clone(),
        ))
        .add_service(AgentServiceServer::with_interceptor(
            agent_service,
            auth.clone(),
        ))
        .add_service(HealthServiceServer::with_interceptor(
            health_service,
            auth.clone(),
        ))
        .add_service(RegistryServiceServer::with_interceptor(
            registry_service,
            auth.clone(),
        ))
        .add_service(GitServiceServer::with_interceptor(
            git_service,
            auth.clone(),
        ))
        .add_service(UserServiceServer::with_interceptor(
            user_service,
            auth.clone(),
        ))
        .add_service(OrgServiceServer::with_interceptor(
            org_service,
            auth.clone(),
        ))
        .add_service(TenantServiceServer::with_interceptor(
            tenant_service,
            auth.clone(),
        ))
        .add_service(PipelineServiceServer::with_interceptor(
            pipeline_service,
            auth,
        ))
        .serve_with_shutdown(grpc_addr, async move {
            if let Err(e) = shutdown_signal().await {
                error!(error = %e, "Failed to install signal handler, cancelling immediately");
            }
            cancel_grpc.cancel();
        })
        .await
        .context("gRPC server error")?;

    Ok(())
}
