// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared helpers for E2E tests that require a Docker registry + real agent.
#![allow(dead_code)]

use std::sync::Arc;

use super::{dummy_executor, with_tenant};
use muli_test::grpc_helpers::test_submit_request;

// ---------------------------------------------------------------------------
// Docker / registry helpers
// ---------------------------------------------------------------------------

pub fn has_command(cmd: &str) -> bool {
    std::process::Command::new("which")
        .arg(cmd)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn is_docker_desktop() -> bool {
    std::process::Command::new("docker")
        .args(["context", "show"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .starts_with("desktop")
        })
        .unwrap_or(false)
}

fn docker_registry_host() -> &'static str {
    if is_docker_desktop() {
        "host.docker.internal"
    } else {
        "127.0.0.1"
    }
}

fn docker_has_insecure_registry_for_host_internal() -> bool {
    let output = std::process::Command::new("docker")
        .args(["info", "--format", "{{json .RegistryConfig}}"])
        .output()
        .ok();
    match output {
        Some(o) if o.status.success() => {
            let text = String::from_utf8_lossy(&o.stdout);
            text.contains("host.docker.internal") || text.contains("192.168.65.")
        }
        _ => false,
    }
}

pub fn e2e_skip_guards() -> bool {
    if !has_command("docker") {
        eprintln!("SKIP: docker CLI not found");
        return true;
    }
    let docker_host = docker_registry_host();
    if docker_host == "host.docker.internal" && !docker_has_insecure_registry_for_host_internal() {
        eprintln!(
            "SKIP: Docker Desktop detected but 'host.docker.internal' is not in \
             insecure-registries. Add it via Docker Desktop Settings > Docker Engine."
        );
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// E2E registry
// ---------------------------------------------------------------------------

pub struct E2eRegistry {
    pub port: u16,
    pub docker_host: &'static str,
    pub plaintext_token: &'static str,
    pub docker_config: std::path::PathBuf,
    _registry_tmp: tempfile::TempDir,
    _docker_cfg_dir: tempfile::TempDir,
}

impl E2eRegistry {
    pub fn registry_addr(&self) -> String {
        format!("{}:{}", self.docker_host, self.port)
    }
}

pub async fn start_e2e_registry() -> E2eRegistry {
    use muli_core::registry::model::{RegistryPermission, RegistryToken};
    use muli_core::traits::RegistryTokenStore;
    use muli_registry::api::{RegistryConfig, registry_router};
    use muli_registry::auth::{RegistryAuth, hash_token, token_prefix};
    use muli_registry::storage::FilesystemStorage;
    use muli_registry::tenant::TenantConfig;
    use muli_store::memory::MemoryRegistryTokenStore;
    use tempfile::TempDir;

    let docker_host = docker_registry_host();
    let plaintext_token: &'static str = "e2e-test-token-secret";

    let registry_tmp = TempDir::new().expect("registry temp dir");
    let storage = Arc::new(
        FilesystemStorage::new(registry_tmp.path())
            .await
            .expect("registry storage"),
    );

    let token_hash = hash_token(plaintext_token);
    let prefix = token_prefix(plaintext_token);
    let token_store = Arc::new(MemoryRegistryTokenStore::new());
    let token = RegistryToken::new(
        "test-tenant".to_string(),
        token_hash,
        prefix,
        vec![RegistryPermission::Pull, RegistryPermission::Push],
        "e2e test".to_string(),
        None,
    );
    token_store
        .create_token(&token)
        .await
        .expect("insert token");

    let tenant_config = TenantConfig::new("localhost").with_default_tenant("test-tenant");
    let auth = RegistryAuth::new(token_store);
    let registry_router = registry_router(
        storage,
        Some(auth),
        tenant_config,
        None,
        RegistryConfig {
            npm_enabled: false,
            maven_enabled: false,
            cargo_enabled: false,
        },
    );

    let registry_listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .expect("bind registry");
    let port = registry_listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(registry_listener, registry_router).await.ok();
    });

    let docker_cfg_dir = TempDir::new().expect("docker config dir");
    let registry_addr = format!("{docker_host}:{port}");
    let auth_string = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(format!("user:{plaintext_token}"))
    };
    let config_json = serde_json::json!({
        "auths": {
            registry_addr: {
                "auth": auth_string
            }
        }
    });
    std::fs::write(
        docker_cfg_dir.path().join("config.json"),
        serde_json::to_string_pretty(&config_json).unwrap(),
    )
    .unwrap();
    let docker_config = docker_cfg_dir.path().to_path_buf();

    E2eRegistry {
        port,
        docker_host,
        plaintext_token,
        docker_config,
        _registry_tmp: registry_tmp,
        _docker_cfg_dir: docker_cfg_dir,
    }
}

// ---------------------------------------------------------------------------
// Docker image build/push
// ---------------------------------------------------------------------------

pub async fn build_and_push_image(
    registry: &E2eRegistry,
    image_name: &str,
    dockerfile: &str,
) -> String {
    use tempfile::TempDir;

    let image_ref = format!("{}/{}", registry.registry_addr(), image_name);

    let build_dir = TempDir::new().expect("build dir");
    std::fs::write(build_dir.path().join("Dockerfile"), dockerfile).unwrap();

    let build_ctx = build_dir.path().to_path_buf();
    let tag = image_ref.clone();
    let cfg = registry.docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["build", "-t", &tag, "."])
            .current_dir(&build_ctx)
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker build")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker build failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let img = image_ref.clone();
    let cfg = registry.docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["push", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker push")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker push failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let img = image_ref.clone();
    let cfg = registry.docker_config.clone();
    let _ = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["rmi", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
    })
    .await;

    image_ref
}

// ---------------------------------------------------------------------------
// Job submission helper
// ---------------------------------------------------------------------------

pub async fn submit_e2e_job(
    server_port: u16,
    image_ref: &str,
    registry: &E2eRegistry,
    timeout_seconds: u64,
) -> String {
    let mut job_cl = muli_test::grpc_helpers::job_client(server_port).await;

    let mut req = test_submit_request();
    req.runner_image = image_ref.to_string();
    req.registry_credentials = Some(muli_proto::RegistryCredentials {
        server: format!("http://{}:{}", registry.docker_host, registry.port),
        username: "user".into(),
        password: registry.plaintext_token.into(),
    });
    if let Some(ref mut res) = req.resources {
        res.timeout_seconds = timeout_seconds;
    }

    let resp = job_cl
        .submit_job(with_tenant(req, "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;
    assert!(!job_id.is_empty(), "job_id should not be empty");
    job_id
}

// ---------------------------------------------------------------------------
// Real agent handle
// ---------------------------------------------------------------------------

pub struct AgentHandle {
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
    pub hb_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    pub worker_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    pub agent_id: String,
}

impl AgentHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = tokio::join!(self.hb_handle, self.worker_handle);
    }
}

pub async fn start_real_agent(server_port: u16) -> AgentHandle {
    use muli_agent::config::AgentConfig;
    use muli_agent::heartbeat::heartbeat_loop;
    use muli_agent::registration;
    use muli_agent::worker::worker_loop;

    let config = AgentConfig {
        name: format!("e2e-agent-{}", uuid::Uuid::new_v4()),
        server_url: format!("http://127.0.0.1:{server_port}"),
        heartbeat_interval_secs: 1,
        max_concurrent_jobs: 4,
        total_cpu_millicores: 4000,
        total_memory_bytes: 8_589_934_592,
        labels: vec![],
        shutdown_timeout_secs: 10,
        api_key: None,
        tls_ca_cert: None,
    };

    let (client, agent_id) = registration::register(&config)
        .await
        .expect("agent registration");

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let (assignment_tx, assignment_rx) = tokio::sync::mpsc::channel(32);
    let running_jobs = Arc::new(std::sync::atomic::AtomicU32::new(0));

    let hb_handle = tokio::spawn(heartbeat_loop(
        client.clone(),
        agent_id.clone(),
        config.clone(),
        running_jobs.clone(),
        assignment_tx,
        shutdown_tx.subscribe(),
    ));

    let executor = dummy_executor().await;
    let worker_handle = tokio::spawn(worker_loop(
        client,
        agent_id.clone(),
        executor,
        running_jobs,
        assignment_rx,
        shutdown_tx.subscribe(),
    ));

    AgentHandle {
        shutdown_tx,
        hb_handle,
        worker_handle,
        agent_id,
    }
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

pub async fn e2e_cleanup_image(image_ref: &str, docker_config: &std::path::Path) {
    let img = image_ref.to_string();
    let cfg = docker_config.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || {
        let _ = std::process::Command::new("docker")
            .args(["rmi", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output();
    })
    .await;

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}
