// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embedded agent for single-node deployments.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::Context;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use muli_agent::{
    config::AgentConfig as EmbeddedAgentConfig, heartbeat::heartbeat_loop, registration,
    worker::worker_loop,
};
use muli_engine::executor::DockerExecutor;
use muli_engine::resource_manager::ResourceManager;

use crate::config::ServerConfig;

/// Spawn an in-process agent that connects back to the local gRPC server.
pub async fn spawn(config: &ServerConfig, cancel: CancellationToken) -> anyhow::Result<()> {
    info!("Embedded agent enabled — spawning in-process agent");

    let agent_docker =
        muli_engine::docker::client::DockerClient::new().context("embedded agent: Docker")?;
    agent_docker
        .check_connection()
        .await
        .context("embedded agent: Docker check")?;
    info!("embedded agent: Docker daemon reachable");

    let agent_rm = Arc::new(ResourceManager::new(
        config.total_cpu_millicores,
        config.total_memory_bytes,
        config.max_concurrent_jobs,
    ));
    let agent_executor = Arc::new(DockerExecutor::new(agent_docker, agent_rm));

    let agent_cfg = EmbeddedAgentConfig {
        name: "embedded".to_string(),
        server_url: format!("http://127.0.0.1:{}", config.grpc_port),
        heartbeat_interval_secs: 10,
        max_concurrent_jobs: config.max_concurrent_jobs as u32,
        total_cpu_millicores: config.total_cpu_millicores,
        total_memory_bytes: config.total_memory_bytes,
        labels: vec![],
        shutdown_timeout_secs: config.shutdown_timeout_seconds,
        api_key: config.api_key.clone(),
        tls_ca_cert: None,
    };

    let (agent_shutdown_tx, _) = broadcast::channel::<()>(1);

    // Forward CancellationToken → agent broadcast shutdown
    let agent_shutdown_fwd = agent_shutdown_tx.clone();
    tokio::spawn(async move {
        cancel.cancelled().await;
        let _ = agent_shutdown_fwd.send(());
    });

    tokio::spawn(async move {
        let (client, agent_id) = match registration::register(&agent_cfg).await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "embedded agent registration failed");
                return;
            }
        };
        info!(agent_id = %agent_id, "embedded agent registered");

        let running_jobs = Arc::new(AtomicU32::new(0));
        let (assignment_tx, assignment_rx) = mpsc::channel(32);

        tokio::spawn(heartbeat_loop(
            client.clone(),
            agent_id.clone(),
            agent_cfg.clone(),
            running_jobs.clone(),
            assignment_tx,
            agent_shutdown_tx.subscribe(),
        ));
        tokio::spawn(worker_loop(
            client,
            agent_id,
            agent_executor,
            running_jobs,
            assignment_rx,
            agent_shutdown_tx.subscribe(),
        ));
    });

    Ok(())
}
