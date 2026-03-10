// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Agent entry point and CLI argument parsing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use muli_agent::config::AgentConfig;
use muli_agent::heartbeat::heartbeat_loop;
use muli_agent::registration;
use muli_agent::worker::worker_loop;
use muli_engine::docker::client::DockerClient;
use muli_engine::executor::DockerExecutor;
use muli_engine::resource_manager::ResourceManager;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "muli_agent=info".into()),
        )
        .init();

    let _ = dotenvy::dotenv();
    let config = AgentConfig::parse();

    info!(name = %config.name, server = %config.server_url, "starting muli-agent");

    let shutdown_timeout = Duration::from_secs(config.shutdown_timeout_secs);

    // Connect to Docker daemon
    let docker =
        DockerClient::new().map_err(|e| anyhow::anyhow!("Failed to connect to Docker: {e}"))?;
    docker
        .check_connection()
        .await
        .map_err(|e| anyhow::anyhow!("Docker connection check failed: {e}"))?;

    // Initialize resource manager and executor
    let resource_manager = Arc::new(ResourceManager::new(
        config.total_cpu_millicores,
        config.total_memory_bytes,
        config.max_concurrent_jobs as usize,
    ));
    let executor = Arc::new(DockerExecutor::new(docker, resource_manager));

    // Register with server
    let (client, agent_id) = registration::register(&config).await?;

    // Shared state
    let running_jobs = Arc::new(AtomicU32::new(0));
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let (assignment_tx, assignment_rx) = mpsc::channel(32);

    // Spawn heartbeat loop
    let hb_client = client.clone();
    let hb_agent_id = agent_id.clone();
    let hb_config = config.clone();
    let hb_running = running_jobs.clone();
    let hb_shutdown = shutdown_tx.subscribe();
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(
            hb_client,
            hb_agent_id,
            hb_config,
            hb_running,
            assignment_tx,
            hb_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "heartbeat loop failed");
        }
    });

    // Spawn worker loop
    let w_client = client.clone();
    let w_agent_id = agent_id.clone();
    let w_executor = executor;
    let w_running = running_jobs.clone();
    let w_shutdown = shutdown_tx.subscribe();
    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker_loop(
            w_client,
            w_agent_id,
            w_executor,
            w_running,
            assignment_rx,
            w_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "worker loop failed");
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("received shutdown signal");

    // Signal heartbeat and worker loops to stop accepting new work
    let _ = shutdown_tx.send(());

    // Wait for running jobs to complete with a timeout
    let drain_running = running_jobs.clone();
    let drain_result = tokio::time::timeout(shutdown_timeout, async move {
        loop {
            let count = drain_running.load(Ordering::Relaxed);
            if count == 0 {
                break;
            }
            info!(running_jobs = count, "waiting for running jobs to finish");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    match drain_result {
        Ok(()) => info!("all running jobs completed"),
        Err(_) => {
            let remaining = running_jobs.load(Ordering::Relaxed);
            warn!(
                remaining_jobs = remaining,
                timeout_secs = shutdown_timeout.as_secs(),
                "shutdown timeout reached, proceeding with deregistration"
            );
        }
    }

    // Deregister gracefully
    let mut dereg_client = client;
    let _ = registration::deregister(&mut dereg_client, &agent_id, "graceful shutdown").await;

    let _ = heartbeat_handle.await;
    let _ = worker_handle.await;

    info!("muli-agent stopped");
    Ok(())
}
