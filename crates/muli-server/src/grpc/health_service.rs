// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Health check gRPC service.

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use muli_engine::docker::client::DockerClient;

use muli_proto::health_service_server::HealthService;
use muli_proto::{ComponentHealth, HealthCheckRequest, HealthCheckResponse, ServingStatus};

/// Capacity of the broadcast channel used to fan out health updates to all Watch subscribers.
const BROADCAST_CAPACITY: usize = 16;

/// Interval between Docker health polls (10 seconds).
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

pub struct HealthServiceImpl {
    pub docker: DockerClient,
    /// Shared sender for the single background polling loop.
    /// Each `watch()` call subscribes to this channel.
    broadcast_tx: Arc<broadcast::Sender<HealthCheckResponse>>,
}

impl HealthServiceImpl {
    /// Create a new `HealthServiceImpl` and spawn the single background Docker
    /// polling loop. All `watch()` subscribers receive updates from that loop.
    pub fn new(docker: DockerClient) -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        let tx = Arc::new(tx);

        // Spawn the single shared polling task.
        let poll_docker = docker.clone();
        let poll_tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let docker_healthy = poll_docker.check_connection().await.is_ok();

                let response = build_health_response(docker_healthy);

                // It is fine if there are no receivers yet — just ignore the error.
                let _ = poll_tx.send(response);

                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });

        Self {
            docker,
            broadcast_tx: tx,
        }
    }
}

fn build_health_response(docker_healthy: bool) -> HealthCheckResponse {
    let status = if docker_healthy {
        ServingStatus::Serving as i32
    } else {
        ServingStatus::NotServing as i32
    };
    HealthCheckResponse {
        status,
        components: vec![ComponentHealth {
            name: "docker".to_string(),
            status,
            message: if docker_healthy {
                "Docker daemon is reachable".to_string()
            } else {
                "Docker daemon is unreachable".to_string()
            },
        }],
    }
}

#[tonic::async_trait]
impl HealthService for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let docker_healthy = self.docker.check_connection().await.is_ok();
        Ok(Response::new(build_health_response(docker_healthy)))
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // Subscribe to the shared broadcast channel.
        let mut broadcast_rx = self.broadcast_tx.subscribe();
        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(response) => {
                        if tx.send(Ok(response)).await.is_err() {
                            // Client disconnected.
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Subscriber fell behind; skip missed messages and continue.
                        tracing::warn!(
                            skipped = n,
                            "health watch subscriber lagged; skipping messages"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Sender dropped — server is shutting down.
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
