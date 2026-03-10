// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Background cleanup service for orphaned containers and volumes.

use std::collections::HashMap;
use std::time::Duration;

use bollard::container::{ListContainersOptions, RemoveContainerOptions};
use bollard::network::ListNetworksOptions;
use tracing::{debug, info, warn};

use muli_core::error::Result;

use super::client::DockerClient;

/// Periodic cleanup service for orphaned containers and networks.
pub struct CleanupService {
    docker: DockerClient,
    interval: Duration,
    max_age: Duration,
}

impl CleanupService {
    pub fn new(docker: DockerClient, interval: Duration, max_age: Duration) -> Self {
        Self {
            docker,
            interval,
            max_age,
        }
    }

    /// Spawn the periodic cleanup task. Returns a JoinHandle.
    pub fn run(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            loop {
                interval.tick().await;
                if let Err(e) = self.cleanup_once().await {
                    warn!(error = %e, "Cleanup cycle failed");
                }
            }
        })
    }

    async fn cleanup_once(&self) -> Result<()> {
        self.cleanup_exited_containers().await?;
        self.cleanup_orphan_networks().await?;
        Ok(())
    }

    /// Remove exited containers with `managed-by=muli` label older than max_age.
    async fn cleanup_exited_containers(&self) -> Result<()> {
        let mut filters = HashMap::new();
        filters.insert("label", vec!["managed-by=muli"]);
        filters.insert("status", vec!["exited", "dead"]);

        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        let containers = match self.docker.inner().list_containers(Some(options)).await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "Failed to list containers for cleanup");
                return Ok(());
            }
        };

        let cutoff = chrono::Utc::now().timestamp() - self.max_age.as_secs() as i64;

        for container in containers {
            let created = container.created.unwrap_or(0);
            if created < cutoff {
                let id = container.id.as_deref().unwrap_or("unknown");
                let name = container
                    .names
                    .as_ref()
                    .and_then(|n| n.first())
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");

                debug!(container_id = %id, name = %name, "Removing old container");

                let remove_options = RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                };

                if let Err(e) = self
                    .docker
                    .inner()
                    .remove_container(id, Some(remove_options))
                    .await
                {
                    warn!(container_id = %id, error = %e, "Failed to remove container");
                }
            }
        }

        Ok(())
    }

    /// Remove networks with `managed-by=muli` label that have no connected containers.
    async fn cleanup_orphan_networks(&self) -> Result<()> {
        let mut filters = HashMap::new();
        filters.insert("label", vec!["managed-by=muli"]);

        let options = ListNetworksOptions { filters };

        let networks = match self.docker.inner().list_networks(Some(options)).await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "Failed to list networks for cleanup");
                return Ok(());
            }
        };

        for network in networks {
            let has_containers = network
                .containers
                .as_ref()
                .map(|c| !c.is_empty())
                .unwrap_or(false);

            if !has_containers {
                let id = network.id.as_deref().unwrap_or("unknown");
                let name = network.name.as_deref().unwrap_or("unknown");

                debug!(network_id = %id, name = %name, "Removing orphan network");

                if let Err(e) = self.docker.inner().remove_network(id).await {
                    warn!(network_id = %id, error = %e, "Failed to remove network");
                }
            }
        }

        info!("Cleanup cycle completed");
        Ok(())
    }
}
