// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker network creation and cleanup.

use std::collections::HashMap;

use bollard::models::EndpointSettings;
use bollard::network::{ConnectNetworkOptions, CreateNetworkOptions};
use tracing::{debug, info};

use muli_core::error::{MuliError, Result};

use super::client::DockerClient;

/// Create an isolated bridge network for a job.
pub async fn create_network(docker: &DockerClient, job_id: &str) -> Result<String> {
    let network_name = format!("muli-net-{job_id}");

    let mut labels = HashMap::new();
    labels.insert("managed-by", "muli");
    labels.insert("job-id", job_id);

    let options = CreateNetworkOptions {
        name: network_name.as_str(),
        driver: "bridge",
        labels,
        ..Default::default()
    };

    let response = docker
        .inner()
        .create_network(options)
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to create network: {e}")))?;

    let network_id = response.id;

    info!(
        network_id = %network_id,
        job_id = %job_id,
        "Network created"
    );

    Ok(network_id)
}

/// Remove a job's network.
pub async fn remove_network(docker: &DockerClient, job_id: &str) -> Result<()> {
    let network_name = format!("muli-net-{job_id}");

    docker
        .inner()
        .remove_network(&network_name)
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to remove network: {e}")))?;

    debug!(job_id = %job_id, "Network removed");
    Ok(())
}

/// Connect a container to a network.
pub async fn connect_container(
    docker: &DockerClient,
    network_id: &str,
    container_id: &str,
) -> Result<()> {
    let options = ConnectNetworkOptions {
        container: container_id.to_string(),
        endpoint_config: EndpointSettings::default(),
    };

    docker
        .inner()
        .connect_network(network_id, options)
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to connect container to network: {e}")))?;

    debug!(
        network_id = %network_id,
        container_id = %container_id,
        "Container connected to network"
    );

    Ok(())
}
