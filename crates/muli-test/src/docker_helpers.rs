// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker test container management.

use bollard::container::{ListContainersOptions, RemoveContainerOptions};
use bollard::image::CreateImageOptions;
use futures::StreamExt;
use std::collections::HashMap;
use tracing::info;

use muli_engine::docker::client::DockerClient;

/// Check if Docker daemon is available for integration tests.
pub async fn docker_available() -> bool {
    match DockerClient::new() {
        Ok(client) => client.check_connection().await.is_ok(),
        Err(_) => false,
    }
}

/// Create a DockerClient, panicking if Docker is unavailable.
pub async fn require_docker() -> DockerClient {
    let client = DockerClient::new().expect("Failed to create DockerClient");
    client
        .check_connection()
        .await
        .expect("Docker daemon is not available — skipping integration test");
    client
}

/// Pull a test image if not already present.
pub async fn ensure_test_image(docker: &DockerClient, image: &str) {
    let options = CreateImageOptions {
        from_image: image,
        ..Default::default()
    };

    let mut stream = docker.inner().create_image(Some(options), None, None);
    while let Some(result) = stream.next().await {
        match result {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to pull test image '{image}': {e}");
            }
        }
    }

    info!(image = %image, "Test image ready");
}

/// Clean up any muli test containers (labeled with managed-by=muli).
///
/// Only removes containers that have already stopped/exited — never force-kills
/// running containers, which would interfere with concurrently-running tests.
pub async fn cleanup_test_containers(docker: &DockerClient) {
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec!["managed-by=muli".to_string()]);
    // Only list containers that are no longer running so we do not SIGKILL
    // containers owned by other tests that are still executing in parallel.
    filters.insert(
        "status".to_string(),
        vec!["exited".to_string(), "dead".to_string()],
    );

    let options = ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    };

    match docker.inner().list_containers(Some(options)).await {
        Ok(containers) => {
            for container in containers {
                if let Some(id) = container.id {
                    let remove_options = RemoveContainerOptions {
                        force: true,
                        v: true,
                        ..Default::default()
                    };
                    if let Err(e) = docker
                        .inner()
                        .remove_container(&id, Some(remove_options))
                        .await
                    {
                        tracing::warn!(container_id = %id, error = %e, "Failed to remove test container");
                    } else {
                        info!(container_id = %id, "Removed test container");
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to list test containers for cleanup");
        }
    }
}
