// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker image pulling with progress tracking.

use std::time::Duration;

use bollard::auth::DockerCredentials;
use bollard::image::CreateImageOptions;
use futures::StreamExt;
use tracing::{debug, info, warn};

use muli_core::error::{MuliError, Result};
use muli_core::job::model::RegistryCredentials;

use super::client::DockerClient;

/// Default timeout for an entire image pull operation (10 minutes).
const IMAGE_PULL_TIMEOUT: Duration = Duration::from_secs(600);

/// Ensure an image is available locally, pulling if necessary.
pub async fn ensure_image(
    docker: &DockerClient,
    image: &str,
    credentials: Option<&RegistryCredentials>,
) -> Result<()> {
    // Check if image exists locally
    match docker.inner().inspect_image(image).await {
        Ok(_) => {
            debug!(image = %image, "Image already available locally");
            return Ok(());
        }
        Err(_) => {
            info!(image = %image, "Image not found locally, pulling...");
        }
    }

    pull_image_with_retry(docker, image, credentials, 3).await
}

/// Pull an image with exponential backoff retry.
async fn pull_image_with_retry(
    docker: &DockerClient,
    image: &str,
    credentials: Option<&RegistryCredentials>,
    max_attempts: u32,
) -> Result<()> {
    let mut attempt = 0;

    loop {
        attempt += 1;
        match pull_image(docker, image, credentials).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt >= max_attempts {
                    return Err(MuliError::ImagePullFailed {
                        image: image.to_string(),
                        reason: format!("Failed after {max_attempts} attempts: {e}"),
                    });
                }
                let delay = std::time::Duration::from_secs(1 << attempt);
                warn!(
                    image = %image,
                    attempt = attempt,
                    max_attempts = max_attempts,
                    delay_secs = delay.as_secs(),
                    error = %e,
                    "Image pull failed, retrying..."
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Pull a single image from a registry with an overall timeout.
async fn pull_image(
    docker: &DockerClient,
    image: &str,
    credentials: Option<&RegistryCredentials>,
) -> Result<()> {
    tokio::time::timeout(
        IMAGE_PULL_TIMEOUT,
        pull_image_inner(docker, image, credentials),
    )
    .await
    .map_err(|_| MuliError::Timeout(IMAGE_PULL_TIMEOUT))?
}

/// Inner pull implementation without timeout.
async fn pull_image_inner(
    docker: &DockerClient,
    image: &str,
    credentials: Option<&RegistryCredentials>,
) -> Result<()> {
    let options = CreateImageOptions {
        from_image: image,
        ..Default::default()
    };

    let docker_creds = credentials.map(|c| DockerCredentials {
        username: Some(c.username.clone()),
        password: Some(c.password.clone()),
        serveraddress: Some(c.server.clone()),
        ..Default::default()
    });

    let mut stream = docker
        .inner()
        .create_image(Some(options), None, docker_creds);

    while let Some(result) = stream.next().await {
        match result {
            Ok(info) => {
                if let Some(status) = &info.status {
                    debug!(image = %image, status = %status, "Pull progress");
                }
            }
            Err(e) => {
                return Err(MuliError::Docker(format!("Image pull error: {e}")));
            }
        }
    }

    info!(image = %image, "Image pulled successfully");
    Ok(())
}
