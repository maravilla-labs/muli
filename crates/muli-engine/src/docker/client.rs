// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker daemon client wrapper (via bollard).

use std::sync::Arc;

use bollard::Docker;
use tracing::debug;

use muli_core::error::{MuliError, Result};

/// Wrapper around the Bollard Docker client with convenience methods.
#[derive(Clone)]
pub struct DockerClient {
    inner: Arc<Docker>,
}

impl DockerClient {
    /// Connect to the Docker daemon using local defaults.
    pub fn new() -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| MuliError::Docker(format!("Failed to connect to Docker: {e}")))?;

        Ok(Self {
            inner: Arc::new(docker),
        })
    }

    /// Check that the Docker daemon is reachable.
    pub async fn check_connection(&self) -> Result<()> {
        let version = self
            .inner
            .version()
            .await
            .map_err(|e| MuliError::Docker(format!("Docker connection check failed: {e}")))?;

        debug!(
            version = version.version.as_deref().unwrap_or("unknown"),
            api_version = version.api_version.as_deref().unwrap_or("unknown"),
            "Connected to Docker daemon"
        );

        Ok(())
    }

    /// Get a reference to the inner Bollard Docker client.
    pub fn inner(&self) -> &Docker {
        &self.inner
    }
}
