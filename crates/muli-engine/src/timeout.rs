// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job timeout enforcement with cancellation support.

use std::time::Duration;

use tracing::{info, warn};

use muli_core::error::{MuliError, Result};

use crate::docker::client::DockerClient;
use crate::docker::container;

const GRACE_PERIOD_SECS: i64 = 10;

/// Enforce a timeout on a running container.
/// Sends SIGTERM via stop, waits grace period, then container is force-killed.
pub async fn enforce_timeout(
    docker: &DockerClient,
    container_id: &str,
    duration: Duration,
) -> Result<()> {
    info!(
        container_id = %container_id,
        timeout_secs = duration.as_secs(),
        "Enforcing timeout"
    );

    // First try graceful stop with SIGTERM
    match container::stop_container(docker, container_id, GRACE_PERIOD_SECS).await {
        Ok(()) => {
            info!(container_id = %container_id, "Container stopped gracefully after timeout");
        }
        Err(e) => {
            warn!(
                container_id = %container_id,
                error = %e,
                "Graceful stop failed, container may already be stopped"
            );
        }
    }

    Err(MuliError::Timeout(duration))
}
