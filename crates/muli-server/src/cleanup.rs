// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Background cleanup tasks.

use std::sync::Arc;
use std::time::Duration;

use muli_core::traits::{GitTokenStore, JobStore, RegistryTokenStore};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Spawn a background task to delete expired registry tokens every hour.
pub fn spawn_registry_token_cleanup(
    token_store: Arc<dyn RegistryTokenStore>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        interval.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match token_store.delete_expired_tokens().await {
                        Ok(0) => {}
                        Ok(n) => info!(count = n, "Cleaned up expired registry tokens"),
                        Err(e) => warn!(error = %e, "Failed to clean up expired tokens"),
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}

/// Spawn a background task to delete expired git tokens every hour.
pub fn spawn_git_token_cleanup(token_store: Arc<dyn GitTokenStore>, cancel: CancellationToken) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        interval.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match token_store.delete_expired_tokens().await {
                        Ok(0) => {}
                        Ok(n) => info!(count = n, "Cleaned up expired git tokens"),
                        Err(e) => warn!(error = %e, "Failed to clean up expired git tokens"),
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}

/// Spawn a background task to delete terminal-state jobs older than the configured age.
pub fn spawn_job_cleanup(
    store: Arc<dyn JobStore>,
    cancel: CancellationToken,
    cleanup_interval: Duration,
    cleanup_max_age: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match store.cleanup_old(cleanup_max_age).await {
                        Ok(0) => {}
                        Ok(n) => info!(count = n, "Cleaned up old terminal jobs"),
                        Err(e) => warn!(error = %e, "Failed to clean up old jobs"),
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}
