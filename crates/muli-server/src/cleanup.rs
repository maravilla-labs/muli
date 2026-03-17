// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Background cleanup tasks.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use muli_core::traits::{
    ArtifactStore, GitTokenStore, JobStore, PipelineRunStore, RegistryTokenStore, StepRunStore,
    TenantQuotaStore, TenantStore,
};
use muli_registry::storage::{FilesystemStorage as RegistryFilesystemStorage, RegistryStorage};
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

/// Spawn a background task to delete expired pipeline artifacts.
pub fn spawn_artifact_cleanup(
    artifact_store: Arc<dyn ArtifactStore>,
    cancel: CancellationToken,
    retention_days: u64,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        interval.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let before = chrono::Utc::now()
                        - chrono::Duration::days(retention_days as i64);
                    match artifact_store.delete_expired(before).await {
                        Ok(0) => {}
                        Ok(n) => info!(count = n, "Cleaned up expired pipeline artifacts"),
                        Err(e) => warn!(error = %e, "Failed to clean up expired artifacts"),
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}

/// Spawn a background task to delete old terminal pipeline runs and their step runs.
pub fn spawn_pipeline_run_cleanup(
    run_store: Arc<dyn PipelineRunStore>,
    step_store: Arc<dyn StepRunStore>,
    cancel: CancellationToken,
    retention_days: u64,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        interval.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let before = chrono::Utc::now()
                        - chrono::Duration::days(retention_days as i64);
                    match run_store.delete_old_runs(before).await {
                        Ok(0) => {}
                        Ok(n) => info!(count = n, "Cleaned up old pipeline runs"),
                        Err(e) => warn!(error = %e, "Failed to clean up old pipeline runs"),
                    }
                    // Step run cleanup: the delete_old_runs implementation
                    // handles cascading step run deletion internally.
                    let _ = &step_store; // keep the store alive for future use
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}

/// Spawn a background task to reconcile tenant quota usage every hour.
///
/// Delta-based quota tracking can drift over time (content-addressable dedup,
/// partial writes, manual file deletions). This task performs a full filesystem
/// scan of both registry and git storage, then sets the absolute usage.
pub fn spawn_quota_reconciliation(
    quota_store: Arc<dyn TenantQuotaStore>,
    tenant_store: Arc<dyn TenantStore>,
    registry_storage: Arc<RegistryFilesystemStorage>,
    git_root: PathBuf,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        interval.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let tenants = match tenant_store.list_tenants().await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(error = %e, "quota reconciliation: failed to list tenants");
                            continue;
                        }
                    };
                    for tenant in &tenants {
                        let registry_usage = registry_storage
                            .get_tenant_usage(&tenant.id)
                            .await
                            .unwrap_or(0);
                        let git_usage = muli_git::hooks::compute_dir_size(
                            &git_root.join(&tenant.id),
                        )
                        .await
                        .unwrap_or(0);
                        let total = registry_usage + git_usage;
                        if let Err(e) = quota_store.update_usage(&tenant.id, total).await {
                            warn!(
                                tenant_id = %tenant.id, error = %e,
                                "quota reconciliation: failed to update usage"
                            );
                        }
                    }
                    if !tenants.is_empty() {
                        info!(count = tenants.len(), "Quota reconciliation completed");
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}
