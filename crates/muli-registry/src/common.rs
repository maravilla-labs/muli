// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared registry types and utilities.

use std::sync::Arc;

use axum::{
    Extension, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::warn;

use muli_core::traits::TenantQuotaStore;

use crate::storage::{FilesystemStorage, RegistryStorage};

/// Check whether adding `additional_bytes` would exceed the tenant's quota.
/// Returns an error response if over quota, or Ok(()) if within limits (or no quota configured).
pub async fn check_quota(
    quota_store: &Option<Extension<Arc<dyn TenantQuotaStore>>>,
    tenant_id: &str,
    additional_bytes: u64,
) -> Result<(), Response> {
    if let Some(Extension(store)) = quota_store
        && let Ok(Some(quota)) = store.get_quota(tenant_id).await
        && quota.max_storage_bytes > 0
        && quota.would_exceed(additional_bytes)
    {
        return Err(error_json(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!(
                "tenant quota exceeded: {} used of {} allowed, {} requested",
                quota.current_usage_bytes, quota.max_storage_bytes, additional_bytes
            ),
        ));
    }
    Ok(())
}

/// Atomically check quota and reserve `additional_bytes` for a tenant.
///
/// Returns `Ok(())` if the reservation succeeded (usage has been incremented),
/// or an error response if the quota would be exceeded.
/// If no quota store is configured, always succeeds.
///
/// **Use this on write-finalization paths** (blob complete, manifest PUT, npm/cargo/maven publish)
/// instead of `check_quota` + `adjust_quota_usage` to eliminate the TOCTOU race
/// where concurrent requests all pass the check before any increment lands.
pub async fn reserve_quota(
    quota_store: &Option<Extension<Arc<dyn TenantQuotaStore>>>,
    tenant_id: &str,
    additional_bytes: u64,
) -> Result<(), Response> {
    if additional_bytes == 0 {
        return Ok(());
    }
    if let Some(Extension(store)) = quota_store {
        match store.try_reserve(tenant_id, additional_bytes).await {
            Ok(true) => {} // reserved
            Ok(false) => {
                return Err(error_json(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!("tenant quota exceeded: cannot reserve {additional_bytes} bytes"),
                ));
            }
            Err(e) => {
                warn!(tenant_id, error = %e, "quota reservation failed, allowing write");
            }
        }
    }
    Ok(())
}

/// Adjust the tenant's quota usage by a signed byte delta (O(1) — no filesystem scan).
/// Use positive delta for writes, negative for deletes.
/// Non-blocking: spawns a background task so the HTTP response is not delayed.
/// Retries once on transient failure before logging a warning.
pub fn adjust_quota_usage(
    quota_store: &Option<Extension<Arc<dyn TenantQuotaStore>>>,
    tenant_id: &str,
    delta: i64,
) {
    if delta == 0 {
        return;
    }
    if let Some(Extension(store)) = quota_store {
        let store = store.clone();
        let tenant_id = tenant_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = store.adjust_usage(&tenant_id, delta).await {
                // Retry once after a short delay
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if let Err(e2) = store.adjust_usage(&tenant_id, delta).await {
                    warn!(
                        tenant_id, delta,
                        error = %e, retry_error = %e2,
                        "failed to adjust tenant quota usage after retry"
                    );
                }
            }
        });
    }
}

/// Recalculate and set the tenant's absolute usage from a filesystem scan.
/// Expensive (O(n) in stored files) — use only for periodic reconciliation,
/// not on the hot write/delete path.
pub async fn recalculate_quota_usage(
    quota_store: &Option<Extension<Arc<dyn TenantQuotaStore>>>,
    storage: &FilesystemStorage,
    tenant_id: &str,
) {
    if let Some(Extension(store)) = quota_store {
        match storage.get_tenant_usage(tenant_id).await {
            Ok(usage) => {
                if let Err(e) = store.update_usage(tenant_id, usage).await {
                    warn!(tenant_id, error = %e, "failed to update tenant quota usage");
                }
            }
            Err(e) => {
                warn!(tenant_id, error = %e, "failed to get tenant usage for quota update");
            }
        }
    }
}

/// Build a simple JSON error response with a message.
pub fn error_json(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({
        "error": message
    });
    (status, Json(body)).into_response()
}
