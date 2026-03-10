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

/// Update the tenant's usage in the quota store after a successful storage operation.
pub async fn update_quota_usage(
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
