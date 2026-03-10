// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI manifest push and pull handlers.

use std::sync::Arc;

use axum::{
    Extension,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};

use muli_core::traits::TenantQuotaStore;

use crate::metrics::RegistryMetrics;
use crate::storage::{FilesystemStorage, RegistryStorage};
use crate::tenant::TenantContext;

use super::{oci_error, validate_name, validate_ref};
use crate::common::{check_quota, update_quota_usage};

/// HEAD /v2/{name}/manifests/{reference}
pub async fn head_manifest(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((name, reference)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_ref(&reference) {
        return e;
    }
    let start = std::time::Instant::now();
    let response = match storage
        .get_manifest(&tenant.tenant_id, &name, &reference)
        .await
    {
        Ok(data) => {
            metrics.record_pull(&tenant.tenant_id);
            let digest = format!("sha256:{}", hex::encode(Sha256::digest(&data)));
            let content_type = detect_manifest_media_type(&data);
            (
                StatusCode::OK,
                [
                    ("Content-Length", data.len().to_string()),
                    ("Docker-Content-Digest", digest),
                    ("Content-Type", content_type.to_string()),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StorageError::ManifestNotFound { .. }) => oci_error(
            StatusCode::NOT_FOUND,
            "MANIFEST_UNKNOWN",
            "manifest not found",
        ),
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MANIFEST_UNKNOWN",
            "storage error",
        ),
    };
    metrics.observe_request_duration("HEAD", "manifests", start.elapsed());
    response
}

/// GET /v2/{name}/manifests/{reference}
pub async fn get_manifest(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((name, reference)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_ref(&reference) {
        return e;
    }
    let start = std::time::Instant::now();
    let response = match storage
        .get_manifest(&tenant.tenant_id, &name, &reference)
        .await
    {
        Ok(data) => {
            metrics.record_pull(&tenant.tenant_id);
            let digest = format!("sha256:{}", hex::encode(Sha256::digest(&data)));
            let content_type = detect_manifest_media_type(&data);
            (
                StatusCode::OK,
                [
                    ("Docker-Content-Digest", digest),
                    ("Content-Type", content_type.to_string()),
                ],
                data,
            )
                .into_response()
        }
        Err(crate::storage::StorageError::ManifestNotFound { .. }) => oci_error(
            StatusCode::NOT_FOUND,
            "MANIFEST_UNKNOWN",
            "manifest not found",
        ),
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MANIFEST_UNKNOWN",
            "storage error",
        ),
    };
    metrics.observe_request_duration("GET", "manifests", start.elapsed());
    response
}

/// PUT /v2/{name}/manifests/{reference}
pub async fn put_manifest(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path((name, reference)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_ref(&reference) {
        return e;
    }
    let start = std::time::Instant::now();

    // Check quota before storing manifest
    if let Err(e) = check_quota(&quota_store, &tenant.tenant_id, body.len() as u64).await {
        return e;
    }

    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&body)));

    let response = match storage
        .store_manifest(&tenant.tenant_id, &name, &reference, &body)
        .await
    {
        Ok(()) => {
            // Also store by digest for content-addressable access
            let _ = storage
                .store_manifest(&tenant.tenant_id, &name, &digest, &body)
                .await;

            metrics.record_push(&tenant.tenant_id);

            // Update tenant quota usage after successful manifest store
            update_quota_usage(&quota_store, &storage, &tenant.tenant_id).await;

            let location = format!("/v2/{name}/manifests/{reference}");
            (
                StatusCode::CREATED,
                [
                    ("Location", location),
                    ("Docker-Content-Digest", digest),
                    ("Content-Length", "0".to_string()),
                ],
            )
                .into_response()
        }
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MANIFEST_INVALID",
            "failed to store manifest",
        ),
    };
    metrics.observe_request_duration("PUT", "manifests", start.elapsed());
    response
}

/// DELETE /v2/{name}/manifests/{reference}
pub async fn delete_manifest(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path((name, reference)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_ref(&reference) {
        return e;
    }
    let start = std::time::Instant::now();
    let response = match storage
        .delete_manifest(&tenant.tenant_id, &name, &reference)
        .await
    {
        Ok(()) => {
            // Update tenant quota usage after manifest deletion
            update_quota_usage(&quota_store, &storage, &tenant.tenant_id).await;
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => oci_error(
            StatusCode::NOT_FOUND,
            "MANIFEST_UNKNOWN",
            "manifest not found",
        ),
    };
    metrics.observe_request_duration("DELETE", "manifests", start.elapsed());
    response
}

fn detect_manifest_media_type(data: &[u8]) -> &'static str {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(data) {
        if let Some(mt) = json.get("mediaType").and_then(|v| v.as_str()) {
            // Return known OCI media types
            match mt {
                "application/vnd.oci.image.manifest.v1+json" => {
                    return "application/vnd.oci.image.manifest.v1+json";
                }
                "application/vnd.oci.image.index.v1+json" => {
                    return "application/vnd.oci.image.index.v1+json";
                }
                "application/vnd.docker.distribution.manifest.v2+json" => {
                    return "application/vnd.docker.distribution.manifest.v2+json";
                }
                "application/vnd.docker.distribution.manifest.list.v2+json" => {
                    return "application/vnd.docker.distribution.manifest.list.v2+json";
                }
                _ => {}
            }
        }
        // Check if it has schemaVersion for Docker v2
        if json.get("schemaVersion").is_some() {
            if json.get("manifests").is_some() {
                return "application/vnd.docker.distribution.manifest.list.v2+json";
            }
            return "application/vnd.docker.distribution.manifest.v2+json";
        }
    }
    "application/vnd.oci.image.manifest.v1+json"
}
