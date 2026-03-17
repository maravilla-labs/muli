// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI blob download and HEAD handlers.

use std::sync::Arc;

use axum::{
    Extension,
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use muli_core::traits::TenantQuotaStore;

use crate::common::adjust_quota_usage;
use crate::metrics::RegistryMetrics;
use crate::storage::{FilesystemStorage, RegistryStorage};
use crate::tenant::TenantContext;

use super::{oci_error, validate_digest_param, validate_name};

/// HEAD /v2/{name}/blobs/{digest}
pub async fn head_blob(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((name, digest)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_digest_param(&digest) {
        return e;
    }
    let start = std::time::Instant::now();
    let response = match storage.has_blob(&tenant.tenant_id, &digest).await {
        Ok(true) => match storage.blob_size(&tenant.tenant_id, &digest).await {
            Ok(size) => {
                metrics.record_pull(&tenant.tenant_id);
                (
                    StatusCode::OK,
                    [
                        ("Content-Length", size.to_string()),
                        ("Docker-Content-Digest", digest),
                    ],
                )
                    .into_response()
            }
            Err(_) => oci_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "BLOB_UNKNOWN",
                "failed to stat blob",
            ),
        },
        Ok(false) => oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob not found"),
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "BLOB_UNKNOWN",
            "storage error",
        ),
    };
    metrics.observe_request_duration("HEAD", "blobs", start.elapsed());
    response
}

/// GET /v2/{name}/blobs/{digest}
pub async fn get_blob(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((name, digest)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_digest_param(&digest) {
        return e;
    }
    let start = std::time::Instant::now();
    let response = match storage.get_blob(&tenant.tenant_id, &digest).await {
        Ok((stream, size)) => {
            metrics.record_pull(&tenant.tenant_id);
            let body = Body::from_stream(stream);
            (
                StatusCode::OK,
                [
                    ("Content-Length", size.to_string()),
                    ("Docker-Content-Digest", digest),
                    ("Content-Type", "application/octet-stream".to_string()),
                ],
                body,
            )
                .into_response()
        }
        Err(crate::storage::StorageError::BlobNotFound(_)) => {
            oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob not found")
        }
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "BLOB_UNKNOWN",
            "storage error",
        ),
    };
    metrics.observe_request_duration("GET", "blobs", start.elapsed());
    response
}

/// DELETE /v2/{name}/blobs/{digest}
pub async fn delete_blob(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path((name, digest)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_digest_param(&digest) {
        return e;
    }
    let start = std::time::Instant::now();
    // Stat blob size before delete so we can decrement quota
    let freed_bytes = storage
        .blob_size(&tenant.tenant_id, &digest)
        .await
        .unwrap_or(0);
    let response = match storage.delete_blob(&tenant.tenant_id, &digest).await {
        Ok(()) => {
            if freed_bytes > 0 {
                adjust_quota_usage(&quota_store, &tenant.tenant_id, -(freed_bytes as i64));
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob not found"),
    };
    metrics.observe_request_duration("DELETE", "blobs", start.elapsed());
    response
}
