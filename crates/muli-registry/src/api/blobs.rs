// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI blob upload handlers.

use std::sync::Arc;

use axum::{
    Extension,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use uuid::Uuid;

use muli_core::traits::TenantQuotaStore;

use crate::common::{check_quota, reserve_quota};
use crate::metrics::RegistryMetrics;
use crate::storage::{FilesystemStorage, RegistryStorage};
use crate::tenant::TenantContext;

use super::{oci_error, validate_digest_param, validate_name};

// Re-export download/read handlers so existing route references work.
pub use super::blobs_download::{delete_blob, get_blob, head_blob};

#[derive(Deserialize)]
pub struct DigestQuery {
    pub digest: String,
}

#[derive(Deserialize)]
pub struct UploadQuery {
    /// Digest of blob to mount from another repository.
    pub mount: Option<String>,
    /// Source repository to mount the blob from.
    pub from: Option<String>,
}

/// POST /v2/{name}/blobs/uploads/
/// Supports cross-repository blob mounting via `?mount={digest}&from={source_repo}`.
/// Since blobs are content-addressed per tenant, mounting within a tenant
/// just verifies the blob exists and returns 201 Created.
pub async fn start_upload(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path(name): Path<String>,
    Query(query): Query<UploadQuery>,
    body: axum::body::Bytes,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    let start = std::time::Instant::now();

    // Check quota before accepting upload data
    if !body.is_empty()
        && let Err(e) = check_quota(&quota_store, &tenant.tenant_id, body.len() as u64).await
    {
        return e;
    }

    // Handle cross-repository blob mount
    if let (Some(mount_digest), Some(from_repo)) = (&query.mount, &query.from) {
        if let Err(e) = validate_digest_param(mount_digest) {
            return e;
        }
        if let Err(e) = validate_name(from_repo) {
            return e;
        }

        // Verify source repo exists within the same tenant
        let repos = match storage.list_repositories(&tenant.tenant_id).await {
            Ok(r) => r,
            Err(_) => {
                return oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "BLOB_UPLOAD_UNKNOWN",
                    "failed to list repositories",
                );
            }
        };

        if repos.iter().any(|r| r == from_repo) {
            // Check if the blob exists for this tenant (blobs are tenant-scoped, not repo-scoped)
            match storage.has_blob(&tenant.tenant_id, mount_digest).await {
                Ok(true) => {
                    // Blob exists — mount successful
                    metrics.record_upload(&tenant.tenant_id);
                    metrics.observe_request_duration("POST", "blob_mount", start.elapsed());
                    let location = format!("/v2/{name}/blobs/{mount_digest}");
                    return (
                        StatusCode::CREATED,
                        [
                            ("Location", location),
                            ("Docker-Content-Digest", mount_digest.clone()),
                            ("Content-Length", "0".to_string()),
                        ],
                    )
                        .into_response();
                }
                Ok(false) => {
                    // Blob not found — fall through to normal upload
                }
                Err(_) => {
                    return oci_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "BLOB_UNKNOWN",
                        "storage error during mount check",
                    );
                }
            }
        }
        // Source repo doesn't exist or blob not found — fall through to normal upload
    }

    let upload_id = Uuid::new_v4().to_string();

    if storage
        .create_upload(&tenant.tenant_id, &upload_id)
        .await
        .is_err()
    {
        return oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "BLOB_UPLOAD_UNKNOWN",
            "failed to create upload",
        );
    }

    if !body.is_empty() {
        match storage
            .append_upload(&tenant.tenant_id, &upload_id, &body)
            .await
        {
            Ok(_) => {}
            Err(crate::storage::StorageError::BlobTooLarge(msg)) => {
                return oci_error(StatusCode::PAYLOAD_TOO_LARGE, "SIZE_LIMIT", &msg);
            }
            Err(_) => {
                return oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "BLOB_UPLOAD_UNKNOWN",
                    "failed to write upload data",
                );
            }
        }
    }

    metrics.record_upload(&tenant.tenant_id);
    if !body.is_empty() {
        metrics.observe_blob_size(&tenant.tenant_id, body.len() as u64);
    }
    metrics.observe_request_duration("POST", "blob_uploads", start.elapsed());

    let location = format!("/v2/{name}/blobs/uploads/{upload_id}");
    (
        StatusCode::ACCEPTED,
        [
            ("Location", location),
            ("Docker-Upload-UUID", upload_id),
            ("Range", "0-0".to_string()),
            ("Content-Length", "0".to_string()),
        ],
    )
        .into_response()
}

/// PATCH /v2/{name}/blobs/uploads/{id}
pub async fn patch_upload(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path((name, upload_id)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    let start = std::time::Instant::now();

    // Check quota before appending data
    if !body.is_empty()
        && let Err(e) = check_quota(&quota_store, &tenant.tenant_id, body.len() as u64).await
    {
        return e;
    }

    let start_offset = match storage.upload_offset(&tenant.tenant_id, &upload_id).await {
        Ok(o) => o,
        Err(_) => {
            return oci_error(
                StatusCode::NOT_FOUND,
                "BLOB_UPLOAD_UNKNOWN",
                "upload not found",
            );
        }
    };

    let response = match storage
        .append_upload(&tenant.tenant_id, &upload_id, &body)
        .await
    {
        Ok(end_offset) => {
            metrics.record_upload(&tenant.tenant_id);
            metrics.observe_blob_size(&tenant.tenant_id, body.len() as u64);
            let location = format!("/v2/{name}/blobs/uploads/{upload_id}");
            let range = format!("{}-{}", start_offset, end_offset.saturating_sub(1));
            (
                StatusCode::ACCEPTED,
                [
                    ("Location", location),
                    ("Docker-Upload-UUID", upload_id),
                    ("Range", range),
                    ("Content-Length", "0".to_string()),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StorageError::BlobTooLarge(msg)) => {
            oci_error(StatusCode::PAYLOAD_TOO_LARGE, "SIZE_LIMIT", &msg)
        }
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "BLOB_UPLOAD_UNKNOWN",
            "failed to append data",
        ),
    };
    metrics.observe_request_duration("PATCH", "blob_uploads", start.elapsed());
    response
}

/// PUT /v2/{name}/blobs/uploads/{id}?digest=sha256:...
pub async fn complete_upload(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path((name, upload_id)): Path<(String, String)>,
    Query(query): Query<DigestQuery>,
    body: axum::body::Bytes,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    if let Err(e) = validate_digest_param(&query.digest) {
        return e;
    }
    let start = std::time::Instant::now();

    // Check quota before appending final data
    if !body.is_empty()
        && let Err(e) = check_quota(&quota_store, &tenant.tenant_id, body.len() as u64).await
    {
        return e;
    }

    // Append any remaining data
    if !body.is_empty() {
        match storage
            .append_upload(&tenant.tenant_id, &upload_id, &body)
            .await
        {
            Ok(_) => {}
            Err(crate::storage::StorageError::BlobTooLarge(msg)) => {
                return oci_error(StatusCode::PAYLOAD_TOO_LARGE, "SIZE_LIMIT", &msg);
            }
            Err(_) => {
                return oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "BLOB_UPLOAD_UNKNOWN",
                    "failed to append final data",
                );
            }
        }
    }

    // Check if blob already exists (content-addressable dedup: skip quota for duplicates)
    let blob_is_new = !storage
        .has_blob(&tenant.tenant_id, &query.digest)
        .await
        .unwrap_or(true);

    let response = match storage
        .complete_upload(&tenant.tenant_id, &upload_id, &query.digest)
        .await
    {
        Ok(()) => {
            metrics.record_upload(&tenant.tenant_id);
            if !body.is_empty() {
                metrics.observe_blob_size(&tenant.tenant_id, body.len() as u64);
            }
            // Atomic quota reservation — only for genuinely new blobs
            if blob_is_new {
                if let Ok(size) = storage.blob_size(&tenant.tenant_id, &query.digest).await {
                    if let Err(e) = reserve_quota(&quota_store, &tenant.tenant_id, size).await {
                        // Blob is already on disk — log but don't fail the request.
                        // Reconciliation will correct the accounting.
                        tracing::warn!(
                            tenant_id = %tenant.tenant_id,
                            digest = %query.digest,
                            "blob stored but quota reservation failed: over quota"
                        );
                        let _ = e;
                    }
                }
            }
            let location = format!("/v2/{}/blobs/{}", name, query.digest);
            (
                StatusCode::CREATED,
                [
                    ("Location", location),
                    ("Docker-Content-Digest", query.digest),
                    ("Content-Length", "0".to_string()),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StorageError::InvalidDigest(msg)) => {
            oci_error(StatusCode::BAD_REQUEST, "DIGEST_INVALID", &msg)
        }
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "BLOB_UPLOAD_UNKNOWN",
            "failed to complete upload",
        ),
    };
    metrics.observe_request_duration("PUT", "blob_uploads", start.elapsed());
    response
}
