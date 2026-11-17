// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! LFS object transfer handlers (upload, download, verify).

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use super::{OBJECT_NOT_FOUND, lfs_error, resolve};
use crate::api::GitState;
use crate::lfs::storage::LfsStorageError;
use crate::lfs::types::{self, VerifyRequest};
use crate::tenant::TenantContext;

/// PUT `/{namespace}/{repo}/info/lfs/objects/{oid}`
///
/// Streaming upload of an LFS object. The body is the raw object bytes. The
/// server hashes them (also when the oid is already stored), and only then
/// records the oid for this repository.
pub async fn upload(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, raw_repo, oid)): Path<(String, String, String)>,
    request: axum::extract::Request,
) -> Response {
    let repo = match resolve(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    if !types::validate_oid(&oid) {
        return lfs_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid oid");
    }

    let content_length = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    let body = request.into_body();

    match repo
        .storage
        .put_object(&tenant.tenant_id, &oid, content_length, body)
        .await
    {
        Ok(()) => {}
        Err(LfsStorageError::TooLarge { size, limit }) => {
            return lfs_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("object size {size} exceeds limit {limit}"),
            );
        }
        Err(LfsStorageError::DigestMismatch { expected, computed }) => {
            return lfs_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!("digest mismatch: expected {expected}, got {computed}"),
            );
        }
        Err(e) => {
            tracing::error!(error = %e, %oid, "LFS upload failed");
            return lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed");
        }
    }

    match repo
        .objects
        .add_lfs_object(&repo.tenant_id, &repo.repo_id, &oid)
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!(error = %e, %oid, "LFS object index write failed");
            lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed")
        }
    }
}

/// GET `/{namespace}/{repo}/info/lfs/objects/{oid}`
///
/// Streaming download of an LFS object of this repository.
pub async fn download(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, raw_repo, oid)): Path<(String, String, String)>,
) -> Response {
    let repo = match resolve(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    if !types::validate_oid(&oid) {
        return lfs_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid oid");
    }

    match repo.owns(&oid).await {
        Ok(true) => {}
        Ok(false) => return lfs_error(StatusCode::NOT_FOUND, OBJECT_NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, %oid, "LFS object index read failed");
            return lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "download failed");
        }
    }

    match repo.storage.get_object(&tenant.tenant_id, &oid).await {
        Ok((stream, size)) => {
            let body = axum::body::Body::from_stream(stream);
            (
                StatusCode::OK,
                [
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/octet-stream".to_string(),
                    ),
                    (axum::http::header::CONTENT_LENGTH, size.to_string()),
                ],
                body,
            )
                .into_response()
        }
        Err(LfsStorageError::NotFound(_)) => lfs_error(StatusCode::NOT_FOUND, OBJECT_NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, %oid, "LFS download failed");
            lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "download failed")
        }
    }
}

/// POST `/{namespace}/{repo}/info/lfs/objects/verify`
///
/// Verify that an uploaded object belongs to this repository and has the
/// correct size. Verify records nothing: the upload itself did, after hashing
/// the bytes (an oid and a size are no proof of holding the object).
pub async fn verify(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, raw_repo)): Path<(String, String)>,
    Json(req): Json<VerifyRequest>,
) -> Response {
    let repo = match resolve(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    if !types::validate_oid(&req.oid) {
        return lfs_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid oid");
    }

    match repo.owns(&req.oid).await {
        Ok(true) => {}
        Ok(false) => return lfs_error(StatusCode::NOT_FOUND, OBJECT_NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, oid = %req.oid, "LFS object index read failed");
            return lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "verify failed");
        }
    }

    match repo.storage.object_size(&tenant.tenant_id, &req.oid).await {
        Ok(Some(size)) if size == req.size => StatusCode::OK.into_response(),
        Ok(Some(actual)) => lfs_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("size mismatch: expected {}, got {actual}", req.size),
        ),
        Ok(None) => lfs_error(StatusCode::NOT_FOUND, OBJECT_NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, oid = %req.oid, "LFS verify failed");
            lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "verify failed")
        }
    }
}
