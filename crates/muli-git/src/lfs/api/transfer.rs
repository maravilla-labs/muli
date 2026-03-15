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

use crate::api::GitState;
use crate::lfs::storage::LfsStorageError;
use crate::lfs::types::{self, VerifyRequest, APPLICATION_VND_GIT_LFS_JSON};
use crate::tenant::TenantContext;

/// PUT `/{namespace}/{repo}/info/lfs/objects/{oid}`
///
/// Streaming upload of an LFS object. The body is the raw object bytes.
pub async fn upload(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((_namespace, _raw_repo, oid)): Path<(String, String, String)>,
    request: axum::extract::Request,
) -> Response {
    let lfs = match &state.lfs_storage {
        Some(s) => s,
        None => return lfs_error(StatusCode::NOT_FOUND, "LFS is not enabled"),
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

    match lfs
        .put_object(&tenant.tenant_id, &oid, content_length, body)
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(LfsStorageError::TooLarge { size, limit }) => lfs_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("object size {size} exceeds limit {limit}"),
        ),
        Err(LfsStorageError::DigestMismatch { expected, computed }) => lfs_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("digest mismatch: expected {expected}, got {computed}"),
        ),
        Err(e) => {
            tracing::error!(error = %e, %oid, "LFS upload failed");
            lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed")
        }
    }
}

/// GET `/{namespace}/{repo}/info/lfs/objects/{oid}`
///
/// Streaming download of an LFS object.
pub async fn download(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((_namespace, _raw_repo, oid)): Path<(String, String, String)>,
) -> Response {
    let lfs = match &state.lfs_storage {
        Some(s) => s,
        None => return lfs_error(StatusCode::NOT_FOUND, "LFS is not enabled"),
    };

    if !types::validate_oid(&oid) {
        return lfs_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid oid");
    }

    match lfs.get_object(&tenant.tenant_id, &oid).await {
        Ok((stream, size)) => {
            let body = axum::body::Body::from_stream(stream);
            (
                StatusCode::OK,
                [
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/octet-stream".to_string(),
                    ),
                    (
                        axum::http::header::CONTENT_LENGTH,
                        size.to_string(),
                    ),
                ],
                body,
            )
                .into_response()
        }
        Err(LfsStorageError::NotFound(_)) => {
            lfs_error(StatusCode::NOT_FOUND, "object not found")
        }
        Err(e) => {
            tracing::error!(error = %e, %oid, "LFS download failed");
            lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "download failed")
        }
    }
}

/// POST `/{namespace}/{repo}/info/lfs/objects/verify`
///
/// Verify that an uploaded object exists and has the correct size.
pub async fn verify(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((_namespace, _raw_repo)): Path<(String, String)>,
    Json(req): Json<VerifyRequest>,
) -> Response {
    let lfs = match &state.lfs_storage {
        Some(s) => s,
        None => return lfs_error(StatusCode::NOT_FOUND, "LFS is not enabled"),
    };

    if !types::validate_oid(&req.oid) {
        return lfs_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid oid");
    }

    match lfs.object_size(&tenant.tenant_id, &req.oid).await {
        Ok(Some(size)) if size == req.size => StatusCode::OK.into_response(),
        Ok(Some(actual)) => lfs_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("size mismatch: expected {}, got {actual}", req.size),
        ),
        Ok(None) => lfs_error(StatusCode::NOT_FOUND, "object not found"),
        Err(e) => {
            tracing::error!(error = %e, oid = %req.oid, "LFS verify failed");
            lfs_error(StatusCode::INTERNAL_SERVER_ERROR, "verify failed")
        }
    }
}

fn lfs_error(status: StatusCode, msg: &str) -> Response {
    let body = serde_json::json!({
        "message": msg,
        "request_id": "",
    });
    (
        status,
        [(axum::http::header::CONTENT_TYPE, APPLICATION_VND_GIT_LFS_JSON)],
        Json(body),
    )
        .into_response()
}
