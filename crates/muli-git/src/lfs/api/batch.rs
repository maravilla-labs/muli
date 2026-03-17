// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! LFS Batch API handler (POST /{ns}/{repo}/info/lfs/objects/batch).

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use crate::api::GitState;
use crate::api::helpers::strip_git_suffix;
use crate::lfs::storage::LfsStorageError;
use crate::lfs::types::{
    self, APPLICATION_VND_GIT_LFS_JSON, Action, Actions, BatchRequest, BatchResponse, ObjectError,
    ObjectResponse, Operation,
};
use crate::tenant::TenantContext;

/// Default presigned URL TTL in seconds.
const PRESIGN_TTL_SECS: u64 = 3600;

/// POST `/{namespace}/{repo}/info/lfs/objects/batch`
pub async fn batch(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, raw_repo)): Path<(String, String)>,
    headers: HeaderMap,
    Json(req): Json<BatchRequest>,
) -> Response {
    let lfs = match &state.lfs_storage {
        Some(s) => s,
        None => return lfs_error(StatusCode::NOT_FOUND, "LFS is not enabled"),
    };

    let _repo_name = strip_git_suffix(&raw_repo);

    // Build base URL for transfer hrefs from the incoming request.
    let base_url = build_base_url(&headers, &namespace, &raw_repo);

    let mut objects = Vec::with_capacity(req.objects.len());

    for obj in &req.objects {
        if !types::validate_oid(&obj.oid) {
            objects.push(ObjectResponse {
                oid: obj.oid.clone(),
                size: obj.size,
                authenticated: Some(true),
                actions: None,
                error: Some(ObjectError {
                    code: 422,
                    message: "invalid oid".to_string(),
                }),
            });
            continue;
        }

        let resp = match req.operation {
            Operation::Download => {
                build_download_response(lfs.as_ref(), &tenant.tenant_id, obj, &base_url).await
            }
            Operation::Upload => {
                build_upload_response(lfs.as_ref(), &tenant.tenant_id, obj, &base_url).await
            }
        };
        objects.push(resp);
    }

    let body = BatchResponse {
        transfer: "basic".to_string(),
        objects,
        hash_algo: Some("sha256".to_string()),
    };

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            APPLICATION_VND_GIT_LFS_JSON,
        )],
        Json(body),
    )
        .into_response()
}

async fn build_download_response(
    lfs: &dyn crate::lfs::storage::LfsStorage,
    tenant_id: &str,
    obj: &types::ObjectSpec,
    base_url: &str,
) -> ObjectResponse {
    // Check for presigned URL first (S3 backend).
    if let Ok(Some(url)) = lfs
        .presigned_download_url(tenant_id, &obj.oid, PRESIGN_TTL_SECS)
        .await
    {
        return ObjectResponse {
            oid: obj.oid.clone(),
            size: obj.size,
            authenticated: Some(true),
            actions: Some(Actions {
                download: Some(Action {
                    href: url,
                    header: None,
                    expires_in: Some(PRESIGN_TTL_SECS),
                }),
                upload: None,
                verify: None,
            }),
            error: None,
        };
    }

    match lfs.object_size(tenant_id, &obj.oid).await {
        Ok(Some(size)) => ObjectResponse {
            oid: obj.oid.clone(),
            size,
            authenticated: Some(true),
            actions: Some(Actions {
                download: Some(Action {
                    href: format!("{base_url}/{}", obj.oid),
                    header: None,
                    expires_in: None,
                }),
                upload: None,
                verify: None,
            }),
            error: None,
        },
        Ok(None) => ObjectResponse {
            oid: obj.oid.clone(),
            size: obj.size,
            authenticated: Some(true),
            actions: None,
            error: Some(ObjectError {
                code: 404,
                message: "Not found".to_string(),
            }),
        },
        Err(e) => error_object(&obj.oid, obj.size, &e),
    }
}

async fn build_upload_response(
    lfs: &dyn crate::lfs::storage::LfsStorage,
    tenant_id: &str,
    obj: &types::ObjectSpec,
    base_url: &str,
) -> ObjectResponse {
    // If object already exists, return empty actions (dedup — no upload needed).
    match lfs.has_object(tenant_id, &obj.oid).await {
        Ok(true) => {
            return ObjectResponse {
                oid: obj.oid.clone(),
                size: obj.size,
                authenticated: Some(true),
                actions: None,
                error: None,
            };
        }
        Ok(false) => {}
        Err(e) => return error_object(&obj.oid, obj.size, &e),
    }

    // Check for presigned upload URL (S3 backend).
    if let Ok(Some(url)) = lfs
        .presigned_upload_url(tenant_id, &obj.oid, obj.size, PRESIGN_TTL_SECS)
        .await
    {
        return ObjectResponse {
            oid: obj.oid.clone(),
            size: obj.size,
            authenticated: Some(true),
            actions: Some(Actions {
                upload: Some(Action {
                    href: url,
                    header: None,
                    expires_in: Some(PRESIGN_TTL_SECS),
                }),
                download: None,
                verify: Some(Action {
                    href: format!("{base_url}/verify"),
                    header: None,
                    expires_in: None,
                }),
            }),
            error: None,
        };
    }

    ObjectResponse {
        oid: obj.oid.clone(),
        size: obj.size,
        authenticated: Some(true),
        actions: Some(Actions {
            upload: Some(Action {
                href: format!("{base_url}/{}", obj.oid),
                header: None,
                expires_in: None,
            }),
            download: None,
            verify: Some(Action {
                href: format!("{base_url}/verify"),
                header: None,
                expires_in: None,
            }),
        }),
        error: None,
    }
}

/// Build the base URL for LFS object transfer endpoints.
fn build_base_url(headers: &HeaderMap, namespace: &str, raw_repo: &str) -> String {
    let scheme = if headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s == "https")
    {
        "https"
    } else {
        "http"
    };
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");

    format!("{scheme}://{host}/{namespace}/{raw_repo}/info/lfs/objects")
}

fn error_object(oid: &str, size: u64, e: &LfsStorageError) -> ObjectResponse {
    ObjectResponse {
        oid: oid.to_string(),
        size,
        authenticated: Some(true),
        actions: None,
        error: Some(ObjectError {
            code: 500,
            message: e.to_string(),
        }),
    }
}

fn lfs_error(status: StatusCode, msg: &str) -> Response {
    let body = serde_json::json!({
        "message": msg,
        "request_id": "",
    });
    (
        status,
        [(
            axum::http::header::CONTENT_TYPE,
            APPLICATION_VND_GIT_LFS_JSON,
        )],
        Json(body),
    )
        .into_response()
}
