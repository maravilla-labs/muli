// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Smart protocol API routes.

use std::sync::Arc;

use axum::{
    Extension,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::api::GitState;
use crate::api::helpers::repo_fs_path;
use crate::repo_target::TargetRepo;
use crate::ssh::{compute_ref_updates, read_refs};
use crate::tenant::TenantContext;

/// Maximum allowed body size for git protocol requests (100 MB).
const MAX_GIT_BODY: usize = 100 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub struct InfoRefsQuery {
    service: Option<String>,
}

/// GET /{namespace}/{repo}/info/refs
pub async fn info_refs(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    TargetRepo(target): TargetRepo,
    Path((_namespace, _raw_repo)): Path<(String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let service = query.service.as_deref().unwrap_or("").to_string();

    let path = repo_fs_path(&state, &target);

    if service.contains("git-receive-pack") {
        crate::protocol::receive_pack::info_refs_receive(path, &headers, &tenant.tenant_id)
            .await
            .into_response()
    } else {
        crate::protocol::upload_pack::info_refs_upload(path, &headers)
            .await
            .into_response()
    }
}

/// POST /{namespace}/{repo}/git-upload-pack
pub async fn upload_pack(
    State(state): State<Arc<GitState>>,
    Extension(_tenant): Extension<TenantContext>,
    TargetRepo(target): TargetRepo,
    Path((_namespace, _raw_repo)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    let path = repo_fs_path(&state, &target);
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), MAX_GIT_BODY).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!(error = %e, "failed to read upload-pack request body");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    crate::protocol::upload_pack::post_upload_pack(path, &headers, body)
        .await
        .into_response()
}

/// POST /{namespace}/{repo}/git-receive-pack
pub async fn receive_pack(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    TargetRepo(target): TargetRepo,
    Path((_namespace, _raw_repo)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    let repo = target.clone();
    let path = repo_fs_path(&state, &target);
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), MAX_GIT_BODY).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!(error = %e, "failed to read receive-pack request body");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    // Pre-push quota gate: reject if tenant is already over quota.
    if let Some(ref store) = state.post_push_hooks.quota_store {
        if let Ok(Some(quota)) = store.get_quota(&tenant.tenant_id).await {
            if quota.max_storage_bytes > 0 && quota.current_usage_bytes >= quota.max_storage_bytes {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "tenant storage quota exceeded",
                )
                    .into_response();
            }
        }
    }

    // Snapshot refs BEFORE the push so we can diff after.
    let old_refs = read_refs(&path).await;

    // Snapshot repo size before push for quota delta calculation.
    let repo_size_before = crate::hooks::compute_dir_size(&path).await.ok();

    let response = crate::protocol::receive_pack::post_receive_pack(
        path.clone(),
        &headers,
        body,
        &tenant.tenant_id,
    )
    .await;

    // After a successful push, compute ref diff and fire post-push hooks.
    if response.status().is_success() {
        let new_refs = read_refs(&path).await;
        let ref_updates = compute_ref_updates(&old_refs, &new_refs);
        state.post_push_hooks.fire(
            tenant.tenant_id.clone(),
            repo.id.clone(),
            repo.name.clone(),
            ref_updates,
            repo_size_before,
            path,
        );
    }

    response.into_response()
}
