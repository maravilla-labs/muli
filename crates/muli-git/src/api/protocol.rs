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
use crate::api::helpers::{resolve_repo, strip_git_suffix};
use crate::tenant::TenantContext;

/// Maximum allowed body size for git protocol requests (100 MB).
const MAX_GIT_BODY: usize = 100 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub struct InfoRefsQuery {
    service: Option<String>,
}

/// Parse the pkt-line ref-update section at the start of a git receive-pack body.
/// Returns `Vec<(old_sha, new_sha, ref_name)>` for each updated ref.
fn parse_ref_updates(body: &[u8]) -> Vec<(String, String, String)> {
    let mut updates = Vec::new();
    let mut pos = 0;
    while pos + 4 <= body.len() {
        let len_hex = match std::str::from_utf8(&body[pos..pos + 4]) {
            Ok(s) => s,
            Err(_) => break,
        };
        let pkt_len = match u16::from_str_radix(len_hex, 16) {
            Ok(n) => n as usize,
            Err(_) => break,
        };
        if pkt_len == 0 {
            break;
        }
        if pkt_len < 4 || pos + pkt_len > body.len() {
            break;
        }
        let line = &body[pos + 4..pos + pkt_len];
        if line.len() >= 83 {
            let old_sha = String::from_utf8_lossy(&line[0..40]).into_owned();
            let new_sha = String::from_utf8_lossy(&line[41..81]).into_owned();
            let ref_end = line[82..]
                .iter()
                .position(|&b| b == 0 || b == b'\n')
                .unwrap_or(line.len() - 82);
            let ref_name = String::from_utf8_lossy(&line[82..82 + ref_end]).into_owned();
            if !ref_name.is_empty() {
                updates.push((old_sha, new_sha, ref_name));
            }
        }
        pos += pkt_len;
    }
    updates
}

/// GET /{namespace}/{repo}/info/refs
pub async fn info_refs(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, raw_repo)): Path<(String, String)>,
    Query(query): Query<InfoRefsQuery>,
) -> Response {
    let service = query.service.as_deref().unwrap_or("").to_string();

    if let Err(r) = resolve_repo(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
        return r;
    }
    let repo_name = strip_git_suffix(&raw_repo);

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, repo_name);

    if service.contains("git-receive-pack") {
        crate::protocol::receive_pack::info_refs_receive(path, &tenant.tenant_id)
            .await
            .into_response()
    } else {
        crate::protocol::upload_pack::info_refs_upload(path)
            .await
            .into_response()
    }
}

/// POST /{namespace}/{repo}/git-upload-pack
pub async fn upload_pack(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, raw_repo)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    if let Err(r) = resolve_repo(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
        return r;
    }
    let repo_name = strip_git_suffix(&raw_repo);

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, repo_name);
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
    Path((namespace, raw_repo)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    let repo = match resolve_repo(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let repo_name = strip_git_suffix(&raw_repo).to_string();

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), MAX_GIT_BODY).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!(error = %e, "failed to read receive-pack request body");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    let ref_updates = parse_ref_updates(&body);

    let response =
        crate::protocol::receive_pack::post_receive_pack(path, &headers, body, &tenant.tenant_id)
            .await;

    // After a successful push, fire webhooks (with backpressure) and invalidate cache.
    if response.status().is_success() {
        let webhook_store = state.webhook_store.clone();
        let http_client = state.http_client.clone();
        let semaphore = state.webhook_semaphore.clone();
        let allow_localhost = state.allow_localhost_webhooks;
        let tenant_id = tenant.tenant_id.clone();
        let repo_id = repo.id.clone();
        let repo_name_clone = repo_name.clone();
        let zero_sha = "0".repeat(40);
        tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            if ref_updates.is_empty() {
                crate::hooks::deliver_webhooks(
                    webhook_store,
                    http_client,
                    &tenant_id,
                    &repo_id,
                    &crate::hooks::HookDelivery {
                        repo_id: repo_id.clone(),
                        event: muli_core::git::WebhookEvent::Push,
                        payload: serde_json::json!({
                            "repository": repo_name_clone,
                        }),
                    },
                    allow_localhost,
                )
                .await;
            } else {
                for (old_sha, new_sha, ref_name) in &ref_updates {
                    if new_sha == &zero_sha {
                        continue;
                    }
                    crate::hooks::deliver_webhooks(
                        webhook_store.clone(),
                        http_client.clone(),
                        &tenant_id,
                        &repo_id,
                        &crate::hooks::HookDelivery {
                            repo_id: repo_id.clone(),
                            event: muli_core::git::WebhookEvent::Push,
                            payload: serde_json::json!({
                                "repository": repo_name_clone,
                                "ref": ref_name,
                                "before": old_sha,
                                "after": new_sha,
                            }),
                        },
                        allow_localhost,
                    )
                    .await;
                }
            }
        });

        // Invalidate tree-commits cache
        if let Some(cache) = state.cache_store.clone() {
            let tid = tenant.tenant_id.clone();
            let rid = repo.id.clone();
            tokio::spawn(async move {
                let _ = cache.invalidate_repo(&tid, &rid).await;
            });
        }
    }

    response.into_response()
}
