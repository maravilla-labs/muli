// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pull request CRUD endpoints.

pub use crate::api::pulls_comments::{
    CommentResponse, CreateCommentRequest, add_comment, list_comments,
};
pub use crate::api::pulls_diff::{
    DiffFile, DiffHunk, DiffLine, build_diff, build_diff_from_git2_diff, get_pr_diff,
};
pub use crate::api::pulls_types::{
    CreatePrRequest, ListPrsQuery, PatchPrRequest, PrResponse, parse_pr_state,
};

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde_json::json;

use muli_core::git::WebhookEvent;
use muli_core::pr::{NewPullRequest, PrState, PullRequest};

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo_full};
use crate::api::pulls_merge::{MergeError, perform_merge};
use crate::auth::AuthenticatedUser;
use crate::tenant::TenantContext;

// ── Constants ────────────────────────────────────────────────────────────────

const MAX_PR_TITLE: usize = 256;
const MAX_PR_DESCRIPTION: usize = 65536;

// ── Handlers ─────────────────────────────────────────────────────────────────

/// POST /api/v1/repos/{namespace}/{repo}/pulls
pub async fn create_pr(
    Extension(tenant): Extension<TenantContext>,
    Extension(auth_user): Extension<AuthenticatedUser>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo)): Path<(String, String)>,
    Json(req): Json<CreatePrRequest>,
) -> Response {
    let (repo_id, _, _) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    if req.source_branch.is_empty() || req.target_branch.is_empty() || req.title.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "source_branch, target_branch, and title are required",
        );
    }
    if req.title.len() > MAX_PR_TITLE {
        return error_response(StatusCode::BAD_REQUEST, "title exceeds 256 characters");
    }
    if let Some(ref desc) = req.description
        && desc.len() > MAX_PR_DESCRIPTION
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "description exceeds 65536 characters",
        );
    }

    let number = match state.pr_store.next_pr_number(&repo_id).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "failed to get next PR number");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    let pr = match PullRequest::new(NewPullRequest {
        number,
        tenant_id: tenant.tenant_id.clone(),
        repo_id: repo_id.clone(),
        author_user_id: auth_user.user_id,
        source_branch: req.source_branch,
        target_branch: req.target_branch,
        title: req.title,
        description: req.description.unwrap_or_default(),
    }) {
        Ok(pr) => pr,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, &e.to_string());
        }
    };

    match state.pr_store.create_pr(&pr).await {
        Ok(_) => {
            fire_webhook(
                &state,
                &tenant.tenant_id,
                &repo_id,
                WebhookEvent::PrOpened,
                pr.number,
            );
            (StatusCode::CREATED, Json(PrResponse::from(pr))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to create PR");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to create PR")
        }
    }
}

/// GET /api/v1/repos/{namespace}/{repo}/pulls
pub async fn list_prs(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo)): Path<(String, String)>,
    Query(query): Query<ListPrsQuery>,
) -> Response {
    let (repo_id, _, _) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    let state_filter = query.state.as_deref().and_then(parse_pr_state);

    match state.pr_store.list_prs(&repo_id, state_filter).await {
        Ok(prs) => {
            let list: Vec<PrResponse> = prs.into_iter().map(PrResponse::from).collect();
            Json(list).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to list PRs");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to list PRs")
        }
    }
}

/// GET /api/v1/repos/{namespace}/{repo}/pulls/{number}
pub async fn get_pr(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo, number)): Path<(String, String, u64)>,
) -> Response {
    let (repo_id, _, _) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    match state.pr_store.get_pr_by_number(&repo_id, number).await {
        Ok(Some(pr)) => Json(PrResponse::from(pr)).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "PR not found"),
        Err(e) => {
            tracing::error!(error = %e, "failed to get PR");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// PATCH /api/v1/repos/{namespace}/{repo}/pulls/{number}
pub async fn patch_pr(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo, number)): Path<(String, String, u64)>,
    Json(req): Json<PatchPrRequest>,
) -> Response {
    let (repo_id, _repo_name, repo_path) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    let mut pr = match state.pr_store.get_pr_by_number(&repo_id, number).await {
        Ok(Some(pr)) => pr,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "PR not found"),
        Err(e) => {
            tracing::error!(error = %e, "failed to get PR");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    if pr.state != PrState::Open {
        return error_response(StatusCode::UNPROCESSABLE_ENTITY, "PR is not open");
    }

    match req.action.as_str() {
        "close" => handle_close(&state, &tenant.tenant_id, &repo_id, &mut pr).await,
        "merge" => handle_merge(&state, &tenant.tenant_id, &repo_id, repo_path, &mut pr).await,
        _ => error_response(StatusCode::BAD_REQUEST, "action must be 'merge' or 'close'"),
    }
}

// ── Private helpers ──────────────────────────────────────────────────────────

async fn handle_close(
    state: &GitState,
    tenant_id: &str,
    repo_id: &str,
    pr: &mut PullRequest,
) -> Response {
    pr.state = PrState::Closed;
    pr.updated_at = Utc::now();
    match state.pr_store.update_pr(pr).await {
        Ok(()) => {
            fire_webhook(state, tenant_id, repo_id, WebhookEvent::PrClosed, pr.number);
            Json(PrResponse::from(pr.clone())).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to close PR");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to close PR")
        }
    }
}

async fn handle_merge(
    state: &GitState,
    tenant_id: &str,
    repo_id: &str,
    repo_path: std::path::PathBuf,
    pr: &mut PullRequest,
) -> Response {
    let source = pr.source_branch.clone();
    let target = pr.target_branch.clone();

    let merge_result =
        tokio::task::spawn_blocking(move || perform_merge(&repo_path, &source, &target)).await;

    match merge_result {
        Ok(Ok(merge_sha)) => {
            pr.state = PrState::Merged;
            pr.merge_commit_sha = Some(merge_sha);
            pr.updated_at = Utc::now();
            match state.pr_store.update_pr(pr).await {
                Ok(()) => {
                    fire_webhook(state, tenant_id, repo_id, WebhookEvent::PrMerged, pr.number);
                    Json(PrResponse::from(pr.clone())).into_response()
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to update PR after merge");
                    error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "merge succeeded but failed to update PR record",
                    )
                }
            }
        }
        Ok(Err(MergeError::Conflict(files))) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "merge conflict",
                "conflicting_files": files,
            })),
        )
            .into_response(),
        Ok(Err(MergeError::Other(msg))) => {
            tracing::error!(error = %msg, "merge failed");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("merge failed: {msg}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "merge task panicked");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error during merge",
            )
        }
    }
}

fn fire_webhook(
    state: &GitState,
    tenant_id: &str,
    repo_id: &str,
    event: WebhookEvent,
    pr_number: u64,
) {
    let webhook_store = state.webhook_store.clone();
    let http_client = state.http_client.clone();
    let semaphore = state.webhook_semaphore.clone();
    let tenant_id = tenant_id.to_string();
    let repo_id = repo_id.to_string();
    tokio::spawn(async move {
        let _permit = semaphore.acquire().await;
        crate::hooks::deliver_webhooks(
            webhook_store,
            http_client,
            &tenant_id,
            &repo_id,
            &crate::hooks::HookDelivery {
                repo_id: repo_id.clone(),
                event,
                payload: json!({ "number": pr_number }),
            },
        )
        .await;
    });
}
