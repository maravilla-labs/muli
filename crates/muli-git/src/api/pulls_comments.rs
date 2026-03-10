// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pull request comment endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use muli_core::pr::PrComment;

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo_full};
use crate::auth::AuthenticatedUser;
use crate::tenant::TenantContext;

const MAX_COMMENT_BODY: usize = 65536;

#[derive(Debug, Deserialize)]
pub struct CreateCommentRequest {
    pub body: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub start_line: Option<u32>,
    pub reply_to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CommentResponse {
    pub id: String,
    pub pr_id: String,
    pub user_id: String,
    pub body: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
}

impl From<PrComment> for CommentResponse {
    fn from(c: PrComment) -> Self {
        CommentResponse {
            id: c.id,
            pr_id: c.pr_id,
            user_id: c.user_id,
            body: c.body,
            created_at: c.created_at.to_rfc3339(),
            file: c.file,
            line: c.line,
            start_line: c.start_line,
            reply_to: c.reply_to,
        }
    }
}

/// POST /api/v1/repos/{namespace}/{repo}/pulls/{number}/comments
pub async fn add_comment(
    Extension(tenant): Extension<TenantContext>,
    Extension(auth_user): Extension<AuthenticatedUser>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo, number)): Path<(String, String, u64)>,
    Json(req): Json<CreateCommentRequest>,
) -> Response {
    let (repo_id, _, _) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    let pr = match state.pr_store.get_pr_by_number(&repo_id, number).await {
        Ok(Some(pr)) => pr,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "PR not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to get PR");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    if req.body.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "body is required");
    }
    if req.body.len() > MAX_COMMENT_BODY {
        return error_response(
            StatusCode::BAD_REQUEST,
            "comment body exceeds 65536 characters",
        );
    }

    let mut comment = PrComment::new(pr.id.clone(), auth_user.user_id, req.body);
    comment.file = req.file;
    comment.line = req.line;
    comment.start_line = req.start_line;
    comment.reply_to = req.reply_to;

    if let Err(e) = comment.validate() {
        return error_response(StatusCode::BAD_REQUEST, &e.to_string());
    }

    match state.pr_comment_store.add_comment(&comment).await {
        Ok(_) => (StatusCode::CREATED, Json(CommentResponse::from(comment))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to add comment");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to add comment"})),
            )
                .into_response()
        }
    }
}

/// GET /api/v1/repos/{namespace}/{repo}/pulls/{number}/comments
pub async fn list_comments(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo, number)): Path<(String, String, u64)>,
) -> Response {
    let (repo_id, _, _) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    let pr = match state.pr_store.get_pr_by_number(&repo_id, number).await {
        Ok(Some(pr)) => pr,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "PR not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to get PR");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    match state.pr_comment_store.list_comments(&pr.id).await {
        Ok(comments) => {
            let list: Vec<CommentResponse> =
                comments.into_iter().map(CommentResponse::from).collect();
            Json(list).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to list comments");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to list comments"})),
            )
                .into_response()
        }
    }
}
