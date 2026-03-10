// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Branch and tag listing endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo, validate_ref_name};
use crate::tenant::TenantContext;

#[derive(Debug, Serialize)]
pub struct RefInfo {
    pub name: String,
    pub shorthand: String,
    pub target: String,
    pub kind: String,
}

/// GET /api/v1/repos/{namespace}/{repo}/refs
pub async fn list_refs(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&path).map_err(|e| e.to_string())?;
        let mut refs_list = Vec::new();
        for reference in repo.references().map_err(|e| e.to_string())? {
            let reference = reference.map_err(|e| e.to_string())?;
            let name = reference.name().unwrap_or("").to_string();
            let shorthand = reference.shorthand().unwrap_or("").to_string();
            let target = reference
                .target()
                .map(|oid| oid.to_string())
                .unwrap_or_default();
            let kind = if reference.is_branch() {
                "branch"
            } else if reference.is_tag() {
                "tag"
            } else {
                "other"
            };
            refs_list.push(RefInfo {
                name,
                shorthand,
                target,
                kind: kind.to_string(),
            });
        }
        Ok::<Vec<RefInfo>, String>(refs_list)
    })
    .await;

    match result {
        Ok(Ok(refs_list)) => Json(refs_list).into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to list refs");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to list refs: {e}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in list_refs");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateBranchRequest {
    pub name: String,
    pub from_ref: String,
}

/// POST /api/v1/repos/{namespace}/{repo}/branches
pub async fn create_branch(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Json(body): Json<CreateBranchRequest>,
) -> Response {
    if !validate_ref_name(&body.name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid branch name");
    }

    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let name = body.name.clone();
    let from_ref = body.from_ref.clone();

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&from_ref)
            .map_err(|e| format!("ref not found: {e}"))?;
        let commit = obj.peel_to_commit().map_err(|e| e.to_string())?;
        let ref_name = format!("refs/heads/{name}");
        repo.reference(&ref_name, commit.id(), false, "branch created via API")
            .map_err(|e| e.to_string())?;
        Ok::<String, String>(commit.id().to_string())
    })
    .await;

    match result {
        Ok(Ok(sha)) => (
            StatusCode::CREATED,
            Json(json!({ "name": body.name, "target": sha })),
        )
            .into_response(),
        Ok(Err(e)) if e.contains("ref not found") => error_response(StatusCode::NOT_FOUND, &e),
        Ok(Err(e)) if e.contains("already exists") || e.contains("Cannot create") => {
            error_response(StatusCode::CONFLICT, "branch already exists")
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to create branch");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in create_branch");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
