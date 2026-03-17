// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository CRUD endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use muli_core::git::Repository;

use crate::api::GitState;
use crate::api::helpers::{domain_err_to_http, validate_path_component};
use crate::tenant::TenantContext;

#[derive(Debug, Deserialize)]
pub struct CreateRepoRequest {
    pub namespace: String,
    pub name: String,
    pub description: Option<String>,
    pub is_private: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct RepoResponse {
    pub id: String,
    pub tenant_id: String,
    pub namespace: String,
    pub name: String,
    pub description: String,
    pub is_private: bool,
    pub default_branch: String,
    pub fork_of: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Repository> for RepoResponse {
    fn from(r: Repository) -> Self {
        Self {
            id: r.id,
            tenant_id: r.tenant_id,
            namespace: r.namespace,
            name: r.name,
            description: r.description,
            is_private: r.is_private,
            default_branch: r.default_branch,
            fork_of: r.fork_of,
            created_at: r.created_at.to_rfc3339(),
            updated_at: r.updated_at.to_rfc3339(),
        }
    }
}

/// POST /api/v1/repos
pub async fn create_repo(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Json(req): Json<CreateRepoRequest>,
) -> Response {
    if !validate_path_component(&req.namespace) || !validate_path_component(&req.name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid namespace or name"})),
        )
            .into_response();
    }

    match state
        .repo_service
        .create(
            &tenant.tenant_id,
            &req.namespace,
            &req.name,
            &req.description.clone().unwrap_or_default(),
            req.is_private.unwrap_or(false),
        )
        .await
    {
        Ok(repo) => (StatusCode::CREATED, Json(RepoResponse::from(repo))).into_response(),
        Err(e) => domain_err_to_http(e),
    }
}

/// DELETE /api/v1/repos/{namespace}/{repo}
pub async fn delete_repo(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
) -> Response {
    match state
        .repo_service
        .delete(&tenant.tenant_id, &namespace, &repo_name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => domain_err_to_http(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct TransferRepoRequest {
    pub new_namespace: String,
}

/// POST /api/v1/repos/{namespace}/{repo}/transfer
pub async fn transfer_repo(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Json(req): Json<TransferRepoRequest>,
) -> Response {
    let repo_name = repo_name
        .strip_suffix(".git")
        .unwrap_or(&repo_name)
        .to_string();

    if !validate_path_component(&req.new_namespace) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid new_namespace"})),
        )
            .into_response();
    }

    match state
        .repo_service
        .transfer(
            &tenant.tenant_id,
            &namespace,
            &repo_name,
            &req.new_namespace,
        )
        .await
    {
        Ok(repo) => Json(RepoResponse::from(repo)).into_response(),
        Err(e) => domain_err_to_http(e),
    }
}

/// GET /api/v1/repos
pub async fn list_repos(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
) -> Response {
    match state.repo_store.list_repositories(&tenant.tenant_id).await {
        Ok(repos) => {
            let list: Vec<RepoResponse> = repos.into_iter().map(RepoResponse::from).collect();
            Json(list).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to list repositories");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to list repositories"})),
            )
                .into_response()
        }
    }
}
