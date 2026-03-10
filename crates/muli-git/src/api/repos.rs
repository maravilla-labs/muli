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
use crate::api::helpers::validate_path_component;
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
    // Validate namespace and name to prevent path traversal attacks
    if !validate_path_component(&req.namespace) || !validate_path_component(&req.name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid namespace or name"})),
        )
            .into_response();
    }

    // Check for duplicate name within this tenant+namespace before creating
    match state
        .repo_store
        .get_repository_by_name(&tenant.tenant_id, &req.namespace, &req.name)
        .await
    {
        Ok(Some(_)) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "repository already exists"})),
            )
                .into_response();
        }
        Ok(None) => {}
        Err(e) => {
            tracing::error!(error = %e, "failed to check repository existence");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    let repo = match Repository::new(
        tenant.tenant_id.clone(),
        req.namespace.clone(),
        req.name.clone(),
        req.description.unwrap_or_default(),
        req.is_private.unwrap_or(false),
    ) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Persist repo record
    match state.repo_store.create_repository(&repo).await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "failed to create repository record");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to create repository"})),
            )
                .into_response();
        }
    }

    // Initialize bare git repo on disk
    match state
        .storage
        .init_repo(&tenant.tenant_id, &req.namespace, &req.name)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            // Roll back repository record
            let _ = state.repo_store.delete_repository(&repo.id).await;
            tracing::error!(error = %e, "failed to init bare repo");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("failed to initialize repository: {}", e)})),
            )
                .into_response();
        }
    }

    let resp: RepoResponse = repo.into();
    (StatusCode::CREATED, Json(resp)).into_response()
}

/// DELETE /api/v1/repos/{namespace}/{repo}
pub async fn delete_repo(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
) -> Response {
    // Look up repo record
    let repo = match state
        .repo_store
        .get_repository_by_name(&tenant.tenant_id, &namespace, &repo_name)
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "repository not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to look up repository");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    // Delete from store
    if let Err(e) = state.repo_store.delete_repository(&repo.id).await {
        tracing::error!(error = %e, "failed to delete repository record");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to delete repository"})),
        )
            .into_response();
    }

    // Delete from filesystem (best-effort; log but don't fail)
    if let Err(e) = state
        .storage
        .delete_repo(&tenant.tenant_id, &namespace, &repo_name)
        .await
    {
        tracing::warn!(error = %e, "failed to delete repository from filesystem");
    }

    StatusCode::NO_CONTENT.into_response()
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

    let repo = match state
        .repo_store
        .get_repository_by_name(&tenant.tenant_id, &namespace, &repo_name)
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "repository not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to look up repository");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    if repo.namespace == req.new_namespace {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "new_namespace is same as current namespace"})),
        )
            .into_response();
    }

    // Move filesystem repo first
    if let Err(e) = state
        .storage
        .transfer_repo(
            &tenant.tenant_id,
            &namespace,
            &repo_name,
            &req.new_namespace,
        )
        .await
    {
        tracing::error!(error = %e, "failed to transfer repository on filesystem");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("failed to transfer repository: {}", e)})),
        )
            .into_response();
    }

    // Update DB record
    if let Err(e) = state
        .repo_store
        .transfer_repository(&repo.id, &req.new_namespace)
        .await
    {
        // Best-effort rollback filesystem
        let _ = state
            .storage
            .transfer_repo(
                &tenant.tenant_id,
                &req.new_namespace,
                &repo_name,
                &namespace,
            )
            .await;
        tracing::error!(error = %e, "failed to update repository namespace in store");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to update repository record"})),
        )
            .into_response();
    }

    let mut updated = repo;
    updated.namespace = req.new_namespace;
    Json(RepoResponse::from(updated)).into_response()
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
