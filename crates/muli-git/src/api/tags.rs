// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git tag management endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo, validate_ref_name};
use crate::tenant::TenantContext;

#[derive(Debug, Deserialize)]
pub struct CreateTagRequest {
    pub name: String,
    pub target: String,
    pub message: Option<String>,
}

/// POST /api/v1/repos/{namespace}/{repo}/tags
pub async fn create_tag(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Json(req): Json<CreateTagRequest>,
) -> Response {
    if !validate_ref_name(&req.name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid tag name");
    }

    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let name = req.name.clone();
    let target = req.target.clone();
    let message = req.message.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let repo = git2::Repository::open_bare(&path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&target)
            .map_err(|e| format!("target not found: {e}"))?;
        if let Some(msg) = message {
            let sig = repo
                .signature()
                .or_else(|_| git2::Signature::now("muli", "muli@localhost"))
                .map_err(|e| e.to_string())?;
            repo.tag(&name, &obj, &sig, &msg, false)
                .map_err(|e| e.to_string())?;
        } else {
            repo.tag_lightweight(&name, &obj, false)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    })
    .await;

    match result {
        Ok(Ok(())) => (
            StatusCode::CREATED,
            Json(json!({ "name": req.name, "target": req.target })),
        )
            .into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to create tag");
            error_response(StatusCode::BAD_REQUEST, &e)
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in create_tag");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// DELETE /api/v1/repos/{namespace}/{repo}/tags/{tag}
pub async fn delete_tag(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path((namespace, repo_name, tag)): Path<(String, String, String)>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);

    let result = tokio::task::spawn_blocking(move || -> Result<(), git2::Error> {
        let repo = git2::Repository::open_bare(&path)?;
        let refname = format!("refs/tags/{tag}");
        let mut reference = repo.find_reference(&refname)?;
        reference.delete()?;
        Ok(())
    })
    .await;

    match result {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => {
            if e.code() == git2::ErrorCode::NotFound {
                error_response(StatusCode::NOT_FOUND, "tag not found")
            } else {
                tracing::error!(error = %e, "failed to delete tag");
                error_response(StatusCode::BAD_REQUEST, e.message())
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in delete_tag");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
