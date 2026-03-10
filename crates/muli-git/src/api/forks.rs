// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository fork endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use muli_core::git::Repository;
use serde::Deserialize;

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo, validate_path_component};
use crate::api::repos::RepoResponse;
use crate::tenant::TenantContext;

#[derive(Debug, Deserialize)]
pub struct ForkRequest {
    pub dest_namespace: String,
    pub dest_name: Option<String>,
}

/// POST /api/v1/repos/{namespace}/{repo}/forks
pub async fn fork_repo(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Json(req): Json<ForkRequest>,
) -> Response {
    let source = match resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        Ok(r) => r,
        Err(e) => return e,
    };

    let dest_name = req.dest_name.unwrap_or_else(|| source.name.clone());
    let dest_namespace = req.dest_namespace.clone();

    // Validate to prevent path traversal attacks (e.g. dest_namespace = "../../etc")
    if !validate_path_component(&dest_namespace) || !validate_path_component(&dest_name) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid destination namespace or name",
        );
    }

    match state
        .storage
        .fork_repo(
            &tenant.tenant_id,
            &namespace,
            &repo_name,
            &tenant.tenant_id,
            &dest_namespace,
            &dest_name,
        )
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "failed to fork repository on filesystem");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to fork repository",
            );
        }
    }

    let mut fork = match Repository::new(
        tenant.tenant_id.clone(),
        dest_namespace,
        dest_name,
        source.description.clone(),
        source.is_private,
    ) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, &e.to_string());
        }
    };
    fork.fork_of = Some(source.id.clone());

    match state.repo_store.create_repository(&fork).await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "failed to create fork repository record");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save fork record",
            );
        }
    }

    let resp: RepoResponse = fork.into();
    (StatusCode::CREATED, Json(resp)).into_response()
}
