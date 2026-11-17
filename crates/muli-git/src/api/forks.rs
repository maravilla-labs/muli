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
use serde::Deserialize;

use crate::api::GitState;
use crate::api::helpers::{domain_err_to_http, error_response, validate_path_component};
use crate::api::repos::RepoResponse;
use crate::repo_target::TargetRepo;
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
    TargetRepo(target): TargetRepo,
    Path((_namespace, _repo_name)): Path<(String, String)>,
    Json(req): Json<ForkRequest>,
) -> Response {
    let dest_name = req.dest_name.unwrap_or_else(|| target.name.clone());

    if !validate_path_component(&req.dest_namespace) || !validate_path_component(&dest_name) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid destination namespace or name",
        );
    }

    match state
        .repo_service
        .fork(
            &tenant.tenant_id,
            &target.namespace,
            &target.name,
            &req.dest_namespace,
            &dest_name,
        )
        .await
    {
        Ok(repo) => (StatusCode::CREATED, Json(RepoResponse::from(repo))).into_response(),
        Err(e) => domain_err_to_http(e),
    }
}
