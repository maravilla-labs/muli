// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REST handlers for pipeline runs.

use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct ListRunsQuery {
    pub state: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Serialize)]
pub struct RunSummary {
    pub id: String,
    pub run_number: u64,
    pub state: String,
    pub commit_sha: String,
    pub ref_name: String,
    pub created_at: String,
}

pub async fn list_runs(
    Path((_namespace, _repo)): Path<(String, String)>,
    Query(_query): Query<ListRunsQuery>,
) -> Response {
    Json(serde_json::json!({ "runs": [], "total": 0 })).into_response()
}

pub async fn get_run(
    Path((_namespace, _repo, _run_number)): Path<(String, String, u64)>,
) -> Response {
    StatusCode::NOT_FOUND.into_response()
}

pub async fn cancel_run(
    Path((_namespace, _repo, _run_number)): Path<(String, String, u64)>,
) -> Response {
    StatusCode::NOT_FOUND.into_response()
}

pub async fn retry_run(
    Path((_namespace, _repo, _run_number)): Path<(String, String, u64)>,
) -> Response {
    StatusCode::NOT_FOUND.into_response()
}

pub async fn get_step_logs(
    Path((_namespace, _repo, _run_number, _step)): Path<(String, String, u64, String)>,
) -> Response {
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Deserialize)]
pub struct TriggerRequest {
    pub branch: Option<String>,
    pub commit_sha: Option<String>,
}

pub async fn trigger_run(
    Path((_namespace, _repo)): Path<(String, String)>,
    Json(_req): Json<TriggerRequest>,
) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}
