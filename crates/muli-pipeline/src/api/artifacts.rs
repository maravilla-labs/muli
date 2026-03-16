// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REST handlers for pipeline artifacts.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

pub async fn list_artifacts(
    Path((_namespace, _repo, _run_number)): Path<(String, String, u64)>,
) -> Response {
    Json(serde_json::json!({ "artifacts": [] })).into_response()
}

pub async fn download_artifact(
    Path((_namespace, _repo, _run_number, _name)): Path<(String, String, u64, String)>,
) -> Response {
    StatusCode::NOT_FOUND.into_response()
}
