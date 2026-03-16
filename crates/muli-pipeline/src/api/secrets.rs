// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REST handlers for pipeline secrets.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct SetSecretRequest {
    pub name: String,
    pub value: String,
}

pub async fn set_secret(
    Path((_namespace, _repo)): Path<(String, String)>,
    Json(_req): Json<SetSecretRequest>,
) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

pub async fn list_secrets(
    Path((_namespace, _repo)): Path<(String, String)>,
) -> Response {
    Json(serde_json::json!({ "secrets": [] })).into_response()
}

pub async fn delete_secret(
    Path((_namespace, _repo, _name)): Path<(String, String, String)>,
) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}
