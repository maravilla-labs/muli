// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git authentication token management endpoints.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use muli_core::git::{GitPermission, GitToken};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::GitState;
use crate::tenant::TenantContext;
use axum::Extension;

#[derive(Debug, Deserialize)]
pub struct CreateTokenRequest {
    /// Argon2id PHC hash of the plaintext token (hashing done by the caller).
    pub token_hash: String,
    /// Short prefix of the plaintext token for O(1) DB lookup.
    #[serde(default)]
    pub token_prefix: String,
    pub description: String,
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub id: String,
    pub description: String,
    pub permissions: Vec<String>,
    pub created_at: String,
}

/// POST /api/v1/tokens
/// Register a pre-hashed git token for the tenant.
pub async fn create_token(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Json(body): Json<CreateTokenRequest>,
) -> Result<Json<Value>, StatusCode> {
    let permissions: Vec<GitPermission> = body
        .permissions
        .iter()
        .filter_map(|p| match p.as_str() {
            "pull" => Some(GitPermission::Pull),
            "push" => Some(GitPermission::Push),
            "admin" => Some(GitPermission::Admin),
            _ => None,
        })
        .collect();

    // Default to pull+push if none specified
    let permissions = if permissions.is_empty() {
        vec![GitPermission::Pull, GitPermission::Push]
    } else {
        permissions
    };

    let token = GitToken::new(
        tenant.tenant_id.clone(),
        body.token_hash,
        body.token_prefix,
        permissions.clone(),
        body.description.clone(),
        None, // no expiry
    );

    let id = state
        .token_store
        .create_token(&token)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let perm_strings: Vec<String> = permissions
        .iter()
        .map(|p| format!("{p:?}").to_lowercase())
        .collect();

    Ok(Json(json!({
        "id": id,
        "description": body.description,
        "permissions": perm_strings,
        "created_at": token.created_at.to_rfc3339(),
    })))
}

/// DELETE /api/v1/tokens/{token_id}
/// Revoke a git token by its ID.
pub async fn revoke_token(
    State(state): State<Arc<GitState>>,
    Extension(tenant): Extension<TenantContext>,
    Path(token_id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    // Verify the token belongs to this tenant before revoking
    let token = state
        .token_store
        .get_token_by_id(&token_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match token {
        Some(t) if t.tenant_id == tenant.tenant_id => {}
        _ => return Err(StatusCode::NOT_FOUND),
    }

    state
        .token_store
        .revoke_token(&token_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}
