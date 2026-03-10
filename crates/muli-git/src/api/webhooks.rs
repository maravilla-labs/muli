// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Webhook configuration endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;

use muli_core::git::{Webhook, WebhookEvent, validate_webhook_target};

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo};
use crate::tenant::TenantContext;

/// Sign a webhook payload using HMAC-SHA256.
/// Returns the signature in the format `sha256={hex_digest}`.
pub fn sign_payload(secret: &str, payload: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(payload);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[derive(Debug, Deserialize)]
pub struct CreateWebhookRequest {
    pub url: String,
    pub secret: String,
    pub events: Vec<WebhookEvent>,
    pub active: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct WebhookResponse {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    pub url: String,
    pub events: Vec<WebhookEvent>,
    pub active: bool,
    pub created_at: String,
}

impl From<Webhook> for WebhookResponse {
    fn from(w: Webhook) -> Self {
        Self {
            id: w.id,
            tenant_id: w.tenant_id,
            repo_id: w.repo_id,
            url: w.url,
            events: w.events,
            active: w.active,
            created_at: w.created_at.to_rfc3339(),
        }
    }
}

/// POST /api/v1/repos/{namespace}/{repo}/hooks
pub async fn create_webhook(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Json(req): Json<CreateWebhookRequest>,
) -> Response {
    // Validate webhook URL (SSRF protection) — skip in test/dev mode
    if !state.allow_localhost_webhooks
        && let Err(msg) = validate_webhook_target(&req.url).await
    {
        return error_response(StatusCode::BAD_REQUEST, &msg);
    }

    let repo = match resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        Ok(r) => r,
        Err(e) => return e,
    };

    let mut webhook = Webhook::new(
        tenant.tenant_id.clone(),
        repo.id.clone(),
        req.url,
        req.secret,
        req.events,
    );
    if let Some(active) = req.active {
        webhook.active = active;
    }

    match state.webhook_store.create_webhook(&webhook).await {
        Ok(_) => {
            let resp: WebhookResponse = webhook.into();
            (StatusCode::CREATED, Json(resp)).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to create webhook");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to create webhook"})),
            )
                .into_response()
        }
    }
}

/// GET /api/v1/repos/{namespace}/{repo}/hooks
pub async fn list_webhooks(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
) -> Response {
    let repo = match resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        Ok(r) => r,
        Err(e) => return e,
    };

    match state
        .webhook_store
        .list_webhooks(&tenant.tenant_id, &repo.id)
        .await
    {
        Ok(hooks) => {
            let list: Vec<WebhookResponse> = hooks.into_iter().map(WebhookResponse::from).collect();
            Json(list).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to list webhooks");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to list webhooks"})),
            )
                .into_response()
        }
    }
}

/// DELETE /api/v1/repos/{namespace}/{repo}/hooks/{hook_id}
pub async fn delete_webhook(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, hook_id)): Path<(String, String, String)>,
) -> Response {
    let repo = match resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Verify the webhook belongs to this repo and tenant before deleting
    match state.webhook_store.get_webhook(&hook_id).await {
        Ok(Some(hook)) if hook.repo_id == repo.id && hook.tenant_id == tenant.tenant_id => {}
        Ok(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "webhook not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to look up webhook");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to look up webhook"})),
            )
                .into_response();
        }
    }

    match state.webhook_store.delete_webhook(&hook_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to delete webhook");
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "webhook not found"})),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_payload_format() {
        let sig = sign_payload("my-secret", b"hello world");
        assert!(sig.starts_with("sha256="));
        assert_eq!(sig.len(), 7 + 64); // "sha256=" + 64 hex chars
    }

    #[test]
    fn sign_payload_deterministic() {
        let sig1 = sign_payload("secret", b"payload");
        let sig2 = sign_payload("secret", b"payload");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn sign_payload_different_secrets() {
        let sig1 = sign_payload("secret-a", b"payload");
        let sig2 = sign_payload("secret-b", b"payload");
        assert_ne!(sig1, sig2);
    }
}
