// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Registry authentication middleware.

use std::sync::Arc;

use axum::{
    extract::Request,
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use muli_core::auth::extract_any_token;
use muli_core::token_hash;
use tracing::{debug, warn};

use muli_core::registry::model::RegistryPermission;
use muli_core::traits::RegistryTokenStore;

use crate::metrics::RegistryMetrics;
use crate::tenant::TenantContext;

/// Registry authentication configuration.
#[derive(Clone)]
pub struct RegistryAuth {
    pub token_store: Arc<dyn RegistryTokenStore>,
    pub anonymous_pull: bool,
}

impl RegistryAuth {
    pub fn new(token_store: Arc<dyn RegistryTokenStore>) -> Self {
        Self {
            token_store,
            anonymous_pull: false,
        }
    }

    pub fn with_anonymous_pull(mut self, allow: bool) -> Self {
        self.anonymous_pull = allow;
        self
    }
}

/// Hash a plaintext token with Argon2id.
pub fn hash_token(plaintext: &str) -> String {
    token_hash::hash_token(plaintext).expect("Argon2id hashing failed")
}

/// Extract the lookup prefix from a plaintext token.
pub fn token_prefix(plaintext: &str) -> String {
    token_hash::token_prefix(plaintext)
}

/// Determine the required permission based on the HTTP method.
fn required_permission(method: &Method) -> RegistryPermission {
    match *method {
        Method::GET | Method::HEAD => RegistryPermission::Pull,
        Method::PUT | Method::POST | Method::PATCH => RegistryPermission::Push,
        Method::DELETE => RegistryPermission::Admin,
        _ => RegistryPermission::Admin,
    }
}

pub async fn auth_middleware(request: Request, next: Next) -> Response {
    // Skip auth for /v2/ version check endpoint (OCI spec requires unauthenticated access)
    if request.uri().path() == "/v2/" {
        return next.run(request).await;
    }

    let auth = match request.extensions().get::<RegistryAuth>() {
        Some(a) => a.clone(),
        None => return next.run(request).await,
    };

    let metrics = request.extensions().get::<RegistryMetrics>().cloned();
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let is_read = matches!(method, Method::GET | Method::HEAD);

    debug!(%method, %path, "auth: processing request");

    // Allow anonymous pull if configured
    if auth.anonymous_pull && is_read {
        return next.run(request).await;
    }

    let tenant_ctx = request.extensions().get::<TenantContext>().cloned();

    let tenant_id = tenant_ctx
        .as_ref()
        .map(|t| t.tenant_id.as_str())
        .unwrap_or("unknown");

    // Extract token (Bearer, Basic, or raw)
    let plaintext = match extract_any_token(request.headers()) {
        Some(t) => t,
        None => {
            warn!(%path, tenant_id, "auth: no token in request");
            if let Some(metrics) = &metrics {
                metrics.record_auth_failure(tenant_id, "missing_token");
            }
            return unauthorized_response("missing_token");
        }
    };

    // Prefix lookup + Argon2id verify
    let prefix = token_hash::token_prefix(&plaintext);
    let token = match auth.token_store.get_token_by_prefix(&prefix).await {
        Ok(Some(t)) => {
            let hash = t.token_hash.clone();
            let pt = plaintext.clone();
            let valid = tokio::task::spawn_blocking(move || token_hash::verify_token(&pt, &hash))
                .await
                .unwrap_or(false);
            if !valid {
                warn!(
                    tenant_id,
                    "auth: token prefix matched but Argon2id verification failed"
                );
                if let Some(metrics) = &metrics {
                    metrics.record_auth_failure(tenant_id, "invalid_token");
                }
                return unauthorized_response("token_not_found");
            }
            t
        }
        Ok(None) => {
            warn!(tenant_id, "auth: token prefix not found in store");
            if let Some(metrics) = &metrics {
                metrics.record_auth_failure(tenant_id, "invalid_token");
            }
            return unauthorized_response("token_not_found");
        }
        Err(e) => {
            warn!(tenant_id, error = %e, "auth: token store lookup failed");
            if let Some(metrics) = &metrics {
                metrics.record_auth_failure(tenant_id, "store_error");
            }
            return unauthorized_response("store_error");
        }
    };

    // Check token validity (expiration and revocation)
    if !token.is_valid() {
        let reason = if token.revoked {
            "revoked_token"
        } else {
            "expired_token"
        };
        warn!(tenant_id, token_id = %token.id, reason, "auth: token invalid");
        if let Some(metrics) = &metrics {
            metrics.record_auth_failure(tenant_id, reason);
        }
        return unauthorized_response(reason);
    }

    // Verify tenant_id matches
    if let Some(ctx) = &tenant_ctx
        && token.tenant_id != ctx.tenant_id
    {
        warn!(request_tenant = tenant_id, token_tenant = %token.tenant_id, "auth: tenant mismatch");
        if let Some(metrics) = &metrics {
            metrics.record_auth_failure(tenant_id, "tenant_mismatch");
        }
        return forbidden_response("token does not belong to this tenant");
    }

    // Check permission
    let required = required_permission(&method);
    if !token.has_permission(required) {
        warn!(tenant_id, required = ?required, "auth: insufficient permission");
        if let Some(metrics) = &metrics {
            metrics.record_auth_failure(tenant_id, "insufficient_permission");
        }
        return forbidden_response("insufficient permissions");
    }

    next.run(request).await
}

fn unauthorized_response(reason: &str) -> Response {
    let body = serde_json::json!({
        "errors": [{
            "code": "UNAUTHORIZED",
            "message": "authentication required",
            "detail": reason
        }]
    });
    (
        StatusCode::UNAUTHORIZED,
        [("WWW-Authenticate", "Basic realm=\"muli-registry\"")],
        axum::Json(body),
    )
        .into_response()
}

fn forbidden_response(message: &str) -> Response {
    let body = serde_json::json!({
        "errors": [{
            "code": "DENIED",
            "message": message,
            "detail": null
        }]
    });
    (StatusCode::FORBIDDEN, axum::Json(body)).into_response()
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
