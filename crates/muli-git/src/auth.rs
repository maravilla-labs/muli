// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git authentication: token-based HTTP auth and SSH public key auth.

use std::sync::Arc;

use axum::{
    Json,
    extract::Request,
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use muli_core::token_hash;
use serde_json::json;
use tracing::warn;

use muli_core::git::{GitPermission, HasPermissions};
use muli_core::traits::{CollaboratorStore, GitTokenStore, RepositoryStore};

use crate::tenant::TenantContext;

/// Authenticated user context, injected into request extensions by the auth middleware.
#[derive(Clone, Debug)]
pub struct AuthenticatedUser {
    pub user_id: String,
}

/// Git authentication configuration.
#[derive(Clone)]
pub struct GitAuth {
    pub token_store: Arc<dyn GitTokenStore>,
    pub repo_store: Option<Arc<dyn RepositoryStore>>,
    pub collaborator_store: Option<Arc<dyn CollaboratorStore>>,
    pub anonymous_pull: bool,
}

impl GitAuth {
    pub fn new(token_store: Arc<dyn GitTokenStore>) -> Self {
        Self {
            token_store,
            repo_store: None,
            collaborator_store: None,
            anonymous_pull: false,
        }
    }

    pub fn with_repo_store(mut self, store: Arc<dyn RepositoryStore>) -> Self {
        self.repo_store = Some(store);
        self
    }

    pub fn with_collaborator_store(mut self, store: Arc<dyn CollaboratorStore>) -> Self {
        self.collaborator_store = Some(store);
        self
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

/// Determine the required git permission based on HTTP method and path.
///
/// For the Git Smart HTTP protocol:
/// - GET /info/refs?service=git-upload-pack  → Pull
/// - GET /info/refs?service=git-receive-pack → Push
/// - POST /git-upload-pack                   → Pull
/// - POST /git-receive-pack                  → Push
/// - DELETE (REST API)                       → Admin
/// - GET/HEAD (REST API)                     → Pull
/// - POST/PUT/PATCH (REST API)               → Push
fn required_permission(method: &Method, path: &str, query: Option<&str>) -> GitPermission {
    // Git Smart HTTP protocol: service routing
    if path.ends_with("/info/refs") {
        let service = query.unwrap_or("");
        if service.contains("git-receive-pack") {
            return GitPermission::Push;
        }
        return GitPermission::Pull;
    }
    if path.ends_with("/git-receive-pack") {
        return GitPermission::Push;
    }
    if path.ends_with("/git-upload-pack") {
        return GitPermission::Pull;
    }
    // REST API: method-based
    match *method {
        Method::GET | Method::HEAD => GitPermission::Pull,
        Method::DELETE => GitPermission::Admin,
        _ => GitPermission::Push,
    }
}

/// Extract the bearer token from the Authorization header.
fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty())
}

/// Extract a token from Basic auth.
/// Format: `Authorization: Basic base64(username:token)` — we use the password as the token.
pub fn extract_basic_auth_token(headers: &HeaderMap) -> Option<String> {
    use base64::Engine;

    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    let decoded_str = String::from_utf8(decoded).ok()?;
    let (_username, token) = decoded_str.split_once(':')?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

/// Extract a raw token (no scheme prefix).
pub fn extract_raw_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get("authorization").and_then(|v| v.to_str().ok())?;
    if value.starts_with("Bearer ") || value.starts_with("Basic ") {
        return None;
    }
    Some(value.trim())
}

/// Extract a token from any supported auth scheme (Bearer, Basic, or raw).
pub fn extract_any_token(headers: &HeaderMap) -> Option<String> {
    if let Some(token) = extract_bearer_token(headers) {
        return Some(token.to_string());
    }
    if let Some(token) = extract_basic_auth_token(headers) {
        return Some(token);
    }
    if let Some(token) = extract_raw_token(headers) {
        return Some(token.to_string());
    }
    None
}

/// Extract (namespace, repo_name) from paths like:
/// /{namespace}/{repo}.git/...  or /api/v1/repos/{namespace}/{repo}/...
fn extract_repo_from_path(path: &str) -> Option<(&str, &str)> {
    // REST API path: /api/v1/repos/{namespace}/{repo}
    if let Some(rest) = path.strip_prefix("/api/v1/repos/") {
        let parts: Vec<&str> = rest.splitn(3, '/').collect();
        if parts.len() >= 2 {
            return Some((parts[0], parts[1]));
        }
    }
    // Git smart HTTP: /{namespace}/{repo}.git/...
    let parts: Vec<&str> = path.trim_start_matches('/').splitn(3, '/').collect();
    if parts.len() >= 2 {
        let repo_name = parts[1].trim_end_matches(".git");
        return Some((parts[0], repo_name));
    }
    None
}

pub async fn auth_middleware(mut request: Request, next: Next) -> Response {
    // Skip auth for health check
    if request.uri().path() == "/-/health" {
        return next.run(request).await;
    }

    let auth = match request.extensions().get::<GitAuth>() {
        Some(a) => a.clone(),
        None => return next.run(request).await,
    };

    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(|q| q.to_string());
    let is_read = matches!(method, Method::GET | Method::HEAD);

    let tenant_ctx = request.extensions().get::<TenantContext>().cloned();

    let _tenant_id = tenant_ctx
        .as_ref()
        .map(|t| t.tenant_id.as_str())
        .unwrap_or("unknown");

    let is_anonymous_pull = auth.anonymous_pull && is_read;

    // Authenticate: skip only for anonymous pull on public repos
    let token = if is_anonymous_pull {
        // Try to extract token if present, but don't require it
        if let Some(plaintext) = extract_any_token(request.headers()) {
            let prefix = token_hash::token_prefix(&plaintext);
            if let Ok(Some(t)) = auth.token_store.get_token_by_prefix(&prefix).await {
                let hash = t.token_hash.clone();
                let pt = plaintext.clone();
                let valid =
                    tokio::task::spawn_blocking(move || token_hash::verify_token(&pt, &hash))
                        .await
                        .unwrap_or(false);
                if valid { Some(t) } else { None }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        // Extract token from any supported scheme
        let plaintext = match extract_any_token(request.headers()) {
            Some(t) => t,
            None => {
                return unauthorized_response();
            }
        };

        // Prefix lookup + Argon2id verify
        let prefix = token_hash::token_prefix(&plaintext);
        let token = match auth.token_store.get_token_by_prefix(&prefix).await {
            Ok(Some(t)) => {
                let hash = t.token_hash.clone();
                let pt = plaintext.clone();
                let valid =
                    tokio::task::spawn_blocking(move || token_hash::verify_token(&pt, &hash))
                        .await
                        .unwrap_or(false);
                if !valid {
                    return unauthorized_response();
                }
                t
            }
            Ok(None) => {
                return unauthorized_response();
            }
            Err(e) => {
                warn!(error = %e, "git token store lookup failed");
                return unauthorized_response();
            }
        };

        // Check token validity (expiration and revocation)
        if !token.is_valid() {
            return unauthorized_response();
        }

        // Verify tenant_id matches
        if let Some(ctx) = &tenant_ctx
            && token.tenant_id != ctx.tenant_id
        {
            return forbidden_response("token does not belong to this tenant");
        }

        // Check permission based on method + path
        let required = required_permission(&method, &path, query.as_deref());
        if !token.has_permission(required) {
            return forbidden_response("insufficient permissions");
        }

        Some(token)
    };

    // Per-repo ACL for private repositories
    // Parse namespace and repo from path: /{namespace}/{repo}.git/... or /api/v1/repos/{ns}/{repo}/...
    if let (Some(repo_store), Some(collab_store)) = (&auth.repo_store, &auth.collaborator_store)
        && let Some((namespace, repo_name)) = extract_repo_from_path(&path)
    {
        let tenant_id_str = tenant_ctx
            .as_ref()
            .map(|t| t.tenant_id.as_str())
            .unwrap_or("");
        if !tenant_id_str.is_empty()
            && let Ok(Some(repo)) = repo_store
                .get_repository_by_name(tenant_id_str, namespace, repo_name)
                .await
            && repo.is_private
        {
            let required = required_permission(&method, &path, query.as_deref());

            // Anonymous users cannot access private repos
            let token = match &token {
                Some(t) => t,
                None => return forbidden_response("access denied to private repository"),
            };

            // Service tokens without user_id cannot access private repos
            let user_id = match token.user_id.as_deref() {
                Some(uid) if !uid.is_empty() => uid,
                _ => {
                    return forbidden_response("service tokens cannot access private repositories");
                }
            };

            // Check owner
            let is_owner = !repo.owner_id.is_empty() && repo.owner_id == user_id;

            // Check collaborator
            let is_collaborator = if !is_owner {
                collab_store
                    .get_collaborator(&repo.id, user_id)
                    .await
                    .ok()
                    .flatten()
                    .map(|c| c.has_permission(required))
                    .unwrap_or(false)
            } else {
                false
            };

            if !is_owner && !is_collaborator {
                return forbidden_response("access denied to private repository");
            }
        }
    }

    // Inject authenticated user context if token has a user_id
    if let Some(ref t) = token {
        if let Some(ref uid) = t.user_id {
            if !uid.is_empty() {
                request.extensions_mut().insert(AuthenticatedUser {
                    user_id: uid.clone(),
                });
            }
        }
    }

    next.run(request).await
}

fn unauthorized_response() -> Response {
    let body = json!({
        "error": "authentication required"
    });
    (
        StatusCode::UNAUTHORIZED,
        [("WWW-Authenticate", "Basic realm=\"muli-git\"")],
        Json(body),
    )
        .into_response()
}

fn forbidden_response(message: &str) -> Response {
    let body = json!({
        "error": message
    });
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
