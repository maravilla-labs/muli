// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git authentication: token-based HTTP auth and SSH public key auth.

use std::sync::Arc;

use axum::{
    Json,
    extract::Request,
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use muli_core::auth::extract_any_token;
use muli_core::token_hash;
use serde_json::json;
use tracing::warn;

use muli_core::git::{
    GitPermission, HasPermissions, RepoAccessVerdict, check_repo_access_with_org_lookup,
};
use muli_core::traits::{
    CollaboratorStore, GitTokenStore, OrgMemberStore, OrgStore, RepositoryStore,
};

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
    pub org_store: Option<Arc<dyn OrgStore>>,
    pub org_member_store: Option<Arc<dyn OrgMemberStore>>,
    pub anonymous_pull: bool,
}

impl GitAuth {
    pub fn new(token_store: Arc<dyn GitTokenStore>) -> Self {
        Self {
            token_store,
            repo_store: None,
            collaborator_store: None,
            org_store: None,
            org_member_store: None,
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

    pub fn with_org_stores(
        mut self,
        org_store: Arc<dyn OrgStore>,
        org_member_store: Arc<dyn OrgMemberStore>,
    ) -> Self {
        self.org_store = Some(org_store);
        self.org_member_store = Some(org_member_store);
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
    let required = required_permission(&method, &path, query.as_deref());

    // Authenticate: skip only for anonymous pull on public repos
    let token = if is_anonymous_pull {
        // Try to extract token if present, but don't require it. A token
        // counts only when it would be accepted on the authenticated path
        // too (hash, expiry/revocation, tenant, permission); anything else
        // is treated as no token at all, so it can never unlock more than an
        // anonymous reader gets.
        if let Some(plaintext) = extract_any_token(request.headers()) {
            let prefix = token_hash::token_prefix(&plaintext);
            if let Ok(Some(t)) = auth.token_store.get_token_by_prefix(&prefix).await {
                let hash = t.token_hash.clone();
                let pt = plaintext.clone();
                let valid =
                    tokio::task::spawn_blocking(move || token_hash::verify_token(&pt, &hash))
                        .await
                        .unwrap_or(false);
                let tenant_ok = tenant_ctx
                    .as_ref()
                    .is_none_or(|ctx| ctx.tenant_id == t.tenant_id);
                if valid && t.is_valid() && tenant_ok && t.has_permission(required) {
                    Some(t)
                } else {
                    None
                }
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
        if !token.has_permission(required) {
            return forbidden_response("insufficient permissions");
        }

        Some(token)
    };

    let tenant_id_str = tenant_ctx
        .as_ref()
        .map(|t| t.tenant_id.as_str())
        .unwrap_or("");

    // The repository the request is about, when the path names one that
    // exists in this tenant.
    let repo = match (&auth.repo_store, extract_repo_from_path(&path)) {
        (Some(repo_store), Some((namespace, repo_name))) if !tenant_id_str.is_empty() => repo_store
            .get_repository_by_name(tenant_id_str, namespace, repo_name)
            .await
            .ok()
            .flatten(),
        _ => None,
    };

    // A repository-scoped token reaches exactly its repository: any other
    // repository, a path naming none, or one that cannot be resolved is
    // refused, never handed on to the ordinary checks.
    if let Some(token) = token.as_ref()
        && let Some(scope) = token.repo_id.as_deref()
        && !repo_scope_allows(scope, repo.as_ref().map(|r| r.id.as_str()))
    {
        return forbidden_response("token is restricted to another repository");
    }

    // Per-repo ACL check:
    //   - Private repos: owner or collaborator required for any access
    //   - Public repos: anyone can read, but only owner or collaborator can push
    if let (Some(repo), Some(collab_store)) = (repo.as_ref(), &auth.collaborator_store) {
        // A repository-scoped pull token reads its own repository without a
        // user (CI checkout, a platform copying a template). Its validity
        // and tenant are checked again here, whichever branch produced it.
        if let Some(token) = token.as_ref()
            && required == GitPermission::Pull
            && token.repo_id.as_deref() == Some(repo.id.as_str())
            && token.is_valid()
            && token.tenant_id == tenant_id_str
            && token.has_permission(GitPermission::Pull)
        {
            if let Some(uid) = token.user_id.as_ref()
                && !uid.is_empty()
            {
                request.extensions_mut().insert(AuthenticatedUser {
                    user_id: uid.clone(),
                });
            }
            return next.run(request).await;
        }

        let user_id = token.as_ref().and_then(|t| t.user_id.as_deref());

        let verdict = check_repo_access_with_org_lookup(
            repo,
            user_id,
            required,
            collab_store.as_ref(),
            auth.org_store.as_deref(),
            auth.org_member_store.as_deref(),
            tenant_id_str,
        )
        .await;
        if let RepoAccessVerdict::Denied { reason } = verdict {
            return forbidden_response(reason);
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

/// Whether a token scoped to repository `scope` may serve a request about
/// `resolved` (the id of the repository the path names, if any). Only an
/// exact match; no repository at all is a refusal. Pure.
pub(crate) fn repo_scope_allows(scope: &str, resolved: Option<&str>) -> bool {
    !scope.is_empty() && resolved == Some(scope)
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
