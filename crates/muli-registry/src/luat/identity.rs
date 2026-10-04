// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Who a Luat token acts for, and which scopes it may publish to.
//!
//! A scope is a muli namespace of the tenant: the user's own handle, or the
//! handle of an organization the user belongs to with a role that may write
//! (owner, admin or member — viewers may not publish). Registry tokens carry
//! the user through `RegistryToken::user_id`; a token not bound to a user can
//! read but never publish, yank or answer `me`.

use axum::{http::StatusCode, response::Response};
use tracing::warn;

use muli_core::org::OrgRole;
use muli_core::user::TenantUser;

use crate::auth::AuthenticatedToken;
use crate::common::error_json;

use super::LuatConfig;
use super::validation::is_valid_part;

fn store_failure(e: impl std::fmt::Display) -> Response {
    warn!(error = %e, "luat: identity store lookup failed");
    error_json(StatusCode::INTERNAL_SERVER_ERROR, "identity lookup failed")
}

fn can_write(role: OrgRole) -> bool {
    matches!(role, OrgRole::Owner | OrgRole::Admin | OrgRole::Member)
}

/// Resolve the tenant user behind an authenticated request.
///
/// 401 when the request carries no verified token (only possible when the
/// registry runs without auth), 403 when the token is not bound to a user of
/// this tenant.
pub async fn resolve_user(
    config: &LuatConfig,
    tenant_id: &str,
    token: Option<&AuthenticatedToken>,
) -> Result<TenantUser, Response> {
    let Some(AuthenticatedToken(token)) = token else {
        return Err(error_json(
            StatusCode::UNAUTHORIZED,
            "authentication required",
        ));
    };
    let Some(user_id) = token.user_id.as_deref() else {
        return Err(error_json(
            StatusCode::FORBIDDEN,
            "token is not bound to a user; create a registry token with a user_id",
        ));
    };
    match config.user_store.get_user(user_id).await {
        Ok(Some(user)) if user.tenant_id == tenant_id => Ok(user),
        Ok(_) => Err(error_json(
            StatusCode::FORBIDDEN,
            "the token's user does not exist in this registry",
        )),
        Err(e) => Err(store_failure(e)),
    }
}

/// Whether `user` may publish (and yank) packages in `scope`.
pub async fn may_publish(
    config: &LuatConfig,
    tenant_id: &str,
    user: &TenantUser,
    scope: &str,
) -> Result<bool, Response> {
    // User and org handles are separate namespaces, so a user can carry an
    // org's handle. An org's scope belongs to the org: only its writers may
    // publish there, never a user who merely shares the name. A user's own
    // handle is their scope only while no org holds it.
    let org = match config.org_store.get_org_by_handle(tenant_id, scope).await {
        Ok(Some(org)) => org,
        Ok(None) => return Ok(user.handle == scope),
        Err(e) => return Err(store_failure(e)),
    };
    match config.org_member_store.get_member(&org.id, &user.id).await {
        Ok(member) => Ok(member.is_some_and(|m| can_write(m.role))),
        Err(e) => Err(store_failure(e)),
    }
}

/// Every scope `user` may publish to, sorted.
pub async fn publishable_scopes(
    config: &LuatConfig,
    tenant_id: &str,
    user: &TenantUser,
) -> Result<Vec<String>, Response> {
    let mut scopes = Vec::new();
    let orgs = config
        .org_store
        .list_orgs(tenant_id)
        .await
        .map_err(store_failure)?;
    // The user's own handle, unless an org holds it (see `may_publish`).
    if is_valid_part(&user.handle) && !orgs.iter().any(|o| o.handle == user.handle) {
        scopes.push(user.handle.clone());
    }
    for org in orgs {
        if !is_valid_part(&org.handle) || scopes.contains(&org.handle) {
            continue;
        }
        let member = config
            .org_member_store
            .get_member(&org.id, &user.id)
            .await
            .map_err(store_failure)?;
        if member.is_some_and(|m| can_write(m.role)) {
            scopes.push(org.handle);
        }
    }
    scopes.sort();
    Ok(scopes)
}
