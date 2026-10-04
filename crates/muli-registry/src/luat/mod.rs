// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat package registry (server side of the Luat registry HTTP API).
//!
//! The contract is `luat/docs/packages.md`. Routes are served at the root of
//! a tenant's registry host, the same tenant-by-Host mechanism the other
//! formats use (`{tenant}.{MULI_REGISTRY_DOMAIN}`), so the public registry
//! `luat.registry.maravilla.cloud` is simply the tenant `luat`:
//!
//! ```text
//! GET    /index/@{scope}/{name}                              NDJSON index (ETag)
//! GET    /api/v1/packages?q=&page=&per_page=                 search
//! GET    /api/v1/packages/@{scope}/{name}                    package page data
//! GET    /api/v1/packages/@{scope}/{name}/{version}          one version's page data
//! PUT    /api/v1/packages/@{scope}/{name}/{version}          publish (tarball body)
//! GET    /api/v1/packages/@{scope}/{name}/{version}/download tarball
//! DELETE /api/v1/packages/@{scope}/{name}/{version}/yank
//! PUT    /api/v1/packages/@{scope}/{name}/{version}/unyank
//! GET    /api/v1/me                                          token's user + scopes
//! ```
//!
//! Reads follow the tenant's registry visibility (anonymous on a `public`
//! tenant). Writes need a `Push` registry token bound to a tenant user who
//! owns the scope: the scope is the user's own handle or an org they belong
//! to (see [`identity`]). These paths do not overlap cargo's
//! (`/index/config.json`, `/index/{prefix}/.../{crate}`, `/api/v1/crates/...`)
//! because a cargo index prefix can never start with `@`.

pub mod api;
pub mod catalog;
pub mod cors;
pub mod identity;
pub mod manifest;
pub mod publish;
pub mod storage;
pub mod tarball;
pub mod validation;

use std::sync::Arc;

use axum::{
    Extension, Router,
    extract::DefaultBodyLimit,
    routing::{delete, get, put},
};

use muli_core::traits::{OrgMemberStore, OrgStore, UserStore};

use crate::storage::FilesystemStorage;

/// The identity stores Luat needs to map a token's user to publishable scopes.
#[derive(Clone)]
pub struct LuatConfig {
    pub user_store: Arc<dyn UserStore>,
    pub org_store: Arc<dyn OrgStore>,
    pub org_member_store: Arc<dyn OrgMemberStore>,
}

impl std::fmt::Debug for LuatConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LuatConfig").finish_non_exhaustive()
    }
}

/// Whether a request path belongs to the Luat API (used by the shared auth
/// middleware for permission mapping and error shape).
pub fn is_luat_path(path: &str) -> bool {
    cors::is_read_path(path) || is_identity_path(path)
}

/// `GET /api/v1/me` must see the token even on a public registry.
pub fn is_identity_path(path: &str) -> bool {
    path == "/api/v1/me"
}

/// Create the Luat sub-router.
pub fn luat_router(config: LuatConfig) -> Router<Arc<FilesystemStorage>> {
    Router::new()
        .route("/index/@{scope}/{name}", get(api::index))
        .route("/api/v1/packages", get(api::search))
        .route("/api/v1/packages/@{scope}/{name}", get(api::package_info))
        .route(
            "/api/v1/packages/@{scope}/{name}/{version}",
            // The publish handler reads the body itself with the 10 MiB cap
            // so an oversized tarball gets a JSON 413, not the generic one.
            get(api::version_info)
                .put(publish::publish)
                .layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/api/v1/packages/@{scope}/{name}/{version}/download",
            get(api::download),
        )
        .route(
            "/api/v1/packages/@{scope}/{name}/{version}/yank",
            delete(publish::yank),
        )
        .route(
            "/api/v1/packages/@{scope}/{name}/{version}/unyank",
            put(publish::unyank),
        )
        .route("/api/v1/me", get(api::me))
        .layer(Extension(config))
}
