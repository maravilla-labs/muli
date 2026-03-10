// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo registry protocol handlers.

pub mod api;
pub mod index;
pub mod storage;
pub mod validation;
pub mod wire;

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, put},
};

use crate::storage::FilesystemStorage;

/// Create the cargo registry sub-router.
///
/// Cargo sparse protocol uses two path spaces:
///   /index/...  -- sparse index (config.json + per-crate NDJSON files)
///   /api/v1/... -- web API (publish, download, yank, unyank)
pub fn cargo_router(
    _storage: Arc<FilesystemStorage>,
    base_domain: &str,
) -> Router<Arc<FilesystemStorage>> {
    // Store the base domain so index/config.json can reference it
    let base_domain = base_domain.to_string();

    Router::new()
        // Sparse index endpoints
        .route(
            "/index/config.json",
            get({
                let domain = base_domain.clone();
                move |state, tenant, headers| index::config_json(state, tenant, headers, domain.clone())
            }),
        )
        // Index lookup for 1-char crate names
        .route("/index/1/{name}", get(index::lookup_crate))
        // Index lookup for 2-char crate names
        .route("/index/2/{name}", get(index::lookup_crate))
        // Index lookup for 3-char crate names
        .route("/index/3/{first}/{name}", get(index::lookup_crate_3))
        // Index lookup for 4+ char crate names
        .route(
            "/index/{first}/{second}/{name}",
            get(index::lookup_crate_4plus),
        )
        // Web API endpoints
        .route("/api/v1/crates", get(api::list_crates_api))
        .route("/api/v1/crates/new", put(api::publish))
        .route(
            "/api/v1/crates/{name}/{version}/download",
            get(api::download),
        )
        .route(
            "/api/v1/crates/{name}/{version}/yank",
            axum::routing::delete(api::yank),
        )
        .route(
            "/api/v1/crates/{name}/{version}/unyank",
            put(api::unyank),
        )
}
