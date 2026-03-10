// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm registry protocol handlers.

pub mod api;
pub mod download;
pub mod packument;
pub mod storage;
pub mod validation;

use std::sync::Arc;

use axum::{Router, routing::get};

use crate::storage::FilesystemStorage;

/// Create the npm registry sub-router.
/// Routes:
///   GET /-/ping — health check
///   GET /-/whoami — authenticated user info
///   GET /-/v1/search — search packages
///   GET /-/npm/{package} — get package metadata (packument)
///   GET /-/npm/@{scope}/{name} — get scoped package metadata
///   GET /-/npm/{package}/-/{filename} — download tarball
///   GET /-/npm/@{scope}/{name}/-/{filename} — download scoped tarball
///   PUT /-/npm/{package} — publish package
///   PUT /-/npm/@{scope}/{name} — publish scoped package
///
/// NOTE: Scoped packages use separate route handlers for @{scope}/{name} pattern.
/// The `/-/npm/` prefix avoids conflicts with OCI routes.
pub fn npm_router(_storage: Arc<FilesystemStorage>) -> Router<Arc<FilesystemStorage>> {
    Router::new()
        .route("/-/ping", get(api::ping))
        .route("/-/whoami", get(api::whoami))
        .route("/-/v1/search", get(api::search))
        // Package metadata and publish (unscoped)
        .route(
            "/-/npm/{package}",
            get(api::get_packument).put(api::publish),
        )
        // Package metadata and publish (scoped)
        .route(
            "/-/npm/@{scope}/{name}",
            get(api::get_scoped_packument).put(api::publish_scoped),
        )
        // Tarball download (unscoped)
        .route(
            "/-/npm/{package}/-/{filename}",
            get(api::download_tarball),
        )
        // Tarball download (scoped)
        .route(
            "/-/npm/@{scope}/{name}/-/{filename}",
            get(api::download_scoped_tarball),
        )
}
