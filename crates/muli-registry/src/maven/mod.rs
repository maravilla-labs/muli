// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Maven repository protocol handlers.

pub mod api;
pub mod checksum;
pub mod metadata;
pub mod storage;
pub mod validation;

use std::sync::Arc;

use axum::{Router, routing::get};

use crate::storage::FilesystemStorage;

/// Create the Maven registry sub-router.
///
/// Routes:
///   GET  /-/maven/{*path} — download artifact, metadata, or checksum
///   PUT  /-/maven/{*path} — upload artifact, metadata, or checksum
///   HEAD /-/maven/{*path} — existence check + content-length + checksum headers
///
/// The `{*path}` wildcard captures the full Maven coordinate path, e.g.:
///   `org/apache/commons/commons-lang3/3.14.0/commons-lang3-3.14.0.jar`
pub fn maven_router(_storage: Arc<FilesystemStorage>) -> Router<Arc<FilesystemStorage>> {
    Router::new()
        .route("/-/maven/_list", get(api::list_artifacts_api))
        .route(
            "/-/maven/{*path}",
            get(api::get_artifact)
                .put(api::put_artifact)
                .head(api::head_artifact),
        )
}
