// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo sparse registry index.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use sha2::Digest;

use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::storage as cargo_storage;

/// GET /index/config.json -- sparse registry configuration
pub async fn config_json(
    State(_storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    base_domain: String,
) -> Response {
    // Use the request Host header so URLs work for localhost (tests) and production alike
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(|h| h.to_string())
        .unwrap_or_else(|| format!("{}.{}", tenant.tenant_id, base_domain));

    // Use http for localhost/127.0.0.1 (dev/test), https otherwise
    let scheme = if host.starts_with("localhost") || host.starts_with("127.0.0.1") {
        "http"
    } else {
        "https"
    };

    let config = serde_json::json!({
        "dl": format!("{}://{}/api/v1/crates", scheme, host),
        "api": format!("{}://{}", scheme, host),
        "auth-required": true,
    });
    (StatusCode::OK, Json(config)).into_response()
}

/// GET /index/1/{name} or /index/2/{name}
/// Lookup a crate in the sparse index (1 or 2 char names).
pub async fn lookup_crate(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path(name): Path<String>,
) -> Response {
    serve_index(&storage, &tenant, &name).await
}

/// GET /index/3/{first}/{name}
/// Lookup a crate in the sparse index (3 char names).
pub async fn lookup_crate_3(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((_first, name)): Path<(String, String)>,
) -> Response {
    serve_index(&storage, &tenant, &name).await
}

/// GET /index/{first}/{second}/{name}
/// Lookup a crate in the sparse index (4+ char names).
pub async fn lookup_crate_4plus(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((_first, _second, name)): Path<(String, String, String)>,
) -> Response {
    serve_index(&storage, &tenant, &name).await
}

/// Serve the index file for a crate with proper HTTP caching headers.
async fn serve_index(storage: &FilesystemStorage, tenant: &TenantContext, name: &str) -> Response {
    if let Err(e) = super::validation::validate_crate_name(name) {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    match cargo_storage::read_index(storage, &tenant.tenant_id, name).await {
        Ok(content) => {
            // Compute ETag from content hash
            let etag = format!(
                "\"{}\"",
                hex::encode(sha2::Sha256::digest(content.as_bytes()))
            );
            (
                StatusCode::OK,
                [
                    ("Content-Type", "text/plain"),
                    ("ETag", &etag),
                    ("Cache-Control", "max-age=0, must-revalidate"),
                ],
                content,
            )
                .into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
