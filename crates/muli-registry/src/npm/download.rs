// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm tarball download handler.

use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use crate::common::error_json;
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::storage as npm_storage;

/// GET /-/npm/{package} -- get packument for unscoped package
pub async fn get_packument(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    headers: HeaderMap,
    Path(package): Path<String>,
) -> Response {
    serve_packument(&storage, &tenant, &metrics, &headers, &package).await
}

/// GET /-/npm/@{scope}/{name} -- get packument for scoped package
pub async fn get_scoped_packument(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    headers: HeaderMap,
    Path((scope, name)): Path<(String, String)>,
) -> Response {
    let package = format!("@{scope}/{name}");
    serve_packument(&storage, &tenant, &metrics, &headers, &package).await
}

async fn serve_packument(
    storage: &FilesystemStorage,
    tenant: &TenantContext,
    metrics: &RegistryMetrics,
    headers: &HeaderMap,
    package: &str,
) -> Response {
    metrics.record_npm_download(&tenant.tenant_id);

    match npm_storage::read_packument(storage, &tenant.tenant_id, package).await {
        Some(packument) => {
            let host = headers
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("localhost");
            let scheme = if host.starts_with("localhost") || host.starts_with("127.0.0.1") {
                "http"
            } else {
                "https"
            };
            let base = format!("{scheme}://{host}");
            let packument = packument.with_absolute_tarball_urls(&base);

            let accept = headers
                .get("accept")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            if accept.contains("application/vnd.npm.install-v1+json") {
                let abbreviated = packument.to_abbreviated();
                (
                    StatusCode::OK,
                    [("Content-Type", "application/vnd.npm.install-v1+json")],
                    Json(abbreviated),
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    [("Content-Type", "application/json")],
                    Json(packument),
                )
                    .into_response()
            }
        }
        None => error_json(StatusCode::NOT_FOUND, "package not found"),
    }
}

/// GET /-/npm/{package}/-/{filename} -- download tarball for unscoped package
pub async fn download_tarball(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((package, filename)): Path<(String, String)>,
) -> Response {
    serve_tarball(&storage, &tenant, &metrics, &package, &filename).await
}

/// GET /-/npm/@{scope}/{name}/-/{filename} -- download tarball for scoped package
pub async fn download_scoped_tarball(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((scope, name, filename)): Path<(String, String, String)>,
) -> Response {
    let package = format!("@{scope}/{name}");
    serve_tarball(&storage, &tenant, &metrics, &package, &filename).await
}

async fn serve_tarball(
    storage: &FilesystemStorage,
    tenant: &TenantContext,
    metrics: &RegistryMetrics,
    package: &str,
    filename: &str,
) -> Response {
    metrics.record_npm_download(&tenant.tenant_id);

    match npm_storage::read_tarball(storage, &tenant.tenant_id, package, filename).await {
        Ok(data) => {
            let body = Body::from(data);
            (
                StatusCode::OK,
                [("Content-Type", "application/octet-stream")],
                body,
            )
                .into_response()
        }
        Err(_) => error_json(StatusCode::NOT_FOUND, "tarball not found"),
    }
}

#[derive(serde::Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    pub text: String,
    #[serde(default = "default_size")]
    pub size: usize,
}

fn default_size() -> usize {
    20
}

/// GET /-/v1/search
pub async fn search(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let packages = npm_storage::list_packages(&storage, &tenant.tenant_id)
        .await
        .unwrap_or_default();

    let text = query.text.to_lowercase();
    let mut results: Vec<serde_json::Value> = Vec::new();
    for name in packages
        .into_iter()
        .filter(|name| text.is_empty() || name.to_lowercase().contains(&text))
        .take(query.size)
    {
        // Surface the real latest version + description from the packument
        // (`dist-tags.latest`, falling back to the highest version key) rather
        // than a placeholder, so search/listing UIs show the actual version.
        let (version, description) =
            match npm_storage::read_packument(&storage, &tenant.tenant_id, &name).await {
                Some(p) => {
                    let v = p
                        .dist_tags
                        .get("latest")
                        .cloned()
                        .or_else(|| p.versions.keys().max().cloned())
                        .unwrap_or_else(|| "0.0.0".to_string());
                    (v, p.description.unwrap_or_default())
                }
                None => ("0.0.0".to_string(), String::new()),
            };
        results.push(serde_json::json!({
            "package": {
                "name": name,
                "version": version,
                "description": description,
            }
        }));
    }

    let body = serde_json::json!({
        "objects": results,
        "total": results.len(),
    });
    (StatusCode::OK, Json(body)).into_response()
}
