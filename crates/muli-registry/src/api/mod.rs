// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI Distribution API router and handlers.

pub mod blobs;
pub mod blobs_download;
pub mod catalog;
pub mod manifests;
pub mod tags;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, head, patch, post},
};

use muli_core::traits::TenantQuotaStore;

use tower_http::trace::TraceLayer;

use crate::auth::RegistryAuth;
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantConfig;

/// Configuration for which registry protocols are enabled.
#[derive(Clone, Debug, Default)]
pub struct RegistryConfig {
    pub npm_enabled: bool,
    pub cargo_enabled: bool,
    pub maven_enabled: bool,
}

/// Create the registry router with OCI and optionally npm/cargo sub-routers.
pub fn registry_router(
    storage: Arc<FilesystemStorage>,
    auth: Option<RegistryAuth>,
    tenant_config: TenantConfig,
    quota_store: Option<Arc<dyn TenantQuotaStore>>,
    config: RegistryConfig,
) -> Router {
    let metrics = RegistryMetrics::new();

    // Protected routes (all except version check)
    let mut protected = Router::new()
        .route("/v2/_catalog", get(catalog::catalog))
        .route("/v2/{name}/tags/list", get(tags::list_tags))
        .route(
            "/v2/{name}/manifests/{reference}",
            head(manifests::head_manifest)
                .get(manifests::get_manifest)
                .put(manifests::put_manifest)
                .delete(manifests::delete_manifest)
                .layer(DefaultBodyLimit::max(10 * 1024 * 1024)), // 10 MB for manifests
        )
        .route(
            "/v2/{name}/blobs/{digest}",
            head(blobs::head_blob)
                .get(blobs::get_blob)
                .delete(blobs::delete_blob),
        )
        .route(
            "/v2/{name}/blobs/uploads/",
            post(blobs::start_upload).layer(DefaultBodyLimit::max(1024 * 1024 * 1024)), // 1 GB for blob uploads
        )
        .route(
            "/v2/{name}/blobs/uploads/{id}",
            patch(blobs::patch_upload)
                .put(blobs::complete_upload)
                .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)), // 1 GB for blob uploads
        );

    // Mount npm sub-router if enabled
    if config.npm_enabled {
        let npm_router = crate::npm::npm_router(storage.clone());
        protected = protected.merge(npm_router);
    }

    // Mount cargo sub-router if enabled
    if config.cargo_enabled {
        let cargo_router = crate::cargo::cargo_router(storage.clone(), &tenant_config.base_domain);
        protected = protected.merge(cargo_router);
    }

    // Mount maven sub-router if enabled
    if config.maven_enabled {
        let maven_router = crate::maven::maven_router(storage.clone());
        protected = protected.merge(maven_router);
    }

    let mut app = protected
        .with_state(storage)
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024)) // 10 MB default for metadata endpoints
        .layer(axum::Extension(metrics));

    if let Some(qs) = quota_store {
        app = app.layer(axum::Extension(qs));
    }

    if let Some(auth) = auth {
        // Extension must be outer so RegistryAuth is in request extensions
        // when auth_middleware runs.
        app = app
            .layer(axum::middleware::from_fn(crate::auth::auth_middleware))
            .layer(axum::Extension(auth));
    }

    // Tenant extraction: TenantConfig extension must be added before the middleware
    // so that the middleware can read the config from request extensions.
    app = app
        .layer(axum::middleware::from_fn(crate::tenant::tenant_middleware))
        .layer(axum::Extension(tenant_config));

    // Request-level tracing for all incoming requests
    app = app.layer(TraceLayer::new_for_http());

    // Version check endpoint is always unauthenticated (OCI spec)
    Router::new().route("/v2/", get(version_check)).merge(app)
}

/// GET /v2/ - API version check
async fn version_check() -> Response {
    (
        StatusCode::OK,
        [("Docker-Distribution-API-Version", "registry/2.0")],
    )
        .into_response()
}

/// Helper to build OCI-compliant error responses
pub fn oci_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = serde_json::json!({
        "errors": [{
            "code": code,
            "message": message,
            "detail": null
        }]
    });
    (status, Json(body)).into_response()
}

/// Validate a repository name, returning an OCI error response on failure.
// axum `Response` is intentionally large; boxing the rare error path is not worth it.
#[allow(clippy::result_large_err)]
pub fn validate_name(name: &str) -> Result<(), Response> {
    crate::validation::validate_repository_name(name).map_err(|_| {
        oci_error(
            StatusCode::BAD_REQUEST,
            "NAME_INVALID",
            "invalid repository name",
        )
    })
}

/// Validate a tag or digest reference, returning an OCI error response on failure.
#[allow(clippy::result_large_err)]
pub fn validate_ref(reference: &str) -> Result<(), Response> {
    crate::validation::validate_reference(reference)
        .map_err(|_| oci_error(StatusCode::BAD_REQUEST, "TAG_INVALID", "invalid reference"))
}

/// Validate a digest, returning an OCI error response on failure.
#[allow(clippy::result_large_err)]
pub fn validate_digest_param(digest: &str) -> Result<(), Response> {
    crate::validation::validate_digest(digest).map_err(|_| {
        oci_error(
            StatusCode::BAD_REQUEST,
            "DIGEST_INVALID",
            "invalid digest format",
        )
    })
}
