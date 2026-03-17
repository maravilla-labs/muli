// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo registry API endpoint handlers.

use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sha2::Digest;
use tracing::warn;

use muli_core::traits::TenantQuotaStore;

use crate::common::{adjust_quota_usage, reserve_quota};
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::storage as cargo_storage;
use super::validation;
use super::wire;

/// PUT /api/v1/crates/new -- publish a crate
pub async fn publish(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    body: axum::body::Bytes,
) -> Response {
    // Parse the binary wire format
    let parsed = match wire::parse_publish_body(&body) {
        Ok(p) => p,
        Err(e) => {
            return cargo_error(StatusCode::BAD_REQUEST, &e);
        }
    };

    let name = &parsed.metadata.name;
    let version = &parsed.metadata.version;

    // Validate crate name and version
    if let Err(e) = validation::validate_crate_name(name) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }
    if let Err(e) = validation::validate_version(version) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }

    // Check if version already exists in the index
    if let Ok(index_content) = cargo_storage::read_index(&storage, &tenant.tenant_id, name).await {
        for line in index_content.lines() {
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line)
                && entry.get("vers").and_then(|v| v.as_str()) == Some(version)
            {
                return cargo_error(
                    StatusCode::CONFLICT,
                    &format!("crate `{name}@{version}` already exists"),
                );
            }
        }
    }

    // Atomic quota reservation
    let crate_size = parsed.crate_data.len() as u64;
    if let Err(e) = reserve_quota(&quota_store, &tenant.tenant_id, crate_size).await {
        return e;
    }

    // Compute checksum
    let cksum = hex::encode(sha2::Sha256::digest(&parsed.crate_data));

    // Store the .crate file
    if let Err(e) = cargo_storage::store_crate(
        &storage,
        &tenant.tenant_id,
        name,
        version,
        &parsed.crate_data,
    )
    .await
    {
        // Release reserved bytes on failure
        adjust_quota_usage(&quota_store, &tenant.tenant_id, -(crate_size as i64));
        warn!(error = %e, "failed to store crate file");
        return cargo_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to store crate");
    }

    // Build index entry (one JSON line)
    let deps: Vec<serde_json::Value> = parsed
        .metadata
        .deps
        .iter()
        .map(|d| {
            serde_json::json!({
                "name": d.name,
                "req": d.req,
                "features": d.features,
                "optional": d.optional,
                "default_features": d.default_features,
                "target": d.target,
                "kind": d.kind,
                "registry": d.registry,
                "package": d.package,
            })
        })
        .collect();

    let index_entry = serde_json::json!({
        "name": name,
        "vers": version,
        "deps": deps,
        "cksum": cksum,
        "features": parsed.metadata.features,
        "yanked": false,
        "links": parsed.metadata.links,
    });

    let entry_line = match serde_json::to_string(&index_entry) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "failed to serialize index entry");
            return cargo_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to serialize index entry",
            );
        }
    };

    // Append to index file
    if let Err(e) =
        cargo_storage::append_index_entry(&storage, &tenant.tenant_id, name, &entry_line).await
    {
        warn!(error = %e, "failed to update crate index");
        return cargo_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update index");
    }

    // Quota already reserved via reserve_quota above.

    metrics.record_cargo_publish(&tenant.tenant_id);

    // Cargo expects a specific JSON response format
    let response = serde_json::json!({
        "warnings": {
            "invalid_categories": [],
            "invalid_badges": [],
            "other": []
        }
    });
    (StatusCode::OK, Json(response)).into_response()
}

/// GET /api/v1/crates/{name}/{version}/download
pub async fn download(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((name, version)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validation::validate_crate_name(&name) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }
    if let Err(e) = validation::validate_version(&version) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }

    metrics.record_cargo_download(&tenant.tenant_id);

    match cargo_storage::read_crate(&storage, &tenant.tenant_id, &name, &version).await {
        Ok(data) => {
            let body = Body::from(data);
            (
                StatusCode::OK,
                [("Content-Type", "application/x-tar")],
                body,
            )
                .into_response()
        }
        Err(_) => cargo_error(StatusCode::NOT_FOUND, "crate not found"),
    }
}

/// DELETE /api/v1/crates/{name}/{version}/yank
pub async fn yank(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((name, version)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validation::validate_crate_name(&name) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }
    if let Err(e) = validation::validate_version(&version) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }

    match cargo_storage::update_index_version(&storage, &tenant.tenant_id, &name, &version, true)
        .await
    {
        Ok(true) => {
            let response = serde_json::json!({ "ok": true });
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(false) => cargo_error(StatusCode::NOT_FOUND, "version not found"),
        Err(e) => {
            warn!(error = %e, "failed to yank crate");
            cargo_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to yank")
        }
    }
}

/// PUT /api/v1/crates/{name}/{version}/unyank
pub async fn unyank(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((name, version)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validation::validate_crate_name(&name) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }
    if let Err(e) = validation::validate_version(&version) {
        return cargo_error(StatusCode::BAD_REQUEST, &e.to_string());
    }

    match cargo_storage::update_index_version(&storage, &tenant.tenant_id, &name, &version, false)
        .await
    {
        Ok(true) => {
            let response = serde_json::json!({ "ok": true });
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(false) => cargo_error(StatusCode::NOT_FOUND, "version not found"),
        Err(e) => {
            warn!(error = %e, "failed to unyank crate");
            cargo_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to unyank")
        }
    }
}

/// GET /api/v1/crates -- list all crates for the tenant
pub async fn list_crates_api(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
) -> Response {
    match cargo_storage::list_crates(&storage, &tenant.tenant_id).await {
        Ok(crates) => {
            let entries: Vec<serde_json::Value> = crates
                .into_iter()
                .map(|(name, versions_count, latest_version)| {
                    serde_json::json!({
                        "name": name,
                        "latest_version": latest_version,
                        "versions_count": versions_count,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({ "crates": entries })),
            )
                .into_response()
        }
        Err(e) => {
            warn!(error = %e, "failed to list crates");
            cargo_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list crates")
        }
    }
}

/// Format error responses in Cargo's expected JSON format.
fn cargo_error(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({
        "errors": [{
            "detail": message
        }]
    });
    (status, Json(body)).into_response()
}
