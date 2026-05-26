// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm registry API endpoint handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use base64::Engine;
use sha2::Digest;
use tracing::warn;

use muli_core::traits::TenantQuotaStore;

use crate::common::{adjust_quota_usage, error_json, reserve_quota};
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::packument::{Dist, Packument, PublishRequest, VersionMetadata};
use super::storage as npm_storage;
use super::validation;

// Re-export download handlers so existing route references work.
pub use super::download::{
    download_scoped_tarball, download_tarball, get_packument, get_scoped_packument, search,
};

/// GET /-/ping
pub async fn ping() -> Response {
    (StatusCode::OK, "{}").into_response()
}

/// GET /-/whoami
pub async fn whoami(Extension(tenant): Extension<TenantContext>) -> Response {
    let body = serde_json::json!({ "username": tenant.tenant_id });
    (StatusCode::OK, Json(body)).into_response()
}

/// PUT /-/npm/{package} -- publish unscoped package
pub async fn publish(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path(package): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    handle_publish(&storage, &tenant, &metrics, &quota_store, &package, &body).await
}

/// PUT /-/npm/@{scope}/{name} -- publish scoped package
pub async fn publish_scoped(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path((scope, name)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Response {
    let package = format!("@{scope}/{name}");
    handle_publish(&storage, &tenant, &metrics, &quota_store, &package, &body).await
}

async fn handle_publish(
    storage: &FilesystemStorage,
    tenant: &TenantContext,
    metrics: &RegistryMetrics,
    quota_store: &Option<Extension<Arc<dyn TenantQuotaStore>>>,
    package: &str,
    body: &[u8],
) -> Response {
    // Parse publish request
    let publish_req: PublishRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return error_json(
                StatusCode::BAD_REQUEST,
                &format!("invalid publish request: {e}"),
            );
        }
    };

    // Validate package name matches URL
    if publish_req.name != package {
        return error_json(
            StatusCode::BAD_REQUEST,
            "package name in body does not match URL",
        );
    }

    if let Err(e) = validation::validate_package_name(package) {
        return error_json(StatusCode::BAD_REQUEST, &e.to_string());
    }

    // Process each attachment (tarball) with per-tarball quota reservation
    for (filename, attachment) in &publish_req.attachments {
        // npm/pnpm key scoped attachments by the full package name, e.g.
        // "@solutas/shared-protocol-0.0.1.tgz". Strip the scope so the stored
        // filename is the unscoped basename ("shared-protocol-0.0.1.tgz") that
        // the read step and tarball URL below both expect — and so it passes
        // the path-component validation in store_tarball (which rejects '/').
        let stored_filename = filename.rsplit('/').next().unwrap_or(filename);

        let tarball_data = match base64::engine::general_purpose::STANDARD.decode(&attachment.data)
        {
            Ok(d) => d,
            Err(_) => {
                return error_json(StatusCode::BAD_REQUEST, "invalid base64 in attachment");
            }
        };

        // Atomic quota reservation per tarball
        if let Err(e) =
            reserve_quota(quota_store, &tenant.tenant_id, tarball_data.len() as u64).await
        {
            return e;
        }

        // Store tarball
        if let Err(e) = npm_storage::store_tarball(
            storage,
            &tenant.tenant_id,
            package,
            stored_filename,
            &tarball_data,
        )
        .await
        {
            // Release reserved bytes on failure
            adjust_quota_usage(quota_store, &tenant.tenant_id, -(tarball_data.len() as i64));
            warn!(error = %e, "failed to store npm tarball");
            return error_json(StatusCode::INTERNAL_SERVER_ERROR, "failed to store tarball");
        }
    }

    // Build or update packument
    let now = chrono::Utc::now().to_rfc3339();
    let mut packument = npm_storage::read_packument(storage, &tenant.tenant_id, package)
        .await
        .unwrap_or_else(|| Packument {
            name: package.to_string(),
            description: publish_req.description.clone(),
            dist_tags: HashMap::new(),
            versions: HashMap::new(),
            time: {
                let mut t = HashMap::new();
                t.insert("created".to_string(), now.clone());
                t
            },
            rev: "1-0".to_string(),
        });

    // Reject duplicate versions (mirrors npmjs.com behavior)
    for version_str in publish_req.versions.keys() {
        if packument.versions.contains_key(version_str) {
            return error_json(
                StatusCode::CONFLICT,
                &format!("package `{package}@{version_str}` already exists"),
            );
        }
    }

    // Add each version
    for (version_str, version_value) in &publish_req.versions {
        if let Err(e) = validation::validate_version(version_str) {
            return error_json(StatusCode::BAD_REQUEST, &e.to_string());
        }

        // Build the expected tarball filename
        let tarball_filename = format!(
            "{}-{}.tgz",
            package.split('/').next_back().unwrap_or(package),
            version_str
        );

        // Look up the tarball to compute checksums
        let tarball_data =
            match npm_storage::read_tarball(storage, &tenant.tenant_id, package, &tarball_filename)
                .await
            {
                Ok(d) => d,
                Err(_) => {
                    // Try finding it in attachments by any matching key
                    let mut found = None;
                    for (fname, att) in &publish_req.attachments {
                        if fname.contains(version_str) {
                            found = base64::engine::general_purpose::STANDARD
                                .decode(&att.data)
                                .ok();
                            break;
                        }
                    }
                    match found {
                        Some(d) => d,
                        None => {
                            return error_json(
                                StatusCode::BAD_REQUEST,
                                &format!("tarball not found for version {version_str}"),
                            );
                        }
                    }
                }
            };

        // Compute checksums
        let shasum = hex::encode(sha1::Sha1::digest(&tarball_data));
        let integrity = {
            let hash = sha2::Sha512::digest(&tarball_data);
            format!(
                "sha512-{}",
                base64::engine::general_purpose::STANDARD.encode(hash)
            )
        };

        // Build tarball URL (resolved relative to the registry URL by the client)
        let tarball_url = format!("/-/npm/{package}/-/{tarball_filename}");

        // Parse the version metadata from the publish request
        let mut version_meta: VersionMetadata = match serde_json::from_value(version_value.clone())
        {
            Ok(m) => m,
            Err(_) => {
                // Build minimal metadata if parsing fails
                VersionMetadata {
                    name: package.to_string(),
                    version: version_str.clone(),
                    description: publish_req.description.clone(),
                    main: None,
                    dependencies: None,
                    dev_dependencies: None,
                    peer_dependencies: None,
                    dist: Dist {
                        tarball: tarball_url.clone(),
                        shasum: shasum.clone(),
                        integrity: Some(integrity.clone()),
                    },
                    extra: HashMap::new(),
                }
            }
        };

        // Override dist with our computed values
        version_meta.dist = Dist {
            tarball: tarball_url,
            shasum,
            integrity: Some(integrity),
        };

        packument.versions.insert(version_str.clone(), version_meta);
        packument.time.insert(version_str.clone(), now.clone());
    }

    // Update dist-tags
    for (tag, version) in &publish_req.dist_tags {
        packument.dist_tags.insert(tag.clone(), version.clone());
    }

    // Update rev and modified time
    let rev_num: u64 = packument
        .rev
        .split('-')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
        + 1;
    packument.rev = format!("{}-{}", rev_num, uuid::Uuid::new_v4());
    packument.time.insert("modified".to_string(), now);

    // Write packument
    if let Err(e) =
        npm_storage::write_packument(storage, &tenant.tenant_id, package, &packument).await
    {
        warn!(error = %e, "failed to write npm packument");
        return error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to store metadata",
        );
    }

    // Quota already reserved per-tarball via reserve_quota above.

    metrics.record_npm_publish(&tenant.tenant_id);

    let body = serde_json::json!({
        "ok": true,
        "success": true,
        "rev": packument.rev,
    });
    (StatusCode::OK, Json(body)).into_response()
}
