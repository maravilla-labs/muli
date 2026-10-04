// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat write endpoints: publish, yank, unyank.
//!
//! The auth middleware has already verified the token (401) and its `Push`
//! permission for this tenant; these handlers add the per-scope check (403).

use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde_json::json;
use sha2::Digest;

use muli_core::traits::TenantQuotaStore;

use crate::auth::AuthenticatedToken;
use crate::common::{adjust_quota_usage, error_json, reserve_quota};
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::api::internal;
use super::storage::{self as luat_storage, IndexEntry, VersionMeta};
use super::tarball::{self, MAX_COMPRESSED_BYTES, TarballError};
use super::{LuatConfig, identity, manifest, validation};

type QuotaStore = Option<Extension<Arc<dyn TenantQuotaStore>>>;

/// Validate the URL and check the caller may write to the scope.
async fn authorize(
    config: &LuatConfig,
    tenant: &TenantContext,
    token: Option<Extension<AuthenticatedToken>>,
    scope: &str,
    name: &str,
    version: &str,
) -> Result<(), Response> {
    let bad =
        |e: validation::LuatValidationError| error_json(StatusCode::BAD_REQUEST, &e.to_string());
    validation::validate_parts(scope, name).map_err(bad)?;
    validation::parse_version(version).map_err(bad)?;
    let token = token.map(|Extension(t)| t);
    let user = identity::resolve_user(config, &tenant.tenant_id, token.as_ref()).await?;
    if identity::may_publish(config, &tenant.tenant_id, &user, scope).await? {
        Ok(())
    } else {
        Err(error_json(
            StatusCode::FORBIDDEN,
            &format!("`{}` may not publish to scope @{scope}", user.handle),
        ))
    }
}

async fn version_exists(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
    version: &str,
) -> Result<bool, Response> {
    match luat_storage::read_index(storage, tenant_id, scope, name).await {
        Ok(Some(content)) => {
            Ok(luat_storage::find_version(&luat_storage::parse_index(&content), version).is_some())
        }
        Ok(None) => Ok(false),
        Err(e) => Err(internal(e, "failed to read package index")),
    }
}

fn conflict(scope: &str, name: &str, version: &str) -> Response {
    error_json(
        StatusCode::CONFLICT,
        &format!("@{scope}/{name}@{version} already exists"),
    )
}

fn too_large() -> Response {
    error_json(
        StatusCode::PAYLOAD_TOO_LARGE,
        &format!("tarball is larger than {MAX_COMPRESSED_BYTES} bytes compressed"),
    )
}

/// PUT /api/v1/packages/@{scope}/{name}/{version}
#[allow(clippy::too_many_arguments)]
pub async fn publish(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Extension(config): Extension<LuatConfig>,
    token: Option<Extension<AuthenticatedToken>>,
    quota_store: QuotaStore,
    Path((scope, name, version)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Err(resp) = authorize(&config, &tenant, token, &scope, &name, &version).await {
        return resp;
    }
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|len| len > MAX_COMPRESSED_BYTES as u64) {
        return too_large();
    }
    match version_exists(&storage, &tenant.tenant_id, &scope, &name, &version).await {
        Ok(true) => return conflict(&scope, &name, &version),
        Ok(false) => {}
        Err(resp) => return resp,
    }
    // The body is read with a hard cap: a missing or lying Content-Length
    // cannot make the server buffer more than the limit.
    let bytes = match axum::body::to_bytes(body, MAX_COMPRESSED_BYTES).await {
        Ok(b) => b,
        Err(_) => return too_large(),
    };

    let inspected = {
        let bytes = bytes.clone();
        match tokio::task::spawn_blocking(move || tarball::inspect(&bytes)).await {
            Ok(Ok(i)) => i,
            Ok(Err(TarballError::Invalid(msg))) => {
                return error_json(StatusCode::BAD_REQUEST, &msg);
            }
            Ok(Err(TarballError::TooLarge(msg))) => {
                return error_json(StatusCode::PAYLOAD_TOO_LARGE, &msg);
            }
            Err(e) => return internal(e, "tarball inspection failed"),
        }
    };
    let manifest = match manifest::parse(&inspected.manifest_source) {
        Ok(m) => m,
        Err(msg) => return error_json(StatusCode::BAD_REQUEST, &msg),
    };
    let full_name = format!("@{scope}/{name}");
    if manifest.name != full_name || manifest.version != version {
        return error_json(
            StatusCode::BAD_REQUEST,
            &format!(
                "luat.toml declares {}@{} but the URL publishes {full_name}@{version}",
                manifest.name, manifest.version
            ),
        );
    }

    let cksum = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));
    let size = bytes.len() as u64;
    let meta = VersionMeta {
        name: full_name.clone(),
        vers: version.clone(),
        description: manifest.description,
        license: manifest.license,
        repository: manifest.repository,
        keywords: manifest.keywords,
        readme: inspected.readme,
        dependencies: manifest.dependencies.clone(),
        files: inspected.files,
        luat: manifest.luat.clone(),
        cksum: cksum.clone(),
        size,
        published_at: Utc::now(),
    };
    let entry = IndexEntry {
        name: full_name.clone(),
        vers: version.clone(),
        deps: manifest.dependencies,
        cksum: cksum.clone(),
        luat: manifest.luat,
        yanked: false,
    };

    let _guard = luat_storage::write_lock().await;
    // Re-check under the lock: a concurrent publish may have won the race.
    match version_exists(&storage, &tenant.tenant_id, &scope, &name, &version).await {
        Ok(true) => return conflict(&scope, &name, &version),
        Ok(false) => {}
        Err(resp) => return resp,
    }
    if let Err(resp) = reserve_quota(&quota_store, &tenant.tenant_id, size).await {
        return resp;
    }
    let stored = luat_storage::store_version(
        &storage,
        &tenant.tenant_id,
        &scope,
        &name,
        bytes.to_vec(),
        &meta,
        entry,
    )
    .await;
    if let Err(e) = stored {
        adjust_quota_usage(&quota_store, &tenant.tenant_id, -(size as i64));
        return internal(e, "failed to store package");
    }
    metrics.record_luat_publish(&tenant.tenant_id);
    (
        StatusCode::CREATED,
        Json(json!({ "name": full_name, "vers": version, "cksum": cksum })),
    )
        .into_response()
}

async fn set_yanked(
    storage: &FilesystemStorage,
    config: &LuatConfig,
    tenant: &TenantContext,
    token: Option<Extension<AuthenticatedToken>>,
    (scope, name, version): (String, String, String),
    yanked: bool,
) -> Response {
    if let Err(resp) = authorize(config, tenant, token, &scope, &name, &version).await {
        return resp;
    }
    let _guard = luat_storage::write_lock().await;
    match luat_storage::set_yanked(storage, &tenant.tenant_id, &scope, &name, &version, yanked)
        .await
    {
        Ok(true) => Json(json!({
            "name": format!("@{scope}/{name}"),
            "vers": version,
            "yanked": yanked,
        }))
        .into_response(),
        Ok(false) => error_json(StatusCode::NOT_FOUND, "version not found"),
        Err(e) => internal(e, "failed to update package index"),
    }
}

/// DELETE /api/v1/packages/@{scope}/{name}/{version}/yank
pub async fn yank(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(config): Extension<LuatConfig>,
    token: Option<Extension<AuthenticatedToken>>,
    Path(path): Path<(String, String, String)>,
) -> Response {
    set_yanked(&storage, &config, &tenant, token, path, true).await
}

/// PUT /api/v1/packages/@{scope}/{name}/{version}/unyank
pub async fn unyank(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(config): Extension<LuatConfig>,
    token: Option<Extension<AuthenticatedToken>>,
    Path(path): Path<(String, String, String)>,
) -> Response {
    set_yanked(&storage, &config, &tenant, token, path, false).await
}
