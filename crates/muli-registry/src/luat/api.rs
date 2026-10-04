// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat read endpoints: index, download, package/version info, search, me.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use sha2::Digest;
use tracing::warn;

use crate::auth::AuthenticatedToken;
use crate::common::error_json;
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::storage::{self as luat_storage, IndexEntry, VersionMeta};
use super::{LuatConfig, catalog, identity, validation};

pub(crate) fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(crate) fn internal(e: impl std::fmt::Display, what: &str) -> Response {
    warn!(error = %e, "luat: {what}");
    error_json(StatusCode::INTERNAL_SERVER_ERROR, what)
}

/// Load a package's index entries; 400 for a bad name, 404 when absent.
async fn load_entries(
    storage: &FilesystemStorage,
    tenant: &TenantContext,
    scope: &str,
    name: &str,
) -> Result<(String, Vec<IndexEntry>), Response> {
    validation::validate_parts(scope, name)
        .map_err(|e| error_json(StatusCode::BAD_REQUEST, &e.to_string()))?;
    match luat_storage::read_index(storage, &tenant.tenant_id, scope, name).await {
        Ok(Some(content)) => {
            let entries = luat_storage::parse_index(&content);
            Ok((content, entries))
        }
        Ok(None) => Err(error_json(StatusCode::NOT_FOUND, "package not found")),
        Err(e) => Err(internal(e, "failed to read package index")),
    }
}

fn if_none_match_hits(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim())
        .any(|t| t == "*" || t.strip_prefix("W/").unwrap_or(t) == etag)
}

/// GET /index/@{scope}/{name}
pub async fn index(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((scope, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let (content, _) = match load_entries(&storage, &tenant, &scope, &name).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let etag = format!(
        "\"{}\"",
        hex::encode(sha2::Sha256::digest(content.as_bytes()))
    );
    let cache = [
        (header::ETAG, etag.clone()),
        (
            header::CACHE_CONTROL,
            "max-age=0, must-revalidate".to_string(),
        ),
    ];
    if if_none_match_hits(&headers, &etag) {
        return (StatusCode::NOT_MODIFIED, cache).into_response();
    }
    let mut resp = (StatusCode::OK, cache, content).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    resp
}

/// GET /api/v1/packages/@{scope}/{name}/{version}/download
pub async fn download(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path((scope, name, version)): Path<(String, String, String)>,
) -> Response {
    let (_, entries) = match load_entries(&storage, &tenant, &scope, &name).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let Some(entry) = luat_storage::find_version(&entries, &version) else {
        return error_json(StatusCode::NOT_FOUND, "version not found");
    };
    match luat_storage::read_tarball(&storage, &tenant.tenant_id, &scope, &name, &entry.vers).await
    {
        Ok(Some(bytes)) => {
            metrics.record_luat_download(&tenant.tenant_id);
            let filename = format!("attachment; filename=\"{name}-{}.tgz\"", entry.vers);
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/gzip".to_string()),
                    (header::CONTENT_DISPOSITION, filename),
                    (header::ETAG, format!("\"{}\"", entry.cksum)),
                ],
                Body::from(bytes),
            )
                .into_response()
        }
        Ok(None) => error_json(StatusCode::NOT_FOUND, "version not found"),
        Err(e) => internal(e, "failed to read tarball"),
    }
}

/// Build the package-page shape for `meta`, with the package's version list.
fn page(
    entries: &[IndexEntry],
    latest: Option<&str>,
    meta: Option<VersionMeta>,
    metas: &[Option<VersionMeta>],
) -> Value {
    let versions: Vec<Value> = entries
        .iter()
        .zip(metas)
        .map(|(e, m)| {
            json!({
                "vers": e.vers,
                "published_at": m.as_ref().map(|m| rfc3339(m.published_at)),
                "yanked": e.yanked,
                "size": m.as_ref().map(|m| m.size),
            })
        })
        .collect();
    let name = entries.first().map(|e| e.name.clone()).unwrap_or_default();
    let m = meta.as_ref();
    json!({
        "name": m.map(|m| m.name.clone()).unwrap_or(name),
        "vers": m.map(|m| m.vers.clone()),
        "description": m.and_then(|m| m.description.clone()),
        "latest": latest,
        "license": m.and_then(|m| m.license.clone()),
        "repository": m.and_then(|m| m.repository.clone()),
        "keywords": m.map(|m| m.keywords.clone()).unwrap_or_default(),
        "readme": m.and_then(|m| m.readme.clone()),
        "versions": versions,
        "dependencies": m.map(|m| m.dependencies.clone()).unwrap_or_default(),
        "files": m.map(|m| m.files.clone()).unwrap_or_default(),
    })
}

async fn page_response(
    storage: &FilesystemStorage,
    tenant: &TenantContext,
    scope: &str,
    name: &str,
    version: Option<&str>,
) -> Response {
    let (_, entries) = match load_entries(storage, tenant, scope, name).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let latest = luat_storage::latest(&entries).map(|e| e.vers.clone());
    let shown = match version {
        Some(v) => match luat_storage::find_version(&entries, v) {
            Some(e) => Some(e.vers.clone()),
            None => return error_json(StatusCode::NOT_FOUND, "version not found"),
        },
        None => latest.clone(),
    };
    let mut metas = Vec::with_capacity(entries.len());
    for e in &entries {
        match luat_storage::read_version_meta(storage, &tenant.tenant_id, scope, name, &e.vers)
            .await
        {
            Ok(m) => metas.push(m),
            Err(e) => return internal(e, "failed to read version data"),
        }
    }
    let meta = shown.and_then(|v| {
        entries
            .iter()
            .position(|e| e.vers == v)
            .and_then(|i| metas[i].clone())
    });
    Json(page(&entries, latest.as_deref(), meta, &metas)).into_response()
}

/// GET /api/v1/packages/@{scope}/{name}
pub async fn package_info(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((scope, name)): Path<(String, String)>,
) -> Response {
    page_response(&storage, &tenant, &scope, &name, None).await
}

/// GET /api/v1/packages/@{scope}/{name}/{version}
pub async fn version_info(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path((scope, name, version)): Path<(String, String, String)>,
) -> Response {
    page_response(&storage, &tenant, &scope, &name, Some(&version)).await
}

/// GET /api/v1/packages?q=&page=&per_page=
pub async fn search(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let number = |key: &str, default: usize| -> Result<usize, Response> {
        match params.get(key).map(|v| v.trim()).filter(|v| !v.is_empty()) {
            None => Ok(default),
            Some(v) => v.parse().map_err(|_| {
                error_json(
                    StatusCode::BAD_REQUEST,
                    &format!("`{key}` must be a positive integer"),
                )
            }),
        }
    };
    let (page, per_page) = match (
        number("page", 1),
        number("per_page", catalog::DEFAULT_PER_PAGE),
    ) {
        (Ok(p), Ok(pp)) => (p, pp),
        (Err(resp), _) | (_, Err(resp)) => return resp,
    };
    let all = match catalog::list_summaries(&storage, &tenant.tenant_id).await {
        Ok(all) => all,
        Err(e) => return internal(e, "failed to list packages"),
    };
    let (items, total) = catalog::search(all, params.get("q").map(String::as_str), page, per_page);
    let packages: Vec<Value> = items
        .into_iter()
        .map(|s| {
            json!({
                "name": s.name,
                "description": s.description,
                "latest": s.latest,
                "updated_at": rfc3339(s.updated_at),
            })
        })
        .collect();
    Json(json!({ "packages": packages, "total": total })).into_response()
}

/// GET /api/v1/me
pub async fn me(
    Extension(tenant): Extension<TenantContext>,
    Extension(config): Extension<LuatConfig>,
    token: Option<Extension<AuthenticatedToken>>,
) -> Response {
    let token = token.map(|Extension(t)| t);
    let user = match identity::resolve_user(&config, &tenant.tenant_id, token.as_ref()).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    match identity::publishable_scopes(&config, &tenant.tenant_id, &user).await {
        Ok(scopes) => Json(json!({ "user": user.handle, "scopes": scopes })).into_response(),
        Err(resp) => resp,
    }
}
