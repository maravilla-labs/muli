// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Maven repository API handlers (PUT/GET/HEAD).

use std::sync::Arc;
use std::time::SystemTime;

use axum::{
    Extension,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use tracing::{debug, warn};

use muli_core::traits::TenantQuotaStore;

use crate::common::{check_quota, error_json, update_quota_usage};
use crate::metrics::RegistryMetrics;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantContext;

use super::checksum::{self, ChecksumAlgo};
use super::metadata;
use super::storage as maven_storage;
use super::validation::{self, MavenPathKind};

// ── LIST handler ────────────────────────────────────────────────────

/// GET /-/maven/_list — list all Maven artifacts for the tenant.
pub async fn list_artifacts_api(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
) -> Response {
    match maven_storage::list_artifacts(&storage, &tenant.tenant_id).await {
        Ok(artifacts) => {
            let entries: Vec<serde_json::Value> = artifacts
                .into_iter()
                .map(|a| {
                    serde_json::json!({
                        "group_id": a.group_id,
                        "artifact_id": a.artifact_id,
                        "latest_version": a.latest_version,
                        "versions": a.versions,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({ "artifacts": entries })),
            )
                .into_response()
        }
        Err(e) => {
            warn!(error = %e, "failed to list maven artifacts");
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list maven artifacts",
            )
        }
    }
}

// ── PUT handler ─────────────────────────────────────────────────────

/// PUT /-/maven/{*path} — upload artifact, metadata, or checksum.
pub async fn put_artifact(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    quota_store: Option<Extension<Arc<dyn TenantQuotaStore>>>,
    Path(path): Path<String>,
    body: Bytes,
) -> Response {
    let tenant_id = &tenant.tenant_id;

    // Parse and validate the Maven path.
    let (coord, kind) = match validation::parse_maven_path(&path) {
        Ok(v) => v,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    // Check quota before writing.
    if let Err(resp) = check_quota(&quota_store, tenant_id, body.len() as u64).await {
        return resp;
    }

    let file_path = match maven_storage::artifact_path(&storage, tenant_id, &coord) {
        Ok(p) => p,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    match &kind {
        MavenPathKind::Checksum {
            algorithm,
            base_kind,
        } => {
            // Client is uploading a checksum file.
            // Verify that the checksum matches the base artifact if it exists.
            let algo = match ChecksumAlgo::from_extension(algorithm) {
                Some(a) => a,
                None => {
                    return error_json(
                        StatusCode::BAD_REQUEST,
                        &format!("unsupported checksum algorithm: {algorithm}"),
                    );
                }
            };

            // Build path for the base artifact (without checksum extension).
            let base_coord = super::validation::MavenCoordinate {
                group_id: coord.group_id.clone(),
                artifact_id: coord.artifact_id.clone(),
                version: coord.version.clone(),
                filename: coord.filename.as_ref().map(|f| {
                    // Strip the checksum extension from the filename.
                    f.strip_suffix(&format!(".{algorithm}"))
                        .unwrap_or(f)
                        .to_string()
                }),
            };
            let base_path = match maven_storage::artifact_path(&storage, tenant_id, &base_coord) {
                Ok(p) => p,
                Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
            };

            // If the base artifact already exists, verify the checksum matches.
            if let Ok(Some(base_data)) = maven_storage::read_file(&base_path).await {
                let client_checksum = String::from_utf8_lossy(&body);
                if !checksum::verify_checksum(&base_data, &client_checksum, algo) {
                    return error_json(
                        StatusCode::CONFLICT,
                        "checksum mismatch: provided checksum does not match artifact content",
                    );
                }
            }

            // Release immutability: checksums follow the same rule as their base artifact
            if !coord.is_snapshot()
                && matches!(**base_kind, MavenPathKind::Artifact)
                && maven_storage::file_exists(&file_path).await
            {
                return error_json(
                    StatusCode::CONFLICT,
                    "release checksum already exists and cannot be overwritten",
                );
            }

            // Write the checksum file.
            if let Err(e) = maven_storage::atomic_write(&file_path, &body).await {
                warn!(error = %e, "failed to write checksum file");
                return error_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to write checksum",
                );
            }
        }

        MavenPathKind::ArtifactLevelMetadata | MavenPathKind::VersionLevelMetadata => {
            // Client is uploading maven-metadata.xml — accept as-is (trust client merge logic).
            if let Err(e) = maven_storage::atomic_write(&file_path, &body).await {
                warn!(error = %e, "failed to write metadata");
                return error_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to write metadata",
                );
            }
            // Generate checksum sidecars for the metadata file.
            if let Err(e) = maven_storage::write_checksum_sidecars(&file_path, &body).await {
                warn!(error = %e, "failed to write metadata checksum sidecars");
            }
        }

        MavenPathKind::Artifact => {
            // Release immutability: non-SNAPSHOT artifacts cannot be re-deployed.
            if !coord.is_snapshot() && maven_storage::file_exists(&file_path).await {
                return error_json(
                    StatusCode::CONFLICT,
                    "release artifact already exists and cannot be re-deployed",
                );
            }

            // Write the artifact atomically.
            if let Err(e) = maven_storage::atomic_write(&file_path, &body).await {
                warn!(error = %e, "failed to write artifact");
                return error_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to write artifact",
                );
            }

            // Auto-generate checksum sidecars (.sha1, .md5).
            if let Err(e) = maven_storage::write_checksum_sidecars(&file_path, &body).await {
                warn!(error = %e, "failed to write checksum sidecars");
            }

            // After artifact upload, regenerate artifact-level metadata.
            if let Some(xml) = metadata::regenerate_artifact_metadata(
                &storage,
                tenant_id,
                &coord.group_id,
                &coord.artifact_id,
            )
            .await
            {
                let meta_coord = validation::MavenCoordinate {
                    group_id: coord.group_id.clone(),
                    artifact_id: coord.artifact_id.clone(),
                    version: None,
                    filename: Some("maven-metadata.xml".to_string()),
                };
                if let Ok(meta_path) =
                    maven_storage::artifact_path(&storage, tenant_id, &meta_coord)
                {
                    let xml_bytes = xml.as_bytes();
                    if let Err(e) = maven_storage::atomic_write(&meta_path, xml_bytes).await {
                        warn!(error = %e, "failed to regenerate artifact metadata");
                    } else if let Err(e) =
                        maven_storage::write_checksum_sidecars(&meta_path, xml_bytes).await
                    {
                        warn!(error = %e, "failed to write metadata checksum sidecars");
                    }
                }
            }

            metrics.record_maven_publish(tenant_id);
        }
    }

    // Update quota after successful write.
    update_quota_usage(&quota_store, &storage, tenant_id).await;

    (StatusCode::CREATED, "").into_response()
}

// ── GET handler ─────────────────────────────────────────────────────

/// GET /-/maven/{*path} — download artifact, metadata, or checksum.
pub async fn get_artifact(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Extension(metrics): Extension<RegistryMetrics>,
    Path(path): Path<String>,
) -> Response {
    let tenant_id = &tenant.tenant_id;

    let (coord, kind) = match validation::parse_maven_path(&path) {
        Ok(v) => v,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let file_path = match maven_storage::artifact_path(&storage, tenant_id, &coord) {
        Ok(p) => p,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    // Try reading the file.
    let data = match maven_storage::read_file(&file_path).await {
        Ok(Some(data)) => data,
        Ok(None) => {
            // Metadata fallback: if metadata doesn't exist, try to regenerate it.
            if matches!(kind, MavenPathKind::ArtifactLevelMetadata)
                && let Some(xml) = metadata::regenerate_artifact_metadata(
                    &storage,
                    tenant_id,
                    &coord.group_id,
                    &coord.artifact_id,
                )
                .await
            {
                let xml_bytes = xml.into_bytes();
                return build_download_response(&xml_bytes, &coord, &kind, &file_path);
            }
            return error_json(StatusCode::NOT_FOUND, "artifact not found");
        }
        Err(e) => {
            warn!(error = %e, "failed to read artifact");
            return error_json(StatusCode::INTERNAL_SERVER_ERROR, "failed to read artifact");
        }
    };

    metrics.record_maven_download(tenant_id);

    build_download_response(&data, &coord, &kind, &file_path)
}

// ── HEAD handler ────────────────────────────────────────────────────

/// HEAD /-/maven/{*path} — existence check with headers.
pub async fn head_artifact(
    State(storage): State<Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path(path): Path<String>,
) -> Response {
    let tenant_id = &tenant.tenant_id;

    let (coord, kind) = match validation::parse_maven_path(&path) {
        Ok(v) => v,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let file_path = match maven_storage::artifact_path(&storage, tenant_id, &coord) {
        Ok(p) => p,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    match maven_storage::file_metadata(&file_path).await {
        Ok(Some(meta)) => {
            let content_type = content_type_for_path(&coord, &kind);
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            headers.insert(header::CONTENT_LENGTH, meta.len().into());

            if let Ok(modified) = meta.modified()
                && let Ok(duration) = modified.duration_since(SystemTime::UNIX_EPOCH)
                && let Ok(v) = httpdate_from_epoch(duration.as_secs()).parse()
            {
                headers.insert(header::LAST_MODIFIED, v);
            }

            (StatusCode::OK, headers).into_response()
        }
        Ok(None) => {
            // Metadata fallback for HEAD.
            if matches!(kind, MavenPathKind::ArtifactLevelMetadata)
                && metadata::regenerate_artifact_metadata(
                    &storage,
                    tenant_id,
                    &coord.group_id,
                    &coord.artifact_id,
                )
                .await
                .is_some()
            {
                return StatusCode::OK.into_response();
            }
            StatusCode::NOT_FOUND.into_response()
        }
        Err(e) => {
            warn!(error = %e, "failed to stat artifact");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ── Response building ───────────────────────────────────────────────

/// Build a full download response with appropriate headers.
fn build_download_response(
    data: &[u8],
    coord: &validation::MavenCoordinate,
    kind: &MavenPathKind,
    file_path: &std::path::Path,
) -> Response {
    let content_type = content_type_for_path(coord, kind);

    let sha1 = checksum::compute_sha1(data);
    let md5_hash = checksum::compute_md5(data);

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    headers.insert(header::CONTENT_LENGTH, data.len().into());
    headers.insert("x-checksum-sha1", sha1.parse().unwrap());
    headers.insert("x-checksum-md5", md5_hash.parse().unwrap());
    // ETag for cache validation
    headers.insert(header::ETAG, format!("\"{sha1}\"").parse().unwrap());

    // Last-Modified from file metadata
    if let Ok(meta) = std::fs::metadata(file_path)
        && let Ok(modified) = meta.modified()
        && let Ok(duration) = modified.duration_since(SystemTime::UNIX_EPOCH)
        && let Ok(v) = httpdate_from_epoch(duration.as_secs()).parse()
    {
        headers.insert(header::LAST_MODIFIED, v);
    }

    debug!(content_type, size = data.len(), "serving maven artifact");

    (StatusCode::OK, headers, data.to_vec()).into_response()
}

/// Determine content type from file extension.
fn content_type_for_path(
    coord: &validation::MavenCoordinate,
    kind: &MavenPathKind,
) -> &'static str {
    // Checksum files are always text/plain.
    if matches!(kind, MavenPathKind::Checksum { .. }) {
        return "text/plain";
    }

    // Metadata files are XML.
    if matches!(
        kind,
        MavenPathKind::ArtifactLevelMetadata | MavenPathKind::VersionLevelMetadata
    ) {
        return "application/xml";
    }

    // Determine by filename extension.
    let filename = coord.filename.as_deref().unwrap_or("");
    if filename.ends_with(".jar") {
        "application/java-archive"
    } else if filename.ends_with(".pom") || filename.ends_with(".xml") {
        "application/xml"
    } else if filename.ends_with(".war") || filename.ends_with(".ear") {
        "application/x-webarchive"
    } else if filename.ends_with(".aar") {
        "application/octet-stream"
    } else if filename.ends_with(".asc") {
        "application/pgp-signature"
    } else if filename.ends_with(".sha1")
        || filename.ends_with(".md5")
        || filename.ends_with(".sha256")
        || filename.ends_with(".sha512")
    {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}

/// Format epoch seconds as an HTTP-date string (RFC 7231).
fn httpdate_from_epoch(secs: u64) -> String {
    use chrono::{DateTime, Utc};
    let dt = DateTime::<Utc>::from_timestamp(secs as i64, 0).unwrap_or_default();
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}
