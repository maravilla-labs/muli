// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Maven artifact filesystem storage.

use std::path::{Path, PathBuf};

use tokio::fs;
use tracing::warn;

use crate::storage::{FilesystemStorage, StorageError, StorageResult};

use super::checksum;
use super::validation::{MavenCoordinate, MavenValidationError};

impl From<MavenValidationError> for StorageError {
    fn from(e: MavenValidationError) -> Self {
        StorageError::InvalidInput(e.to_string())
    }
}

/// Get the maven storage root for a tenant.
pub fn maven_root(storage: &FilesystemStorage, tenant_id: &str) -> PathBuf {
    storage.root_path().join(tenant_id).join("maven")
}

/// Build the filesystem path for a Maven coordinate.
///
/// Layout: `{root}/{tenant_id}/maven/{group_path}/{artifact_id}/{version?}/{filename?}`
/// where `group_path` converts dots to directory separators.
pub fn artifact_path(
    storage: &FilesystemStorage,
    tenant_id: &str,
    coord: &MavenCoordinate,
) -> StorageResult<PathBuf> {
    let root = maven_root(storage, tenant_id);

    // groupId dots → directory separators: "org.apache.commons" → "org/apache/commons"
    let group_path: PathBuf = coord.group_id.split('.').collect();
    let mut path = root.join(group_path).join(&coord.artifact_id);

    if let Some(ref version) = coord.version {
        path = path.join(version);
    }
    if let Some(ref filename) = coord.filename {
        path = path.join(filename);
    }

    Ok(path)
}

/// Write a file atomically: write to `.tmp` then rename.
/// This prevents partial reads during concurrent writes.
pub async fn atomic_write(path: &Path, data: &[u8]) -> StorageResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("dat")
    ));
    fs::write(&tmp, data).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

/// Read a file, returning None if it doesn't exist.
pub async fn read_file(path: &Path) -> StorageResult<Option<Vec<u8>>> {
    match fs::read(path).await {
        Ok(data) => Ok(Some(data)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// Check if a file exists.
pub async fn file_exists(path: &Path) -> bool {
    fs::metadata(path).await.is_ok()
}

/// Get file metadata (size, mtime).
pub async fn file_metadata(path: &Path) -> StorageResult<Option<std::fs::Metadata>> {
    match fs::metadata(path).await {
        Ok(meta) => Ok(Some(meta)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// List version directories under an artifact path.
/// Returns directory names (version strings), sorted.
pub async fn list_versions(
    storage: &FilesystemStorage,
    tenant_id: &str,
    group_id: &str,
    artifact_id: &str,
) -> StorageResult<Vec<String>> {
    let root = maven_root(storage, tenant_id);
    let group_path: PathBuf = group_id.split('.').collect();
    let artifact_dir = root.join(group_path).join(artifact_id);

    if !artifact_dir.exists() {
        return Ok(Vec::new());
    }

    let mut versions = Vec::new();
    let mut entries = fs::read_dir(&artifact_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir()
            && let Some(name) = entry.file_name().to_str()
        {
            versions.push(name.to_string());
        }
    }
    versions.sort();
    Ok(versions)
}

/// List files in a version directory (for snapshot metadata regeneration).
pub async fn list_version_files(
    storage: &FilesystemStorage,
    tenant_id: &str,
    group_id: &str,
    artifact_id: &str,
    version: &str,
) -> StorageResult<Vec<String>> {
    let root = maven_root(storage, tenant_id);
    let group_path: PathBuf = group_id.split('.').collect();
    let version_dir = root.join(group_path).join(artifact_id).join(version);

    if !version_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    let mut entries = fs::read_dir(&version_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file()
            && let Some(name) = entry.file_name().to_str()
        {
            files.push(name.to_string());
        }
    }
    files.sort();
    Ok(files)
}

/// Summary of a Maven artifact for listing purposes.
#[derive(Debug)]
pub struct MavenArtifactSummary {
    pub group_id: String,
    pub artifact_id: String,
    pub versions: Vec<String>,
    pub latest_version: Option<String>,
}

/// List all Maven artifacts for a tenant.
///
/// Walks `{root}/{tenant_id}/maven/` to find artifact directories
/// (directories that contain version subdirectories with actual files).
/// Reconstructs group_id from the directory path.
pub async fn list_artifacts(
    storage: &FilesystemStorage,
    tenant_id: &str,
) -> StorageResult<Vec<MavenArtifactSummary>> {
    let root = maven_root(storage, tenant_id);
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    // Walk the maven root to discover artifact directories.
    // An artifact dir is one that has version subdirectories (dirs containing files).
    // Layout: {maven_root}/{group/path/segments}/{artifact_id}/{version}/files
    let mut stack: Vec<(std::path::PathBuf, Vec<String>)> = vec![(root.clone(), Vec::new())];

    while let Some((dir, group_parts)) = stack.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        let mut subdirs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                subdirs.push(entry);
            }
        }

        for entry in subdirs {
            let entry_name = entry.file_name().to_string_lossy().to_string();

            // Check if this directory looks like an artifact dir:
            // it should have version subdirectories (which themselves contain files)
            let versions = list_versions(storage, tenant_id, &group_parts.join("."), &entry_name)
                .await
                .unwrap_or_default();

            if !versions.is_empty() {
                let group_id = group_parts.join(".");
                let latest_version = versions.last().cloned();
                results.push(MavenArtifactSummary {
                    group_id,
                    artifact_id: entry_name,
                    versions,
                    latest_version,
                });
            } else {
                // Not an artifact dir — it's part of the group path. Recurse.
                let mut new_group = group_parts.clone();
                new_group.push(entry_name);
                stack.push((entry.path(), new_group));
            }
        }
    }

    results.sort_by(|a, b| (&a.group_id, &a.artifact_id).cmp(&(&b.group_id, &b.artifact_id)));
    Ok(results)
}

/// Write checksum sidecar files (.sha1, .md5) for an artifact.
pub async fn write_checksum_sidecars(artifact_path: &Path, data: &[u8]) -> StorageResult<()> {
    let sha1 = checksum::compute_sha1(data);
    let md5 = checksum::compute_md5(data);

    let sha1_path = PathBuf::from(format!("{}.sha1", artifact_path.display()));
    let md5_path = PathBuf::from(format!("{}.md5", artifact_path.display()));

    if let Err(e) = fs::write(&sha1_path, sha1.as_bytes()).await {
        warn!(path = %sha1_path.display(), error = %e, "failed to write sha1 sidecar");
    }
    if let Err(e) = fs::write(&md5_path, md5.as_bytes()).await {
        warn!(path = %md5_path.display(), error = %e, "failed to write md5 sidecar");
    }

    Ok(())
}
