// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm tarball and packument storage.

use std::path::PathBuf;

use tokio::fs;

use crate::storage::{FilesystemStorage, StorageError, StorageResult};

use super::packument::Packument;

/// Get the npm storage root for a tenant.
fn npm_root(storage: &FilesystemStorage, tenant_id: &str) -> PathBuf {
    storage.root_path().join(tenant_id).join("npm")
}

/// Validate that a package name is safe to use in a filesystem path.
/// For scoped packages (@scope/name), validates each component separately.
/// Rejects '..', '/', '\', '\0' in each component.
fn validate_npm_package_name(name: &str) -> StorageResult<()> {
    if name.is_empty() {
        return Err(StorageError::InvalidInput(
            "empty npm package name".to_string(),
        ));
    }

    let components: Vec<&str> = if let Some(scoped) = name.strip_prefix('@') {
        let parts: Vec<&str> = scoped.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(StorageError::InvalidInput(format!(
                "invalid scoped npm package name: {name:?}"
            )));
        }
        parts
    } else {
        vec![name]
    };

    for component in &components {
        validate_path_component(component)?;
    }
    Ok(())
}

/// Validate a single path component: reject empty, '..', path separators, null bytes.
fn validate_path_component(s: &str) -> StorageResult<()> {
    if s.is_empty() || s.contains("..") || s.contains('/') || s.contains('\\') || s.contains('\0') {
        return Err(StorageError::InvalidInput(format!(
            "invalid path component: {s:?}"
        )));
    }
    Ok(())
}

/// Validate a tarball filename: must not contain path separators or traversal.
fn validate_filename(filename: &str) -> StorageResult<()> {
    if filename.is_empty()
        || filename.contains("..")
        || filename.contains('/')
        || filename.contains('\\')
        || filename.contains('\0')
    {
        return Err(StorageError::InvalidInput(format!(
            "invalid filename: {filename:?}"
        )));
    }
    Ok(())
}

/// Get the package directory path.
/// Scoped package names (containing '/') are stored with '__' replacing '/'
/// to avoid nested directories.
fn package_dir(
    storage: &FilesystemStorage,
    tenant_id: &str,
    package_name: &str,
) -> StorageResult<PathBuf> {
    validate_npm_package_name(package_name)?;
    let safe_name = package_name.replace('/', "__");
    Ok(npm_root(storage, tenant_id).join(&safe_name))
}

/// Read a packument from storage.
pub async fn read_packument(
    storage: &FilesystemStorage,
    tenant_id: &str,
    package_name: &str,
) -> Option<Packument> {
    let dir = package_dir(storage, tenant_id, package_name).ok()?;
    let path = dir.join("metadata.json");
    let data = fs::read(&path).await.ok()?;
    serde_json::from_slice(&data).ok()
}

/// Write a packument to storage.
pub async fn write_packument(
    storage: &FilesystemStorage,
    tenant_id: &str,
    package_name: &str,
    packument: &Packument,
) -> StorageResult<()> {
    let dir = package_dir(storage, tenant_id, package_name)?;
    fs::create_dir_all(&dir).await?;
    let path = dir.join("metadata.json");
    let data = serde_json::to_vec_pretty(packument)
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
    fs::write(&path, &data).await?;
    Ok(())
}

/// Store a tarball for a specific version.
pub async fn store_tarball(
    storage: &FilesystemStorage,
    tenant_id: &str,
    package_name: &str,
    filename: &str,
    data: &[u8],
) -> StorageResult<()> {
    validate_filename(filename)?;
    let dir = package_dir(storage, tenant_id, package_name)?.join("-");
    fs::create_dir_all(&dir).await?;
    let path = dir.join(filename);
    fs::write(&path, data).await?;
    Ok(())
}

/// Read a tarball.
pub async fn read_tarball(
    storage: &FilesystemStorage,
    tenant_id: &str,
    package_name: &str,
    filename: &str,
) -> StorageResult<Vec<u8>> {
    validate_filename(filename)?;
    let path = package_dir(storage, tenant_id, package_name)?
        .join("-")
        .join(filename);
    Ok(fs::read(&path).await?)
}

/// List all package names for a tenant (for search).
pub async fn list_packages(
    storage: &FilesystemStorage,
    tenant_id: &str,
) -> std::io::Result<Vec<String>> {
    let root = npm_root(storage, tenant_id);
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut packages = Vec::new();
    let mut entries = fs::read_dir(&root).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Restore scoped package names: __ back to /
            let package_name = name.replace("__", "/");
            packages.push(package_name);
        }
    }
    packages.sort();
    Ok(packages)
}
