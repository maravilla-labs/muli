// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo crate file storage.

use std::path::PathBuf;

use tempfile::NamedTempFile;
use tokio::fs;

use crate::storage::{FilesystemStorage, StorageError, StorageResult};

use super::validation;

/// Get the cargo storage root for a tenant.
fn cargo_root(storage: &FilesystemStorage, tenant_id: &str) -> PathBuf {
    storage.root_path().join(tenant_id).join("cargo")
}

/// Validate a path component doesn't contain traversal characters.
fn validate_path_component(s: &str, label: &str) -> StorageResult<()> {
    if s.is_empty() || s.contains("..") || s.contains('/') || s.contains('\\') || s.contains('\0') {
        return Err(StorageError::InvalidInput(format!(
            "invalid {label}: {s:?}"
        )));
    }
    Ok(())
}

/// Get the crate file path for a specific version.
fn crate_path(
    storage: &FilesystemStorage,
    tenant_id: &str,
    name: &str,
    version: &str,
) -> StorageResult<PathBuf> {
    validate_path_component(name, "crate name")?;
    validate_path_component(version, "version")?;
    Ok(cargo_root(storage, tenant_id)
        .join("crates")
        .join(name)
        .join(format!("{name}-{version}.crate")))
}

/// Get the index file path for a crate.
fn index_path(storage: &FilesystemStorage, tenant_id: &str, name: &str) -> StorageResult<PathBuf> {
    validate_path_component(name, "crate name")?;
    let prefix = validation::index_prefix(name);
    Ok(cargo_root(storage, tenant_id).join("index").join(prefix))
}

/// Store a .crate file.
pub async fn store_crate(
    storage: &FilesystemStorage,
    tenant_id: &str,
    name: &str,
    version: &str,
    data: &[u8],
) -> StorageResult<()> {
    let path = crate_path(storage, tenant_id, name, version)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&path, data).await?;
    Ok(())
}

/// Read a .crate file.
pub async fn read_crate(
    storage: &FilesystemStorage,
    tenant_id: &str,
    name: &str,
    version: &str,
) -> StorageResult<Vec<u8>> {
    let path = crate_path(storage, tenant_id, name, version)?;
    Ok(fs::read(&path).await?)
}

/// Read the sparse index file for a crate (NDJSON, one line per version).
pub async fn read_index(
    storage: &FilesystemStorage,
    tenant_id: &str,
    name: &str,
) -> StorageResult<String> {
    let path = index_path(storage, tenant_id, name)?;
    Ok(fs::read_to_string(&path).await?)
}

/// Append a line to the sparse index file for a crate.
pub async fn append_index_entry(
    storage: &FilesystemStorage,
    tenant_id: &str,
    name: &str,
    entry: &str,
) -> StorageResult<()> {
    let path = index_path(storage, tenant_id, name)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    // Read existing content and append
    let mut content = match fs::read_to_string(&path).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e.into()),
    };
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(entry);
    content.push('\n');
    // Atomic write: write to temp file then rename
    let dir = path.parent().ok_or_else(|| {
        StorageError::InvalidInput("index path has no parent directory".to_string())
    })?;
    let tmp = tokio::task::spawn_blocking({
        let dir = dir.to_path_buf();
        let content = content.into_bytes();
        move || -> Result<NamedTempFile, std::io::Error> {
            let mut tmp = NamedTempFile::new_in(&dir)?;
            std::io::Write::write_all(&mut tmp, &content)?;
            Ok(tmp)
        }
    })
    .await
    .map_err(std::io::Error::other)??;
    tmp.persist(&path).map_err(|e| e.error)?;
    Ok(())
}

/// List all crates in the index for a tenant.
///
/// Walks the `{root}/{tenant_id}/cargo/index/` directory tree recursively,
/// reads each NDJSON index file, and extracts crate metadata.
/// Returns a list of `(name, versions_count, latest_non_yanked_version)` tuples.
pub async fn list_crates(
    storage: &FilesystemStorage,
    tenant_id: &str,
) -> StorageResult<Vec<(String, usize, Option<String>)>> {
    let index_dir = cargo_root(storage, tenant_id).join("index");
    if !index_dir.exists() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    let mut dirs = vec![index_dir];

    while let Some(dir) = dirs.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };
        while let Some(entry) = entries.next_entry().await? {
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                dirs.push(entry.path());
            } else if ft.is_file() {
                let file_name = entry.file_name();
                let name_str = file_name.to_string_lossy();
                // Skip config.json
                if name_str == "config.json" {
                    continue;
                }
                // Read NDJSON index file and parse crate metadata
                if let Ok(content) = fs::read_to_string(entry.path()).await {
                    let mut crate_name: Option<String> = None;
                    let mut versions_count: usize = 0;
                    let mut latest_version: Option<String> = None;

                    for line in content.lines() {
                        if line.is_empty() {
                            continue;
                        }
                        if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) {
                            if crate_name.is_none() {
                                crate_name =
                                    entry.get("name").and_then(|v| v.as_str()).map(String::from);
                            }
                            versions_count += 1;
                            let yanked = entry
                                .get("yanked")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            if !yanked {
                                latest_version =
                                    entry.get("vers").and_then(|v| v.as_str()).map(String::from);
                            }
                        }
                    }

                    if let Some(name) = crate_name {
                        results.push((name, versions_count, latest_version));
                    }
                }
            }
        }
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(results)
}

/// Update a specific version in the index (e.g., to set yanked=true).
pub async fn update_index_version(
    storage: &FilesystemStorage,
    tenant_id: &str,
    name: &str,
    version: &str,
    yanked: bool,
) -> StorageResult<bool> {
    let path = index_path(storage, tenant_id, name)?;
    let content = fs::read_to_string(&path).await?;
    let mut found = false;
    let mut new_lines = Vec::new();
    for line in content.lines() {
        if line.is_empty() {
            continue;
        }
        if let Ok(mut entry) = serde_json::from_str::<serde_json::Value>(line) {
            if entry.get("vers").and_then(|v| v.as_str()) == Some(version) {
                entry["yanked"] = serde_json::Value::Bool(yanked);
                found = true;
            }
            new_lines.push(serde_json::to_string(&entry).unwrap_or_else(|_| line.to_string()));
        } else {
            new_lines.push(line.to_string());
        }
    }
    if found {
        let mut output = new_lines.join("\n");
        output.push('\n');
        // Atomic write: write to temp file then rename
        let dir = path.parent().ok_or_else(|| {
            StorageError::InvalidInput("index path has no parent directory".to_string())
        })?;
        let tmp = tokio::task::spawn_blocking({
            let dir = dir.to_path_buf();
            let content = output.into_bytes();
            move || -> Result<NamedTempFile, std::io::Error> {
                let mut tmp = NamedTempFile::new_in(&dir)?;
                std::io::Write::write_all(&mut tmp, &content)?;
                Ok(tmp)
            }
        })
        .await
        .map_err(std::io::Error::other)??;
        tmp.persist(&path).map_err(|e| e.error)?;
    }
    Ok(found)
}
