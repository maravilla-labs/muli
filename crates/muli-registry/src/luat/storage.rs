// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat package storage.
//!
//! Layout under `{root}/{tenant_id}/luat/packages/{scope}/{name}/`:
//!
//! ```text
//! index                  NDJSON, one line per version, oldest first (served as-is)
//! summary.json           search/listing data (name, latest, description, updated_at)
//! versions/{v}.tgz       the tarball, byte for byte as published
//! versions/{v}.json      per-version package-page data (readme, files, ...)
//! ```
//!
//! `scope` and `name` are validated against `[a-z0-9][a-z0-9._-]{0,63}` and
//! versions are parsed as semver before they reach this module, so every path
//! component is a single safe file name. Nothing is ever deleted.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::sync::{Mutex, MutexGuard};

use crate::storage::{FilesystemStorage, StorageError, StorageResult};

use super::validation;

/// One line of a package index. Field order is the wire order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexEntry {
    pub name: String,
    pub vers: String,
    pub deps: BTreeMap<String, String>,
    pub cksum: String,
    pub luat: Option<String>,
    pub yanked: bool,
}

/// Package-page data kept for every published version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionMeta {
    pub name: String,
    pub vers: String,
    pub description: Option<String>,
    pub license: Option<String>,
    pub repository: Option<String>,
    pub keywords: Vec<String>,
    pub readme: Option<String>,
    pub dependencies: BTreeMap<String, String>,
    pub files: Vec<String>,
    pub luat: Option<String>,
    pub cksum: String,
    /// Compressed tarball size in bytes.
    pub size: u64,
    pub published_at: DateTime<Utc>,
}

/// Search/listing row, rewritten on every publish, yank and unyank.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageSummary {
    pub name: String,
    pub description: Option<String>,
    pub latest: Option<String>,
    pub keywords: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

/// Serializes Luat writes (publish, yank, unyank) within this process so the
/// read-modify-write of an index can never interleave.
pub async fn write_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

pub(crate) fn packages_root(storage: &FilesystemStorage, tenant_id: &str) -> PathBuf {
    storage
        .root_path()
        .join(tenant_id)
        .join("luat")
        .join("packages")
}

fn package_dir(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
) -> StorageResult<PathBuf> {
    validation::validate_parts(scope, name)
        .map_err(|e| StorageError::InvalidInput(e.to_string()))?;
    Ok(packages_root(storage, tenant_id).join(scope).join(name))
}

fn version_file(dir: &Path, version: &str, ext: &str) -> StorageResult<PathBuf> {
    validation::parse_version(version).map_err(|e| StorageError::InvalidInput(e.to_string()))?;
    Ok(dir.join("versions").join(format!("{version}.{ext}")))
}

async fn read_optional(path: &Path) -> StorageResult<Option<Vec<u8>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Write a file atomically (temp file in the same directory, then rename).
async fn atomic_write(path: PathBuf, bytes: Vec<u8>) -> StorageResult<()> {
    let dir = path
        .parent()
        .ok_or_else(|| StorageError::InvalidInput("path has no parent".into()))?
        .to_path_buf();
    fs::create_dir_all(&dir).await?;
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let mut tmp = NamedTempFile::new_in(&dir)?;
        std::io::Write::write_all(&mut tmp, &bytes)?;
        tmp.persist(&path).map_err(|e| e.error)?;
        Ok(())
    })
    .await
    .map_err(std::io::Error::other)??;
    Ok(())
}

fn to_json<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| StorageError::InvalidInput(e.to_string()))
}

/// The raw index (NDJSON), or `None` when the package does not exist.
pub async fn read_index(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
) -> StorageResult<Option<String>> {
    let path = package_dir(storage, tenant_id, scope, name)?.join("index");
    Ok(read_optional(&path)
        .await?
        .map(|b| String::from_utf8_lossy(&b).into_owned()))
}

/// Parse an index into its entries (unparseable lines are skipped).
pub fn parse_index(content: &str) -> Vec<IndexEntry> {
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn render_index(entries: &[IndexEntry]) -> StorageResult<Vec<u8>> {
    let mut out = Vec::new();
    for entry in entries {
        out.extend(to_json(entry)?);
        out.push(b'\n');
    }
    Ok(out)
}

/// The entry for one version, by semver precedence (build metadata ignored).
pub fn find_version<'a>(entries: &'a [IndexEntry], version: &str) -> Option<&'a IndexEntry> {
    entries
        .iter()
        .find(|e| validation::same_release(&e.vers, version))
}

/// Store a new version: tarball and page data first, then the index line that
/// makes it visible, then the search summary. Caller holds `write_lock`.
pub async fn store_version(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
    tarball: Vec<u8>,
    meta: &VersionMeta,
    entry: IndexEntry,
) -> StorageResult<()> {
    let dir = package_dir(storage, tenant_id, scope, name)?;
    atomic_write(version_file(&dir, &meta.vers, "tgz")?, tarball).await?;
    atomic_write(version_file(&dir, &meta.vers, "json")?, to_json(meta)?).await?;
    let mut entries = match read_index(storage, tenant_id, scope, name).await? {
        Some(content) => parse_index(&content),
        None => Vec::new(),
    };
    entries.push(entry);
    atomic_write(dir.join("index"), render_index(&entries)?).await?;
    refresh_summary(
        storage,
        tenant_id,
        scope,
        name,
        &entries,
        Some(meta.published_at),
    )
    .await
}

/// Flip a version's `yanked` flag. `Ok(false)` when the version (or package)
/// does not exist. Caller holds `write_lock`.
pub async fn set_yanked(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
    version: &str,
    yanked: bool,
) -> StorageResult<bool> {
    let Some(content) = read_index(storage, tenant_id, scope, name).await? else {
        return Ok(false);
    };
    let mut entries = parse_index(&content);
    let Some(entry) = entries
        .iter_mut()
        .find(|e| validation::same_release(&e.vers, version))
    else {
        return Ok(false);
    };
    if entry.yanked != yanked {
        entry.yanked = yanked;
        let dir = package_dir(storage, tenant_id, scope, name)?;
        atomic_write(dir.join("index"), render_index(&entries)?).await?;
        refresh_summary(storage, tenant_id, scope, name, &entries, None).await?;
    }
    Ok(true)
}

/// The version shown as "latest": the highest non-yanked release, else the
/// highest non-yanked pre-release, else (everything yanked) the highest version.
pub fn latest(entries: &[IndexEntry]) -> Option<&IndexEntry> {
    let parsed: Vec<(semver::Version, &IndexEntry)> = entries
        .iter()
        .filter_map(|e| semver::Version::parse(&e.vers).ok().map(|v| (v, e)))
        .collect();
    let best = |pred: &dyn Fn(&(semver::Version, &IndexEntry)) -> bool| {
        parsed
            .iter()
            .filter(|p| pred(p))
            .max_by(|a, b| a.0.cmp_precedence(&b.0))
            .map(|p| p.1)
    };
    best(&|(v, e)| !e.yanked && v.pre.is_empty())
        .or_else(|| best(&|(_, e)| !e.yanked))
        .or_else(|| best(&|_| true))
}

async fn refresh_summary(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
    entries: &[IndexEntry],
    updated_at: Option<DateTime<Utc>>,
) -> StorageResult<()> {
    let dir = package_dir(storage, tenant_id, scope, name)?;
    let path = dir.join("summary.json");
    let previous: Option<PackageSummary> = read_optional(&path)
        .await?
        .and_then(|b| serde_json::from_slice(&b).ok());
    let latest_vers = latest(entries).map(|e| e.vers.clone());
    let meta = match &latest_vers {
        Some(v) => read_version_meta(storage, tenant_id, scope, name, v).await?,
        None => None,
    };
    let summary = PackageSummary {
        name: format!("@{scope}/{name}"),
        description: meta.as_ref().and_then(|m| m.description.clone()),
        keywords: meta.map(|m| m.keywords).unwrap_or_default(),
        latest: latest_vers,
        updated_at: updated_at
            .or(previous.map(|p| p.updated_at))
            .unwrap_or_else(Utc::now),
    };
    atomic_write(path, to_json(&summary)?).await
}

/// The stored tarball for a version, if any.
pub async fn read_tarball(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
    version: &str,
) -> StorageResult<Option<Vec<u8>>> {
    let dir = package_dir(storage, tenant_id, scope, name)?;
    read_optional(&version_file(&dir, version, "tgz")?).await
}

/// The stored page data for a version, if any.
pub async fn read_version_meta(
    storage: &FilesystemStorage,
    tenant_id: &str,
    scope: &str,
    name: &str,
    version: &str,
) -> StorageResult<Option<VersionMeta>> {
    let dir = package_dir(storage, tenant_id, scope, name)?;
    Ok(read_optional(&version_file(&dir, version, "json")?)
        .await?
        .and_then(|b| serde_json::from_slice(&b).ok()))
}
