// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! One-time backfill of the LFS object index.
//!
//! Before the index existed, LFS objects were only stored per tenant, so an
//! installation upgraded from that state has objects no repository owns. This
//! job gives each existing repository the oids its own git object database
//! points at (LFS pointer files, `oid sha256:<hex>`) and that are stored, then
//! marks the migration done installation-wide.
//!
//! It runs at startup BEFORE the git HTTP service listens, and only until it
//! completed once. After that it never runs again, for any tenant or
//! repository, old or new: from then on a pointer pushed into a repository
//! grants nothing; only an upload the server hashed (or a fork) adds a row.
//! Running it again later would let anyone who can push a pointer naming
//! another repository's oid claim that object.
//!
//! Idempotent: rows are inserted with "ignore if present", and an interrupted
//! run (crash, error) leaves the marker unset, so the next start repeats it.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use muli_core::traits::{LfsObjectStore, RepositoryStore};
use tracing::{info, warn};

use crate::lfs::storage::LfsStorage;
use crate::lfs::types::validate_oid;
use crate::storage::FilesystemStorage;

/// Name of the migration marker.
pub const MIGRATION: &str = "lfs_repo_index_v1";

/// Largest blob read as a possible pointer (the spec caps pointers at 1024).
const MAX_POINTER_SIZE: usize = 1024;

/// What the backfill did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillReport {
    /// The marker was already set: nothing was scanned.
    pub skipped: bool,
    pub repositories: usize,
    /// (repository, oid) rows written (oids pointed at AND stored).
    pub rows: usize,
    /// Pointers whose object is not stored (never indexed).
    pub missing: usize,
}

/// The oid of an LFS pointer file, if `data` is one. Pure.
pub fn pointer_oid(data: &[u8]) -> Option<String> {
    if data.len() > MAX_POINTER_SIZE {
        return None;
    }
    let text = std::str::from_utf8(data).ok()?;
    let mut lines = text.lines();
    let version = lines.next()?;
    if version != "version https://git-lfs.github.com/spec/v1"
        && version != "version https://hawser.github.com/spec/v1"
    {
        return None;
    }
    lines.find_map(|l| {
        let oid = l.strip_prefix("oid sha256:")?.trim();
        validate_oid(oid).then(|| oid.to_string())
    })
}

/// Every LFS oid pointed at by a blob in the repository's object database
/// (all refs and anything else it holds). Blocking: run on a blocking thread.
pub fn scan_repository(path: &Path) -> Result<BTreeSet<String>, git2::Error> {
    let repo = git2::Repository::open_bare(path)?;
    let odb = repo.odb()?;
    let mut candidates = Vec::new();
    odb.foreach(|id| {
        if let Ok((size, kind)) = odb.read_header(*id)
            && kind == git2::ObjectType::Blob
            && size <= MAX_POINTER_SIZE
        {
            candidates.push(*id);
        }
        true
    })?;
    let mut oids = BTreeSet::new();
    for id in candidates {
        if let Ok(obj) = odb.read(id)
            && let Some(oid) = pointer_oid(obj.data())
        {
            oids.insert(oid);
        }
    }
    Ok(oids)
}

/// Run the backfill unless it already completed. `tenant_ids` are the tenants
/// whose repositories are scanned.
pub async fn run(
    tenant_ids: &[String],
    repo_store: &Arc<dyn RepositoryStore>,
    git_storage: &FilesystemStorage,
    lfs_storage: &Arc<dyn LfsStorage>,
    objects: &Arc<dyn LfsObjectStore>,
) -> anyhow::Result<BackfillReport> {
    let mut report = BackfillReport::default();
    if objects.lfs_migration_done(MIGRATION).await? {
        report.skipped = true;
        return Ok(report);
    }
    info!("LFS object index: backfilling repositories from their history");

    for tenant_id in tenant_ids {
        for repo in repo_store.list_repositories(tenant_id).await? {
            let path = git_storage.repo_path(tenant_id, &repo.namespace, &repo.name);
            if !path.exists() {
                continue;
            }
            report.repositories += 1;
            let scanned = tokio::task::spawn_blocking(move || scan_repository(&path)).await?;
            let pointed = match scanned {
                Ok(oids) => oids,
                Err(e) => {
                    // A repository git cannot open has nothing to serve either;
                    // the other repositories still get their rows.
                    warn!(error = %e, tenant_id = %tenant_id, repo = %repo.name,
                        "LFS backfill: repository unreadable, skipped");
                    continue;
                }
            };
            let mut stored = Vec::new();
            for oid in pointed {
                if lfs_storage
                    .has_object(tenant_id, &oid)
                    .await
                    .map_err(|e| anyhow::anyhow!("LFS storage: {e}"))?
                {
                    stored.push(oid);
                } else {
                    report.missing += 1;
                }
            }
            if !stored.is_empty() {
                objects
                    .add_lfs_objects(tenant_id, &repo.id, &stored)
                    .await?;
                report.rows += stored.len();
            }
        }
    }

    objects.mark_lfs_migration_done(MIGRATION).await?;
    info!(
        repositories = report.repositories,
        rows = report.rows,
        missing = report.missing,
        "LFS object index: backfill complete"
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pointer(oid: &str) -> String {
        format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize 12\n")
    }

    #[test]
    fn reads_a_pointer() {
        let oid = "ab".repeat(32);
        assert_eq!(pointer_oid(pointer(&oid).as_bytes()), Some(oid));
    }

    #[test]
    fn ignores_what_is_not_a_pointer() {
        assert_eq!(pointer_oid(b"hello"), None);
        assert_eq!(
            pointer_oid(b"version https://git-lfs.github.com/spec/v1\noid sha256:xyz\n"),
            None
        );
        let mut big = pointer(&"ab".repeat(32)).into_bytes();
        big.extend(std::iter::repeat_n(b'x', 2000));
        assert_eq!(pointer_oid(&big), None);
    }
}
