// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Unused blob and layer garbage collection.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{info, warn};

use crate::storage::{FilesystemStorage, RegistryStorage, StorageResult};

pub struct GarbageCollector {
    storage: Arc<FilesystemStorage>,
    max_size_bytes: u64,
}

impl GarbageCollector {
    pub fn new(storage: Arc<FilesystemStorage>, max_size_gb: f64) -> Self {
        Self {
            storage,
            max_size_bytes: (max_size_gb * 1_073_741_824.0) as u64,
        }
    }

    fn lock_path(&self, tenant_id: &str) -> PathBuf {
        self.storage.root_path().join(tenant_id).join(".gc_lock")
    }

    /// Acquire a file-based GC lock for a tenant. Returns Err if GC is already running.
    /// Uses create_new(true) for atomic creation to avoid TOCTOU races.
    /// On stale lock (>1 hour), removes it and retries exactly once.
    async fn acquire_lock(&self, tenant_id: &str) -> StorageResult<()> {
        let lock_path = self.lock_path(tenant_id);

        // Ensure tenant directory exists
        if let Some(parent) = lock_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        match Self::try_create_lock(&lock_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Lock exists — check if stale (older than 1 hour)
                if Self::is_lock_stale(&lock_path).await {
                    warn!(tenant_id = %tenant_id, "Removing stale GC lock file");
                    let _ = tokio::fs::remove_file(&lock_path).await;
                    // Single atomic retry — if another process won the race, fail
                    if Self::try_create_lock(&lock_path).await.is_ok() {
                        return Ok(());
                    }
                }
                Err(crate::storage::StorageError::InvalidInput(
                    "GC is already running (lock file exists)".to_string(),
                ))
            }
            Err(e) => Err(crate::storage::StorageError::Io(e)),
        }
    }

    /// Atomically create a lock file using create_new(true).
    async fn try_create_lock(lock_path: &std::path::Path) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
            .await?;
        let _ = file
            .write_all(format!("{}", std::process::id()).as_bytes())
            .await;
        Ok(())
    }

    /// Check if a lock file is stale (older than 1 hour).
    async fn is_lock_stale(lock_path: &std::path::Path) -> bool {
        if let Ok(meta) = tokio::fs::metadata(lock_path).await
            && let Ok(modified) = meta.modified()
        {
            let age = std::time::SystemTime::now()
                .duration_since(modified)
                .unwrap_or_default();
            return age >= std::time::Duration::from_secs(3600);
        }
        false
    }

    /// Release the GC lock file for a tenant.
    async fn release_lock(&self, tenant_id: &str) {
        let _ = tokio::fs::remove_file(&self.lock_path(tenant_id)).await;
    }

    /// Run garbage collection for a specific tenant: remove unreferenced blobs and enforce size limits.
    /// Uses a per-tenant lock file to prevent concurrent GC runs.
    pub async fn run(&self, tenant_id: &str) -> StorageResult<GcReport> {
        self.acquire_lock(tenant_id).await?;
        let result = self.run_inner(tenant_id).await;
        self.release_lock(tenant_id).await;
        result
    }

    async fn run_inner(&self, tenant_id: &str) -> StorageResult<GcReport> {
        let mut report = GcReport::default();

        // Phase 1: Collect all digests referenced by manifests
        let referenced = self.collect_referenced_digests(tenant_id).await?;
        info!(tenant_id = %tenant_id, referenced_count = referenced.len(), "collected referenced digests");

        // Phase 2: List all blobs
        let all_blobs = self.storage.list_blobs(tenant_id).await?;
        info!(tenant_id = %tenant_id, blob_count = all_blobs.len(), "found blobs in storage");

        // Phase 3: Delete unreferenced blobs.
        // Collect referenced digests once more at deletion time to guard against
        // manifests pushed between phase 1 and now, then diff the two sets in O(N+M).
        let all_blobs_set: HashSet<String> = all_blobs.into_iter().collect();
        let current_refs = self.collect_referenced_digests(tenant_id).await?;
        let unreferenced: Vec<String> = all_blobs_set.difference(&current_refs).cloned().collect();
        for digest in &unreferenced {
            info!(tenant_id = %tenant_id, digest = %digest, "GC: deleting unreferenced blob");
            if let Err(e) = self.storage.delete_blob(tenant_id, digest).await {
                warn!(tenant_id = %tenant_id, digest = %digest, error = %e, "GC: failed to delete blob");
            } else {
                report.deleted_blobs += 1;
            }
        }

        // Phase 4: Size-based eviction if over max size
        if self.max_size_bytes > 0 {
            self.evict_lru(tenant_id, &mut report).await?;
        }

        info!(
            tenant_id = %tenant_id,
            deleted = report.deleted_blobs,
            evicted = report.evicted_blobs,
            "garbage collection complete"
        );
        Ok(report)
    }

    async fn collect_referenced_digests(&self, tenant_id: &str) -> StorageResult<HashSet<String>> {
        let mut referenced = HashSet::new();
        let manifests = self.storage.all_manifests(tenant_id).await?;

        for manifest_data in manifests {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&manifest_data) {
                // Extract layer digests
                if let Some(layers) = json.get("layers").and_then(|v| v.as_array()) {
                    for layer in layers {
                        if let Some(digest) = layer.get("digest").and_then(|v| v.as_str()) {
                            referenced.insert(digest.to_string());
                        }
                    }
                }
                // Extract config digest
                if let Some(config) = json.get("config")
                    && let Some(digest) = config.get("digest").and_then(|v| v.as_str())
                {
                    referenced.insert(digest.to_string());
                }
                // Handle manifest lists / indexes
                if let Some(manifests) = json.get("manifests").and_then(|v| v.as_array()) {
                    for m in manifests {
                        if let Some(digest) = m.get("digest").and_then(|v| v.as_str()) {
                            referenced.insert(digest.to_string());
                        }
                    }
                }
            }
        }

        Ok(referenced)
    }

    async fn evict_lru(&self, tenant_id: &str, report: &mut GcReport) -> StorageResult<()> {
        let blobs = self.storage.list_blobs(tenant_id).await?;
        let mut total_size: u64 = 0;

        // Collect sizes
        let mut blob_sizes: Vec<(String, u64)> = Vec::new();
        for digest in &blobs {
            if let Ok(size) = self.storage.blob_size(tenant_id, digest).await {
                total_size += size;
                blob_sizes.push((digest.clone(), size));
            }
        }

        if total_size <= self.max_size_bytes {
            return Ok(());
        }

        info!(
            tenant_id = %tenant_id,
            total_size_mb = total_size / (1024 * 1024),
            max_size_mb = self.max_size_bytes / (1024 * 1024),
            "storage over limit, evicting"
        );

        // Simple eviction: remove smallest blobs first to free space
        // (access times are not tracked, so smallest-first is used as a heuristic)
        blob_sizes.sort_by_key(|(_, size)| *size);

        for (digest, size) in blob_sizes {
            if total_size <= self.max_size_bytes {
                break;
            }
            info!(tenant_id = %tenant_id, digest = %digest, size_bytes = size, "GC: evicting blob (size limit)");
            if let Err(e) = self.storage.delete_blob(tenant_id, &digest).await {
                warn!(tenant_id = %tenant_id, digest = %digest, error = %e, "GC: failed to evict blob");
            } else {
                total_size -= size;
                report.evicted_blobs += 1;
                report.evicted_bytes += size;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct GcReport {
    pub deleted_blobs: usize,
    pub evicted_blobs: usize,
    pub evicted_bytes: u64,
}
