// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Dependency cache with zstd compression.

use std::path::{Path, PathBuf};

use muli_core::error::{MuliError, Result};
use sha2::{Digest, Sha256};
use tokio::fs;

/// Manages dependency cache on the filesystem with zstd compression.
pub struct CacheManager {
    base_dir: PathBuf,
}

impl CacheManager {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            base_dir: data_dir.join("pipeline").join("cache"),
        }
    }

    /// Save cache data. Returns (size_bytes, sha256).
    pub async fn save(
        &self,
        tenant_id: &str,
        repo_id: &str,
        cache_key: &str,
        data: &[u8],
    ) -> Result<(u64, String)> {
        validate_component(tenant_id)?;
        validate_component(repo_id)?;
        validate_component(cache_key)?;

        let dir = self.base_dir.join(tenant_id).join(repo_id);
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| MuliError::Storage(format!("create cache dir: {e}")))?;

        let compressed =
            zstd::encode_all(data, 3).map_err(|e| MuliError::Storage(format!("zstd compress: {e}")))?;

        let file_path = dir.join(format!("{cache_key}.tar.zst"));
        fs::write(&file_path, &compressed)
            .await
            .map_err(|e| MuliError::Storage(format!("write cache: {e}")))?;

        let mut hasher = Sha256::new();
        hasher.update(&compressed);
        let sha256 = hex::encode(hasher.finalize());

        Ok((compressed.len() as u64, sha256))
    }

    /// Restore cache data. Returns decompressed data.
    pub async fn restore(
        &self,
        tenant_id: &str,
        repo_id: &str,
        cache_key: &str,
    ) -> Result<Option<Vec<u8>>> {
        validate_component(tenant_id)?;
        validate_component(repo_id)?;
        validate_component(cache_key)?;

        let file_path = self
            .base_dir
            .join(tenant_id)
            .join(repo_id)
            .join(format!("{cache_key}.tar.zst"));
        if !file_path.exists() {
            return Ok(None);
        }

        let compressed = fs::read(&file_path)
            .await
            .map_err(|e| MuliError::Storage(format!("read cache: {e}")))?;
        let decompressed = zstd::decode_all(compressed.as_slice())
            .map_err(|e| MuliError::Storage(format!("zstd decompress: {e}")))?;

        Ok(Some(decompressed))
    }

    /// Delete a specific cache entry.
    pub async fn delete(
        &self,
        tenant_id: &str,
        repo_id: &str,
        cache_key: &str,
    ) -> Result<()> {
        validate_component(tenant_id)?;
        validate_component(repo_id)?;
        validate_component(cache_key)?;

        let file_path = self
            .base_dir
            .join(tenant_id)
            .join(repo_id)
            .join(format!("{cache_key}.tar.zst"));
        if file_path.exists() {
            fs::remove_file(&file_path)
                .await
                .map_err(|e| MuliError::Storage(format!("delete cache: {e}")))?;
        }
        Ok(())
    }
}

fn validate_component(s: &str) -> Result<()> {
    if s.is_empty()
        || s.contains("..")
        || s.contains('/')
        || s.contains('\\')
        || s.contains('\0')
    {
        return Err(MuliError::Validation(format!(
            "invalid path component: '{s}'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_save_restore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::new(dir.path());
        let data = b"cargo target directory content";
        let (size, sha) = cache.save("t1", "repo-1", "cargo-lock", data).await.unwrap();
        assert!(size > 0);
        assert!(!sha.is_empty());

        let restored = cache.restore("t1", "repo-1", "cargo-lock").await.unwrap().unwrap();
        assert_eq!(restored, data);
    }

    #[tokio::test]
    async fn test_restore_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::new(dir.path());
        let result = cache.restore("t1", "repo-1", "missing").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::new(dir.path());
        cache.save("t1", "repo-1", "cargo-lock", b"data").await.unwrap();
        cache.delete("t1", "repo-1", "cargo-lock").await.unwrap();
        let result = cache.restore("t1", "repo-1", "cargo-lock").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_path_traversal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::new(dir.path());
        // tenant_id with ".."
        assert!(cache.save("../evil", "repo-1", "key", b"x").await.is_err());
        // repo_id with "/"
        assert!(cache.save("t1", "repo/evil", "key", b"x").await.is_err());
        // cache_key with ".."
        assert!(cache.save("t1", "repo-1", "../secrets", b"x").await.is_err());
        // empty
        assert!(cache.save("", "repo-1", "key", b"x").await.is_err());

        // Also test restore and delete with traversal
        assert!(cache.restore("../evil", "repo-1", "key").await.is_err());
        assert!(cache.delete("t1", "../evil", "key").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_ok() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::new(dir.path());
        // Deleting a non-existent cache should not error
        cache.delete("t1", "repo-1", "nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_zstd_compression_reduces_size() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::new(dir.path());
        // Highly compressible data
        let data = vec![0u8; 10_000];
        let (compressed_size, _) = cache.save("t1", "repo-1", "zeros", &data).await.unwrap();
        assert!(compressed_size < data.len() as u64, "zstd should compress zeros significantly");
    }
}
