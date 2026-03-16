// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Artifact filesystem storage.

use std::path::{Path, PathBuf};

use muli_core::error::{MuliError, Result};
use sha2::{Digest, Sha256};
use tokio::fs;

/// Manages artifact storage on the filesystem.
pub struct ArtifactStorage {
    base_dir: PathBuf,
}

impl ArtifactStorage {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            base_dir: data_dir.join("pipeline").join("artifacts"),
        }
    }

    /// Store artifact data. Returns (size_bytes, sha256).
    pub async fn upload(
        &self,
        tenant_id: &str,
        run_id: &str,
        artifact_name: &str,
        data: &[u8],
    ) -> Result<(u64, String)> {
        validate_component(tenant_id)?;
        validate_component(run_id)?;
        validate_component(artifact_name)?;

        let dir = self
            .base_dir
            .join(tenant_id)
            .join(run_id)
            .join(artifact_name);
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| MuliError::Storage(format!("create artifact dir: {e}")))?;

        let file_path = dir.join("data");
        fs::write(&file_path, data)
            .await
            .map_err(|e| MuliError::Storage(format!("write artifact: {e}")))?;

        let mut hasher = Sha256::new();
        hasher.update(data);
        let sha256 = hex::encode(hasher.finalize());

        Ok((data.len() as u64, sha256))
    }

    /// Retrieve artifact data.
    pub async fn download(
        &self,
        tenant_id: &str,
        run_id: &str,
        artifact_name: &str,
    ) -> Result<Vec<u8>> {
        validate_component(tenant_id)?;
        validate_component(run_id)?;
        validate_component(artifact_name)?;

        let file_path = self
            .base_dir
            .join(tenant_id)
            .join(run_id)
            .join(artifact_name)
            .join("data");
        fs::read(&file_path)
            .await
            .map_err(|e| MuliError::Storage(format!("read artifact: {e}")))
    }

    /// Delete all artifacts for a run.
    pub async fn delete_run(&self, tenant_id: &str, run_id: &str) -> Result<()> {
        validate_component(tenant_id)?;
        validate_component(run_id)?;

        let dir = self.base_dir.join(tenant_id).join(run_id);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .await
                .map_err(|e| MuliError::Storage(format!("delete artifacts: {e}")))?;
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
    async fn test_upload_download() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ArtifactStorage::new(dir.path());
        let data = b"hello world binary data";
        let (size, sha) = storage.upload("t1", "run-1", "binary", data).await.unwrap();
        assert_eq!(size, data.len() as u64);
        assert!(!sha.is_empty());

        let downloaded = storage.download("t1", "run-1", "binary").await.unwrap();
        assert_eq!(downloaded, data);
    }

    #[tokio::test]
    async fn test_path_traversal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ArtifactStorage::new(dir.path());
        // tenant_id with ".."
        assert!(storage.upload("../evil", "run-1", "binary", b"x").await.is_err());
        // run_id with "/"
        assert!(storage.upload("t1", "run/evil", "binary", b"x").await.is_err());
        // artifact_name with ".."
        assert!(storage.upload("t1", "run-1", "../passwd", b"x").await.is_err());
        // empty component
        assert!(storage.upload("", "run-1", "binary", b"x").await.is_err());
        // backslash
        assert!(storage.upload("t1", "run-1", "a\\b", b"x").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_run() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ArtifactStorage::new(dir.path());
        storage.upload("t1", "run-1", "binary", b"data").await.unwrap();
        storage.upload("t1", "run-1", "report", b"data2").await.unwrap();
        storage.delete_run("t1", "run-1").await.unwrap();
        assert!(storage.download("t1", "run-1", "binary").await.is_err());
        assert!(storage.download("t1", "run-1", "report").await.is_err());
    }

    #[tokio::test]
    async fn test_download_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ArtifactStorage::new(dir.path());
        assert!(storage.download("t1", "run-1", "missing").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_run_ok() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ArtifactStorage::new(dir.path());
        // Deleting a non-existent run should not error
        storage.delete_run("t1", "nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_sha256_correctness() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ArtifactStorage::new(dir.path());
        let data = b"test data for sha256";
        let (_, sha) = storage.upload("t1", "run-1", "file", data).await.unwrap();

        let mut hasher = Sha256::new();
        hasher.update(data);
        let expected = hex::encode(hasher.finalize());
        assert_eq!(sha, expected);
    }
}
