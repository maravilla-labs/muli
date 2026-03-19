// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! ArtifactManager: tar/untar integration with ArtifactStorage.
//!
//! # Tests
//!
//! ```text
//! cargo test -p muli-pipeline artifact::manager
//! ```

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};

use muli_core::error::{MuliError, Result};
use muli_core::job::artifact_handler::ArtifactHandler;
use muli_core::pipeline::model::Artifact;
use muli_core::traits::ArtifactStore;

use super::storage::ArtifactStorage;

pub struct ArtifactManager {
    storage: Arc<ArtifactStorage>,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl ArtifactManager {
    pub fn new(storage: Arc<ArtifactStorage>) -> Self {
        Self {
            storage,
            artifact_store: None,
        }
    }

    pub fn with_artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self
    }
}

#[async_trait]
impl ArtifactHandler for ArtifactManager {
    async fn upload_artifacts(
        &self,
        tenant_id: &str,
        run_id: &str,
        job_name: &str,
        workspace: &Path,
        paths: &[String],
    ) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }

        let workspace = workspace.to_path_buf();
        let paths = paths.to_vec();

        // Build tar archive in a blocking task (tar crate is synchronous)
        let tar_bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let buf = Vec::new();
            let mut builder = tar::Builder::new(buf);

            for rel_path in &paths {
                let full_path = workspace.join(rel_path.trim_end_matches('/'));
                if !full_path.exists() {
                    warn!(path = %full_path.display(), "artifact path does not exist, skipping");
                    continue;
                }
                if full_path.is_dir() {
                    builder.append_dir_all(rel_path, &full_path).map_err(|e| {
                        MuliError::Storage(format!("tar directory '{rel_path}': {e}"))
                    })?;
                } else {
                    let mut f = std::fs::File::open(&full_path)
                        .map_err(|e| MuliError::Storage(format!("open '{rel_path}': {e}")))?;
                    builder
                        .append_file(rel_path, &mut f)
                        .map_err(|e| MuliError::Storage(format!("tar file '{rel_path}': {e}")))?;
                }
            }

            builder
                .into_inner()
                .map_err(|e| MuliError::Storage(format!("finalize tar: {e}")))
        })
        .await
        .map_err(|e| MuliError::Storage(format!("spawn_blocking: {e}")))??;

        let (size_bytes, sha256) = self
            .storage
            .upload(tenant_id, run_id, job_name, &tar_bytes)
            .await?;

        // Create artifact metadata in DB so DownloadArtifact can find it
        if let Some(ref store) = self.artifact_store {
            let artifact = Artifact::new(
                tenant_id.to_string(),
                run_id.to_string(),
                job_name.to_string(), // step_name
                job_name.to_string(), // name
                size_bytes,
                sha256,
                None, // expires_at
            );
            if let Err(e) = store.create_artifact(&artifact).await {
                warn!(error = %e, "failed to create artifact metadata record");
            }
        }

        info!(
            tenant_id,
            run_id,
            job_name,
            size_bytes,
            "uploaded job artifacts"
        );

        Ok(())
    }

    async fn restore_artifacts(
        &self,
        tenant_id: &str,
        run_id: &str,
        needed_jobs: &[&str],
        workspace: &Path,
    ) -> Result<()> {
        for &job_name in needed_jobs {
            let tar_bytes = match self.storage.download(tenant_id, run_id, job_name).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        tenant_id,
                        run_id,
                        job_name,
                        error = %e,
                        "artifact not found, skipping restore"
                    );
                    continue;
                }
            };

            let workspace = workspace.to_path_buf();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let cursor = std::io::Cursor::new(tar_bytes);
                let mut archive = tar::Archive::new(cursor);
                // tar 0.4.16+ rejects entries with absolute paths or `..` traversal.
                // set_preserve_permissions(false) prevents setuid/setgid bit restoration.
                archive.set_preserve_permissions(false);
                archive.set_overwrite(true);
                archive
                    .unpack(&workspace)
                    .map_err(|e| MuliError::Storage(format!("unpack artifact: {e}")))?;
                Ok(())
            })
            .await
            .map_err(|e| MuliError::Storage(format!("spawn_blocking: {e}")))??;

            info!(tenant_id, run_id, job_name, "restored job artifacts");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::storage::ArtifactStorage;

    fn make_manager(dir: &std::path::Path) -> ArtifactManager {
        ArtifactManager::new(Arc::new(ArtifactStorage::new(dir)))
    }

    /// Single file roundtrip: upload → restore → file present in new workspace.
    #[tokio::test]
    async fn test_upload_restore_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let manager = make_manager(dir.path());

        // Create workspace with a file
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("output.txt"), b"hello artifact").unwrap();

        manager
            .upload_artifacts(
                "t1",
                "run-1",
                "build",
                workspace.path(),
                &["output.txt".to_string()],
            )
            .await
            .unwrap();

        // Restore into a fresh workspace
        let restore_ws = tempfile::tempdir().unwrap();
        manager
            .restore_artifacts("t1", "run-1", &["build"], restore_ws.path())
            .await
            .unwrap();

        let content = std::fs::read(restore_ws.path().join("output.txt")).unwrap();
        assert_eq!(content, b"hello artifact");
    }

    /// Directory roundtrip: upload → restore → all nested files present.
    #[tokio::test]
    async fn test_upload_restore_directory() {
        let dir = tempfile::tempdir().unwrap();
        let manager = make_manager(dir.path());

        let workspace = tempfile::tempdir().unwrap();
        let sub = workspace.path().join("dist").join("js");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("app.js"), b"console.log('hello')").unwrap();
        std::fs::write(workspace.path().join("dist").join("index.html"), b"<html/>").unwrap();

        manager
            .upload_artifacts(
                "t1",
                "run-2",
                "build",
                workspace.path(),
                &["dist/".to_string()],
            )
            .await
            .unwrap();

        let restore_ws = tempfile::tempdir().unwrap();
        manager
            .restore_artifacts("t1", "run-2", &["build"], restore_ws.path())
            .await
            .unwrap();

        assert!(restore_ws.path().join("dist").join("index.html").exists());
        let js = std::fs::read(restore_ws.path().join("dist").join("js").join("app.js")).unwrap();
        assert_eq!(js, b"console.log('hello')");
    }

    /// Empty paths list → no upload, no error.
    #[tokio::test]
    async fn test_upload_empty_paths_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let manager = make_manager(dir.path());
        let workspace = tempfile::tempdir().unwrap();

        // Should return Ok without creating any artifact
        manager
            .upload_artifacts("t1", "run-3", "build", workspace.path(), &[])
            .await
            .unwrap();
    }

    /// Missing artifact on restore is skipped (warning only, no error).
    #[tokio::test]
    async fn test_restore_missing_artifact_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let manager = make_manager(dir.path());
        let restore_ws = tempfile::tempdir().unwrap();

        // "missing-job" was never uploaded — should not error
        manager
            .restore_artifacts("t1", "run-4", &["missing-job"], restore_ws.path())
            .await
            .unwrap();
    }

    /// Multiple needed jobs restored in sequence — all files land in workspace.
    #[tokio::test]
    async fn test_restore_multiple_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let manager = make_manager(dir.path());

        // Upload artifacts from two jobs
        let ws_a = tempfile::tempdir().unwrap();
        std::fs::write(ws_a.path().join("a.txt"), b"from-a").unwrap();
        manager
            .upload_artifacts("t1", "run-5", "job-a", ws_a.path(), &["a.txt".to_string()])
            .await
            .unwrap();

        let ws_b = tempfile::tempdir().unwrap();
        std::fs::write(ws_b.path().join("b.txt"), b"from-b").unwrap();
        manager
            .upload_artifacts("t1", "run-5", "job-b", ws_b.path(), &["b.txt".to_string()])
            .await
            .unwrap();

        // Restore both into a single workspace
        let restore_ws = tempfile::tempdir().unwrap();
        manager
            .restore_artifacts("t1", "run-5", &["job-a", "job-b"], restore_ws.path())
            .await
            .unwrap();

        assert_eq!(
            std::fs::read(restore_ws.path().join("a.txt")).unwrap(),
            b"from-a"
        );
        assert_eq!(
            std::fs::read(restore_ws.path().join("b.txt")).unwrap(),
            b"from-b"
        );
    }

    /// Non-existent path in upload list is skipped with a warning, rest still uploaded.
    #[tokio::test]
    async fn test_upload_missing_path_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let manager = make_manager(dir.path());

        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("real.txt"), b"exists").unwrap();

        // "phantom.txt" does not exist — should be skipped, real.txt still included
        manager
            .upload_artifacts(
                "t1",
                "run-6",
                "build",
                workspace.path(),
                &["real.txt".to_string(), "phantom.txt".to_string()],
            )
            .await
            .unwrap();

        // Restore and verify real.txt is present
        let restore_ws = tempfile::tempdir().unwrap();
        manager
            .restore_artifacts("t1", "run-6", &["build"], restore_ws.path())
            .await
            .unwrap();

        assert!(restore_ws.path().join("real.txt").exists());
        assert!(!restore_ws.path().join("phantom.txt").exists());
    }
}
