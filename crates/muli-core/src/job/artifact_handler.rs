// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Trait for artifact upload/restore operations, allowing the engine to remain
//! decoupled from the pipeline crate's tar/storage implementation.

use std::path::Path;

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait ArtifactHandler: Send + Sync {
    /// Tar the given workspace-relative paths and upload to artifact storage.
    async fn upload_artifacts(
        &self,
        tenant_id: &str,
        run_id: &str,
        job_name: &str,
        workspace: &Path,
        paths: &[String],
    ) -> Result<()>;

    /// Download and extract artifacts produced by each of the named jobs.
    async fn restore_artifacts(
        &self,
        tenant_id: &str,
        run_id: &str,
        needed_jobs: &[&str],
        workspace: &Path,
    ) -> Result<()>;
}
