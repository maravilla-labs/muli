// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker volume lifecycle management.

use std::path::PathBuf;

use tracing::{debug, info};

use muli_core::error::{MuliError, Result};

/// Create a temporary directory for a job's /workspace mount.
pub fn create_temp_dir(job_id: &str) -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("muli-workspace-{job_id}"));

    std::fs::create_dir_all(&dir).map_err(|e| {
        MuliError::Storage(format!(
            "Failed to create temp directory {}: {}",
            dir.display(),
            e
        ))
    })?;

    info!(path = %dir.display(), job_id = %job_id, "Workspace temp directory created");
    Ok(dir)
}

/// Remove a job's temporary workspace directory.
pub fn cleanup_temp_dir(job_id: &str) -> Result<()> {
    let dir = std::env::temp_dir().join(format!("muli-workspace-{job_id}"));

    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| {
            MuliError::Storage(format!(
                "Failed to remove temp directory {}: {}",
                dir.display(),
                e
            ))
        })?;
        debug!(path = %dir.display(), job_id = %job_id, "Workspace temp directory removed");
    }

    Ok(())
}
