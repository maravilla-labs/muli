// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker volume lifecycle management.

use std::fs;
use std::path::{Path, PathBuf};

use tracing::{debug, info};

use muli_core::error::{MuliError, Result};

/// Create a temporary directory for a job's /workspace mount.
pub fn create_temp_dir(job_id: &str) -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("muli-workspace-{job_id}"));

    fs::create_dir_all(&dir).map_err(|e| {
        MuliError::Storage(format!(
            "Failed to create temp directory {}: {}",
            dir.display(),
            e
        ))
    })?;
    prepare_workspace_for_container(&dir)?;

    info!(path = %dir.display(), job_id = %job_id, "Workspace temp directory created");
    Ok(dir)
}

/// Normalize workspace permissions so capability-dropped containers can write to `/workspace`.
pub fn prepare_workspace_for_container(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        prepare_workspace_permissions_unix(path)?;
    }

    Ok(())
}

/// Remove a job's temporary workspace directory.
pub fn cleanup_temp_dir(job_id: &str) -> Result<()> {
    let dir = std::env::temp_dir().join(format!("muli-workspace-{job_id}"));

    if dir.exists() {
        fs::remove_dir_all(&dir).map_err(|e| {
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

#[cfg(unix)]
fn prepare_workspace_permissions_unix(path: &Path) -> Result<()> {
    prepare_workspace_entry(path)
}

#[cfg(unix)]
fn prepare_workspace_entry(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::symlink_metadata(path).map_err(|e| {
        MuliError::Storage(format!(
            "Failed to stat workspace path {}: {}",
            path.display(),
            e
        ))
    })?;

    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Ok(());
    }

    let mut permissions = metadata.permissions();
    let current_mode = permissions.mode();
    let desired_mode = if file_type.is_dir() {
        current_mode | 0o777
    } else {
        current_mode | 0o222
    };

    if desired_mode != current_mode {
        permissions.set_mode(desired_mode);
        fs::set_permissions(path, permissions).map_err(|e| {
            MuliError::Storage(format!(
                "Failed to set workspace permissions on {}: {}",
                path.display(),
                e
            ))
        })?;
    }

    if file_type.is_dir() {
        for entry in fs::read_dir(path).map_err(|e| {
            MuliError::Storage(format!(
                "Failed to read workspace directory {}: {}",
                path.display(),
                e
            ))
        })? {
            let entry = entry.map_err(|e| {
                MuliError::Storage(format!(
                    "Failed to read workspace entry in {}: {}",
                    path.display(),
                    e
                ))
            })?;
            prepare_workspace_entry(&entry.path())?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn created_workspace_is_writable_for_capability_dropped_containers() {
        let job_id = format!("volume-test-{}", std::process::id());
        let dir = create_temp_dir(&job_id).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o777);
        }

        cleanup_temp_dir(&job_id).unwrap();
    }

    #[test]
    fn prepare_workspace_normalizes_nested_tree_permissions() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("workspace");
        let nested = root.join("src");
        let file = nested.join("index.ts");

        fs::create_dir_all(&nested).unwrap();
        fs::write(&file, "console.log('hi')\n").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
            fs::set_permissions(&nested, fs::Permissions::from_mode(0o755)).unwrap();
            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();

            prepare_workspace_for_container(&root).unwrap();

            let root_mode = fs::metadata(&root).unwrap().permissions().mode() & 0o777;
            let nested_mode = fs::metadata(&nested).unwrap().permissions().mode() & 0o777;
            let file_mode = fs::metadata(&file).unwrap().permissions().mode() & 0o777;

            assert_eq!(root_mode, 0o777);
            assert_eq!(nested_mode, 0o777);
            assert_eq!(file_mode, 0o666);
        }
    }
}
