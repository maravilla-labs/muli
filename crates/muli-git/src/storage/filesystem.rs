// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Bare repository management on the local filesystem.

use std::path::PathBuf;

/// Manages bare git repositories on disk.
///
/// Repository layout: `{root}/{tenant_id}/{namespace}/{repo_name}.git`
pub struct FilesystemStorage {
    root: PathBuf,
}

impl FilesystemStorage {
    /// Create a new `FilesystemStorage`, ensuring the root directory exists.
    pub async fn new(root: &str) -> Result<Self, std::io::Error> {
        tokio::fs::create_dir_all(root).await?;
        Ok(Self {
            root: PathBuf::from(root),
        })
    }

    /// Returns the path to the bare repository:
    /// `{root}/{tenant_id}/{namespace}/{repo_name}.git`
    pub fn repo_path(&self, tenant_id: &str, namespace: &str, name: &str) -> PathBuf {
        self.root
            .join(tenant_id)
            .join(namespace)
            .join(format!("{name}.git"))
    }

    /// Initialize a new bare repository at the computed path.
    pub async fn init_repo(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<PathBuf, GitStorageError> {
        let path = self.repo_path(tenant_id, namespace, name);
        if path.exists() {
            return Err(GitStorageError::AlreadyExists(path.display().to_string()));
        }
        tokio::fs::create_dir_all(&path)
            .await
            .map_err(GitStorageError::Io)?;

        let path_str = path.to_str().ok_or_else(|| {
            GitStorageError::GitCommand("repository path contains invalid UTF-8".into())
        })?;

        let status = tokio::process::Command::new("git")
            .args(["init", "--bare", path_str])
            .status()
            .await
            .map_err(GitStorageError::Io)?;

        if !status.success() {
            return Err(GitStorageError::GitCommand("git init --bare failed".into()));
        }
        Ok(path)
    }

    /// Delete a bare repository directory.
    pub async fn delete_repo(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<(), GitStorageError> {
        let path = self.repo_path(tenant_id, namespace, name);
        if !path.exists() {
            return Err(GitStorageError::NotFound(path.display().to_string()));
        }
        tokio::fs::remove_dir_all(&path)
            .await
            .map_err(GitStorageError::Io)
    }

    /// Fork a repository by cloning it as a bare mirror.
    pub async fn fork_repo(
        &self,
        src_tenant: &str,
        src_namespace: &str,
        src_name: &str,
        dst_tenant: &str,
        dst_namespace: &str,
        dst_name: &str,
    ) -> Result<PathBuf, GitStorageError> {
        let src = self.repo_path(src_tenant, src_namespace, src_name);
        let dst = self.repo_path(dst_tenant, dst_namespace, dst_name);

        if !src.exists() {
            return Err(GitStorageError::NotFound(src.display().to_string()));
        }
        if dst.exists() {
            return Err(GitStorageError::AlreadyExists(dst.display().to_string()));
        }

        // Ensure parent directory exists
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(GitStorageError::Io)?;
        }

        let src_str = src.to_str().ok_or_else(|| {
            GitStorageError::GitCommand("source repository path contains invalid UTF-8".into())
        })?;
        let dst_str = dst.to_str().ok_or_else(|| {
            GitStorageError::GitCommand("destination repository path contains invalid UTF-8".into())
        })?;

        let status = tokio::process::Command::new("git")
            .args(["clone", "--bare", src_str, dst_str])
            .status()
            .await
            .map_err(GitStorageError::Io)?;

        if !status.success() {
            return Err(GitStorageError::GitCommand(
                "git clone --bare failed".into(),
            ));
        }
        Ok(dst)
    }

    /// Transfer (rename) a repository from one namespace to another.
    /// Uses tokio::fs::rename (atomic on the same filesystem).
    pub async fn transfer_repo(
        &self,
        tenant_id: &str,
        old_namespace: &str,
        name: &str,
        new_namespace: &str,
    ) -> Result<(), GitStorageError> {
        let src = self.repo_path(tenant_id, old_namespace, name);
        let dst = self.repo_path(tenant_id, new_namespace, name);

        if !src.exists() {
            return Err(GitStorageError::NotFound(src.display().to_string()));
        }
        if dst.exists() {
            return Err(GitStorageError::AlreadyExists(dst.display().to_string()));
        }

        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(GitStorageError::Io)?;
        }

        tokio::fs::rename(&src, &dst)
            .await
            .map_err(GitStorageError::Io)
    }
}

/// Check that the `git` binary is available and executable.
/// Returns the git version string on success.
pub async fn check_git_available() -> Result<String, GitStorageError> {
    let output = tokio::process::Command::new("git")
        .arg("--version")
        .output()
        .await
        .map_err(|e| {
            GitStorageError::GitCommand(format!(
                "git binary not found: {e}. Install git to use muli-git."
            ))
        })?;

    if !output.status.success() {
        return Err(GitStorageError::GitCommand(
            "git --version returned non-zero exit code".into(),
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum GitStorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("repository already exists: {0}")]
    AlreadyExists(String),

    #[error("repository not found: {0}")]
    NotFound(String),

    #[error("git command failed: {0}")]
    GitCommand(String),
}
