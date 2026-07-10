// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Bare repository management on the local filesystem.

use std::path::PathBuf;

use async_trait::async_trait;
use muli_core::error::{MuliError, Result as CoreResult};
use muli_core::traits::GitStorage;

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
            // Directory already exists (e.g. re-link after unlink which only
            // removes the DB record). Skip git init — the bare repo on disk is
            // still valid and we just need a fresh DB entry.
            return Ok(path);
        }
        tokio::fs::create_dir_all(&path)
            .await
            .map_err(GitStorageError::Io)?;

        let path_str = path.to_str().ok_or_else(|| {
            GitStorageError::GitCommand("repository path contains invalid UTF-8".into())
        })?;

        let status = tokio::process::Command::new("git")
            .args(["init", "--bare", "-b", "main", path_str])
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

    /// Create a lightweight tag `tag` pointing at `sha` in the bare repository.
    ///
    /// Idempotent: if a tag of the same name already exists, this is a no-op and
    /// returns `Ok(())` (a re-run that pushed the same tag must not fail). This is
    /// the real implementation of the pipeline `release.create_tag` flag.
    pub async fn create_tag(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        tag: &str,
        sha: &str,
    ) -> Result<(), GitStorageError> {
        validate_tag_name(tag)?;
        let git_dir = self.existing_git_dir(tenant_id, namespace, name)?;

        // Idempotent: accept a pre-existing tag of the same name.
        let existing = tokio::process::Command::new("git")
            .args([
                "--git-dir",
                &git_dir,
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/tags/{tag}"),
            ])
            .status()
            .await
            .map_err(GitStorageError::Io)?;
        if existing.success() {
            return Ok(());
        }

        // Create a lightweight tag at the target commit. `--` separates the
        // tag/commit operands from options so a leading dash can't be an option.
        let output = tokio::process::Command::new("git")
            .args(["--git-dir", &git_dir, "tag", tag, sha])
            .output()
            .await
            .map_err(GitStorageError::Io)?;
        if !output.status.success() {
            return Err(GitStorageError::GitCommand(format!(
                "git tag {tag} {sha} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    /// Commit subjects reachable from `sha` since the previous tag, one per line
    /// as `- <subject> (<short-sha>)`. Used for `release.notes.from: git_log`.
    ///
    /// "Previous tag" is the most recent tag reachable from `sha`, excluding the
    /// release's own `tag`. When there is no previous tag, all commits reachable
    /// from `sha` are returned (capped). Computed server-side because the job's
    /// checkout is shallow and tag-less.
    pub async fn log_since_previous_tag(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        tag: &str,
        sha: &str,
    ) -> Result<String, GitStorageError> {
        let git_dir = self.existing_git_dir(tenant_id, namespace, name)?;

        // Most recent tag reachable from `sha`, excluding the release's own tag.
        let prev = tokio::process::Command::new("git")
            .args([
                "--git-dir",
                &git_dir,
                "describe",
                "--tags",
                "--abbrev=0",
                "--exclude",
                tag,
                sha,
            ])
            .output()
            .await
            .map_err(GitStorageError::Io)?;
        let range = if prev.status.success() {
            let prev_tag = String::from_utf8_lossy(&prev.stdout).trim().to_string();
            if prev_tag.is_empty() {
                sha.to_string()
            } else {
                format!("{prev_tag}..{sha}")
            }
        } else {
            // No previous tag: log everything reachable from `sha`.
            sha.to_string()
        };

        let log = tokio::process::Command::new("git")
            .args([
                "--git-dir",
                &git_dir,
                "log",
                "--no-merges",
                "--pretty=format:- %s (%h)",
                "-n",
                "200",
                &range,
            ])
            .output()
            .await
            .map_err(GitStorageError::Io)?;
        if !log.status.success() {
            return Err(GitStorageError::GitCommand(format!(
                "git log {range} failed: {}",
                String::from_utf8_lossy(&log.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&log.stdout).trim().to_string())
    }

    /// Resolve the bare repo path, ensure it exists, and return it as a `String`.
    fn existing_git_dir(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<String, GitStorageError> {
        let path = self.repo_path(tenant_id, namespace, name);
        if !path.exists() {
            return Err(GitStorageError::NotFound(path.display().to_string()));
        }
        path.to_str()
            .map(str::to_string)
            .ok_or_else(|| GitStorageError::GitCommand("repository path contains invalid UTF-8".into()))
    }
}

/// Reject tag names that could be misread as `git` options or break ref format.
/// Git validates the full ref format itself; this guards the argv boundary.
fn validate_tag_name(tag: &str) -> Result<(), GitStorageError> {
    if tag.is_empty() || tag.starts_with('-') || tag.contains("..") || tag.contains(char::is_whitespace)
    {
        return Err(GitStorageError::GitCommand(format!(
            "invalid tag name: {tag:?}"
        )));
    }
    Ok(())
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

#[async_trait]
impl GitStorage for FilesystemStorage {
    async fn init_repo(&self, tenant_id: &str, namespace: &str, name: &str) -> CoreResult<PathBuf> {
        self.init_repo(tenant_id, namespace, name)
            .await
            .map_err(|e| MuliError::Storage(e.to_string()))
    }

    async fn delete_repo(&self, tenant_id: &str, namespace: &str, name: &str) -> CoreResult<()> {
        self.delete_repo(tenant_id, namespace, name)
            .await
            .map_err(|e| MuliError::Storage(e.to_string()))
    }

    async fn fork_repo(
        &self,
        src_tenant: &str,
        src_namespace: &str,
        src_name: &str,
        dst_tenant: &str,
        dst_namespace: &str,
        dst_name: &str,
    ) -> CoreResult<()> {
        self.fork_repo(
            src_tenant,
            src_namespace,
            src_name,
            dst_tenant,
            dst_namespace,
            dst_name,
        )
        .await
        .map(|_| ())
        .map_err(|e| MuliError::Storage(e.to_string()))
    }

    async fn transfer_repo(
        &self,
        tenant_id: &str,
        old_namespace: &str,
        name: &str,
        new_namespace: &str,
    ) -> CoreResult<()> {
        self.transfer_repo(tenant_id, old_namespace, name, new_namespace)
            .await
            .map_err(|e| MuliError::Storage(e.to_string()))
    }

    fn repo_path(&self, tenant_id: &str, namespace: &str, name: &str) -> PathBuf {
        self.repo_path(tenant_id, namespace, name)
    }
}

#[cfg(test)]
mod tag_tests {
    use super::*;

    async fn git(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
        tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .expect("git runs")
    }

    #[tokio::test]
    async fn create_tag_is_idempotent_and_git_log_works() {
        // Skip gracefully when the git binary is unavailable.
        if check_git_available().await.is_err() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(root.path().to_str().unwrap())
            .await
            .unwrap();
        storage.init_repo("t1", "ns", "repo").await.unwrap();
        let bare = storage.repo_path("t1", "ns", "repo");
        let bare_str = bare.to_str().unwrap().to_string();

        // Clone the bare repo, make two commits, push them back.
        let work = tempfile::tempdir().unwrap();
        let wt = work.path().join("wt");
        assert!(
            git(root.path(), &["clone", &bare_str, wt.to_str().unwrap()])
                .await
                .status
                .success()
        );
        for msg in ["first commit", "second commit"] {
            let out = git(
                &wt,
                &[
                    "-c",
                    "user.email=t@example.com",
                    "-c",
                    "user.name=T",
                    "commit",
                    "--allow-empty",
                    "-m",
                    msg,
                ],
            )
            .await;
            assert!(
                out.status.success(),
                "commit failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let branch = String::from_utf8_lossy(
            &git(&wt, &["rev-parse", "--abbrev-ref", "HEAD"]).await.stdout,
        )
        .trim()
        .to_string();
        assert!(git(&wt, &["push", "origin", &branch]).await.status.success());
        let sha = String::from_utf8_lossy(&git(&wt, &["rev-parse", "HEAD"]).await.stdout)
            .trim()
            .to_string();

        // create_tag writes the tag...
        storage
            .create_tag("t1", "ns", "repo", "v1.0.0", &sha)
            .await
            .unwrap();
        let show = tokio::process::Command::new("git")
            .args([
                "--git-dir",
                &bare_str,
                "show-ref",
                "--verify",
                "--quiet",
                "refs/tags/v1.0.0",
            ])
            .status()
            .await
            .unwrap();
        assert!(show.success(), "tag should exist after create_tag");

        // ...and a second call is a no-op (idempotent re-run).
        storage
            .create_tag("t1", "ns", "repo", "v1.0.0", &sha)
            .await
            .unwrap();

        // A tag name that looks like an option is rejected at the argv boundary.
        assert!(
            storage
                .create_tag("t1", "ns", "repo", "-rf", &sha)
                .await
                .is_err()
        );

        // No previous tag → all commit subjects reachable from the sha.
        let notes = storage
            .log_since_previous_tag("t1", "ns", "repo", "v1.0.0", &sha)
            .await
            .unwrap();
        assert!(notes.contains("first commit"), "notes: {notes}");
        assert!(notes.contains("second commit"), "notes: {notes}");
    }
}
