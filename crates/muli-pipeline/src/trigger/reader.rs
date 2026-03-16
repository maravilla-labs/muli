// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Read pipeline YAML from bare git repositories.

use std::path::Path;

use muli_core::error::{MuliError, Result};

/// Read `.maravilla/pipeline.yml` from a bare git repository at a specific commit.
pub fn read_pipeline_yaml(repo_path: &Path, commit_sha: &str) -> Result<Option<String>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;

    let oid = git2::Oid::from_str(commit_sha)
        .map_err(|e| MuliError::Pipeline(format!("invalid commit SHA: {e}")))?;

    let commit = repo
        .find_commit(oid)
        .map_err(|e| MuliError::Pipeline(format!("commit not found: {e}")))?;

    let tree = commit
        .tree()
        .map_err(|e| MuliError::Pipeline(format!("cannot read tree: {e}")))?;

    let entry = match tree.get_path(Path::new(".maravilla/pipeline.yml")) {
        Ok(e) => e,
        Err(_) => return Ok(None),
    };

    let blob = repo
        .find_blob(entry.id())
        .map_err(|e| MuliError::Pipeline(format!("cannot read pipeline blob: {e}")))?;

    let content = std::str::from_utf8(blob.content())
        .map_err(|e| MuliError::Pipeline(format!("pipeline.yml is not valid UTF-8: {e}")))?;

    Ok(Some(content.to_string()))
}

/// Read pipeline YAML from HEAD of a bare repository.
pub fn read_pipeline_yaml_from_head(repo_path: &Path) -> Result<Option<String>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;

    let head = match repo.head() {
        Ok(h) => h,
        Err(_) => return Ok(None),
    };

    let commit = head
        .peel_to_commit()
        .map_err(|e| MuliError::Pipeline(format!("cannot peel HEAD to commit: {e}")))?;

    read_pipeline_yaml(repo_path, &commit.id().to_string())
}
