// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pure git-metadata helpers used by the pipeline trigger.
//!
//! These are free functions with no dependency on `PipelineTriggerImpl`; they
//! read a bare repository on disk (via `git2`) to resolve changed paths, commit
//! info, and branch heads. They run inside `spawn_blocking`.

use std::collections::HashSet;
use std::path::Path;

use muli_core::error::MuliError;

use crate::pipeline_clone_url::CiCloneUrlTarget;
use muli_pipeline::yaml::schema::PipelineDef;

pub(crate) fn ci_clone_url_target(pipeline_def: &PipelineDef) -> CiCloneUrlTarget {
    if pipeline_def.jobs.is_empty() {
        CiCloneUrlTarget::ContainerClone
    } else {
        CiCloneUrlTarget::HostCheckout
    }
}

pub(crate) fn is_zero_sha(sha: &str) -> bool {
    !sha.is_empty() && sha.chars().all(|c| c == '0')
}

pub(crate) fn collect_tree_paths(tree: &git2::Tree<'_>) -> muli_core::error::Result<Vec<String>> {
    let mut paths = Vec::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |prefix, entry| {
        if entry.kind() == Some(git2::ObjectType::Blob) {
            if let Some(name) = entry.name() {
                paths.push(format!("{prefix}{name}"));
            }
        }
        git2::TreeWalkResult::Ok
    })
    .map_err(|e| MuliError::Pipeline(format!("cannot walk tree: {e}")))?;
    Ok(paths)
}

pub(crate) fn diff_paths_between(
    repo: &git2::Repository,
    from: git2::Oid,
    to: git2::Oid,
) -> muli_core::error::Result<Vec<String>> {
    let from_commit = repo
        .find_commit(from)
        .map_err(|e| MuliError::Pipeline(format!("cannot find base commit: {e}")))?;
    let to_commit = repo
        .find_commit(to)
        .map_err(|e| MuliError::Pipeline(format!("cannot find head commit: {e}")))?;
    let from_tree = from_commit
        .tree()
        .map_err(|e| MuliError::Pipeline(format!("cannot read base tree: {e}")))?;
    let to_tree = to_commit
        .tree()
        .map_err(|e| MuliError::Pipeline(format!("cannot read head tree: {e}")))?;
    let diff = repo
        .diff_tree_to_tree(Some(&from_tree), Some(&to_tree), None)
        .map_err(|e| MuliError::Pipeline(format!("cannot diff trees: {e}")))?;

    let mut paths = HashSet::new();
    for delta in diff.deltas() {
        if let Some(path) = delta.new_file().path().or(delta.old_file().path()) {
            paths.insert(path.to_string_lossy().into_owned());
        }
    }
    Ok(paths.into_iter().collect())
}

pub(crate) fn resolve_push_changed_paths(
    repo_path: &Path,
    old_sha: &str,
    new_sha: &str,
) -> muli_core::error::Result<Vec<String>> {
    if is_zero_sha(new_sha) {
        return Ok(Vec::new());
    }

    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;
    let new_oid = git2::Oid::from_str(new_sha)
        .map_err(|e| MuliError::Pipeline(format!("bad new sha: {e}")))?;

    if is_zero_sha(old_sha) {
        let new_commit = repo
            .find_commit(new_oid)
            .map_err(|e| MuliError::Pipeline(format!("cannot find new commit: {e}")))?;
        let new_tree = new_commit
            .tree()
            .map_err(|e| MuliError::Pipeline(format!("cannot read new tree: {e}")))?;
        return collect_tree_paths(&new_tree);
    }

    let old_oid = git2::Oid::from_str(old_sha)
        .map_err(|e| MuliError::Pipeline(format!("bad old sha: {e}")))?;
    diff_paths_between(&repo, old_oid, new_oid)
}

pub(crate) fn resolve_pr_changed_paths(
    repo_path: &Path,
    target_branch: &str,
    source_branch: &str,
) -> muli_core::error::Result<Vec<String>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;
    let target_oid = git2::Oid::from_str(&resolve_branch_head(repo_path, target_branch)?)
        .map_err(|e| MuliError::Pipeline(format!("bad target sha: {e}")))?;
    let source_oid = git2::Oid::from_str(&resolve_branch_head(repo_path, source_branch)?)
        .map_err(|e| MuliError::Pipeline(format!("bad source sha: {e}")))?;

    let base_oid = repo
        .merge_base(target_oid, source_oid)
        .unwrap_or(target_oid);
    diff_paths_between(&repo, base_oid, source_oid)
}

/// Extract commit message and author name from the given commit SHA.
/// Returns empty strings if the commit cannot be read.
pub(crate) fn resolve_commit_info(repo_path: &Path, commit_sha: &str) -> (String, String) {
    (|| -> Option<(String, String)> {
        let repo = git2::Repository::open(repo_path).ok()?;
        let oid = git2::Oid::from_str(commit_sha).ok()?;
        let commit = repo.find_commit(oid).ok()?;
        let message = commit.message().unwrap_or("").trim().to_string();
        let author = commit.author().name().unwrap_or("").to_string();
        Some((message, author))
    })()
    .unwrap_or_default()
}

/// Resolve the HEAD commit SHA for a branch in a bare repository.
pub(crate) fn resolve_branch_head(
    repo_path: &Path,
    branch: &str,
) -> muli_core::error::Result<String> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;

    let ref_name = format!("refs/heads/{branch}");
    let reference = repo
        .find_reference(&ref_name)
        .map_err(|e| MuliError::Pipeline(format!("branch '{branch}' not found: {e}")))?;

    let commit = reference
        .peel_to_commit()
        .map_err(|e| MuliError::Pipeline(format!("cannot resolve branch HEAD: {e}")))?;

    Ok(commit.id().to_string())
}
