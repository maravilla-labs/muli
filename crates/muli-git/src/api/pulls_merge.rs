// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pull request merge with three-way merge via git2.

/// Merge error variants.
pub enum MergeError {
    Conflict(Vec<String>),
    Other(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::Conflict(files) => write!(f, "conflict in files: {files:?}"),
            MergeError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// Perform a 3-way merge of `source_branch` into `target_branch`.
///
/// Returns the merge commit SHA on success.
pub fn perform_merge(
    repo_path: &std::path::Path,
    source_branch: &str,
    target_branch: &str,
) -> Result<String, MergeError> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MergeError::Other(format!("failed to open repo: {e}")))?;

    let source_ref = repo
        .find_branch(source_branch, git2::BranchType::Local)
        .map_err(|e| MergeError::Other(format!("source branch not found: {e}")))?;
    let source_commit = source_ref
        .get()
        .peel_to_commit()
        .map_err(|e| MergeError::Other(format!("failed to peel source ref: {e}")))?;

    let target_ref = repo
        .find_branch(target_branch, git2::BranchType::Local)
        .map_err(|e| MergeError::Other(format!("target branch not found: {e}")))?;
    let target_commit = target_ref
        .get()
        .peel_to_commit()
        .map_err(|e| MergeError::Other(format!("failed to peel target ref: {e}")))?;

    let merge_base_oid = repo
        .merge_base(source_commit.id(), target_commit.id())
        .map_err(|e| MergeError::Other(format!("failed to find merge base: {e}")))?;
    let merge_base_commit = repo
        .find_commit(merge_base_oid)
        .map_err(|e| MergeError::Other(format!("failed to find merge base commit: {e}")))?;
    let merge_base_tree = merge_base_commit
        .tree()
        .map_err(|e| MergeError::Other(format!("failed to get merge base tree: {e}")))?;

    let source_tree = source_commit
        .tree()
        .map_err(|e| MergeError::Other(format!("failed to get source tree: {e}")))?;
    let target_tree = target_commit
        .tree()
        .map_err(|e| MergeError::Other(format!("failed to get target tree: {e}")))?;

    let mut merge_index = repo
        .merge_trees(&merge_base_tree, &target_tree, &source_tree, None)
        .map_err(|e| MergeError::Other(format!("merge failed: {e}")))?;

    if merge_index.has_conflicts() {
        let conflicts: Vec<String> = merge_index
            .conflicts()
            .map_err(|e| MergeError::Other(format!("failed to list conflicts: {e}")))?
            .filter_map(|c| {
                c.ok().and_then(|conflict| {
                    conflict
                        .our
                        .or(conflict.their)
                        .or(conflict.ancestor)
                        .and_then(|entry| std::str::from_utf8(&entry.path).ok().map(String::from))
                })
            })
            .collect();
        return Err(MergeError::Conflict(conflicts));
    }

    let merged_tree_oid = merge_index
        .write_tree_to(&repo)
        .map_err(|e| MergeError::Other(format!("failed to write merged tree: {e}")))?;
    let merged_tree = repo
        .find_tree(merged_tree_oid)
        .map_err(|e| MergeError::Other(format!("failed to find merged tree: {e}")))?;

    let sig = git2::Signature::now("muli", "muli@localhost")
        .map_err(|e| MergeError::Other(format!("failed to create signature: {e}")))?;

    let merge_commit_oid = repo
        .commit(
            Some(&format!("refs/heads/{target_branch}")),
            &sig,
            &sig,
            &format!("Merge '{source_branch}' into '{target_branch}'"),
            &merged_tree,
            &[&target_commit, &source_commit],
        )
        .map_err(|e| MergeError::Other(format!("failed to create merge commit: {e}")))?;

    Ok(merge_commit_oid.to_string())
}
