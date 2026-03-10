// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pull request diff generation.

use std::cell::RefCell;
use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::json;

use crate::api::GitState;
use crate::api::helpers::resolve_repo_full;
use crate::tenant::TenantContext;

// ── Diff types ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct DiffFile {
    pub file: String,
    pub old_path: Option<String>,
    pub new_path: Option<String>,
    pub is_new: bool,
    pub is_deleted: bool,
    pub hunks: Vec<DiffHunk>,
}

#[derive(Debug, Serialize)]
pub struct DiffHunk {
    pub old_start: u32,
    pub old_lines: u32,
    pub new_start: u32,
    pub new_lines: u32,
    pub lines: Vec<DiffLine>,
}

#[derive(Debug, Serialize)]
pub struct DiffLine {
    #[serde(rename = "type")]
    pub line_type: String,
    pub content: String,
}

// ── Diff builder ─────────────────────────────────────────────────────────────

pub fn build_diff(
    repo_path: &std::path::Path,
    source_branch: &str,
    target_branch: &str,
) -> Result<Vec<DiffFile>, String> {
    let repo =
        git2::Repository::open(repo_path).map_err(|e| format!("failed to open repo: {e}"))?;

    let source_ref = repo
        .find_branch(source_branch, git2::BranchType::Local)
        .map_err(|e| format!("source branch not found: {e}"))?;
    let source_commit = source_ref
        .get()
        .peel_to_commit()
        .map_err(|e| format!("failed to peel source ref: {e}"))?;
    let source_tree = source_commit
        .tree()
        .map_err(|e| format!("failed to get source tree: {e}"))?;

    let target_ref = repo
        .find_branch(target_branch, git2::BranchType::Local)
        .map_err(|e| format!("target branch not found: {e}"))?;
    let target_commit = target_ref
        .get()
        .peel_to_commit()
        .map_err(|e| format!("failed to peel target ref: {e}"))?;
    let target_tree = target_commit
        .tree()
        .map_err(|e| format!("failed to get target tree: {e}"))?;

    let diff = repo
        .diff_tree_to_tree(Some(&target_tree), Some(&source_tree), None)
        .map_err(|e| format!("failed to compute diff: {e}"))?;

    build_diff_from_git2_diff(&diff)
}

/// Build DiffFile list from a git2 Diff object. Shared by PR diff and commit diff.
pub fn build_diff_from_git2_diff(diff: &git2::Diff<'_>) -> Result<Vec<DiffFile>, String> {
    let files: RefCell<Vec<DiffFile>> = RefCell::new(Vec::new());

    diff.foreach(
        &mut |delta, _progress| {
            let old_path = delta
                .old_file()
                .path()
                .and_then(|p| p.to_str())
                .map(String::from);
            let new_path = delta
                .new_file()
                .path()
                .and_then(|p| p.to_str())
                .map(String::from);
            let file_name = new_path
                .clone()
                .or_else(|| old_path.clone())
                .unwrap_or_default();
            let is_new = delta.status() == git2::Delta::Added;
            let is_deleted = delta.status() == git2::Delta::Deleted;
            files.borrow_mut().push(DiffFile {
                file: file_name,
                old_path,
                new_path,
                is_new,
                is_deleted,
                hunks: Vec::new(),
            });
            true
        },
        None,
        Some(&mut |delta, hunk| {
            let old_path = delta
                .old_file()
                .path()
                .and_then(|p| p.to_str())
                .map(String::from);
            let new_path = delta
                .new_file()
                .path()
                .and_then(|p| p.to_str())
                .map(String::from);
            let file_name = new_path
                .clone()
                .or_else(|| old_path.clone())
                .unwrap_or_default();

            let mut borrowed = files.borrow_mut();
            if let Some(file) = borrowed.iter_mut().find(|f| f.file == file_name) {
                file.hunks.push(DiffHunk {
                    old_start: hunk.old_start(),
                    old_lines: hunk.old_lines(),
                    new_start: hunk.new_start(),
                    new_lines: hunk.new_lines(),
                    lines: Vec::new(),
                });
            }
            true
        }),
        Some(&mut |delta, _hunk, line| {
            let old_path = delta
                .old_file()
                .path()
                .and_then(|p| p.to_str())
                .map(String::from);
            let new_path = delta
                .new_file()
                .path()
                .and_then(|p| p.to_str())
                .map(String::from);
            let file_name = new_path
                .clone()
                .or_else(|| old_path.clone())
                .unwrap_or_default();

            let line_type = match line.origin() {
                '+' => "added",
                '-' => "removed",
                _ => "context",
            };
            let content = std::str::from_utf8(line.content())
                .unwrap_or("")
                .trim_end_matches('\n')
                .to_string();

            let mut borrowed = files.borrow_mut();
            if let Some(file) = borrowed.iter_mut().find(|f| f.file == file_name)
                && let Some(hunk) = file.hunks.last_mut()
            {
                hunk.lines.push(DiffLine {
                    line_type: line_type.to_string(),
                    content,
                });
            }
            true
        }),
    )
    .map_err(|e| format!("failed to iterate diff: {e}"))?;

    Ok(files.into_inner())
}

// ── Handler ──────────────────────────────────────────────────────────────────

/// GET /api/v1/repos/{namespace}/{repo}/pulls/{number}/diff
pub async fn get_pr_diff(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, raw_repo, number)): Path<(String, String, u64)>,
) -> Response {
    let (repo_id, _, repo_path) =
        match resolve_repo_full(&state, &tenant.tenant_id, &namespace, &raw_repo).await {
            Ok(r) => r,
            Err(e) => return e,
        };

    let pr = match state.pr_store.get_pr_by_number(&repo_id, number).await {
        Ok(Some(pr)) => pr,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "PR not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to get PR");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    let source = pr.source_branch.clone();
    let target = pr.target_branch.clone();

    let diff_result =
        tokio::task::spawn_blocking(move || build_diff(&repo_path, &source, &target)).await;

    match diff_result {
        Ok(Ok(files)) => Json(files).into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to build diff");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("failed to build diff: {}", e)})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "diff task panicked");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}
