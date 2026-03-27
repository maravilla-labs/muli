// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Commit history endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo};
use crate::api::pulls_diff::build_diff_from_git2_diff;
use crate::tenant::TenantContext;

#[derive(Debug, Deserialize)]
pub struct CommitQuery {
    pub branch: Option<String>,
    pub limit: Option<usize>,
    /// When set, only return commits that touched this file path.
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommitInfo {
    pub sha: String,
    pub message: String,
    pub author_name: String,
    pub author_email: String,
    pub time: String,
    pub parents: Vec<String>,
}

/// Convert a git2 Commit into a CommitInfo value.
pub fn commit_to_info(commit: &git2::Commit, oid: &git2::Oid) -> CommitInfo {
    let author = commit.author();
    let name = author.name().unwrap_or("").to_string();
    let email = author.email().unwrap_or("").to_string();
    let time = {
        let t = commit.time();
        let secs = t.seconds();
        let offset_minutes = t.offset_minutes();
        let sign = if offset_minutes >= 0 { '+' } else { '-' };
        let offset_abs = offset_minutes.unsigned_abs();
        let offset_hours = offset_abs / 60;
        let offset_mins = offset_abs % 60;
        let utc_dt = chrono::DateTime::from_timestamp(secs, 0)
            .map(|dt: chrono::DateTime<chrono::Utc>| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
            .unwrap_or_else(|| "1970-01-01T00:00:00".to_string());
        format!("{utc_dt}{sign}{offset_hours:02}:{offset_mins:02}")
    };
    let parents: Vec<String> = commit.parent_ids().map(|id| id.to_string()).collect();
    let message = commit.message().unwrap_or("").trim_end().to_string();
    CommitInfo {
        sha: oid.to_string(),
        message,
        author_name: name,
        author_email: email,
        time,
        parents,
    }
}

/// GET /api/v1/repos/{namespace}/{repo}/commits
pub async fn list_commits(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Query(query): Query<CommitQuery>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let repo_path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let branch = query.branch.unwrap_or_else(|| "HEAD".to_string());
    let limit = query.limit.unwrap_or(20).min(500);
    let filter_path = query.path;

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&repo_path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&branch)
            .map_err(|e| format!("ref not found: {e}"))?;
        let commit = obj
            .peel_to_commit()
            .map_err(|e| format!("not a commit: {e}"))?;

        let mut revwalk = repo.revwalk().map_err(|e| e.to_string())?;
        revwalk.push(commit.id()).map_err(|e| e.to_string())?;
        revwalk
            .set_sorting(git2::Sort::TIME)
            .map_err(|e| e.to_string())?;

        let mut commits = Vec::new();
        // Walk more commits when filtering by path since most won't match
        let walk_limit = if filter_path.is_some() { limit * 20 } else { limit };
        // Mutable tracked path — updated when a rename is detected (--follow)
        let mut current_path = filter_path.clone();

        for oid in revwalk.take(walk_limit) {
            if commits.len() >= limit {
                break;
            }
            let oid = oid.map_err(|e| e.to_string())?;
            let c = repo.find_commit(oid).map_err(|e| e.to_string())?;

            if let Some(ref fp) = current_path {
                let commit_tree = c.tree().map_err(|e| e.to_string())?;
                let parent_tree = c.parent(0).ok().and_then(|p| p.tree().ok());

                // Diff with rename detection enabled (no pathspec — pathspec
                // prevents rename detection from working).
                let mut opts = git2::DiffOptions::new();
                let diff = repo
                    .diff_tree_to_tree(
                        parent_tree.as_ref(),
                        Some(&commit_tree),
                        Some(&mut opts),
                    )
                    .map_err(|e| format!("diff failed: {e}"))?;

                // Enable rename detection on the computed diff
                let mut find_opts = git2::DiffFindOptions::new();
                find_opts.renames(true);
                let mut diff = diff;
                diff.find_similar(Some(&mut find_opts))
                    .map_err(|e| format!("find_similar failed: {e}"))?;

                // Check if any delta touches the tracked path
                let mut touched = false;
                for delta in diff.deltas() {
                    let new_path = delta.new_file().path().and_then(|p| p.to_str());
                    let old_path = delta.old_file().path().and_then(|p| p.to_str());

                    if new_path == Some(fp.as_str()) || old_path == Some(fp.as_str()) {
                        touched = true;
                        // Follow rename: if this is a rename where new_file
                        // matches our tracked path, continue with old_file
                        if delta.status() == git2::Delta::Renamed {
                            if new_path == Some(fp.as_str()) {
                                if let Some(old) = old_path {
                                    current_path = Some(old.to_string());
                                }
                            }
                        }
                        break;
                    }
                }
                if !touched {
                    continue;
                }
            }

            commits.push(commit_to_info(&c, &oid));
        }
        Ok::<Vec<CommitInfo>, String>(commits)
    })
    .await;

    match result {
        Ok(Ok(commits)) => Json(commits).into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to list commits");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to list commits: {e}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in list_commits");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

// ── Single commit ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct CommitStats {
    pub added: usize,
    pub removed: usize,
    pub files: usize,
}

#[derive(Debug, Serialize)]
pub struct CommitDetail {
    pub sha: String,
    pub message: String,
    pub author: String,
    pub author_email: String,
    pub time: String,
    pub parents: Vec<String>,
    pub stats: CommitStats,
}

/// GET /api/v1/repos/{namespace}/{repo}/commits/{sha}
pub async fn get_commit(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, sha)): Path<(String, String, String)>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&sha)
            .map_err(|e| format!("commit not found: {e}"))?;
        let commit = obj
            .peel_to_commit()
            .map_err(|e| format!("not a commit: {e}"))?;
        let oid = commit.id();
        let info = commit_to_info(&commit, &oid);
        let commit_tree = commit.tree().map_err(|e| e.to_string())?;
        let parent_tree = commit.parent(0).ok().and_then(|p| p.tree().ok());
        let diff = repo
            .diff_tree_to_tree(parent_tree.as_ref(), Some(&commit_tree), None)
            .map_err(|e| format!("failed to compute diff: {e}"))?;
        let stats = diff
            .stats()
            .map_err(|e| format!("failed to get diff stats: {e}"))?;

        Ok::<CommitDetail, String>(CommitDetail {
            sha: info.sha,
            message: info.message,
            author: info.author_name,
            author_email: info.author_email,
            time: info.time,
            parents: info.parents,
            stats: CommitStats {
                added: stats.insertions(),
                removed: stats.deletions(),
                files: stats.files_changed(),
            },
        })
    })
    .await;

    match result {
        Ok(Ok(detail)) => Json(detail).into_response(),
        Ok(Err(e)) if e.contains("commit not found") || e.contains("not a commit") => {
            error_response(StatusCode::NOT_FOUND, &e)
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to get commit");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to get commit: {e}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in get_commit");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// GET /api/v1/repos/{namespace}/{repo}/commits/{sha}/diff
pub async fn get_commit_diff(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, sha)): Path<(String, String, String)>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&sha)
            .map_err(|e| format!("commit not found: {e}"))?;
        let commit = obj
            .peel_to_commit()
            .map_err(|e| format!("not a commit: {e}"))?;
        let commit_tree = commit.tree().map_err(|e| e.to_string())?;
        let parent_tree = commit.parent(0).ok().and_then(|p| p.tree().ok());
        let diff = repo
            .diff_tree_to_tree(parent_tree.as_ref(), Some(&commit_tree), None)
            .map_err(|e| format!("failed to compute diff: {e}"))?;
        build_diff_from_git2_diff(&diff)
    })
    .await;

    match result {
        Ok(Ok(files)) => Json(files).into_response(),
        Ok(Err(e)) if e.contains("commit not found") || e.contains("not a commit") => {
            error_response(StatusCode::NOT_FOUND, &e)
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to get commit diff");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to get commit diff: {e}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in get_commit_diff");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
