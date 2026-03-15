// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git blame endpoint.

use std::sync::Arc;

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo};
use crate::tenant::TenantContext;
use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct BlameQuery {
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BlameHunk {
    pub start_line: usize,
    pub end_line: usize,
    pub commit_sha: String,
    pub author_name: String,
    pub author_email: String,
    pub time: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct BlameResponse {
    pub path: String,
    pub git_ref: String,
    pub hunks: Vec<BlameHunk>,
}

/// Format a git2 commit time as ISO 8601 with timezone offset.
fn format_git_time(commit: &git2::Commit) -> String {
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
}

/// GET /api/v1/repos/{namespace}/{repo}/blame/{*path}
pub async fn get_blame(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, file_path)): Path<(String, String, String)>,
    Query(query): Query<BlameQuery>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let repo_fs_path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let git_ref = query.git_ref.unwrap_or_else(|| "HEAD".to_string());
    let file_path_clone = file_path.clone();
    let git_ref_clone = git_ref.clone();

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&repo_fs_path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&git_ref_clone)
            .map_err(|e| format!("ref not found: {e}"))?;
        let commit = obj
            .peel_to_commit()
            .map_err(|e| format!("not a commit: {e}"))?;

        let mut opts = git2::BlameOptions::new();
        opts.newest_commit(commit.id());
        let blame = repo
            .blame_file(std::path::Path::new(&file_path_clone), Some(&mut opts))
            .map_err(|e| format!("blame failed: {e}"))?;

        let mut hunks = Vec::new();
        for i in 0..blame.len() {
            let hunk = blame.get_index(i).unwrap();
            let sig = hunk.final_signature();
            let oid = hunk.final_commit_id();
            let commit = repo.find_commit(oid).map_err(|e| e.to_string())?;
            let message = commit.summary().unwrap_or("").to_string();
            hunks.push(BlameHunk {
                start_line: hunk.final_start_line(),
                end_line: hunk.final_start_line() + hunk.lines_in_hunk() - 1,
                commit_sha: oid.to_string(),
                author_name: sig.name().unwrap_or("").to_string(),
                author_email: sig.email().unwrap_or("").to_string(),
                time: format_git_time(&commit),
                message,
            });
        }
        Ok::<Vec<BlameHunk>, String>(hunks)
    })
    .await;

    match result {
        Ok(Ok(hunks)) => Json(BlameResponse {
            path: file_path,
            git_ref,
            hunks,
        })
        .into_response(),
        Ok(Err(e)) if e.contains("blame failed") || e.contains("path not found") => {
            error_response(StatusCode::NOT_FOUND, "file not found")
        }
        Ok(Err(e)) if e.contains("ref not found") => error_response(StatusCode::NOT_FOUND, &e),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to get blame");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to get blame: {e}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in get_blame");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
