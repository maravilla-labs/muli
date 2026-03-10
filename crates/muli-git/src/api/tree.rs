// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository tree listing endpoints.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;

use crate::api::GitState;
use crate::api::commits::{CommitInfo, commit_to_info};
use crate::api::helpers::{error_response, resolve_repo};
use crate::tenant::TenantContext;

#[derive(Debug, Deserialize)]
pub struct TreeCommitQuery {
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
    pub path: Option<String>,
}

/// GET /api/v1/repos/{namespace}/{repo}/tree-commits
pub async fn list_tree_commits(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Query(query): Query<TreeCommitQuery>,
) -> Response {
    let repo_record = match resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        Ok(r) => r,
        Err(e) => return e,
    };

    let repo_id = repo_record.id.clone();
    let repo_fs_path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let git_ref = query.git_ref.unwrap_or_else(|| "HEAD".to_string());
    let dir_path = query.path.unwrap_or_default();

    // Phase 1: resolve ref to SHA
    let fs_path_for_sha = repo_fs_path.clone();
    let git_ref_for_sha = git_ref.clone();
    let commit_sha = match tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&fs_path_for_sha).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&git_ref_for_sha)
            .map_err(|e| format!("ref not found: {e}"))?;
        Ok::<String, String>(
            obj.peel_to_commit()
                .map_err(|e| e.to_string())?
                .id()
                .to_string(),
        )
    })
    .await
    {
        Ok(Ok(sha)) => sha,
        Ok(Err(e)) if e.contains("ref not found") => {
            return error_response(StatusCode::NOT_FOUND, &e);
        }
        Ok(Err(e)) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e),
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic resolving ref");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    // Phase 2: check cache
    if let Some(cache) = &state.cache_store
        && let Ok(Some(json_str)) = cache
            .get_cached(&tenant.tenant_id, &repo_id, &commit_sha, &dir_path)
            .await
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str)
    {
        return Json(v).into_response();
    }

    // Phase 3: git walk
    let dir_path_for_cache = dir_path.clone();
    let result =
        tokio::task::spawn_blocking(move || tree_walk(&repo_fs_path, &git_ref, &dir_path)).await;

    match result {
        Ok(Ok(entries)) => {
            let body: Vec<serde_json::Value> = entries
                .into_iter()
                .map(|(name, cinfo)| json!({ "name": name, "last_commit": cinfo }))
                .collect();
            let response_value = serde_json::json!(body);
            if let Some(cache) = &state.cache_store
                && let Ok(json_str) = serde_json::to_string(&response_value)
            {
                let _ = cache
                    .set_cached(
                        &tenant.tenant_id,
                        &repo_id,
                        &commit_sha,
                        &dir_path_for_cache,
                        &json_str,
                    )
                    .await;
            }
            Json(response_value).into_response()
        }
        Ok(Err(e)) if e.contains("path not found") || e.contains("ref not found") => {
            error_response(StatusCode::NOT_FOUND, &e)
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to list tree commits");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in list_tree_commits");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// Walk the git tree to find the last commit that touched each entry in the given directory.
fn tree_walk(
    repo_fs_path: &std::path::Path,
    git_ref: &str,
    dir_path: &str,
) -> Result<Vec<(String, CommitInfo)>, String> {
    use std::collections::{HashMap, HashSet};

    let repo = git2::Repository::open_bare(repo_fs_path).map_err(|e| e.to_string())?;
    let obj = repo
        .revparse_single(git_ref)
        .map_err(|e| format!("ref not found: {e}"))?;
    let start_commit = obj.peel_to_commit().map_err(|e| e.to_string())?;
    let root_tree = start_commit.tree().map_err(|e| e.to_string())?;

    let dir_tree: git2::Tree = if dir_path.is_empty() {
        root_tree
    } else {
        let entry = root_tree
            .get_path(std::path::Path::new(dir_path))
            .map_err(|e| format!("path not found: {e}"))?;
        repo.find_tree(entry.id()).map_err(|e| e.to_string())?
    };

    let mut needed: HashSet<String> = HashSet::new();
    for entry in dir_tree.iter() {
        if let Some(name) = entry.name()
            && matches!(
                entry.kind(),
                Some(git2::ObjectType::Blob) | Some(git2::ObjectType::Tree)
            )
        {
            needed.insert(name.to_string());
        }
    }
    if needed.is_empty() {
        return Ok(vec![]);
    }

    let prefix = if dir_path.is_empty() {
        String::new()
    } else {
        format!("{dir_path}/")
    };
    let mut found: HashMap<String, CommitInfo> = HashMap::new();
    let mut revwalk = repo.revwalk().map_err(|e| e.to_string())?;
    revwalk.push(start_commit.id()).map_err(|e| e.to_string())?;
    revwalk
        .set_sorting(git2::Sort::TIME)
        .map_err(|e| e.to_string())?;

    for (i, oid_result) in revwalk.enumerate() {
        if i >= 2000 || found.len() >= needed.len() {
            break;
        }
        let oid = oid_result.map_err(|e| e.to_string())?;
        let commit = repo.find_commit(oid).map_err(|e| e.to_string())?;
        let new_tree = commit.tree().map_err(|e| e.to_string())?;
        let old_tree = commit.parent(0).ok().and_then(|p| p.tree().ok());
        let diff = repo
            .diff_tree_to_tree(old_tree.as_ref(), Some(&new_tree), None)
            .map_err(|e| e.to_string())?;
        let cinfo = commit_to_info(&commit, &oid);

        for delta in diff.deltas() {
            let changed = delta
                .new_file()
                .path()
                .or_else(|| delta.old_file().path())
                .and_then(|p| p.to_str())
                .unwrap_or("")
                .to_string();

            let entry_name: &str = if prefix.is_empty() {
                match changed.find('/') {
                    Some(idx) => &changed[..idx],
                    None => &changed,
                }
            } else if changed.starts_with(&prefix) {
                let rest = &changed[prefix.len()..];
                match rest.find('/') {
                    Some(idx) => &rest[..idx],
                    None => rest,
                }
            } else {
                continue;
            };

            if !entry_name.is_empty()
                && needed.contains(entry_name)
                && !found.contains_key(entry_name)
            {
                found.insert(entry_name.to_string(), cinfo.clone());
            }
        }
    }
    Ok(found.into_iter().collect::<Vec<_>>())
}
