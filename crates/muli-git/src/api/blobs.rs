// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Raw file content endpoints.

use std::sync::Arc;

use crate::api::GitState;
use crate::api::helpers::{error_response, resolve_repo};
use crate::tenant::TenantContext;
use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct BlobQuery {
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BlobResponse {
    pub path: String,
    pub git_ref: String,
    pub size: usize,
    pub encoding: String,
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct TreeEntryResponse {
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub size: usize,
}

enum ContentResult {
    File(Vec<u8>),
    Directory(Vec<TreeEntryResponse>),
}

/// GET /api/v1/repos/{namespace}/{repo}/contents/{*path}
pub async fn get_blob(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, file_path)): Path<(String, String, String)>,
    Query(query): Query<BlobQuery>,
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
        let tree = commit.tree().map_err(|e| e.to_string())?;
        let entry = tree
            .get_path(std::path::Path::new(&file_path_clone))
            .map_err(|e| format!("path not found: {e}"))?;

        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                let subtree = repo
                    .find_tree(entry.id())
                    .map_err(|e| format!("failed to read tree: {e}"))?;
                let entries = list_tree_entries(&repo, &subtree, &file_path_clone);
                Ok::<ContentResult, String>(ContentResult::Directory(entries))
            }
            _ => {
                let blob = repo
                    .find_blob(entry.id())
                    .map_err(|e| format!("not a blob: {e}"))?;
                Ok(ContentResult::File(blob.content().to_vec()))
            }
        }
    })
    .await;

    match result {
        Ok(Ok(ContentResult::File(content))) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
            Json(BlobResponse {
                path: file_path,
                git_ref,
                size: content.len(),
                encoding: "base64".to_string(),
                content: encoded,
            })
            .into_response()
        }
        Ok(Ok(ContentResult::Directory(entries))) => {
            Json(serde_json::json!(entries)).into_response()
        }
        Ok(Err(e)) if e.contains("path not found") => {
            error_response(StatusCode::NOT_FOUND, "file not found")
        }
        Ok(Err(e)) if e.contains("ref not found") => error_response(StatusCode::NOT_FOUND, &e),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to get blob");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to read file: {e}"),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in get_blob");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

fn list_tree_entries(
    repo: &git2::Repository,
    tree: &git2::Tree<'_>,
    prefix: &str,
) -> Vec<TreeEntryResponse> {
    let mut entries = Vec::new();
    for entry in tree.iter() {
        let name = match entry.name() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let kind = match entry.kind() {
            Some(git2::ObjectType::Blob) => "file",
            Some(git2::ObjectType::Tree) => "dir",
            _ => continue,
        };
        let size = if kind == "file" {
            repo.find_blob(entry.id()).map(|b| b.size()).unwrap_or(0)
        } else {
            0
        };
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        entries.push(TreeEntryResponse {
            name,
            path,
            kind: kind.to_string(),
            size,
        });
    }
    entries
}

/// GET /api/v1/repos/{namespace}/{repo}/contents
pub async fn get_root_contents(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Query(query): Query<BlobQuery>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let repo_fs_path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);
    let git_ref = query.git_ref.unwrap_or_else(|| "HEAD".to_string());
    let git_ref_clone = git_ref.clone();

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&repo_fs_path).map_err(|e| e.to_string())?;
        let obj = repo
            .revparse_single(&git_ref_clone)
            .map_err(|e| format!("ref not found: {e}"))?;
        let commit = obj.peel_to_commit().map_err(|e| e.to_string())?;
        let tree = commit.tree().map_err(|e| e.to_string())?;
        Ok::<Vec<TreeEntryResponse>, String>(list_tree_entries(&repo, &tree, ""))
    })
    .await;

    match result {
        Ok(Ok(entries)) => Json(serde_json::json!(entries)).into_response(),
        Ok(Err(e)) if e.contains("ref not found") => error_response(StatusCode::NOT_FOUND, &e),
        Ok(Err(e)) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e),
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in get_root_contents");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// Raw blob endpoint (currently unused but available).
#[allow(dead_code)]
pub async fn get_raw_blob(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, file_path)): Path<(String, String, String)>,
    Query(query): Query<BlobQuery>,
) -> Response {
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
        let tree = commit.tree().map_err(|e| e.to_string())?;
        let entry = tree
            .get_path(std::path::Path::new(&file_path_clone))
            .map_err(|e| format!("path not found: {e}"))?;
        let blob = repo
            .find_blob(entry.id())
            .map_err(|e| format!("not a blob: {e}"))?;
        Ok::<Vec<u8>, String>(blob.content().to_vec())
    })
    .await;

    match result {
        Ok(Ok(content)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            content,
        )
            .into_response(),
        Ok(Err(e)) => error_response(StatusCode::NOT_FOUND, &e),
        Err(_) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
    }
}
