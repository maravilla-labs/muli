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

use super::tree::{TreeEntryResponse, list_tree_entries};

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

// ---------------------------------------------------------------------------
// Write endpoints
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CommitAuthor {
    pub name: String,
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateFileRequest {
    pub content: String, // base64-encoded
    pub message: String,
    #[serde(default = "default_branch")]
    pub branch: String,
    pub author: Option<CommitAuthor>,
}

fn default_branch() -> String {
    "main".to_string()
}

#[derive(Debug, Deserialize)]
pub struct BatchFileEntry {
    pub path: String,
    pub content: String, // base64-encoded
}

#[derive(Debug, Deserialize)]
pub struct CreateFilesBatchRequest {
    pub files: Vec<BatchFileEntry>,
    /// Paths to remove from the tree before inserting new files.
    /// Enables atomic renames: delete old path + create at new path.
    #[serde(default)]
    pub deletes: Vec<String>,
    pub message: String,
    #[serde(default = "default_branch")]
    pub branch: String,
    pub author: Option<CommitAuthor>,
}

/// Insert or update a blob at the given path within a tree, creating
/// intermediate subtrees as needed for nested paths like `a/b/c.md`.
pub fn insert_blob_in_tree(
    repo: &git2::Repository,
    base_tree: Option<&git2::Tree<'_>>,
    path_segments: &[&str],
    blob_oid: git2::Oid,
) -> Result<git2::Oid, String> {
    if path_segments.is_empty() {
        return Err("empty path".to_string());
    }
    if path_segments.len() == 1 {
        // Leaf — insert blob directly
        let mut builder = if let Some(tree) = base_tree {
            repo.treebuilder(Some(tree)).map_err(|e| e.to_string())?
        } else {
            repo.treebuilder(None).map_err(|e| e.to_string())?
        };
        builder
            .insert(path_segments[0], blob_oid, 0o100644)
            .map_err(|e| e.to_string())?;
        return builder.write().map_err(|e| e.to_string());
    }

    // Intermediate directory — recurse
    let dir_name = path_segments[0];
    let rest = &path_segments[1..];

    let existing_subtree = base_tree.and_then(|t| {
        t.get_name(dir_name).and_then(|entry| {
            if entry.kind() == Some(git2::ObjectType::Tree) {
                repo.find_tree(entry.id()).ok()
            } else {
                None
            }
        })
    });

    let new_subtree_oid = insert_blob_in_tree(
        repo,
        existing_subtree.as_ref(),
        rest,
        blob_oid,
    )?;

    let mut builder = if let Some(tree) = base_tree {
        repo.treebuilder(Some(tree)).map_err(|e| e.to_string())?
    } else {
        repo.treebuilder(None).map_err(|e| e.to_string())?
    };
    builder
        .insert(dir_name, new_subtree_oid, 0o040000)
        .map_err(|e| e.to_string())?;
    builder.write().map_err(|e| e.to_string())
}

/// Remove a blob at the given path from a tree, cleaning up empty
/// intermediate subtrees. Returns the new root tree OID.
pub fn remove_blob_from_tree(
    repo: &git2::Repository,
    base_tree: &git2::Tree<'_>,
    path_segments: &[&str],
) -> Result<git2::Oid, String> {
    if path_segments.is_empty() {
        return Err("empty path".to_string());
    }
    let mut builder = repo.treebuilder(Some(base_tree)).map_err(|e| e.to_string())?;
    if path_segments.len() == 1 {
        // Leaf — remove the entry
        builder
            .remove(path_segments[0])
            .map_err(|e| format!("remove failed: {e}"))?;
        return builder.write().map_err(|e| e.to_string());
    }

    // Intermediate directory — recurse
    let dir_name = path_segments[0];
    let rest = &path_segments[1..];

    let subtree_entry = base_tree
        .get_name(dir_name)
        .ok_or_else(|| format!("directory not found: {dir_name}"))?;
    let subtree = repo
        .find_tree(subtree_entry.id())
        .map_err(|e| format!("not a tree: {e}"))?;

    let new_subtree_oid = remove_blob_from_tree(repo, &subtree, rest)?;

    // Check if the subtree is now empty; if so, remove the directory entry too
    let new_subtree = repo.find_tree(new_subtree_oid).map_err(|e| e.to_string())?;
    if new_subtree.is_empty() {
        builder
            .remove(dir_name)
            .map_err(|e| format!("remove dir failed: {e}"))?;
    } else {
        builder
            .insert(dir_name, new_subtree_oid, 0o040000)
            .map_err(|e| e.to_string())?;
    }
    builder.write().map_err(|e| e.to_string())
}

/// POST /api/v1/repos/{namespace}/{repo}/contents/{*path}
///
/// Create or update a single file and commit.
pub async fn create_or_update_file(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name, file_path)): Path<(String, String, String)>,
    Json(body): Json<CreateFileRequest>,
) -> Response {
    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let repo_fs_path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);

    let content = match base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.content,
    ) {
        Ok(c) => c,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid base64 content"),
    };

    let message = body.message;
    let branch = body.branch;
    let author = body.author;
    let file_path_clone = file_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&repo_fs_path).map_err(|e| e.to_string())?;
        let sig = match &author {
            Some(a) => git2::Signature::now(&a.name, &a.email),
            None => git2::Signature::now("Flightdeck", "system@maravilla.page"),
        }
        .map_err(|e| e.to_string())?;

        // Resolve branch
        let ref_name = format!("refs/heads/{branch}");
        let parent_commit = repo
            .find_reference(&ref_name)
            .and_then(|r| r.peel_to_commit())
            .map_err(|e| format!("branch not found: {e}"))?;

        let base_tree = parent_commit.tree().map_err(|e| e.to_string())?;

        // Create blob
        let blob_oid = repo.blob(&content).map_err(|e| e.to_string())?;

        // Build tree with the file inserted
        let segments: Vec<&str> = file_path_clone.split('/').collect();
        let new_tree_oid = insert_blob_in_tree(&repo, Some(&base_tree), &segments, blob_oid)?;
        let new_tree = repo.find_tree(new_tree_oid).map_err(|e| e.to_string())?;

        // Create commit
        let commit_oid = repo
            .commit(
                Some(&ref_name),
                &sig,
                &sig,
                &message,
                &new_tree,
                &[&parent_commit],
            )
            .map_err(|e| e.to_string())?;

        Ok::<(String, String), String>((file_path_clone, commit_oid.to_string()))
    })
    .await;

    match result {
        Ok(Ok((path, sha))) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "path": path, "sha": sha })),
        )
            .into_response(),
        Ok(Err(e)) if e.contains("branch not found") => error_response(StatusCode::NOT_FOUND, &e),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to create/update file");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in create_or_update_file");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// POST /api/v1/repos/{namespace}/{repo}/contents
///
/// Create or update multiple files in a single commit.
pub async fn create_files_batch(
    Extension(tenant): Extension<TenantContext>,
    State(state): State<Arc<GitState>>,
    Path((namespace, repo_name)): Path<(String, String)>,
    Json(body): Json<CreateFilesBatchRequest>,
) -> Response {
    if body.files.is_empty() && body.deletes.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "files and deletes must not both be empty");
    }

    if let Err(e) = resolve_repo(&state, &tenant.tenant_id, &namespace, &repo_name).await {
        return e;
    }

    let repo_fs_path = state
        .storage
        .repo_path(&tenant.tenant_id, &namespace, &repo_name);

    // Decode all files upfront
    let mut decoded_files = Vec::with_capacity(body.files.len());
    for entry in &body.files {
        match base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &entry.content,
        ) {
            Ok(c) => decoded_files.push((entry.path.clone(), c)),
            Err(_) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("invalid base64 content for path: {}", entry.path),
                );
            }
        }
    }

    let message = body.message;
    let branch = body.branch;
    let author = body.author;
    let deletes = body.deletes;

    let result = tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open_bare(&repo_fs_path).map_err(|e| e.to_string())?;
        let sig = match &author {
            Some(a) => git2::Signature::now(&a.name, &a.email),
            None => git2::Signature::now("Flightdeck", "system@maravilla.page"),
        }
        .map_err(|e| e.to_string())?;

        // Resolve branch — for empty repos, create initial commit
        let ref_name = format!("refs/heads/{branch}");
        let parent_commit = repo
            .find_reference(&ref_name)
            .and_then(|r| r.peel_to_commit())
            .ok();

        let base_tree = match &parent_commit {
            Some(c) => Some(c.tree().map_err(|e| e.to_string())?),
            None => None,
        };

        // Start from the base tree
        let mut current_tree_oid = match &base_tree {
            Some(t) => t.id(),
            None => {
                let builder = repo.treebuilder(None).map_err(|e| e.to_string())?;
                builder.write().map_err(|e| e.to_string())?
            }
        };

        // Apply deletes first (enables atomic renames: delete old + create new)
        for del_path in &deletes {
            let current_tree = repo.find_tree(current_tree_oid).map_err(|e| e.to_string())?;
            let segments: Vec<&str> = del_path.split('/').collect();
            current_tree_oid = remove_blob_from_tree(&repo, &current_tree, &segments)?;
        }

        // Then insert all files
        for (path, content) in &decoded_files {
            let blob_oid = repo.blob(content).map_err(|e| e.to_string())?;
            let current_tree = repo.find_tree(current_tree_oid).map_err(|e| e.to_string())?;
            let segments: Vec<&str> = path.split('/').collect();
            current_tree_oid =
                insert_blob_in_tree(&repo, Some(&current_tree), &segments, blob_oid)?;
        }

        let final_tree = repo.find_tree(current_tree_oid).map_err(|e| e.to_string())?;

        // Create commit
        let parents: Vec<&git2::Commit<'_>> = match &parent_commit {
            Some(c) => vec![c],
            None => vec![],
        };
        let commit_oid = repo
            .commit(Some(&ref_name), &sig, &sig, &message, &final_tree, &parents)
            .map_err(|e| e.to_string())?;

        Ok::<(usize, String), String>((decoded_files.len(), commit_oid.to_string()))
    })
    .await;

    match result {
        Ok(Ok((count, sha))) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "files_committed": count, "sha": sha })),
        )
            .into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to create files batch");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panic in create_files_batch");
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
