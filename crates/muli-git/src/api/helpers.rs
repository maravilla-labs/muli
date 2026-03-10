// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared API helper functions.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

use muli_core::git::Repository;

use crate::api::GitState;

/// Resolve namespace and repo name from a path segment that may include the
/// `.git` suffix.  For example `"myrepo.git"` → `"myrepo"`.
pub fn strip_git_suffix(name: &str) -> &str {
    name.strip_suffix(".git").unwrap_or(name)
}

/// Build a JSON error response with the given status code and message.
pub fn error_response(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": msg}))).into_response()
}

/// Validate a path component used to construct a filesystem path.
///
/// Rejects empty strings, dot-segments, slashes, null bytes, `..`,
/// whitespace, leading/trailing hyphens, and names longer than 255 chars.
pub fn validate_path_component(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s != ".."
        && s != "."
        && !s.contains('/')
        && !s.contains('\\')
        && !s.contains('\0')
        && !s.contains("..")
        && !s.starts_with('-')
        && !s.ends_with('-')
        && !s.chars().any(|c| c.is_whitespace())
}

/// Validate a git ref name (branch or tag).
///
/// Rejects names that could cause path traversal, ref injection, or other issues
/// when used in `refs/heads/{name}` or `refs/tags/{name}`.
pub fn validate_ref_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.starts_with('.')
        && !name.starts_with('-')
        && !name.ends_with('.')
        && !name.ends_with(".lock")
        && !name.contains("..")
        && !name.contains("~")
        && !name.contains("^")
        && !name.contains(":")
        && !name.contains("?")
        && !name.contains("*")
        && !name.contains("[")
        && !name.contains("\\")
        && !name.contains('\0')
        && !name.contains("@{")
        && !name.chars().any(|c| c.is_ascii_control() || c == ' ')
}

/// Look up the repository and return its record.
/// Returns an error Response on failure.
pub async fn resolve_repo(
    state: &GitState,
    tenant_id: &str,
    namespace: &str,
    raw_repo: &str,
) -> Result<Repository, Response> {
    let repo_name = strip_git_suffix(raw_repo);
    match state
        .repo_store
        .get_repository_by_name(tenant_id, namespace, repo_name)
        .await
    {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(error_response(
            StatusCode::NOT_FOUND,
            "repository not found",
        )),
        Err(e) => {
            tracing::error!(error = %e, "failed to look up repository");
            Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error",
            ))
        }
    }
}

/// Look up the repository and return (repo_id, repo_name, repo_path).
/// Returns an error Response on failure.
pub async fn resolve_repo_full(
    state: &GitState,
    tenant_id: &str,
    namespace: &str,
    raw_repo: &str,
) -> Result<(String, String, std::path::PathBuf), Response> {
    let repo_name = strip_git_suffix(raw_repo).to_string();
    let repo = resolve_repo(state, tenant_id, namespace, raw_repo).await?;
    let path = state.storage.repo_path(tenant_id, namespace, &repo_name);
    Ok((repo.id, repo_name, path))
}

// ---------------------------------------------------------------------------
// SSH path validation
// ---------------------------------------------------------------------------

/// Validate that a path component (namespace or repo name) is safe for use
/// in SSH repo path resolution. Same rules as `validate_path_component`.
pub fn validate_ssh_path_segment(s: &str) -> bool {
    validate_path_component(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- validate_path_component --

    #[test]
    fn path_component_valid() {
        assert!(validate_path_component("my-repo"));
        assert!(validate_path_component("acme"));
        assert!(validate_path_component("repo123"));
        assert!(validate_path_component("a"));
    }

    #[test]
    fn path_component_rejects_empty() {
        assert!(!validate_path_component(""));
    }

    #[test]
    fn path_component_rejects_dots() {
        assert!(!validate_path_component(".."));
        assert!(!validate_path_component("."));
        assert!(!validate_path_component("foo..bar"));
    }

    #[test]
    fn path_component_rejects_slashes() {
        assert!(!validate_path_component("a/b"));
        assert!(!validate_path_component("a\\b"));
    }

    #[test]
    fn path_component_rejects_null() {
        assert!(!validate_path_component("a\0b"));
    }

    #[test]
    fn path_component_rejects_leading_trailing_hyphens() {
        assert!(!validate_path_component("-repo"));
        assert!(!validate_path_component("repo-"));
    }

    #[test]
    fn path_component_rejects_whitespace() {
        assert!(!validate_path_component("my repo"));
        assert!(!validate_path_component("my\trepo"));
        assert!(!validate_path_component(" repo"));
    }

    #[test]
    fn path_component_rejects_too_long() {
        let long = "a".repeat(256);
        assert!(!validate_path_component(&long));
        let ok = "a".repeat(255);
        assert!(validate_path_component(&ok));
    }
}
