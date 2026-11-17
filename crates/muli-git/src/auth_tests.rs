// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use axum::http::HeaderMap;
use muli_core::auth::{extract_basic_auth_token, extract_bearer_token};

#[test]
fn hash_token_is_argon2id() {
    let hash = hash_token("test-token-123");
    assert!(
        hash.starts_with("$argon2id$"),
        "hash must be Argon2id PHC format"
    );
}

#[test]
fn hash_token_salted_different_each_time() {
    let a = hash_token("my-token");
    let b = hash_token("my-token");
    assert_ne!(a, b);
    assert!(muli_core::token_hash::verify_token("my-token", &a));
    assert!(muli_core::token_hash::verify_token("my-token", &b));
}

#[test]
fn hash_token_different_inputs_dont_cross_verify() {
    let hash_a = hash_token("token-a");
    assert!(!muli_core::token_hash::verify_token("token-b", &hash_a));
}

#[test]
fn token_prefix_extraction() {
    let prefix = token_prefix("abcdef1234567890extra");
    assert_eq!(prefix, "abcdef1234567890");
}

#[test]
fn required_permission_info_refs_upload() {
    assert_eq!(
        required_permission(
            &Method::GET,
            "/ns/repo.git/info/refs",
            Some("service=git-upload-pack")
        ),
        GitPermission::Pull
    );
}

#[test]
fn required_permission_info_refs_receive() {
    assert_eq!(
        required_permission(
            &Method::GET,
            "/ns/repo.git/info/refs",
            Some("service=git-receive-pack")
        ),
        GitPermission::Push
    );
}

#[test]
fn required_permission_post_upload_pack() {
    assert_eq!(
        required_permission(&Method::POST, "/ns/repo.git/git-upload-pack", None),
        GitPermission::Pull
    );
}

#[test]
fn required_permission_post_receive_pack() {
    assert_eq!(
        required_permission(&Method::POST, "/ns/repo.git/git-receive-pack", None),
        GitPermission::Push
    );
}

#[test]
fn required_permission_rest_delete() {
    assert_eq!(
        required_permission(&Method::DELETE, "/api/v1/repos/ns/repo", None),
        GitPermission::Admin
    );
}

#[test]
fn extract_bearer_valid() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer my-secret-token".parse().unwrap());
    assert_eq!(extract_bearer_token(&headers), Some("my-secret-token"));
}

#[test]
fn extract_basic_valid() {
    use base64::Engine;
    let mut headers = HeaderMap::new();
    let encoded = base64::engine::general_purpose::STANDARD.encode("user:mytoken");
    headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
    assert_eq!(
        extract_basic_auth_token(&headers),
        Some("mytoken".to_string())
    );
}

#[test]
fn repo_scope_allows_only_its_own_repository() {
    assert!(repo_scope_allows("repo-1", Some("repo-1")));
    assert!(
        !repo_scope_allows("repo-1", Some("repo-2")),
        "another repository"
    );
    assert!(
        !repo_scope_allows("repo-1", None),
        "a path naming no repository"
    );
    assert!(
        !repo_scope_allows("", Some("")),
        "an empty scope is no scope"
    );
    assert!(!repo_scope_allows("", None));
}

#[test]
fn only_git_smart_http_paths_are_git_transport() {
    for ok in [
        "/acme/repo.git/info/refs",
        "/acme/repo/info/refs",
        "/acme/repo.git/git-upload-pack",
        "/acme/repo.git/git-receive-pack",
        "/acme/repo.git/info/lfs/objects/batch",
        "/acme/repo.git/info/lfs/objects/verify",
        "/acme/repo.git/info/lfs/objects/0123abcd",
    ] {
        assert!(is_git_transport(ok), "{ok}");
    }
    for no in [
        "/api/v1/repos/acme/repo/refs",
        "/api/v1/repos/acme/repo",
        "/api/v1/repos",
        "/acme/repo.git/info/lfs/objects/",
        "/acme/repo.git/info/lfs/locks",
        "/acme/repo.git/info/lfs/objects/a/b",
        "/api/v1/repos/acme/repo/info/lfs/objects/batch",
        "/acme/repo.git",
        "/acme/repo.git/HEAD",
        "/info/refs",
        "/a/b/c/info/refs",
    ] {
        assert!(!is_git_transport(no), "{no}");
    }
    // Transport-shaped with an invalid repository: still transport (and
    // unresolvable, so refused), never "no repository".
    assert_eq!(
        crate::repo_target::parse("//repo/info/refs"),
        crate::repo_target::RepoPath::Invalid {
            kind: RepoPathKind::Transport
        }
    );
}

fn is_git_transport(path: &str) -> bool {
    crate::repo_target::parse(path).kind() == Some(RepoPathKind::Transport)
}

#[test]
fn lfs_batch_download_needs_pull_everything_else_push() {
    assert!(is_lfs_batch("/acme/repo.git/info/lfs/objects/batch"));
    assert!(is_lfs_batch("/acme/repo/info/lfs/objects/batch"));
    assert!(!is_lfs_batch("/acme/repo.git/info/lfs/objects/verify"));
    assert!(!is_lfs_batch(
        "/api/v1/repos/acme/repo/info/lfs/objects/batch"
    ));

    let download = br#"{"operation":"download","objects":[]}"#;
    let upload = br#"{"operation":"upload","objects":[]}"#;
    assert_eq!(lfs_batch_permission(download), GitPermission::Pull);
    assert_eq!(lfs_batch_permission(upload), GitPermission::Push);
    assert_eq!(
        lfs_batch_permission(br#"{"objects":[]}"#),
        GitPermission::Push
    );
    assert_eq!(lfs_batch_permission(b"not json"), GitPermission::Push);
    assert_eq!(
        lfs_batch_permission(br#"{"operation":"DOWNLOAD"}"#),
        GitPermission::Push
    );
}
