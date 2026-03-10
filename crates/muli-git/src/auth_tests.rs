// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

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
fn extract_repo_from_path_git_http() {
    assert_eq!(
        extract_repo_from_path("/acme/my-repo.git/info/refs"),
        Some(("acme", "my-repo"))
    );
}

#[test]
fn extract_repo_from_path_rest_api() {
    assert_eq!(
        extract_repo_from_path("/api/v1/repos/acme/my-repo"),
        Some(("acme", "my-repo"))
    );
}

#[test]
fn extract_repo_from_path_rest_api_with_suffix() {
    assert_eq!(
        extract_repo_from_path("/api/v1/repos/acme/my-repo/branches"),
        Some(("acme", "my-repo"))
    );
}
