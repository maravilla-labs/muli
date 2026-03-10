// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Unit tests for registry authentication.

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
    // Argon2id is salted — same input produces different hashes
    let a = hash_token("my-token");
    let b = hash_token("my-token");
    assert_ne!(a, b);
    // But both verify against the same plaintext
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
fn required_permission_read_methods() {
    assert_eq!(required_permission(&Method::GET), RegistryPermission::Pull);
    assert_eq!(required_permission(&Method::HEAD), RegistryPermission::Pull);
}

#[test]
fn required_permission_write_methods() {
    assert_eq!(required_permission(&Method::PUT), RegistryPermission::Push);
    assert_eq!(required_permission(&Method::POST), RegistryPermission::Push);
    assert_eq!(
        required_permission(&Method::PATCH),
        RegistryPermission::Push
    );
}

#[test]
fn required_permission_delete_method() {
    assert_eq!(
        required_permission(&Method::DELETE),
        RegistryPermission::Admin
    );
}

#[test]
fn extract_bearer_valid() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer my-secret-token".parse().unwrap());
    assert_eq!(extract_bearer_token(&headers), Some("my-secret-token"));
}

#[test]
fn extract_bearer_missing_header() {
    let headers = HeaderMap::new();
    assert_eq!(extract_bearer_token(&headers), None);
}

#[test]
fn extract_bearer_wrong_scheme() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
    assert_eq!(extract_bearer_token(&headers), None);
}

#[test]
fn extract_bearer_empty_token() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer ".parse().unwrap());
    assert_eq!(extract_bearer_token(&headers), None);
}
