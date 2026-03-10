// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Argon2id token hashing and verification.
//!
//! Tokens are hashed with Argon2id (salted) for storage. A short prefix of the
//! plaintext token is stored alongside the hash for O(1) database lookup; the
//! full token is then verified against the Argon2id hash.

use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};

/// Length of the token prefix used for database lookup (hex chars).
/// 16 hex chars = 8 bytes = 64 bits — collision-free for any realistic token count.
pub const TOKEN_PREFIX_LEN: usize = 16;

/// Extract the lookup prefix from a plaintext token.
pub fn token_prefix(plaintext: &str) -> String {
    plaintext.chars().take(TOKEN_PREFIX_LEN).collect()
}

/// Hash a plaintext token with Argon2id. Returns a PHC-format string
/// containing the algorithm, parameters, salt, and hash.
///
/// This is CPU-intensive (~50ms). Callers in async contexts should use
/// `tokio::task::spawn_blocking`.
pub fn hash_token(plaintext: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(plaintext.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

/// Verify a plaintext token against a stored Argon2id PHC hash string.
///
/// This is CPU-intensive (~50ms). Callers in async contexts should use
/// `tokio::task::spawn_blocking`.
pub fn verify_token(plaintext: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(plaintext.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify() {
        let plaintext = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let hash = hash_token(plaintext).unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_token(plaintext, &hash));
    }

    #[test]
    fn wrong_token_fails() {
        let hash = hash_token("correct-token").unwrap();
        assert!(!verify_token("wrong-token", &hash));
    }

    #[test]
    fn different_hashes_for_same_input() {
        let a = hash_token("same-token").unwrap();
        let b = hash_token("same-token").unwrap();
        // Salted — hashes differ but both verify
        assert_ne!(a, b);
        assert!(verify_token("same-token", &a));
        assert!(verify_token("same-token", &b));
    }

    #[test]
    fn prefix_extraction() {
        let token = "abcdef1234567890extra";
        assert_eq!(token_prefix(token), "abcdef1234567890");
    }

    #[test]
    fn prefix_short_token() {
        let token = "short";
        assert_eq!(token_prefix(token), "short");
    }

    #[test]
    fn invalid_hash_returns_false() {
        assert!(!verify_token("token", "not-a-valid-phc-string"));
    }
}
