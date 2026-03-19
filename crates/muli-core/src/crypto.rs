// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! AES-256-GCM encryption for pipeline secrets.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use rand::RngCore;
use rand::rngs::OsRng;

use crate::error::{MuliError, Result};

/// Encrypt plaintext with AES-256-GCM. Returns base64(nonce || ciphertext).
pub fn encrypt(plaintext: &str, key: &[u8; 32]) -> Result<String> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| MuliError::Pipeline(format!("invalid encryption key: {e}")))?;

    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| MuliError::Pipeline(format!("encryption failed: {e}")))?;

    let mut combined = Vec::with_capacity(12 + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);

    Ok(base64::engine::general_purpose::STANDARD.encode(&combined))
}

/// Decrypt a base64(nonce || ciphertext) string with AES-256-GCM.
pub fn decrypt(encoded: &str, key: &[u8; 32]) -> Result<String> {
    let combined = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| MuliError::Pipeline(format!("invalid base64: {e}")))?;

    if combined.len() < 13 {
        return Err(MuliError::Pipeline(
            "ciphertext too short (missing nonce)".into(),
        ));
    }

    let (nonce_bytes, ciphertext) = combined.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| MuliError::Pipeline(format!("invalid encryption key: {e}")))?;

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| MuliError::Pipeline(format!("decryption failed: {e}")))?;

    String::from_utf8(plaintext).map_err(|e| MuliError::Pipeline(format!("invalid UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte = i as u8;
        }
        key
    }

    #[test]
    fn round_trip() {
        let key = test_key();
        let plaintext = "super-secret-database-url";
        let encrypted = encrypt(plaintext, &key).unwrap();
        assert_ne!(encrypted, plaintext);
        let decrypted = decrypt(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn different_nonces() {
        let key = test_key();
        let e1 = encrypt("hello", &key).unwrap();
        let e2 = encrypt("hello", &key).unwrap();
        // Same plaintext should produce different ciphertexts (random nonce)
        assert_ne!(e1, e2);
        assert_eq!(decrypt(&e1, &key).unwrap(), "hello");
        assert_eq!(decrypt(&e2, &key).unwrap(), "hello");
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] = 255;
        let encrypted = encrypt("secret", &key1).unwrap();
        assert!(decrypt(&encrypted, &key2).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let encrypted = encrypt("secret", &key).unwrap();
        let mut bytes = base64::engine::general_purpose::STANDARD
            .decode(&encrypted)
            .unwrap();
        // Flip a byte in the ciphertext portion
        if let Some(last) = bytes.last_mut() {
            *last ^= 0xFF;
        }
        let tampered = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert!(decrypt(&tampered, &key).is_err());
    }

    #[test]
    fn empty_plaintext() {
        let key = test_key();
        let encrypted = encrypt("", &key).unwrap();
        assert_eq!(decrypt(&encrypted, &key).unwrap(), "");
    }

    #[test]
    fn invalid_base64() {
        let key = test_key();
        assert!(decrypt("not-valid-base64!!!", &key).is_err());
    }

    #[test]
    fn too_short_ciphertext() {
        let key = test_key();
        let short = base64::engine::general_purpose::STANDARD.encode([0u8; 5]);
        assert!(decrypt(&short, &key).is_err());
    }
}
