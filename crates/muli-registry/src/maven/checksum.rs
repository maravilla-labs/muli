// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Checksum computation and verification for Maven artifacts.

use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// Supported checksum algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgo {
    Sha1,
    Md5,
    Sha256,
    Sha512,
}

impl ChecksumAlgo {
    /// Parse from a file extension (without the leading dot).
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "sha1" => Some(Self::Sha1),
            "md5" => Some(Self::Md5),
            "sha256" => Some(Self::Sha256),
            "sha512" => Some(Self::Sha512),
            _ => None,
        }
    }
}

pub fn compute_sha1(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub fn compute_md5(data: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Compute a checksum using the specified algorithm.
pub fn compute(data: &[u8], algo: ChecksumAlgo) -> String {
    match algo {
        ChecksumAlgo::Sha1 => compute_sha1(data),
        ChecksumAlgo::Md5 => compute_md5(data),
        ChecksumAlgo::Sha256 => compute_sha256(data),
        ChecksumAlgo::Sha512 => {
            use sha2::Sha512;
            let mut hasher = Sha512::new();
            hasher.update(data);
            hex::encode(hasher.finalize())
        }
    }
}

/// Verify a checksum using constant-time comparison.
pub fn verify_checksum(data: &[u8], expected: &str, algo: ChecksumAlgo) -> bool {
    let computed = compute(data, algo);
    // Trim whitespace from expected — Maven clients sometimes include trailing newlines
    let expected = expected.trim();
    constant_time_eq(computed.as_bytes(), expected.as_bytes())
}

/// Constant-time byte comparison to prevent timing side-channels.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_roundtrip() {
        let data = b"hello world";
        let hash = compute_sha1(data);
        assert!(verify_checksum(data, &hash, ChecksumAlgo::Sha1));
    }

    #[test]
    fn md5_roundtrip() {
        let data = b"hello world";
        let hash = compute_md5(data);
        assert!(verify_checksum(data, &hash, ChecksumAlgo::Md5));
    }

    #[test]
    fn sha256_roundtrip() {
        let data = b"hello world";
        let hash = compute_sha256(data);
        assert!(verify_checksum(data, &hash, ChecksumAlgo::Sha256));
    }

    #[test]
    fn sha1_known_value() {
        // SHA-1 of "hello world" (well-known)
        let hash = compute_sha1(b"hello world");
        assert_eq!(hash, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[test]
    fn verify_with_whitespace() {
        let data = b"hello world";
        let hash = format!("{}  \n", compute_sha1(data));
        assert!(verify_checksum(data, &hash, ChecksumAlgo::Sha1));
    }

    #[test]
    fn verify_mismatch() {
        let data = b"hello world";
        assert!(!verify_checksum(
            data,
            "0000000000000000000000000000000000000000",
            ChecksumAlgo::Sha1
        ));
    }

    #[test]
    fn algo_from_extension() {
        assert_eq!(
            ChecksumAlgo::from_extension("sha1"),
            Some(ChecksumAlgo::Sha1)
        );
        assert_eq!(ChecksumAlgo::from_extension("md5"), Some(ChecksumAlgo::Md5));
        assert_eq!(
            ChecksumAlgo::from_extension("sha256"),
            Some(ChecksumAlgo::Sha256)
        );
        assert_eq!(
            ChecksumAlgo::from_extension("sha512"),
            Some(ChecksumAlgo::Sha512)
        );
        assert_eq!(ChecksumAlgo::from_extension("unknown"), None);
    }
}
