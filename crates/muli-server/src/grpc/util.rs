// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC utility functions and auth helpers.

use chrono::{DateTime, Utc};
use muli_core::token_hash;
use tonic::{Request, Status};

use muli_proto::Timestamp;

/// Extract the `x-tenant-id` header from gRPC request metadata.
/// Returns an error if the header is missing or empty.
pub fn extract_tenant_id<T>(request: &Request<T>) -> Result<String, Status> {
    request
        .metadata()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .ok_or_else(|| Status::unauthenticated("missing x-tenant-id metadata header"))
}

/// Convert a `chrono::DateTime<Utc>` to a proto `Timestamp`.
pub fn datetime_to_proto(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// Generate a cryptographically random 32-byte token encoded as a 64-char hex string.
pub fn generate_token() -> String {
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(a.as_bytes());
    bytes[16..].copy_from_slice(b.as_bytes());
    hex::encode(bytes)
}

/// Hash a plaintext token with Argon2id.
pub fn hash_token(plaintext: &str) -> String {
    token_hash::hash_token(plaintext).expect("Argon2id hashing failed")
}

/// Extract the lookup prefix from a plaintext token.
pub fn token_prefix(plaintext: &str) -> String {
    token_hash::token_prefix(plaintext)
}
