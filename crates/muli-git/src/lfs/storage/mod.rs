// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! LFS object storage trait and backend implementations.

pub mod filesystem;
#[cfg(feature = "lfs-s3")]
pub mod s3;

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

/// Streaming byte source for LFS object downloads.
pub type LfsByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, std::io::Error>> + Send>>;

/// Errors from LFS storage operations.
#[derive(Debug, thiserror::Error)]
pub enum LfsStorageError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("digest mismatch: expected {expected}, got {computed}")]
    DigestMismatch { expected: String, computed: String },

    #[error("object too large: size {size} exceeds limit {limit}")]
    TooLarge { size: u64, limit: u64 },

    #[error("invalid oid: {0}")]
    InvalidOid(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage backend error: {0}")]
    Backend(String),
}

pub type LfsStorageResult<T> = Result<T, LfsStorageError>;

/// Pluggable storage backend for LFS objects.
///
/// Objects are content-addressable by SHA-256 oid and stored per-tenant
/// (not per-repo) for deduplication.
#[async_trait]
pub trait LfsStorage: Send + Sync + 'static {
    /// Check if an object exists.
    async fn has_object(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<bool>;

    /// Get the size of an object, or `None` if it does not exist.
    async fn object_size(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<Option<u64>>;

    /// Stream-read an object. Returns `(stream, size)`.
    async fn get_object(
        &self,
        tenant_id: &str,
        oid: &str,
    ) -> LfsStorageResult<(LfsByteStream, u64)>;

    /// Write an object from a streaming body, verifying SHA-256 matches `oid`.
    async fn put_object(
        &self,
        tenant_id: &str,
        oid: &str,
        expected_size: u64,
        body: axum::body::Body,
    ) -> LfsStorageResult<()>;

    /// Delete an object.
    async fn delete_object(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<()>;

    /// Total LFS storage usage in bytes for a tenant.
    async fn tenant_usage(&self, tenant_id: &str) -> LfsStorageResult<u64>;

    /// Return a presigned download URL (S3 only; filesystem returns `None`).
    async fn presigned_download_url(
        &self,
        _tenant_id: &str,
        _oid: &str,
        _expires_in_secs: u64,
    ) -> LfsStorageResult<Option<String>> {
        Ok(None)
    }

    /// Return a presigned upload URL (S3 only; filesystem returns `None`).
    async fn presigned_upload_url(
        &self,
        _tenant_id: &str,
        _oid: &str,
        _size: u64,
        _expires_in_secs: u64,
    ) -> LfsStorageResult<Option<String>> {
        Ok(None)
    }
}
