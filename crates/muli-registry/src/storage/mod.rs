// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Package blob and metadata storage.

pub mod filesystem;
pub mod streaming;
pub mod validate;

use std::pin::Pin;

use futures::Stream;
use thiserror::Error;

pub use filesystem::FilesystemStorage;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("blob not found: {0}")]
    BlobNotFound(String),
    #[error("manifest not found: {name}:{reference}")]
    ManifestNotFound { name: String, reference: String },
    #[error("invalid digest format: {0}")]
    InvalidDigest(String),
    #[error("blob too large: {0}")]
    BlobTooLarge(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<crate::validation::ValidationError> for StorageError {
    fn from(e: crate::validation::ValidationError) -> Self {
        match e {
            crate::validation::ValidationError::InvalidDigest(d) => StorageError::InvalidDigest(d),
            other => StorageError::InvalidInput(other.to_string()),
        }
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, std::io::Error>> + Send>>;

pub trait RegistryStorage: Send + Sync + 'static {
    fn store_blob(
        &self,
        tenant_id: &str,
        digest: &str,
        data: &[u8],
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;

    fn get_blob(
        &self,
        tenant_id: &str,
        digest: &str,
    ) -> impl std::future::Future<Output = StorageResult<(ByteStream, u64)>> + Send;

    fn has_blob(
        &self,
        tenant_id: &str,
        digest: &str,
    ) -> impl std::future::Future<Output = StorageResult<bool>> + Send;

    fn delete_blob(
        &self,
        tenant_id: &str,
        digest: &str,
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;

    fn blob_size(
        &self,
        tenant_id: &str,
        digest: &str,
    ) -> impl std::future::Future<Output = StorageResult<u64>> + Send;

    fn store_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
        data: &[u8],
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;

    fn get_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> impl std::future::Future<Output = StorageResult<Vec<u8>>> + Send;

    fn has_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> impl std::future::Future<Output = StorageResult<bool>> + Send;

    fn delete_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;

    fn list_tags(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> impl std::future::Future<Output = StorageResult<Vec<String>>> + Send;

    fn list_repositories(
        &self,
        tenant_id: &str,
    ) -> impl std::future::Future<Output = StorageResult<Vec<String>>> + Send;

    /// Create a temporary upload file and return its path
    fn create_upload(
        &self,
        tenant_id: &str,
        upload_id: &str,
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;

    /// Append data to an in-progress upload
    fn append_upload(
        &self,
        tenant_id: &str,
        upload_id: &str,
        data: &[u8],
    ) -> impl std::future::Future<Output = StorageResult<u64>> + Send;

    /// Finalize an upload: verify digest and move to blob storage
    fn complete_upload(
        &self,
        tenant_id: &str,
        upload_id: &str,
        digest: &str,
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;

    /// Get the current offset of an upload
    fn upload_offset(
        &self,
        tenant_id: &str,
        upload_id: &str,
    ) -> impl std::future::Future<Output = StorageResult<u64>> + Send;

    /// Get all blob digests in storage for a tenant
    fn list_blobs(
        &self,
        tenant_id: &str,
    ) -> impl std::future::Future<Output = StorageResult<Vec<String>>> + Send;

    /// Get all manifest data for a tenant (for GC reference scanning)
    fn all_manifests(
        &self,
        tenant_id: &str,
    ) -> impl std::future::Future<Output = StorageResult<Vec<Vec<u8>>>> + Send;

    /// Get total storage usage in bytes for a tenant
    fn get_tenant_usage(
        &self,
        tenant_id: &str,
    ) -> impl std::future::Future<Output = StorageResult<u64>> + Send;
}
