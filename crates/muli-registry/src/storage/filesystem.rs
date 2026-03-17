// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Filesystem-backed blob and manifest storage.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncReadExt;

use super::streaming::{
    collect_dir_size, collect_manifest_data, collect_repositories, file_to_stream,
};
use super::validate::{ensure_no_traversal, parse_digest, validate_upload_id};
use super::{ByteStream, RegistryStorage, StorageError, StorageResult};
use crate::validation;

pub struct FilesystemStorage {
    root: PathBuf,
    max_blob_size_bytes: u64,
}

impl FilesystemStorage {
    pub async fn new(root_path: impl Into<PathBuf>) -> StorageResult<Self> {
        Self::with_max_blob_size(root_path, 5 * 1024 * 1024 * 1024).await // 5 GB default
    }

    pub async fn with_max_blob_size(
        root_path: impl Into<PathBuf>,
        max_blob_size_bytes: u64,
    ) -> StorageResult<Self> {
        let root = root_path.into();
        fs::create_dir_all(&root).await?;
        Ok(Self {
            root,
            max_blob_size_bytes,
        })
    }

    pub fn root_path(&self) -> &Path {
        &self.root
    }

    fn tenant_root(&self, tenant_id: &str) -> PathBuf {
        self.root.join(tenant_id)
    }

    fn blob_path(&self, tenant_id: &str, digest: &str) -> StorageResult<PathBuf> {
        let (algo, hash) = parse_digest(digest)?;
        let path = self
            .tenant_root(tenant_id)
            .join("blobs")
            .join(algo)
            .join(hash);
        ensure_no_traversal(&path)?;
        Ok(path)
    }

    fn manifest_path(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> StorageResult<PathBuf> {
        validation::validate_repository_name(name)?;
        validation::validate_reference(reference)?;
        let path = self
            .tenant_root(tenant_id)
            .join("manifests")
            .join(name)
            .join(reference);
        ensure_no_traversal(&path)?;
        Ok(path)
    }

    fn upload_path(&self, tenant_id: &str, upload_id: &str) -> StorageResult<PathBuf> {
        validate_upload_id(upload_id)?;
        let path = self.tenant_root(tenant_id).join("uploads").join(upload_id);
        ensure_no_traversal(&path)?;
        Ok(path)
    }
}

impl RegistryStorage for FilesystemStorage {
    async fn store_blob(&self, tenant_id: &str, digest: &str, data: &[u8]) -> StorageResult<()> {
        let path = self.blob_path(tenant_id, digest)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, data).await?;
        Ok(())
    }

    async fn get_blob(&self, tenant_id: &str, digest: &str) -> StorageResult<(ByteStream, u64)> {
        let path = self.blob_path(tenant_id, digest)?;
        if !path.exists() {
            return Err(StorageError::BlobNotFound(digest.to_string()));
        }
        let meta = fs::metadata(&path).await?;
        let size = meta.len();
        let stream = file_to_stream(path);
        Ok((stream, size))
    }

    async fn has_blob(&self, tenant_id: &str, digest: &str) -> StorageResult<bool> {
        let path = self.blob_path(tenant_id, digest)?;
        Ok(path.exists())
    }

    async fn delete_blob(&self, tenant_id: &str, digest: &str) -> StorageResult<()> {
        let path = self.blob_path(tenant_id, digest)?;
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn blob_size(&self, tenant_id: &str, digest: &str) -> StorageResult<u64> {
        let path = self.blob_path(tenant_id, digest)?;
        if !path.exists() {
            return Err(StorageError::BlobNotFound(digest.to_string()));
        }
        let meta = fs::metadata(&path).await?;
        Ok(meta.len())
    }

    async fn store_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
        data: &[u8],
    ) -> StorageResult<()> {
        let path = self.manifest_path(tenant_id, name, reference)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, data).await?;
        Ok(())
    }

    async fn get_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> StorageResult<Vec<u8>> {
        let path = self.manifest_path(tenant_id, name, reference)?;
        if !path.exists() {
            return Err(StorageError::ManifestNotFound {
                name: name.to_string(),
                reference: reference.to_string(),
            });
        }
        let data = fs::read(&path).await?;
        Ok(data)
    }

    async fn has_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> StorageResult<bool> {
        let path = self.manifest_path(tenant_id, name, reference)?;
        Ok(path.exists())
    }

    async fn delete_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> StorageResult<()> {
        let path = self.manifest_path(tenant_id, name, reference)?;
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn list_tags(&self, tenant_id: &str, name: &str) -> StorageResult<Vec<String>> {
        validation::validate_repository_name(name)?;
        let dir = self.tenant_root(tenant_id).join("manifests").join(name);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut tags = Vec::new();
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                tags.push(name.to_string());
            }
        }
        tags.sort();
        Ok(tags)
    }

    async fn list_repositories(&self, tenant_id: &str) -> StorageResult<Vec<String>> {
        let dir = self.tenant_root(tenant_id).join("manifests");
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut repos = Vec::new();
        collect_repositories(&dir, &dir, &mut repos).await?;
        repos.sort();
        Ok(repos)
    }

    async fn create_upload(&self, tenant_id: &str, upload_id: &str) -> StorageResult<()> {
        let path = self.upload_path(tenant_id, upload_id)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, b"").await?;
        Ok(())
    }

    async fn append_upload(
        &self,
        tenant_id: &str,
        upload_id: &str,
        data: &[u8],
    ) -> StorageResult<u64> {
        let path = self.upload_path(tenant_id, upload_id)?;
        if !path.exists() {
            return Err(StorageError::BlobNotFound(format!(
                "upload {upload_id} not found"
            )));
        }
        // Check size limit before appending
        let current_size = fs::metadata(&path).await?.len();
        let new_size = current_size + data.len() as u64;
        if self.max_blob_size_bytes > 0 && new_size > self.max_blob_size_bytes {
            return Err(StorageError::BlobTooLarge(format!(
                "upload would exceed max blob size of {} bytes (current: {}, incoming: {})",
                self.max_blob_size_bytes,
                current_size,
                data.len()
            )));
        }
        use tokio::io::AsyncWriteExt;
        let mut file = fs::OpenOptions::new().append(true).open(&path).await?;
        file.write_all(data).await?;
        file.flush().await?;
        let meta = fs::metadata(&path).await?;
        Ok(meta.len())
    }

    async fn complete_upload(
        &self,
        tenant_id: &str,
        upload_id: &str,
        digest: &str,
    ) -> StorageResult<()> {
        let upload_path = self.upload_path(tenant_id, upload_id)?;
        if !upload_path.exists() {
            return Err(StorageError::BlobNotFound(format!(
                "upload {upload_id} not found"
            )));
        }

        // Verify digest using streaming computation to avoid loading entire blob into memory
        let computed = {
            let mut file = fs::File::open(&upload_path).await?;
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            format!("sha256:{}", hex::encode(hasher.finalize()))
        };
        if computed != digest {
            let _ = fs::remove_file(&upload_path).await;
            return Err(StorageError::InvalidDigest(format!(
                "expected {digest}, got {computed}"
            )));
        }

        // Move to blob storage
        let blob_path = self.blob_path(tenant_id, digest)?;
        if let Some(parent) = blob_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        match fs::rename(&upload_path, &blob_path).await {
            Ok(()) => Ok(()),
            Err(e) => {
                if blob_path.exists() {
                    // Concurrent upload placed the same content — verify digest matches
                    let existing_digest = {
                        let mut file = fs::File::open(&blob_path).await?;
                        let mut hasher = Sha256::new();
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            let n = file.read(&mut buf).await?;
                            if n == 0 {
                                break;
                            }
                            hasher.update(&buf[..n]);
                        }
                        format!("sha256:{}", hex::encode(hasher.finalize()))
                    };
                    if existing_digest == digest {
                        let _ = fs::remove_file(&upload_path).await;
                        return Ok(());
                    }
                    // Digest mismatch on existing file is a serious error
                    let _ = fs::remove_file(&upload_path).await;
                    return Err(StorageError::InvalidDigest(format!(
                        "blob content mismatch for {digest}"
                    )));
                }
                // Actual cross-filesystem case: copy then delete
                fs::copy(&upload_path, &blob_path).await.map_err(|_| {
                    StorageError::Io(std::io::Error::other(format!(
                        "failed to move upload to blob storage: {e}"
                    )))
                })?;
                let _ = fs::remove_file(&upload_path).await;
                Ok(())
            }
        }
    }

    async fn upload_offset(&self, tenant_id: &str, upload_id: &str) -> StorageResult<u64> {
        let path = self.upload_path(tenant_id, upload_id)?;
        if !path.exists() {
            return Err(StorageError::BlobNotFound(format!(
                "upload {upload_id} not found"
            )));
        }
        let meta = fs::metadata(&path).await?;
        Ok(meta.len())
    }

    async fn list_blobs(&self, tenant_id: &str) -> StorageResult<Vec<String>> {
        let blobs_dir = self.tenant_root(tenant_id).join("blobs");
        if !blobs_dir.exists() {
            return Ok(Vec::new());
        }
        let mut digests = Vec::new();
        let mut algos = fs::read_dir(&blobs_dir).await?;
        while let Some(algo_entry) = algos.next_entry().await? {
            if algo_entry.file_type().await?.is_dir() {
                let algo_name = algo_entry.file_name();
                let algo_str = algo_name.to_string_lossy().to_string();
                let mut hashes = fs::read_dir(algo_entry.path()).await?;
                while let Some(hash_entry) = hashes.next_entry().await? {
                    let hash_name = hash_entry.file_name();
                    digests.push(format!("{}:{}", algo_str, hash_name.to_string_lossy()));
                }
            }
        }
        Ok(digests)
    }

    async fn all_manifests(&self, tenant_id: &str) -> StorageResult<Vec<Vec<u8>>> {
        let manifests_dir = self.tenant_root(tenant_id).join("manifests");
        if !manifests_dir.exists() {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        collect_manifest_data(&manifests_dir, &mut result).await?;
        Ok(result)
    }

    async fn get_tenant_usage(&self, tenant_id: &str) -> StorageResult<u64> {
        let root = self.tenant_root(tenant_id);
        if !root.exists() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        // Count all registry storage: blobs, manifests, npm, cargo, maven.
        for subdir in ["blobs", "manifests", "npm", "cargo", "maven"] {
            let dir = root.join(subdir);
            if dir.exists() {
                collect_dir_size(&dir, &mut total).await?;
            }
        }
        Ok(total)
    }
}
