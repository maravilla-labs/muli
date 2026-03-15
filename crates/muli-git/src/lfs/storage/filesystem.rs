// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Filesystem-backed LFS object storage.
//!
//! Layout: `{root}/{tenant_id}/lfs/objects/{oid[0:2]}/{oid[2:4]}/{oid}`
//! Temp:   `{root}/{tenant_id}/lfs/tmp/{uuid}`

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::stream;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{LfsByteStream, LfsStorage, LfsStorageError, LfsStorageResult};
use crate::lfs::types::validate_oid;

/// Filesystem-backed LFS storage with content-addressable deduplication per tenant.
pub struct FilesystemLfsStorage {
    root: PathBuf,
    max_object_size: u64,
}

impl FilesystemLfsStorage {
    /// Create a new filesystem LFS storage rooted at `root`.
    pub async fn new(root: impl Into<PathBuf>, max_object_size: u64) -> LfsStorageResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        Ok(Self {
            root,
            max_object_size,
        })
    }

    /// Path to an LFS object: `{root}/{tenant}/lfs/objects/{oid[0:2]}/{oid[2:4]}/{oid}`
    fn object_path(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<PathBuf> {
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        Ok(self
            .root
            .join(tenant_id)
            .join("lfs")
            .join("objects")
            .join(&oid[..2])
            .join(&oid[2..4])
            .join(oid))
    }

    /// Path for a temporary upload file.
    fn tmp_path(&self, tenant_id: &str) -> PathBuf {
        self.root
            .join(tenant_id)
            .join("lfs")
            .join("tmp")
            .join(uuid::Uuid::new_v4().to_string())
    }

    /// LFS objects directory for a tenant (for usage scanning).
    fn objects_dir(&self, tenant_id: &str) -> PathBuf {
        self.root.join(tenant_id).join("lfs").join("objects")
    }
}

#[async_trait]
impl LfsStorage for FilesystemLfsStorage {
    async fn has_object(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<bool> {
        let path = self.object_path(tenant_id, oid)?;
        Ok(path.exists())
    }

    async fn object_size(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<Option<u64>> {
        let path = self.object_path(tenant_id, oid)?;
        if !path.exists() {
            return Ok(None);
        }
        let meta = fs::metadata(&path).await?;
        Ok(Some(meta.len()))
    }

    async fn get_object(
        &self,
        tenant_id: &str,
        oid: &str,
    ) -> LfsStorageResult<(LfsByteStream, u64)> {
        let path = self.object_path(tenant_id, oid)?;
        if !path.exists() {
            return Err(LfsStorageError::NotFound(oid.to_string()));
        }
        let meta = fs::metadata(&path).await?;
        let size = meta.len();
        let stream = file_to_stream(path);
        Ok((stream, size))
    }

    async fn put_object(
        &self,
        tenant_id: &str,
        oid: &str,
        expected_size: u64,
        body: axum::body::Body,
    ) -> LfsStorageResult<()> {
        if self.max_object_size > 0 && expected_size > self.max_object_size {
            return Err(LfsStorageError::TooLarge {
                size: expected_size,
                limit: self.max_object_size,
            });
        }

        let final_path = self.object_path(tenant_id, oid)?;

        // Already stored — skip (content-addressable dedup).
        if final_path.exists() {
            // Consume and discard the body to avoid connection reset.
            consume_body(body).await;
            return Ok(());
        }

        let tmp = self.tmp_path(tenant_id);
        if let Some(parent) = tmp.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Stream body to temp file while computing SHA-256.
        let computed_oid = stream_to_file_with_hash(&tmp, body, self.max_object_size).await?;

        if computed_oid != oid {
            let _ = fs::remove_file(&tmp).await;
            return Err(LfsStorageError::DigestMismatch {
                expected: oid.to_string(),
                computed: computed_oid,
            });
        }

        // Atomic rename to final path.
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        atomic_move(&tmp, &final_path, oid).await
    }

    async fn delete_object(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<()> {
        let path = self.object_path(tenant_id, oid)?;
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn tenant_usage(&self, tenant_id: &str) -> LfsStorageResult<u64> {
        let dir = self.objects_dir(tenant_id);
        if !dir.exists() {
            return Ok(0);
        }
        let mut total = 0u64;
        collect_dir_size(&dir, &mut total).await?;
        Ok(total)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Stream an axum Body to a file, computing SHA-256 incrementally.
/// Returns the hex-encoded digest.
async fn stream_to_file_with_hash(
    path: &Path,
    body: axum::body::Body,
    max_size: u64,
) -> LfsStorageResult<String> {
    let mut file = fs::File::create(path).await?;
    let mut hasher = Sha256::new();
    let mut written = 0u64;

    let mut body = body.into_data_stream();
    use futures::StreamExt;
    while let Some(chunk) = body.next().await {
        let data = chunk.map_err(|e| {
            std::io::Error::other(format!("body read error: {e}"))
        })?;
        written += data.len() as u64;
        if max_size > 0 && written > max_size {
            let _ = fs::remove_file(path).await;
            return Err(LfsStorageError::TooLarge {
                size: written,
                limit: max_size,
            });
        }
        hasher.update(&data);
        file.write_all(&data).await?;
    }
    file.flush().await?;
    drop(file);

    Ok(hex::encode(hasher.finalize()))
}

/// Atomic move with concurrent-upload dedup handling.
async fn atomic_move(tmp: &Path, final_path: &Path, oid: &str) -> LfsStorageResult<()> {
    match fs::rename(tmp, final_path).await {
        Ok(()) => Ok(()),
        Err(_) if final_path.exists() => {
            // Concurrent upload placed the same content — verify digest.
            let existing_digest = file_sha256(final_path).await?;
            let _ = fs::remove_file(tmp).await;
            if existing_digest == oid {
                Ok(())
            } else {
                Err(LfsStorageError::DigestMismatch {
                    expected: oid.to_string(),
                    computed: existing_digest,
                })
            }
        }
        Err(e) => {
            // Cross-filesystem fallback: copy then delete.
            fs::copy(tmp, final_path).await.map_err(|_| {
                LfsStorageError::Io(std::io::Error::other(format!(
                    "failed to move LFS object to final path: {e}"
                )))
            })?;
            let _ = fs::remove_file(tmp).await;
            Ok(())
        }
    }
}

/// Compute SHA-256 of a file in streaming fashion.
async fn file_sha256(path: &Path) -> LfsStorageResult<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Consume and discard a body stream (prevents connection reset on dedup skip).
async fn consume_body(body: axum::body::Body) {
    use futures::StreamExt;
    let mut stream = body.into_data_stream();
    while let Some(_chunk) = stream.next().await {}
}

/// Convert a file into an async byte stream (64 KiB chunks).
fn file_to_stream(path: PathBuf) -> LfsByteStream {
    const CHUNK_SIZE: usize = 64 * 1024;
    Box::pin(stream::unfold(None::<fs::File>, move |file_opt| {
        let path = path.clone();
        async move {
            let mut file = match file_opt {
                Some(f) => f,
                None => match fs::File::open(&path).await {
                    Ok(f) => f,
                    Err(e) => return Some((Err(e), None)),
                },
            };
            let mut buf = vec![0u8; CHUNK_SIZE];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok(buf), Some(file)))
                }
                Err(e) => Some((Err(e), None)),
            }
        }
    }))
}

/// Recursively sum file sizes in a directory.
async fn collect_dir_size(dir: &Path, total: &mut u64) -> LfsStorageResult<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let ft = entry.file_type().await?;
        if ft.is_file() {
            let meta = fs::metadata(entry.path()).await?;
            *total += meta.len();
        } else if ft.is_dir() {
            Box::pin(collect_dir_size(&entry.path(), total)).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = FilesystemLfsStorage::new(tmp_dir.path(), 0).await.unwrap();

        let data = b"hello world";
        let oid = hex::encode(Sha256::digest(data));

        // Put
        let body = axum::body::Body::from(data.to_vec());
        storage
            .put_object("tenant1", &oid, data.len() as u64, body)
            .await
            .unwrap();

        // Has
        assert!(storage.has_object("tenant1", &oid).await.unwrap());
        assert!(!storage.has_object("tenant2", &oid).await.unwrap());

        // Size
        assert_eq!(
            storage.object_size("tenant1", &oid).await.unwrap(),
            Some(data.len() as u64)
        );

        // Get
        let (stream, size) = storage.get_object("tenant1", &oid).await.unwrap();
        assert_eq!(size, data.len() as u64);
        use futures::StreamExt;
        let chunks: Vec<_> = stream.collect().await;
        let body_bytes: Vec<u8> = chunks.into_iter().flat_map(|r| r.unwrap()).collect();
        assert_eq!(body_bytes, data);

        // Delete
        storage.delete_object("tenant1", &oid).await.unwrap();
        assert!(!storage.has_object("tenant1", &oid).await.unwrap());
    }

    #[tokio::test]
    async fn digest_mismatch_rejected() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = FilesystemLfsStorage::new(tmp_dir.path(), 0).await.unwrap();

        let data = b"some data";
        let wrong_oid = "a".repeat(64);
        let body = axum::body::Body::from(data.to_vec());
        let err = storage
            .put_object("t", &wrong_oid, data.len() as u64, body)
            .await
            .unwrap_err();

        assert!(matches!(err, LfsStorageError::DigestMismatch { .. }));
        // Ensure no file was left behind
        assert!(!storage.has_object("t", &wrong_oid).await.unwrap());
    }

    #[tokio::test]
    async fn dedup_skip_on_existing() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = FilesystemLfsStorage::new(tmp_dir.path(), 0).await.unwrap();

        let data = b"deduplicated";
        let oid = hex::encode(Sha256::digest(data));

        // First upload
        let body = axum::body::Body::from(data.to_vec());
        storage
            .put_object("t", &oid, data.len() as u64, body)
            .await
            .unwrap();

        // Second upload of same content — should succeed silently
        let body = axum::body::Body::from(data.to_vec());
        storage
            .put_object("t", &oid, data.len() as u64, body)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tenant_usage() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = FilesystemLfsStorage::new(tmp_dir.path(), 0).await.unwrap();

        assert_eq!(storage.tenant_usage("t").await.unwrap(), 0);

        let data = b"12345";
        let oid = hex::encode(Sha256::digest(data));
        let body = axum::body::Body::from(data.to_vec());
        storage
            .put_object("t", &oid, data.len() as u64, body)
            .await
            .unwrap();

        assert_eq!(storage.tenant_usage("t").await.unwrap(), 5);
    }

    #[tokio::test]
    async fn size_limit_enforced() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = FilesystemLfsStorage::new(tmp_dir.path(), 10).await.unwrap();

        let data = b"this is more than 10 bytes of data";
        let oid = hex::encode(Sha256::digest(data));
        let body = axum::body::Body::from(data.to_vec());
        let err = storage
            .put_object("t", &oid, data.len() as u64, body)
            .await
            .unwrap_err();

        assert!(matches!(err, LfsStorageError::TooLarge { .. }));
    }
}
