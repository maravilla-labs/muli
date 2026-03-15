// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! S3-compatible LFS object storage backend.
//!
//! Enabled via the `lfs-s3` feature flag. Supports any S3-compatible service
//! (AWS S3, MinIO, Cloudflare R2, etc.) via endpoint override.
//!
//! Key layout: `{tenant_id}/lfs/{oid[0:2]}/{oid[2:4]}/{oid}`

use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::presigning::PresigningConfig;
use sha2::{Digest, Sha256};

use super::{LfsByteStream, LfsStorage, LfsStorageError, LfsStorageResult};
use crate::lfs::types::validate_oid;

/// Configuration for the S3 LFS storage backend.
#[derive(Debug, Clone)]
pub struct S3LfsConfig {
    pub bucket: String,
    pub region: String,
    /// Optional endpoint URL for S3-compatible services (MinIO, R2, etc.).
    pub endpoint: Option<String>,
    /// Enable presigned URL generation for direct client↔S3 transfers.
    pub presign_enabled: bool,
}

/// S3-backed LFS object storage.
pub struct S3LfsStorage {
    client: Client,
    bucket: String,
    presign_enabled: bool,
    max_object_size: u64,
}

impl S3LfsStorage {
    /// Create a new S3 LFS storage backend.
    pub async fn new(config: S3LfsConfig, max_object_size: u64) -> LfsStorageResult<Self> {
        let mut sdk_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(config.region));

        if let Some(endpoint) = &config.endpoint {
            sdk_builder = sdk_builder.endpoint_url(endpoint);
        }

        let sdk_config = sdk_builder.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.endpoint.is_some()) // path-style for MinIO/R2
            .build();
        let client = Client::from_conf(s3_config);

        Ok(Self {
            client,
            bucket: config.bucket,
            presign_enabled: config.presign_enabled,
            max_object_size,
        })
    }

    /// S3 key for an LFS object.
    fn object_key(tenant_id: &str, oid: &str) -> String {
        format!("{tenant_id}/lfs/{}/{}/{oid}", &oid[..2], &oid[2..4])
    }
}

#[async_trait]
impl LfsStorage for S3LfsStorage {
    async fn has_object(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<bool> {
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        let key = Self::object_key(tenant_id, oid);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(LfsStorageError::Backend(e.to_string())),
        }
    }

    async fn object_size(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<Option<u64>> {
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        let key = Self::object_key(tenant_id, oid);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(resp) => Ok(Some(resp.content_length().unwrap_or(0) as u64)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(LfsStorageError::Backend(e.to_string())),
        }
    }

    async fn get_object(
        &self,
        tenant_id: &str,
        oid: &str,
    ) -> LfsStorageResult<(LfsByteStream, u64)> {
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        let key = Self::object_key(tenant_id, oid);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                if is_not_found(&e) {
                    LfsStorageError::NotFound(oid.to_string())
                } else {
                    LfsStorageError::Backend(e.to_string())
                }
            })?;

        let size = resp.content_length().unwrap_or(0) as u64;
        let byte_stream = resp.body;

        // Convert AWS ByteStream to our LfsByteStream
        let stream = futures::stream::unfold(byte_stream, |mut bs| async move {
            match bs.next().await {
                Some(Ok(bytes)) => Some((Ok(bytes.to_vec()), bs)),
                Some(Err(e)) => Some((
                    Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
                    bs,
                )),
                None => None,
            }
        });

        Ok((Box::pin(stream), size))
    }

    async fn put_object(
        &self,
        tenant_id: &str,
        oid: &str,
        expected_size: u64,
        body: axum::body::Body,
    ) -> LfsStorageResult<()> {
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        if self.max_object_size > 0 && expected_size > self.max_object_size {
            return Err(LfsStorageError::TooLarge {
                size: expected_size,
                limit: self.max_object_size,
            });
        }

        let key = Self::object_key(tenant_id, oid);

        // Collect body while computing SHA-256 digest.
        // For very large objects, consider multipart upload in the future.
        let mut hasher = Sha256::new();
        let mut collected = Vec::new();
        let mut stream = body.into_data_stream();
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let data = chunk.map_err(|e| {
                LfsStorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
            hasher.update(&data);
            collected.extend_from_slice(&data);
        }

        let computed = hex::encode(hasher.finalize());
        if computed != oid {
            return Err(LfsStorageError::DigestMismatch {
                expected: oid.to_string(),
                computed,
            });
        }

        let byte_stream = aws_sdk_s3::primitives::ByteStream::from(collected);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .content_length(expected_size as i64)
            .body(byte_stream)
            .send()
            .await
            .map_err(|e| LfsStorageError::Backend(e.to_string()))?;

        Ok(())
    }

    async fn delete_object(&self, tenant_id: &str, oid: &str) -> LfsStorageResult<()> {
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        let key = Self::object_key(tenant_id, oid);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| LfsStorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn tenant_usage(&self, tenant_id: &str) -> LfsStorageResult<u64> {
        let prefix = format!("{tenant_id}/lfs/");
        let mut total = 0u64;
        let mut continuation = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);

            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| LfsStorageError::Backend(e.to_string()))?;

            if let Some(contents) = resp.contents() {
                for obj in contents {
                    total += obj.size().unwrap_or(0) as u64;
                }
            }

            match resp.next_continuation_token() {
                Some(token) => continuation = Some(token.to_string()),
                None => break,
            }
        }

        Ok(total)
    }

    async fn presigned_download_url(
        &self,
        tenant_id: &str,
        oid: &str,
        expires_in_secs: u64,
    ) -> LfsStorageResult<Option<String>> {
        if !self.presign_enabled {
            return Ok(None);
        }
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        let key = Self::object_key(tenant_id, oid);
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(expires_in_secs))
            .map_err(|e| LfsStorageError::Backend(e.to_string()))?;

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(presign_config)
            .await
            .map_err(|e| LfsStorageError::Backend(e.to_string()))?;

        Ok(Some(presigned.uri().to_string()))
    }

    async fn presigned_upload_url(
        &self,
        tenant_id: &str,
        oid: &str,
        _size: u64,
        expires_in_secs: u64,
    ) -> LfsStorageResult<Option<String>> {
        if !self.presign_enabled {
            return Ok(None);
        }
        if !validate_oid(oid) {
            return Err(LfsStorageError::InvalidOid(oid.to_string()));
        }
        let key = Self::object_key(tenant_id, oid);
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(expires_in_secs))
            .map_err(|e| LfsStorageError::Backend(e.to_string()))?;

        let presigned = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(presign_config)
            .await
            .map_err(|e| LfsStorageError::Backend(e.to_string()))?;

        Ok(Some(presigned.uri().to_string()))
    }
}

/// Check if an S3 SDK error is a "not found" (NoSuchKey / 404).
fn is_not_found(
    e: &aws_sdk_s3::error::SdkError<impl std::fmt::Debug>,
) -> bool {
    matches!(
        e,
        aws_sdk_s3::error::SdkError::ServiceError(se)
            if se.raw().status().as_u16() == 404
    )
}
