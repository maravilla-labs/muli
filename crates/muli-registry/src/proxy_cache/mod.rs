// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Upstream registry proxy and caching layer.

mod upstream;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{debug, warn};

use crate::storage::{FilesystemStorage, RegistryStorage, StorageError, StorageResult};

/// Configuration for an upstream OCI registry to proxy through.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamRegistry {
    /// Registry host, e.g. "registry-1.docker.io"
    pub server: String,
    /// Optional username for Basic auth
    pub username: Option<String>,
    /// Optional password for Basic auth
    pub password: Option<String>,
    /// Manifest cache TTL in seconds (default: 86400 = 24h).
    /// Blobs are content-addressed and never expire.
    pub cache_ttl_seconds: u64,
}

impl Default for UpstreamRegistry {
    fn default() -> Self {
        Self {
            server: "registry-1.docker.io".into(),
            username: None,
            password: None,
            cache_ttl_seconds: 86400,
        }
    }
}

/// Metadata stored alongside cached manifests to track freshness.
#[derive(Debug, Serialize, Deserialize)]
struct CacheMetadata {
    cached_at: DateTime<Utc>,
    upstream_server: String,
}

/// Docker Hub token response.
#[derive(Debug, Deserialize)]
struct DockerTokenResponse {
    token: String,
}

/// Pull-through cache that checks local storage first, then fetches from upstream.
pub struct ProxyCache {
    pub(crate) storage: Arc<FilesystemStorage>,
    pub(crate) upstream: UpstreamRegistry,
    pub(crate) client: Client,
}

impl ProxyCache {
    pub fn new(
        upstream: UpstreamRegistry,
        storage: Arc<FilesystemStorage>,
    ) -> Result<Self, reqwest::Error> {
        let client = Client::builder().user_agent("muli-registry/0.1").build()?;
        Ok(Self {
            storage,
            upstream,
            client,
        })
    }

    /// Get a manifest, checking the local cache first.
    /// On miss or stale entry, pulls from upstream and caches locally.
    pub async fn get_manifest(
        &self,
        tenant_id: &str,
        name: &str,
        reference: &str,
    ) -> StorageResult<Vec<u8>> {
        // Check local cache first
        if let Ok(data) = self.storage.get_manifest(tenant_id, name, reference).await {
            if !self.is_manifest_stale(tenant_id, name, reference).await {
                debug!(
                    tenant_id = %tenant_id,
                    name = %name,
                    reference = %reference,
                    "manifest cache hit"
                );
                return Ok(data);
            }
            debug!(
                tenant_id = %tenant_id,
                name = %name,
                reference = %reference,
                "manifest cache stale, re-fetching"
            );
        }

        // Cache miss or stale — fetch from upstream
        let data = self
            .fetch_manifest_from_upstream(name, reference)
            .await
            .map_err(|e| {
                warn!(
                    tenant_id = %tenant_id,
                    name = %name,
                    reference = %reference,
                    error = %e,
                    "upstream manifest fetch failed"
                );
                StorageError::ManifestNotFound {
                    name: name.to_string(),
                    reference: reference.to_string(),
                }
            })?;

        // Store in local cache
        if let Err(e) = self
            .storage
            .store_manifest(tenant_id, name, reference, &data)
            .await
        {
            warn!(error = %e, "failed to cache manifest locally");
        } else {
            self.write_cache_metadata(tenant_id, name, reference).await;
        }

        Ok(data)
    }

    /// Get a blob, checking local storage first.
    /// Blobs are content-addressed and never expire.
    pub async fn get_blob(
        &self,
        tenant_id: &str,
        name: &str,
        digest: &str,
    ) -> StorageResult<Vec<u8>> {
        // Blobs are content-addressed — if present, always valid
        if self
            .storage
            .has_blob(tenant_id, digest)
            .await
            .unwrap_or(false)
        {
            debug!(
                tenant_id = %tenant_id,
                digest = %digest,
                "blob cache hit"
            );
            let (stream, _size) = self.storage.get_blob(tenant_id, digest).await?;
            return collect_stream(stream).await;
        }

        // Fetch from upstream, streaming to a temp file to avoid large in-memory buffers
        let tmp_path = self
            .fetch_blob_from_upstream(name, digest)
            .await
            .map_err(|e| {
                warn!(
                    tenant_id = %tenant_id,
                    digest = %digest,
                    error = %e,
                    "upstream blob fetch failed"
                );
                StorageError::BlobNotFound(digest.to_string())
            })?;

        // Read temp file and store in blob storage, then clean up temp file
        let blob_data: Vec<u8> = match fs::read(&tmp_path).await {
            Ok(d) => d,
            Err(e) => {
                let _ = fs::remove_file(&tmp_path).await;
                return Err(StorageError::Io(e));
            }
        };
        let _ = fs::remove_file(&tmp_path).await;

        // Store locally
        if let Err(e) = self.storage.store_blob(tenant_id, digest, &blob_data).await {
            warn!(error = %e, "failed to cache blob locally");
        }

        Ok(blob_data)
    }

    /// Check if a cached manifest is stale based on its metadata file.
    async fn is_manifest_stale(&self, tenant_id: &str, name: &str, reference: &str) -> bool {
        let meta_path = self
            .storage
            .root_path()
            .join(tenant_id)
            .join("manifests")
            .join(name)
            .join(format!("{reference}.meta"));

        let meta = match fs::read_to_string(&meta_path).await {
            Ok(s) => s,
            Err(_) => return true, // No metadata = treat as stale
        };

        let cache_meta: CacheMetadata = match serde_json::from_str(&meta) {
            Ok(m) => m,
            Err(_) => return true,
        };

        let age = Utc::now()
            .signed_duration_since(cache_meta.cached_at)
            .num_seconds()
            .max(0) as u64;
        age > self.upstream.cache_ttl_seconds
    }

    /// Write cache metadata for a manifest entry.
    async fn write_cache_metadata(&self, tenant_id: &str, name: &str, reference: &str) {
        let meta_path = self
            .storage
            .root_path()
            .join(tenant_id)
            .join("manifests")
            .join(name)
            .join(format!("{reference}.meta"));

        let meta = CacheMetadata {
            cached_at: Utc::now(),
            upstream_server: self.upstream.server.clone(),
        };

        if let Ok(json) = serde_json::to_string(&meta) {
            let _ = fs::write(&meta_path, json).await;
        }
    }
}

/// Collect a byte stream into a Vec<u8>.
async fn collect_stream(stream: crate::storage::ByteStream) -> StorageResult<Vec<u8>> {
    use futures::StreamExt;
    let mut buf = Vec::new();
    let mut stream = stream;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(StorageError::Io)?;
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// Internal error type for proxy cache operations.
#[derive(Debug, thiserror::Error)]
pub enum ProxyCacheError {
    #[error("upstream error: {0}")]
    Upstream(String),
    #[error("auth error: {0}")]
    Auth(String),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}
