// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Upstream registry token exchange and manifest fetching.

use std::path::PathBuf;

use super::ProxyCacheError;
use crate::proxy_cache::ProxyCache;

impl ProxyCache {
    /// Obtain a bearer token for Docker Hub (or compatible registries).
    pub(super) async fn obtain_docker_token(&self, name: &str) -> Result<String, ProxyCacheError> {
        let is_docker_hub = self.upstream.server.contains("docker.io");
        if !is_docker_hub {
            // For non-Docker-Hub registries, return empty (use Basic auth instead)
            return Ok(String::new());
        }

        let url = format!(
            "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{name}:pull"
        );
        let mut req = self.client.get(&url);
        if let (Some(user), Some(pass)) = (&self.upstream.username, &self.upstream.password) {
            req = req.basic_auth(user, Some(pass));
        }

        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(ProxyCacheError::Auth(format!(
                "token exchange failed: {}",
                resp.status()
            )));
        }

        let token_resp: super::DockerTokenResponse = resp.json().await?;
        Ok(token_resp.token)
    }

    /// Build an authenticated request to the upstream registry.
    pub(super) async fn upstream_request(
        &self,
        name: &str,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, ProxyCacheError> {
        let is_docker_hub = self.upstream.server.contains("docker.io");

        let url = format!("https://{}{}", self.upstream.server, path);
        let mut req = self.client.get(&url);

        if is_docker_hub {
            let token = self.obtain_docker_token(name).await?;
            if !token.is_empty() {
                req = req.bearer_auth(token);
            }
        } else if let (Some(user), Some(pass)) = (&self.upstream.username, &self.upstream.password)
        {
            req = req.basic_auth(user, Some(pass));
        }

        // Accept common manifest media types
        req = req.header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json, \
             application/vnd.docker.distribution.manifest.list.v2+json, \
             application/vnd.oci.image.manifest.v1+json, \
             application/vnd.oci.image.index.v1+json",
        );

        Ok(req)
    }

    pub(super) async fn fetch_manifest_from_upstream(
        &self,
        name: &str,
        reference: &str,
    ) -> Result<Vec<u8>, ProxyCacheError> {
        let path = format!("/v2/{name}/manifests/{reference}");
        let req = self.upstream_request(name, &path).await?;

        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(ProxyCacheError::Upstream(format!(
                "manifest fetch {} returned {}",
                path,
                resp.status()
            )));
        }

        Ok(resp.bytes().await?.to_vec())
    }

    pub(super) async fn fetch_blob_from_upstream(
        &self,
        name: &str,
        digest: &str,
    ) -> Result<PathBuf, ProxyCacheError> {
        use futures::StreamExt;
        use tokio::io::AsyncWriteExt;

        let path = format!("/v2/{name}/blobs/{digest}");
        let req = self.upstream_request(name, &path).await?;

        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(ProxyCacheError::Upstream(format!(
                "blob fetch {} returned {}",
                path,
                resp.status()
            )));
        }

        // Stream directly to a temp file to avoid buffering the entire blob in memory.
        let tmp_path = self
            .storage
            .root_path()
            .join(format!(".blob_tmp_{}", uuid::Uuid::new_v4()));
        let mut file = tokio::fs::File::create(&tmp_path)
            .await
            .map_err(|e| ProxyCacheError::Upstream(format!("failed to create temp file: {e}")))?;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await.map_err(|e| {
                ProxyCacheError::Upstream(format!("failed to write to temp file: {e}"))
            })?;
        }
        file.flush()
            .await
            .map_err(|e| ProxyCacheError::Upstream(format!("failed to flush temp file: {e}")))?;
        drop(file);

        Ok(tmp_path)
    }
}
