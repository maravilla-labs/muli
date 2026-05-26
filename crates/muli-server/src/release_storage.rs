// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Release-asset binary storage.
//!
//! Filesystem-backed today, mirroring `muli_pipeline::artifact::storage::
//! ArtifactStorage`; the same shape (upload → (size, sha256); download; delete)
//! makes an S3/object-store backend a drop-in later. Assets are keyed by
//! `tenant_id/release_id/asset_id`.

use std::path::{Path, PathBuf};

use muli_core::error::{MuliError, Result};
use sha2::{Digest, Sha256};
use tokio::fs;

pub struct ReleaseAssetStorage {
    base_dir: PathBuf,
}

impl ReleaseAssetStorage {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            base_dir: data_dir.join("releases").join("assets"),
        }
    }

    /// Storage key for an asset (informational; stored on the asset record).
    pub fn key(tenant_id: &str, release_id: &str, asset_id: &str) -> String {
        format!("{tenant_id}/{release_id}/{asset_id}")
    }

    /// Store asset bytes. Returns `(size_bytes, sha256_hex)`.
    pub async fn upload(
        &self,
        tenant_id: &str,
        release_id: &str,
        asset_id: &str,
        data: &[u8],
    ) -> Result<(u64, String)> {
        validate_component(tenant_id)?;
        validate_component(release_id)?;
        validate_component(asset_id)?;

        let dir = self.base_dir.join(tenant_id).join(release_id);
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| MuliError::Storage(format!("create asset dir: {e}")))?;
        let file_path = dir.join(asset_id);
        fs::write(&file_path, data)
            .await
            .map_err(|e| MuliError::Storage(format!("write asset: {e}")))?;

        let mut hasher = Sha256::new();
        hasher.update(data);
        Ok((data.len() as u64, hex::encode(hasher.finalize())))
    }

    /// Read asset bytes.
    pub async fn download(
        &self,
        tenant_id: &str,
        release_id: &str,
        asset_id: &str,
    ) -> Result<Vec<u8>> {
        validate_component(tenant_id)?;
        validate_component(release_id)?;
        validate_component(asset_id)?;
        let file_path = self
            .base_dir
            .join(tenant_id)
            .join(release_id)
            .join(asset_id);
        fs::read(&file_path)
            .await
            .map_err(|e| MuliError::Storage(format!("read asset: {e}")))
    }

    /// Delete a single asset's bytes (best-effort; missing file is not an error).
    pub async fn delete(&self, tenant_id: &str, release_id: &str, asset_id: &str) -> Result<()> {
        validate_component(tenant_id)?;
        validate_component(release_id)?;
        validate_component(asset_id)?;
        let file_path = self
            .base_dir
            .join(tenant_id)
            .join(release_id)
            .join(asset_id);
        if file_path.exists() {
            fs::remove_file(&file_path)
                .await
                .map_err(|e| MuliError::Storage(format!("delete asset: {e}")))?;
        }
        Ok(())
    }
}

fn validate_component(s: &str) -> Result<()> {
    if s.is_empty() || s.contains("..") || s.contains('/') || s.contains('\\') || s.contains('\0') {
        return Err(MuliError::Validation(format!(
            "invalid path component: {s}"
        )));
    }
    Ok(())
}
