// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait for releases and their assets.

use async_trait::async_trait;

use crate::error::Result;
use crate::release::{Release, ReleaseAsset};

/// Persistent storage for releases.
#[async_trait]
pub trait ReleaseStore: Send + Sync {
    /// Create a new release record. Returns the new release id.
    async fn create_release(&self, release: &Release) -> Result<String>;

    /// Get a release by id.
    async fn get_release(&self, release_id: &str) -> Result<Option<Release>>;

    /// Get a release by repository + tag.
    async fn get_release_by_tag(&self, repo_id: &str, tag: &str) -> Result<Option<Release>>;

    /// List releases for a repository, newest first. When `published_only` is
    /// true, drafts are excluded (used for unauthenticated/public listings).
    async fn list_releases(
        &self,
        repo_id: &str,
        published_only: bool,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<Release>>;

    /// Update an existing release record.
    async fn update_release(&self, release: &Release) -> Result<()>;

    /// Delete a release (and its asset records).
    async fn delete_release(&self, release_id: &str) -> Result<()>;

    // --- Assets ---

    /// Attach an asset record to a release. Returns the new asset id.
    async fn add_asset(&self, asset: &ReleaseAsset) -> Result<String>;

    /// List the assets of a release.
    async fn list_assets(&self, release_id: &str) -> Result<Vec<ReleaseAsset>>;

    /// Get a single asset by id.
    async fn get_asset(&self, asset_id: &str) -> Result<Option<ReleaseAsset>>;

    /// Delete an asset record.
    async fn delete_asset(&self, asset_id: &str) -> Result<()>;
}
