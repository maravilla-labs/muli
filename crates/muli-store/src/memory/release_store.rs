// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory release + asset store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::release::{Release, ReleaseAsset};
use muli_core::traits::ReleaseStore;

#[derive(Debug, Clone, Default)]
pub struct MemoryReleaseStore {
    releases: Arc<DashMap<String, Release>>,
    // asset_id -> asset
    assets: Arc<DashMap<String, ReleaseAsset>>,
}

impl MemoryReleaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ReleaseStore for MemoryReleaseStore {
    async fn create_release(&self, release: &Release) -> Result<String> {
        let id = release.id.clone();
        self.releases.insert(id.clone(), release.clone());
        Ok(id)
    }

    async fn get_release(&self, release_id: &str) -> Result<Option<Release>> {
        Ok(self.releases.get(release_id).map(|e| {
            let mut r = e.value().clone();
            r.assets = self.assets_for(&r.id);
            r
        }))
    }

    async fn get_release_by_tag(&self, repo_id: &str, tag: &str) -> Result<Option<Release>> {
        Ok(self
            .releases
            .iter()
            .find(|e| {
                let r = e.value();
                r.repo_id == repo_id && r.tag == tag
            })
            .map(|e| {
                let mut r = e.value().clone();
                r.assets = self.assets_for(&r.id);
                r
            }))
    }

    async fn list_releases(
        &self,
        repo_id: &str,
        published_only: bool,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<Release>> {
        let mut items: Vec<Release> = self
            .releases
            .iter()
            .filter(|e| {
                let r = e.value();
                r.repo_id == repo_id && (!published_only || !r.draft)
            })
            .map(|e| e.value().clone())
            .collect();
        // Newest first.
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let items: Vec<Release> = items
            .into_iter()
            .skip(offset as usize)
            .take(if limit == 0 {
                usize::MAX
            } else {
                limit as usize
            })
            .map(|mut r| {
                r.assets = self.assets_for(&r.id);
                r
            })
            .collect();
        Ok(items)
    }

    async fn update_release(&self, release: &Release) -> Result<()> {
        self.releases.insert(release.id.clone(), release.clone());
        Ok(())
    }

    async fn delete_release(&self, release_id: &str) -> Result<()> {
        self.releases.remove(release_id);
        self.assets.retain(|_, a| a.release_id != release_id);
        Ok(())
    }

    async fn add_asset(&self, asset: &ReleaseAsset) -> Result<String> {
        let id = asset.id.clone();
        self.assets.insert(id.clone(), asset.clone());
        Ok(id)
    }

    async fn list_assets(&self, release_id: &str) -> Result<Vec<ReleaseAsset>> {
        Ok(self.assets_for(release_id))
    }

    async fn get_asset(&self, asset_id: &str) -> Result<Option<ReleaseAsset>> {
        Ok(self.assets.get(asset_id).map(|e| e.value().clone()))
    }

    async fn delete_asset(&self, asset_id: &str) -> Result<()> {
        self.assets.remove(asset_id);
        Ok(())
    }
}

impl MemoryReleaseStore {
    fn assets_for(&self, release_id: &str) -> Vec<ReleaseAsset> {
        let mut v: Vec<ReleaseAsset> = self
            .assets
            .iter()
            .filter(|e| e.value().release_id == release_id)
            .map(|e| e.value().clone())
            .collect();
        v.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        v
    }
}
