// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Release models: tag-anchored, named snapshots of a repository with optional
//! notes and downloadable binary assets.
//!
//! Releases carry no access control of their own — they inherit the
//! repository's visibility/anonymous-pull gate. The download path enforces the
//! same authorization as git read.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::MuliError;

/// A downloadable binary attached to a release.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseAsset {
    pub id: String,
    pub tenant_id: String,
    pub release_id: String,
    pub name: String,
    pub size: u64,
    pub sha256: String,
    pub content_type: String,
    /// Internal object-storage key. Downloads are served behind the same auth
    /// gate as git read access — never a bare public URL for private repos.
    pub storage_key: String,
    pub created_at: DateTime<Utc>,
}

/// A release: a tag-anchored, named snapshot with notes + assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Release {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    /// Git tag this release is anchored to (e.g. "v1.0.0").
    pub tag: String,
    /// Branch or commit the tag points at (informational).
    pub target_commitish: String,
    pub name: String,
    /// Release notes / changelog (markdown).
    pub body: String,
    /// Draft releases are unpublished and never trigger release pipelines.
    pub draft: bool,
    /// Prerelease marks non-production releases (e.g. v1.0.0-rc1).
    pub prerelease: bool,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Set when the release transitions from draft to published.
    pub published_at: Option<DateTime<Utc>>,
    pub assets: Vec<ReleaseAsset>,
}

/// Parameters for creating a new release.
pub struct NewRelease {
    pub tenant_id: String,
    pub repo_id: String,
    pub tag: String,
    pub target_commitish: String,
    pub name: String,
    pub body: String,
    pub draft: bool,
    pub prerelease: bool,
    pub created_by: String,
}

impl Release {
    pub fn new(params: NewRelease) -> Result<Self, MuliError> {
        if params.tag.is_empty() {
            return Err(MuliError::Validation("tag must not be empty".into()));
        }
        if params.repo_id.is_empty() {
            return Err(MuliError::Validation("repo_id must not be empty".into()));
        }
        let now = Utc::now();
        // A release defaults its display name to the tag when none is given.
        let name = if params.name.is_empty() {
            params.tag.clone()
        } else {
            params.name
        };
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            tenant_id: params.tenant_id,
            repo_id: params.repo_id,
            tag: params.tag,
            target_commitish: params.target_commitish,
            name,
            body: params.body,
            draft: params.draft,
            prerelease: params.prerelease,
            created_by: params.created_by,
            created_at: now,
            updated_at: now,
            // Drafts have no publish timestamp until they are published.
            published_at: if params.draft { None } else { Some(now) },
            assets: Vec::new(),
        })
    }
}

impl ReleaseAsset {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: String,
        release_id: String,
        name: String,
        size: u64,
        sha256: String,
        content_type: String,
        storage_key: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            release_id,
            name,
            size,
            sha256,
            content_type,
            storage_key,
            created_at: Utc::now(),
        }
    }
}
