// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm packument (package document) types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Full packument document -- stored as metadata.json per package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packument {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "dist-tags")]
    pub dist_tags: HashMap<String, String>,
    pub versions: HashMap<String, VersionMetadata>,
    #[serde(default)]
    pub time: HashMap<String, String>,
    #[serde(rename = "_rev", default)]
    pub rev: String,
}

/// Per-version metadata within the packument.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionMetadata {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub main: Option<String>,
    #[serde(default)]
    pub dependencies: Option<HashMap<String, String>>,
    #[serde(rename = "devDependencies", default)]
    pub dev_dependencies: Option<HashMap<String, String>>,
    #[serde(rename = "peerDependencies", default)]
    pub peer_dependencies: Option<HashMap<String, String>>,
    pub dist: Dist,
    /// Preserve any extra fields from the publisher.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// Distribution info for a version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dist {
    pub tarball: String,
    pub shasum: String,
    #[serde(default)]
    pub integrity: Option<String>,
}

/// The publish request body from `npm publish`.
/// npm sends a JSON document with `_attachments` containing base64-encoded tarballs.
#[derive(Debug, Deserialize)]
pub struct PublishRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "dist-tags")]
    pub dist_tags: HashMap<String, String>,
    pub versions: HashMap<String, serde_json::Value>,
    #[serde(rename = "_attachments")]
    pub attachments: HashMap<String, Attachment>,
}

#[derive(Debug, Deserialize)]
pub struct Attachment {
    pub data: String,
    #[serde(default)]
    pub length: Option<u64>,
}

/// Abbreviated packument for install operations.
/// Sent when client requests Accept: application/vnd.npm.install-v1+json
#[derive(Debug, Serialize)]
pub struct AbbreviatedPackument {
    pub name: String,
    #[serde(rename = "dist-tags")]
    pub dist_tags: HashMap<String, String>,
    pub versions: HashMap<String, AbbreviatedVersion>,
    pub modified: String,
}

#[derive(Debug, Serialize)]
pub struct AbbreviatedVersion {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<HashMap<String, String>>,
    #[serde(rename = "devDependencies", skip_serializing_if = "Option::is_none")]
    pub dev_dependencies: Option<HashMap<String, String>>,
    #[serde(rename = "peerDependencies", skip_serializing_if = "Option::is_none")]
    pub peer_dependencies: Option<HashMap<String, String>>,
    pub dist: Dist,
}

impl Packument {
    /// Rewrite relative tarball URLs to absolute using the given base
    /// (e.g., `http://127.0.0.1:4000`). Tarball paths stored as `/-/npm/...`
    /// become `{base}/-/npm/...`.
    pub fn with_absolute_tarball_urls(&self, base: &str) -> Self {
        let mut packument = self.clone();
        for meta in packument.versions.values_mut() {
            if meta.dist.tarball.starts_with('/') {
                meta.dist.tarball = format!("{}{}", base, meta.dist.tarball);
            }
        }
        packument
    }

    /// Convert to abbreviated form for install requests.
    pub fn to_abbreviated(&self) -> AbbreviatedPackument {
        let versions = self
            .versions
            .iter()
            .map(|(v, meta)| {
                let abbrev = AbbreviatedVersion {
                    name: meta.name.clone(),
                    version: meta.version.clone(),
                    dependencies: meta.dependencies.clone(),
                    dev_dependencies: meta.dev_dependencies.clone(),
                    peer_dependencies: meta.peer_dependencies.clone(),
                    dist: meta.dist.clone(),
                };
                (v.clone(), abbrev)
            })
            .collect();

        AbbreviatedPackument {
            name: self.name.clone(),
            dist_tags: self.dist_tags.clone(),
            versions,
            modified: self.time.get("modified").cloned().unwrap_or_default(),
        }
    }
}
