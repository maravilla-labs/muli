// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git LFS 2.0 Batch API implementation.
//!
//! Provides content-addressable large file storage with pluggable backends
//! (filesystem, S3) and multi-tenant isolation. Bytes are stored once per
//! tenant; which repository may read them is the LFS object index
//! (`muli_core::traits::LfsObjectStore`, see `api` and `backfill`).

pub mod api;
pub mod backfill;
pub mod storage;
pub mod types;

/// LFS server URL for the repository NAMED `name` (as stored, never a
/// path segment), rooted at the public base URL clients reach the git HTTP
/// service at (e.g. `https://git.example.com`, `acme`, `app` ->
/// `https://git.example.com/acme/app.git/info/lfs`). The `.git` appended here
/// is the one the transport grammar strips again (`crate::repo_target`), so a
/// repository named `app.git` gets `app.git.git` and resolves to itself.
pub fn lfs_endpoint(public_url: &str, namespace: &str, name: &str) -> String {
    let base = public_url.trim_end_matches('/');
    format!("{base}/{namespace}/{name}.git/info/lfs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lfs_endpoint_uses_public_url() {
        assert_eq!(
            lfs_endpoint("https://git.example.com", "acme", "app"),
            "https://git.example.com/acme/app.git/info/lfs"
        );
    }

    #[test]
    fn lfs_endpoint_trims_slash_and_names_the_repository_exactly() {
        assert_eq!(
            lfs_endpoint("https://git.example.com/", "acme", "app"),
            "https://git.example.com/acme/app.git/info/lfs"
        );
        // A repository NAMED `app.git` is `app.git.git` on the wire.
        assert_eq!(
            lfs_endpoint("https://git.example.com", "acme", "app.git"),
            "https://git.example.com/acme/app.git.git/info/lfs"
        );
    }
}
