// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git LFS 2.0 Batch API implementation.
//!
//! Provides content-addressable large file storage with pluggable backends
//! (filesystem, S3) and multi-tenant isolation.

pub mod api;
pub mod storage;
pub mod types;

/// LFS server URL for a repository, rooted at the public base URL clients
/// reach the git HTTP service at (e.g. `https://git.example.com` →
/// `https://git.example.com/acme/app.git/info/lfs`).
pub fn lfs_endpoint(public_url: &str, namespace: &str, repo: &str) -> String {
    let base = public_url.trim_end_matches('/');
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    format!("{base}/{namespace}/{repo}.git/info/lfs")
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
    fn lfs_endpoint_normalizes_slash_and_git_suffix() {
        assert_eq!(
            lfs_endpoint("https://git.example.com/", "acme", "app.git"),
            "https://git.example.com/acme/app.git/info/lfs"
        );
    }
}
