// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-namespace git storage measurement.
//!
//! Repository metadata (which repos exist in a tenant/namespace) comes from
//! the `RepositoryStore`; sizes come from the bare repository directories at
//! `{git_root}/{tenant}/{namespace}/{repo}.git`. Per-repository sizes are
//! cached for a short TTL so repeated calls (usage polling) cost one metadata
//! query plus cache lookups, and a cold call costs one directory walk per
//! repository of the requested namespace only, never the whole tenant.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::traits::RepositoryStore;

/// How long a measured repository size is reused.
pub const SIZE_TTL: Duration = Duration::from_secs(60);

/// One repository's measured size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoUsage {
    pub name: String,
    pub storage_bytes: u64,
}

/// Measured usage of one namespace.
#[derive(Debug, Clone)]
pub struct NamespaceUsage {
    pub storage_bytes: u64,
    pub repos: Vec<RepoUsage>,
    /// Timestamp of the oldest size included.
    pub measured_at: DateTime<Utc>,
    pub unmeasured_repo_count: u32,
}

#[derive(Clone)]
struct CachedSize {
    bytes: u64,
    at: Instant,
    at_wall: DateTime<Utc>,
}

type CacheKey = (String, String, String);

pub struct NamespaceUsageMeter {
    repo_store: Arc<dyn RepositoryStore>,
    git_root: PathBuf,
    ttl: Duration,
    cache: DashMap<CacheKey, CachedSize>,
}

impl NamespaceUsageMeter {
    pub fn new(repo_store: Arc<dyn RepositoryStore>, git_root: PathBuf) -> Self {
        Self::with_ttl(repo_store, git_root, SIZE_TTL)
    }

    pub fn with_ttl(
        repo_store: Arc<dyn RepositoryStore>,
        git_root: PathBuf,
        ttl: Duration,
    ) -> Self {
        Self {
            repo_store,
            git_root,
            ttl,
            cache: DashMap::new(),
        }
    }

    /// Measure all repositories of `namespace` in `tenant_id`. An unknown
    /// namespace yields an empty result, not an error.
    pub async fn measure(&self, tenant_id: &str, namespace: &str) -> Result<NamespaceUsage> {
        let mut repos: Vec<_> = self
            .repo_store
            .list_repositories(tenant_id)
            .await?
            .into_iter()
            .filter(|r| r.tenant_id == tenant_id && r.namespace == namespace)
            .collect();
        repos.sort_by(|a, b| a.name.cmp(&b.name));

        // Drop cache entries of repos that no longer exist in this namespace.
        self.cache.retain(|(t, n, name), _| {
            t != tenant_id || n != namespace || repos.iter().any(|r| &r.name == name)
        });

        let now_wall = Utc::now();
        let mut out = Vec::with_capacity(repos.len());
        let mut total = 0u64;
        let mut oldest = now_wall;
        let mut unmeasured = 0u32;

        for repo in &repos {
            let key = (
                tenant_id.to_string(),
                namespace.to_string(),
                repo.name.clone(),
            );
            let cached = self
                .cache
                .get(&key)
                .filter(|c| c.at.elapsed() < self.ttl)
                .map(|c| c.clone());
            let (bytes, at_wall) = match cached {
                Some(c) => (c.bytes, c.at_wall),
                None => {
                    let dir = self
                        .git_root
                        .join(tenant_id)
                        .join(namespace)
                        .join(format!("{}.git", repo.name));
                    match muli_git::hooks::compute_dir_size(&dir).await {
                        Ok(bytes) => {
                            let at_wall = Utc::now();
                            self.cache.insert(
                                key,
                                CachedSize {
                                    bytes,
                                    at: Instant::now(),
                                    at_wall,
                                },
                            );
                            (bytes, at_wall)
                        }
                        Err(e) => {
                            tracing::warn!(
                                tenant_id, namespace, repo = %repo.name, error = %e,
                                "namespace usage: repository directory not measurable"
                            );
                            unmeasured += 1;
                            (0, now_wall)
                        }
                    }
                }
            };
            oldest = oldest.min(at_wall);
            total = total.saturating_add(bytes);
            out.push(RepoUsage {
                name: repo.name.clone(),
                storage_bytes: bytes,
            });
        }

        Ok(NamespaceUsage {
            storage_bytes: total,
            repos: out,
            measured_at: oldest,
            unmeasured_repo_count: unmeasured,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::git::Repository;
    use muli_store::memory::MemoryRepositoryStore;

    fn repo(tenant: &str, ns: &str, name: &str) -> Repository {
        Repository::new(
            tenant.to_string(),
            ns.to_string(),
            name.to_string(),
            String::new(),
            false,
        )
        .expect("valid repo")
    }

    async fn write_repo(root: &std::path::Path, t: &str, ns: &str, name: &str, bytes: usize) {
        let dir = root
            .join(t)
            .join(ns)
            .join(format!("{name}.git"))
            .join("objects");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("pack"), vec![0u8; bytes])
            .await
            .unwrap();
    }

    async fn setup() -> (tempfile::TempDir, Arc<MemoryRepositoryStore>) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryRepositoryStore::new());
        for (t, ns, n, b) in [
            ("local", "alice", "a1", 100usize),
            ("local", "alice", "a2", 250),
            ("local", "bob", "b1", 4000),
            ("other", "alice", "x1", 9000),
        ] {
            store.create_repository(&repo(t, ns, n)).await.unwrap();
            write_repo(tmp.path(), t, ns, n, b).await;
        }
        (tmp, store)
    }

    #[tokio::test]
    async fn measures_namespace_repos() {
        let (tmp, store) = setup().await;
        let m = NamespaceUsageMeter::new(store, tmp.path().to_path_buf());
        let u = m.measure("local", "alice").await.unwrap();
        assert_eq!(u.storage_bytes, 350);
        assert_eq!(
            u.repos,
            vec![
                RepoUsage {
                    name: "a1".into(),
                    storage_bytes: 100
                },
                RepoUsage {
                    name: "a2".into(),
                    storage_bytes: 250
                },
            ]
        );
        assert_eq!(u.unmeasured_repo_count, 0);
    }

    #[tokio::test]
    async fn namespaces_and_tenants_are_isolated() {
        let (tmp, store) = setup().await;
        let m = NamespaceUsageMeter::new(store, tmp.path().to_path_buf());
        let bob = m.measure("local", "bob").await.unwrap();
        assert_eq!(bob.storage_bytes, 4000);
        assert!(bob.repos.iter().all(|r| r.name == "b1"));
        // Same namespace name in another tenant is a different namespace.
        let other = m.measure("other", "alice").await.unwrap();
        assert_eq!(other.storage_bytes, 9000);
        assert_eq!(other.repos.len(), 1);
    }

    #[tokio::test]
    async fn sum_over_namespaces_equals_tenant_git_total() {
        let (tmp, store) = setup().await;
        let m = NamespaceUsageMeter::new(store, tmp.path().to_path_buf());
        let sum: u64 = m.measure("local", "alice").await.unwrap().storage_bytes
            + m.measure("local", "bob").await.unwrap().storage_bytes;
        let disk = muli_git::hooks::compute_dir_size(&tmp.path().join("local"))
            .await
            .unwrap();
        assert_eq!(sum, disk);
    }

    #[tokio::test]
    async fn unknown_namespace_is_empty_not_error() {
        let (tmp, store) = setup().await;
        let m = NamespaceUsageMeter::new(store, tmp.path().to_path_buf());
        let u = m.measure("local", "nobody").await.unwrap();
        assert_eq!(u.storage_bytes, 0);
        assert!(u.repos.is_empty());
    }

    #[tokio::test]
    async fn sizes_are_cached_within_ttl_and_refreshed_after() {
        let (tmp, store) = setup().await;
        let m = NamespaceUsageMeter::with_ttl(
            store,
            tmp.path().to_path_buf(),
            Duration::from_millis(150),
        );
        assert_eq!(m.measure("local", "bob").await.unwrap().storage_bytes, 4000);
        write_repo(tmp.path(), "local", "bob", "b1", 5000).await;
        assert_eq!(m.measure("local", "bob").await.unwrap().storage_bytes, 4000);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(m.measure("local", "bob").await.unwrap().storage_bytes, 5000);
    }

    #[tokio::test]
    async fn missing_directory_counts_as_unmeasured() {
        let (tmp, store) = setup().await;
        store
            .create_repository(&repo("local", "bob", "ghost"))
            .await
            .unwrap();
        let m = NamespaceUsageMeter::new(store, tmp.path().to_path_buf());
        let u = m.measure("local", "bob").await.unwrap();
        assert_eq!(u.repos.len(), 2);
        assert_eq!(u.unmeasured_repo_count, 1);
        assert_eq!(u.storage_bytes, 4000);
    }
}
