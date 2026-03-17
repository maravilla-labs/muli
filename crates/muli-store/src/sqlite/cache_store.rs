// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pipeline cache store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::pipeline::CacheEntry;
use muli_core::traits::CacheStore;

use super::factory::SqliteStoreFactory;
use super::util::{dt_to_ms, from_json, store_err, to_json};

pub struct SqliteCacheStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteCacheStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl CacheStore for SqliteCacheStore {
    async fn get_cache(
        &self,
        tenant_id: &str,
        repo_id: &str,
        cache_key: &str,
    ) -> Result<Option<CacheEntry>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let ck = cache_key.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM pipeline_cache WHERE repo_id = ?1 AND cache_key = ?2",
            )?;
            let mut rows = stmt.query(rusqlite::params![rid, ck])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<CacheEntry>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn find_by_prefix(
        &self,
        tenant_id: &str,
        repo_id: &str,
        prefix: &str,
    ) -> Result<Vec<CacheEntry>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let pattern = format!("{prefix}%");
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM pipeline_cache WHERE repo_id = ?1 AND cache_key LIKE ?2",
            )?;
            let mut rows = stmt.query(rusqlite::params![rid, pattern])?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json::<CacheEntry>(&json)?);
            }
            Ok(result)
        })
        .await
        .map_err(store_err)
    }

    async fn upsert_cache(&self, entry: &CacheEntry) -> Result<()> {
        let conn = self.factory.tenant_conn(&entry.tenant_id).await?;
        let e = entry.clone();
        conn.call(move |c| {
            let json = to_json(&e)?;
            c.execute(
                "INSERT OR REPLACE INTO pipeline_cache (id, tenant_id, repo_id, cache_key, size_bytes, last_used_at, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![e.id, e.tenant_id, e.repo_id, e.cache_key, e.size_bytes as i64, dt_to_ms(e.last_used_at), json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn delete_cache(&self, tenant_id: &str, repo_id: &str, cache_key: &str) -> Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let ck = cache_key.to_string();
        conn.call(move |c| {
            c.execute(
                "DELETE FROM pipeline_cache WHERE repo_id = ?1 AND cache_key = ?2",
                rusqlite::params![rid, ck],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn evict_lru(&self, tenant_id: &str, repo_id: &str, max_bytes: u64) -> Result<u64> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let mb = max_bytes as i64;
        conn.call(move |c| {
            let total: i64 = c
                .query_row(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM pipeline_cache WHERE repo_id = ?1",
                    rusqlite::params![rid],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            if total <= mb {
                return Ok(0u64);
            }
            let mut to_free = total - mb;
            let mut stmt = c.prepare(
                "SELECT id, size_bytes FROM pipeline_cache WHERE repo_id = ?1 ORDER BY last_used_at ASC",
            )?;
            let mut rows = stmt.query(rusqlite::params![rid])?;
            let mut ids_to_delete = Vec::new();
            while let Some(row) = rows.next()? {
                if to_free <= 0 {
                    break;
                }
                let id: String = row.get(0)?;
                let size: i64 = row.get(1)?;
                ids_to_delete.push(id);
                to_free -= size;
            }
            drop(rows);
            let count = ids_to_delete.len() as u64;
            for id in ids_to_delete {
                c.execute(
                    "DELETE FROM pipeline_cache WHERE id = ?1",
                    rusqlite::params![id],
                )?;
            }
            Ok(count)
        })
        .await
        .map_err(store_err)
    }

    async fn list_by_repo(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<CacheEntry>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM pipeline_cache WHERE repo_id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![rid])?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json::<CacheEntry>(&json)?);
            }
            Ok(result)
        })
        .await
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::pipeline::CacheEntry;
    use muli_core::traits::CacheStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_cache_upsert_and_get() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteCacheStore::new(factory);
        let entry = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            "cargo-lock".into(),
            5000,
            "sha256abc".into(),
        );
        store.upsert_cache(&entry).await.unwrap();
        let fetched = store
            .get_cache("t1", "repo-1", "cargo-lock")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.cache_key, "cargo-lock");
        assert_eq!(fetched.size_bytes, 5000);
    }

    #[tokio::test]
    async fn test_cache_find_by_prefix() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteCacheStore::new(factory);
        let e1 = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            "cargo-lock-v1".into(),
            100,
            "a".into(),
        );
        let e2 = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            "cargo-lock-v2".into(),
            200,
            "b".into(),
        );
        let e3 = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            "npm-lock".into(),
            300,
            "c".into(),
        );
        store.upsert_cache(&e1).await.unwrap();
        store.upsert_cache(&e2).await.unwrap();
        store.upsert_cache(&e3).await.unwrap();

        let results = store
            .find_by_prefix("t1", "repo-1", "cargo-")
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        let results = store.find_by_prefix("t1", "repo-1", "npm-").await.unwrap();
        assert_eq!(results.len(), 1);

        let results = store
            .find_by_prefix("t1", "repo-1", "missing-")
            .await
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_cache_evict_lru() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteCacheStore::new(factory);
        // Create entries with different last_used_at times
        let mut e1 = CacheEntry::new("t1".into(), "repo-1".into(), "old".into(), 1000, "a".into());
        e1.last_used_at = chrono::Utc::now() - chrono::Duration::hours(3);
        let mut e2 = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            "medium".into(),
            1000,
            "b".into(),
        );
        e2.last_used_at = chrono::Utc::now() - chrono::Duration::hours(2);
        let mut e3 = CacheEntry::new("t1".into(), "repo-1".into(), "new".into(), 1000, "c".into());
        e3.last_used_at = chrono::Utc::now() - chrono::Duration::hours(1);
        store.upsert_cache(&e1).await.unwrap();
        store.upsert_cache(&e2).await.unwrap();
        store.upsert_cache(&e3).await.unwrap();

        // Total is 3000 bytes, evict to 1500 — should remove oldest entries
        let evicted = store.evict_lru("t1", "repo-1", 1500).await.unwrap();
        assert!(evicted >= 1); // At least the oldest should be evicted

        let remaining = store.list_by_repo("t1", "repo-1").await.unwrap();
        let total_size: u64 = remaining.iter().map(|e| e.size_bytes).sum();
        assert!(total_size <= 2000); // Should be under the limit after eviction
    }

    #[tokio::test]
    async fn test_cache_delete() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteCacheStore::new(factory);
        let entry = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            "cargo-lock".into(),
            100,
            "a".into(),
        );
        store.upsert_cache(&entry).await.unwrap();
        store
            .delete_cache("t1", "repo-1", "cargo-lock")
            .await
            .unwrap();
        let fetched = store.get_cache("t1", "repo-1", "cargo-lock").await.unwrap();
        assert!(fetched.is_none());
    }
}
