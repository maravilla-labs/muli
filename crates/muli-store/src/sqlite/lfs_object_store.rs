// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite LFS object index: which oids belong to which repository.
//!
//! Rows live in the tenant DB (`lfs_repo_objects`), the one-time migration
//! markers in `_global.db` (`lfs_migrations`).

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::traits::LfsObjectStore;

use super::factory::SqliteStoreFactory;
use super::util::store_err;

pub struct SqliteLfsObjectStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteLfsObjectStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[async_trait]
impl LfsObjectStore for SqliteLfsObjectStore {
    async fn add_lfs_object(&self, tenant_id: &str, repo_id: &str, oid: &str) -> Result<()> {
        self.add_lfs_objects(tenant_id, repo_id, &[oid.to_string()])
            .await
    }

    async fn add_lfs_objects(&self, tenant_id: &str, repo_id: &str, oids: &[String]) -> Result<()> {
        if oids.is_empty() {
            return Ok(());
        }
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let repo_id = repo_id.to_string();
        let oids = oids.to_vec();
        conn.call(move |c| {
            let tx = c.transaction()?;
            {
                let mut stmt = tx.prepare(
                    "INSERT OR IGNORE INTO lfs_repo_objects (repo_id, oid, created_at)
                     VALUES (?1, ?2, ?3)",
                )?;
                let now = now_ms();
                for oid in &oids {
                    stmt.execute(rusqlite::params![repo_id, oid, now])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn has_lfs_object(&self, tenant_id: &str, repo_id: &str, oid: &str) -> Result<bool> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let repo_id = repo_id.to_string();
        let oid = oid.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT 1 FROM lfs_repo_objects WHERE repo_id = ?1 AND oid = ?2 LIMIT 1",
            )?;
            Ok(stmt.exists(rusqlite::params![repo_id, oid])?)
        })
        .await
        .map_err(store_err)
    }

    async fn copy_lfs_objects(
        &self,
        tenant_id: &str,
        from_repo_id: &str,
        to_repo_id: &str,
    ) -> Result<u64> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let from = from_repo_id.to_string();
        let to = to_repo_id.to_string();
        conn.call(move |c| {
            c.execute(
                "INSERT OR IGNORE INTO lfs_repo_objects (repo_id, oid, created_at)
                 SELECT ?2, oid, ?3 FROM lfs_repo_objects WHERE repo_id = ?1",
                rusqlite::params![from, to, now_ms()],
            )?;
            let n: i64 = c.query_row(
                "SELECT COUNT(*) FROM lfs_repo_objects WHERE repo_id = ?1",
                rusqlite::params![to],
                |r| r.get(0),
            )?;
            Ok(n as u64)
        })
        .await
        .map_err(store_err)
    }

    async fn remove_repo_lfs_objects(&self, tenant_id: &str, repo_id: &str) -> Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let repo_id = repo_id.to_string();
        conn.call(move |c| {
            c.execute(
                "DELETE FROM lfs_repo_objects WHERE repo_id = ?1",
                rusqlite::params![repo_id],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn lfs_migration_done(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.factory
            .global_conn()
            .call(move |c| {
                let mut stmt = c.prepare("SELECT 1 FROM lfs_migrations WHERE name = ?1")?;
                Ok(stmt.exists(rusqlite::params![name])?)
            })
            .await
            .map_err(store_err)
    }

    async fn mark_lfs_migration_done(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        self.factory
            .global_conn()
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO lfs_migrations (name, done_at) VALUES (?1, ?2)",
                    rusqlite::params![name, now_ms()],
                )?;
                Ok(())
            })
            .await
            .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index_roundtrip_and_markers() {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        let s = SqliteLfsObjectStore::new(factory);
        let oid = "a".repeat(64);

        assert!(!s.has_lfs_object("t1", "repo-a", &oid).await.unwrap());
        s.add_lfs_object("t1", "repo-a", &oid).await.unwrap();
        s.add_lfs_object("t1", "repo-a", &oid).await.unwrap(); // idempotent
        assert!(s.has_lfs_object("t1", "repo-a", &oid).await.unwrap());
        assert!(!s.has_lfs_object("t1", "repo-b", &oid).await.unwrap());
        assert!(!s.has_lfs_object("t2", "repo-a", &oid).await.unwrap());

        assert_eq!(
            s.copy_lfs_objects("t1", "repo-a", "repo-b").await.unwrap(),
            1
        );
        assert!(s.has_lfs_object("t1", "repo-b", &oid).await.unwrap());

        s.remove_repo_lfs_objects("t1", "repo-a").await.unwrap();
        assert!(!s.has_lfs_object("t1", "repo-a", &oid).await.unwrap());
        assert!(s.has_lfs_object("t1", "repo-b", &oid).await.unwrap());

        assert!(!s.lfs_migration_done("v1").await.unwrap());
        s.mark_lfs_migration_done("v1").await.unwrap();
        s.mark_lfs_migration_done("v1").await.unwrap();
        assert!(s.lfs_migration_done("v1").await.unwrap());
    }
}
