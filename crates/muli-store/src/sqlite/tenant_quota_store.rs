// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite tenant quota store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::registry::model::TenantQuota;
use muli_core::traits::TenantQuotaStore;

use super::factory::SqliteStoreFactory;
use super::util::store_err;

pub struct SqliteTenantQuotaStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteTenantQuotaStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl TenantQuotaStore for SqliteTenantQuotaStore {
    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT tenant_id, max_storage_bytes, current_usage_bytes FROM tenant_quotas WHERE tenant_id = ?1",
            )?;
            let mut rows = stmt.query(rusqlite::params![tenant_id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(TenantQuota {
                    tenant_id: row.get(0)?,
                    max_storage_bytes: row.get::<_, i64>(1)? as u64,
                    current_usage_bytes: row.get::<_, i64>(2)? as u64,
                }))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn set_quota(&self, tenant_id: &str, max_storage_bytes: u64) -> Result<()> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            c.execute(
                "INSERT INTO tenant_quotas (tenant_id, max_storage_bytes, current_usage_bytes)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(tenant_id) DO UPDATE SET max_storage_bytes = excluded.max_storage_bytes",
                rusqlite::params![tenant_id, max_storage_bytes as i64],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn update_usage(&self, tenant_id: &str, current_usage_bytes: u64) -> Result<()> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            c.execute(
                "INSERT INTO tenant_quotas (tenant_id, max_storage_bytes, current_usage_bytes)
                 VALUES (?1, 0, ?2)
                 ON CONFLICT(tenant_id) DO UPDATE SET current_usage_bytes = excluded.current_usage_bytes",
                rusqlite::params![tenant_id, current_usage_bytes as i64],
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

    async fn make_store() -> (SqliteTenantQuotaStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteTenantQuotaStore::new(factory), dir)
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let (store, _dir) = make_store().await;
        store.set_quota("t1", 1_000_000).await.unwrap();
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.max_storage_bytes, 1_000_000);
        assert_eq!(q.current_usage_bytes, 0);
    }

    #[tokio::test]
    async fn test_upsert() {
        let (store, _dir) = make_store().await;
        store.set_quota("t1", 1_000_000).await.unwrap();
        store.set_quota("t1", 2_000_000).await.unwrap();
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.max_storage_bytes, 2_000_000);
    }

    #[tokio::test]
    async fn test_update_usage() {
        let (store, _dir) = make_store().await;
        store.set_quota("t1", 1_000_000).await.unwrap();
        store.update_usage("t1", 500_000).await.unwrap();
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 500_000);
    }

    #[tokio::test]
    async fn test_update_usage_upserts_when_no_quota() {
        let (store, _dir) = make_store().await;
        store.update_usage("missing", 100).await.unwrap();
        let q = store.get_quota("missing").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 100);
        assert_eq!(q.max_storage_bytes, 0);
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let (store, _dir) = make_store().await;
        assert!(store.get_quota("missing").await.unwrap().is_none());
    }
}
