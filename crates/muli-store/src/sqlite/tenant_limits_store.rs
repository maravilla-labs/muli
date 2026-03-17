// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite tenant limits store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use muli_core::error::Result;
use muli_core::tenant::limits::TenantLimits;
use muli_core::traits::TenantLimitsStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteTenantLimitsStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteTenantLimitsStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl TenantLimitsStore for SqliteTenantLimitsStore {
    async fn get_limits(&self, tenant_id: &str) -> Result<Option<TenantLimits>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM tenant_limits WHERE tenant_id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![tenant_id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<TenantLimits>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn set_limits(&self, limits: &TenantLimits) -> Result<()> {
        let conn = self.factory.global_conn();
        let limits = limits.clone();
        conn.call(move |c| {
            let json = to_json(&limits)?;
            c.execute(
                "INSERT INTO tenant_limits (tenant_id, full_json)
                 VALUES (?1, ?2)
                 ON CONFLICT(tenant_id) DO UPDATE SET full_json = excluded.full_json",
                rusqlite::params![limits.tenant_id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn is_suspended(&self, tenant_id: &str) -> Result<bool> {
        match self.get_limits(tenant_id).await? {
            Some(l) => Ok(l.suspended),
            None => Ok(false),
        }
    }

    async fn increment_daily_run_count(&self, tenant_id: &str) -> Result<u64> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        let today = Utc::now().format("%Y-%m-%d").to_string();
        conn.call(move |c| {
            c.execute(
                "INSERT INTO tenant_daily_run_counts (tenant_id, run_date, count)
                 VALUES (?1, ?2, 1)
                 ON CONFLICT(tenant_id, run_date) DO UPDATE SET count = count + 1",
                rusqlite::params![tenant_id, today],
            )?;
            let count: i64 = c.query_row(
                "SELECT count FROM tenant_daily_run_counts WHERE tenant_id = ?1 AND run_date = ?2",
                rusqlite::params![tenant_id, today],
                |row| row.get(0),
            )?;
            Ok(count as u64)
        })
        .await
        .map_err(store_err)
    }

    async fn get_daily_run_count(&self, tenant_id: &str) -> Result<u64> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        let today = Utc::now().format("%Y-%m-%d").to_string();
        conn.call(move |c| {
            let count: i64 = c
                .query_row(
                    "SELECT count FROM tenant_daily_run_counts WHERE tenant_id = ?1 AND run_date = ?2",
                    rusqlite::params![tenant_id, today],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            Ok(count as u64)
        })
        .await
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> (SqliteTenantLimitsStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteTenantLimitsStore::new(factory), dir)
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let (store, _dir) = make_store().await;
        let limits = TenantLimits::new("t1");
        store.set_limits(&limits).await.unwrap();
        let fetched = store.get_limits("t1").await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id, "t1");
        assert_eq!(fetched.max_concurrent_jobs, 0);
    }

    #[tokio::test]
    async fn test_upsert() {
        let (store, _dir) = make_store().await;
        let mut limits = TenantLimits::new("t1");
        store.set_limits(&limits).await.unwrap();
        limits.max_concurrent_jobs = 5;
        store.set_limits(&limits).await.unwrap();
        let fetched = store.get_limits("t1").await.unwrap().unwrap();
        assert_eq!(fetched.max_concurrent_jobs, 5);
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let (store, _dir) = make_store().await;
        assert!(store.get_limits("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_is_suspended() {
        let (store, _dir) = make_store().await;
        assert!(!store.is_suspended("t1").await.unwrap());
        let mut limits = TenantLimits::new("t1");
        limits.suspended = true;
        store.set_limits(&limits).await.unwrap();
        assert!(store.is_suspended("t1").await.unwrap());
    }

    #[tokio::test]
    async fn test_daily_run_count() {
        let (store, _dir) = make_store().await;
        assert_eq!(store.get_daily_run_count("t1").await.unwrap(), 0);
        let c1 = store.increment_daily_run_count("t1").await.unwrap();
        assert_eq!(c1, 1);
        let c2 = store.increment_daily_run_count("t1").await.unwrap();
        assert_eq!(c2, 2);
        assert_eq!(store.get_daily_run_count("t1").await.unwrap(), 2);
    }
}
