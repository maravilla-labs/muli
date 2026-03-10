// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite tenant configuration store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::tenant::Tenant;
use muli_core::traits::TenantStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err};

pub struct SqliteTenantStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteTenantStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl TenantStore for SqliteTenantStore {
    async fn create_tenant(&self, tenant: &Tenant) -> Result<()> {
        let conn = self.factory.global_conn();
        let tenant = tenant.clone();
        conn.call(move |c| {
            let json = serde_json::to_string(&tenant)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            c.execute(
                "INSERT INTO tenants (id, name, full_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![tenant.id, tenant.name, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn get_tenant(&self, id: &str) -> Result<Option<Tenant>> {
        let conn = self.factory.global_conn();
        let id = id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM tenants WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<Tenant>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_tenants(&self) -> Result<Vec<Tenant>> {
        let conn = self.factory.global_conn();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM tenants ORDER BY id")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                from_json::<Tenant>(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }

    async fn delete_tenant(&self, id: &str) -> Result<()> {
        let conn = self.factory.global_conn();
        let id_owned = id.to_string();
        let rows = conn
            .call(move |c| {
                Ok(c.execute(
                    "DELETE FROM tenants WHERE id = ?1",
                    rusqlite::params![id_owned],
                )?)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            Err(MuliError::Storage(format!("tenant {id} not found")))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> (SqliteTenantStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteTenantStore::new(factory), dir)
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let (store, _dir) = make_store().await;
        let t = Tenant::new("local", "Local Dev", "auto-provisioned");
        store.create_tenant(&t).await.unwrap();
        let fetched = store.get_tenant("local").await.unwrap().unwrap();
        assert_eq!(fetched.id, "local");
        assert_eq!(fetched.name, "Local Dev");
    }

    #[tokio::test]
    async fn test_list_tenants() {
        let (store, _dir) = make_store().await;
        store
            .create_tenant(&Tenant::new("a", "A", ""))
            .await
            .unwrap();
        store
            .create_tenant(&Tenant::new("b", "B", ""))
            .await
            .unwrap();
        let list = store.list_tenants().await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_tenant() {
        let (store, _dir) = make_store().await;
        store
            .create_tenant(&Tenant::new("x", "X", ""))
            .await
            .unwrap();
        store.delete_tenant("x").await.unwrap();
        assert!(store.get_tenant("x").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let (store, _dir) = make_store().await;
        assert!(store.get_tenant("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_id_rejected() {
        let (store, _dir) = make_store().await;
        store
            .create_tenant(&Tenant::new("dup", "Dup", ""))
            .await
            .unwrap();
        assert!(
            store
                .create_tenant(&Tenant::new("dup", "Dup2", ""))
                .await
                .is_err()
        );
    }
}
