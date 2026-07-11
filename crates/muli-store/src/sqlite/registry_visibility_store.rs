// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite per-tenant registry visibility store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::registry::model::RegistryVisibilityLevel;
use muli_core::traits::RegistryVisibilityStore;

use super::factory::SqliteStoreFactory;
use super::util::store_err;

pub struct SqliteRegistryVisibilityStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteRegistryVisibilityStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl RegistryVisibilityStore for SqliteRegistryVisibilityStore {
    async fn get_visibility(&self, tenant_id: &str) -> Result<Option<RegistryVisibilityLevel>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT visibility FROM registry_visibility WHERE tenant_id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![tenant_id])?;
            if let Some(row) = rows.next()? {
                let s: String = row.get(0)?;
                Ok(Some(RegistryVisibilityLevel::parse_lenient(&s)))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn set_visibility(
        &self,
        tenant_id: &str,
        visibility: RegistryVisibilityLevel,
    ) -> Result<()> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        let v = visibility.as_str().to_string();
        conn.call(move |c| {
            c.execute(
                "INSERT INTO registry_visibility (tenant_id, visibility)
                 VALUES (?1, ?2)
                 ON CONFLICT(tenant_id) DO UPDATE SET visibility = excluded.visibility",
                rusqlite::params![tenant_id, v],
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

    async fn make_store() -> (SqliteRegistryVisibilityStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteRegistryVisibilityStore::new(factory), dir)
    }

    #[tokio::test]
    async fn none_when_unset_means_private() {
        let (store, _dir) = make_store().await;
        assert!(store.get_visibility("t1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_and_get_roundtrip() {
        let (store, _dir) = make_store().await;
        store
            .set_visibility("t1", RegistryVisibilityLevel::Public)
            .await
            .unwrap();
        assert_eq!(
            store.get_visibility("t1").await.unwrap(),
            Some(RegistryVisibilityLevel::Public)
        );
    }

    #[tokio::test]
    async fn set_upserts() {
        let (store, _dir) = make_store().await;
        store
            .set_visibility("t1", RegistryVisibilityLevel::Public)
            .await
            .unwrap();
        store
            .set_visibility("t1", RegistryVisibilityLevel::Authenticated)
            .await
            .unwrap();
        assert_eq!(
            store.get_visibility("t1").await.unwrap(),
            Some(RegistryVisibilityLevel::Authenticated)
        );
    }

    #[tokio::test]
    async fn corrupt_value_fails_closed_to_private() {
        let (store, _dir) = make_store().await;
        let conn = store.factory.global_conn();
        conn.call(|c| {
            c.execute(
                "INSERT INTO registry_visibility (tenant_id, visibility) VALUES ('t1', 'bogus')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            store.get_visibility("t1").await.unwrap(),
            Some(RegistryVisibilityLevel::Private)
        );
    }
}
