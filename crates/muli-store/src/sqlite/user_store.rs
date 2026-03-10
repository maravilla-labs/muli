// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite user account store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::traits::UserStore;
use muli_core::user::TenantUser;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err};

pub struct SqliteUserStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteUserStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl UserStore for SqliteUserStore {
    async fn create_user(&self, user: &TenantUser) -> Result<String> {
        let conn = self.factory.tenant_conn(&user.tenant_id).await?;
        let user = user.clone();
        let id = user.id.clone();
        conn.call(move |c| {
            let json = serde_json::to_string(&user)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            c.execute(
                "INSERT INTO tenant_users (id, tenant_id, handle, external_id, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![user.id, user.tenant_id, user.handle, user.external_id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_user(&self, user_id: &str) -> Result<Option<TenantUser>> {
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let uid = user_id.to_string();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare("SELECT full_json FROM tenant_users WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![uid])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json(&json)?))
                    } else {
                        Ok(None)
                    }
                })
                .await
                .map_err(store_err)?;
            if result.is_some() {
                return Ok(result);
            }
        }
        Ok(None)
    }

    async fn get_user_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<TenantUser>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let handle = handle.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM tenant_users WHERE handle = ?1")?;
            let mut rows = stmt.query(rusqlite::params![handle])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn get_user_by_external_id(
        &self,
        tenant_id: &str,
        external_id: &str,
    ) -> Result<Option<TenantUser>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let external_id = external_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM tenant_users WHERE external_id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![external_id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn delete_user(&self, user_id: &str) -> Result<()> {
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let uid = user_id.to_string();
            let rows = conn
                .call(move |c| {
                    Ok(c.execute(
                        "DELETE FROM tenant_users WHERE id = ?1",
                        rusqlite::params![uid],
                    )?)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!("User {user_id} not found")))
    }

    async fn list_users(&self, tenant_id: &str) -> Result<Vec<TenantUser>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM tenant_users")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                from_json(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> (SqliteUserStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteUserStore::new(factory), dir)
    }

    #[tokio::test]
    async fn test_create_and_get_by_handle() {
        let (store, _dir) = make_store().await;
        let user = TenantUser::new(
            "t1".into(),
            "alice".into(),
            "ext-1".into(),
            "alice@example.com".into(),
        );
        store.create_user(&user).await.unwrap();
        let fetched = store
            .get_user_by_handle("t1", "alice")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.handle, "alice");
    }

    #[tokio::test]
    async fn test_get_by_external_id() {
        let (store, _dir) = make_store().await;
        let user = TenantUser::new(
            "t1".into(),
            "bob".into(),
            "ext-bob".into(),
            "bob@example.com".into(),
        );
        store.create_user(&user).await.unwrap();
        let fetched = store
            .get_user_by_external_id("t1", "ext-bob")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.handle, "bob");
    }

    #[tokio::test]
    async fn test_duplicate_handle_rejected() {
        let (store, _dir) = make_store().await;
        let u1 = TenantUser::new(
            "t1".into(),
            "alice".into(),
            "ext-1".into(),
            "a1@example.com".into(),
        );
        let u2 = TenantUser::new(
            "t1".into(),
            "alice".into(),
            "ext-2".into(),
            "a2@example.com".into(),
        );
        store.create_user(&u1).await.unwrap();
        assert!(store.create_user(&u2).await.is_err());
    }

    #[tokio::test]
    async fn test_list_users() {
        let (store, _dir) = make_store().await;
        let u1 = TenantUser::new("t1".into(), "alice".into(), "e1".into(), "a@e.com".into());
        let u2 = TenantUser::new("t1".into(), "bob".into(), "e2".into(), "b@e.com".into());
        store.create_user(&u1).await.unwrap();
        store.create_user(&u2).await.unwrap();
        let list = store.list_users("t1").await.unwrap();
        assert_eq!(list.len(), 2);
    }
}
