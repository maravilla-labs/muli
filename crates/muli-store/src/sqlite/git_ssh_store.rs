// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git SSH key store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::git::SshKey;
use muli_core::traits::SshKeyStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteSshKeyStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteSshKeyStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl SshKeyStore for SqliteSshKeyStore {
    async fn add_key(&self, key: &SshKey) -> Result<String> {
        let conn = self.factory.tenant_conn(&key.tenant_id).await?;
        let key = key.clone();
        let id = key.id.clone();
        conn.call(move |c| {
            let json = to_json(&key)?;
            c.execute(
                "INSERT INTO ssh_keys (id, tenant_id, user_id, fingerprint, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![key.id, key.tenant_id, key.user_id, key.fingerprint, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn remove_key(&self, key_id: &str) -> Result<()> {
        let key_id = key_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let kid = key_id.clone();
            let rows = conn
                .call(move |c| {
                    let rows =
                        c.execute("DELETE FROM ssh_keys WHERE id = ?1", rusqlite::params![kid])?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!("SSH key {key_id} not found")))
    }

    async fn find_by_fingerprint(&self, fingerprint: &str) -> Result<Option<SshKey>> {
        let fp = fingerprint.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let f = fp.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM ssh_keys WHERE fingerprint = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![f])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<SshKey>(&json)?))
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

    async fn list_keys(&self, tenant_id: &str) -> Result<Vec<SshKey>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM ssh_keys")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                from_json::<SshKey>(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }

    async fn list_keys_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<SshKey>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let user_id = user_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM ssh_keys WHERE user_id = ?1")?;
            let rows = stmt.query_map(rusqlite::params![user_id], |row| {
                let json: String = row.get(0)?;
                from_json::<SshKey>(&json)
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

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_ssh_key_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteSshKeyStore::new(factory);
        let key = SshKey::new(
            "t1".into(),
            "SHA256:abc".into(),
            "ssh-ed25519 AAAA".into(),
            "mykey".into(),
            vec![],
        );
        let id = store.add_key(&key).await.unwrap();
        let by_fp = store
            .find_by_fingerprint("SHA256:abc")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_fp.id, id);
        store.remove_key(&id).await.unwrap();
        assert!(
            store
                .find_by_fingerprint("SHA256:abc")
                .await
                .unwrap()
                .is_none()
        );
    }
}
