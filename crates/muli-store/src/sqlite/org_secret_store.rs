// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite organization-level pipeline secret store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::pipeline::OrgSecret;
use muli_core::traits::OrgSecretStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteOrgSecretStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteOrgSecretStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl OrgSecretStore for SqliteOrgSecretStore {
    async fn set_org_secret(&self, secret: &OrgSecret) -> Result<()> {
        let conn = self.factory.tenant_conn(&secret.tenant_id).await?;
        let s = secret.clone();
        conn.call(move |c| {
            let json = to_json(&s)?;
            c.execute(
                "INSERT OR REPLACE INTO org_secrets (id, tenant_id, org_id, name, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![s.id, s.tenant_id, s.org_id, s.name, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn get_org_secret(
        &self,
        tenant_id: &str,
        org_id: &str,
        name: &str,
    ) -> Result<Option<OrgSecret>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let oid = org_id.to_string();
        let n = name.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM org_secrets WHERE org_id = ?1 AND name = ?2")?;
            let mut rows = stmt.query(rusqlite::params![oid, n])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<OrgSecret>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_org_names(&self, tenant_id: &str, org_id: &str) -> Result<Vec<String>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let oid = org_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT name FROM org_secrets WHERE org_id = ?1 ORDER BY name")?;
            let mut rows = stmt.query(rusqlite::params![oid])?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let name: String = row.get(0)?;
                result.push(name);
            }
            Ok(result)
        })
        .await
        .map_err(store_err)
    }

    async fn delete_org_secret(&self, tenant_id: &str, org_id: &str, name: &str) -> Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let oid = org_id.to_string();
        let n = name.to_string();
        conn.call(move |c| {
            c.execute(
                "DELETE FROM org_secrets WHERE org_id = ?1 AND name = ?2",
                rusqlite::params![oid, n],
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
    use muli_core::pipeline::OrgSecret;
    use muli_core::traits::OrgSecretStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_org_secret_set_and_get() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteOrgSecretStore::new(factory);
        let secret = OrgSecret::new(
            "t1".into(),
            "org-1".into(),
            "DB_URL".into(),
            "encrypted_value_here".into(),
        );
        store.set_org_secret(&secret).await.unwrap();
        let fetched = store
            .get_org_secret("t1", "org-1", "DB_URL")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.name, "DB_URL");
        assert_eq!(fetched.encrypted_value, "encrypted_value_here");
    }

    #[tokio::test]
    async fn test_org_secret_list_names() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteOrgSecretStore::new(factory);
        let s1 = OrgSecret::new("t1".into(), "org-1".into(), "API_KEY".into(), "enc1".into());
        let s2 = OrgSecret::new("t1".into(), "org-1".into(), "DB_URL".into(), "enc2".into());
        let s3 = OrgSecret::new("t1".into(), "org-2".into(), "OTHER".into(), "enc3".into());
        store.set_org_secret(&s1).await.unwrap();
        store.set_org_secret(&s2).await.unwrap();
        store.set_org_secret(&s3).await.unwrap();

        let names = store.list_org_names("t1", "org-1").await.unwrap();
        assert_eq!(names, vec!["API_KEY", "DB_URL"]);
    }

    #[tokio::test]
    async fn test_org_secret_delete() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteOrgSecretStore::new(factory);
        let secret = OrgSecret::new("t1".into(), "org-1".into(), "DB_URL".into(), "enc".into());
        store.set_org_secret(&secret).await.unwrap();
        store
            .delete_org_secret("t1", "org-1", "DB_URL")
            .await
            .unwrap();
        let fetched = store.get_org_secret("t1", "org-1", "DB_URL").await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_org_secret_upsert() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteOrgSecretStore::new(factory);
        let s1 = OrgSecret::new(
            "t1".into(),
            "org-1".into(),
            "DB_URL".into(),
            "old_enc".into(),
        );
        store.set_org_secret(&s1).await.unwrap();
        let s2 = OrgSecret::new(
            "t1".into(),
            "org-1".into(),
            "DB_URL".into(),
            "new_enc".into(),
        );
        store.set_org_secret(&s2).await.unwrap();
        let fetched = store
            .get_org_secret("t1", "org-1", "DB_URL")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.encrypted_value, "new_enc");
        let names = store.list_org_names("t1", "org-1").await.unwrap();
        assert_eq!(names.len(), 1);
    }
}
