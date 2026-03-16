// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pipeline secret store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::pipeline::PipelineSecret;
use muli_core::traits::PipelineSecretStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqlitePipelineSecretStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqlitePipelineSecretStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl PipelineSecretStore for SqlitePipelineSecretStore {
    async fn set_secret(&self, secret: &PipelineSecret) -> Result<()> {
        let conn = self.factory.tenant_conn(&secret.tenant_id).await?;
        let s = secret.clone();
        conn.call(move |c| {
            let json = to_json(&s)?;
            c.execute(
                "INSERT OR REPLACE INTO pipeline_secrets (id, tenant_id, repo_id, name, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![s.id, s.tenant_id, s.repo_id, s.name, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn get_secret(&self, tenant_id: &str, repo_id: &str, name: &str) -> Result<Option<PipelineSecret>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let n = name.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM pipeline_secrets WHERE repo_id = ?1 AND name = ?2",
            )?;
            let mut rows = stmt.query(rusqlite::params![rid, n])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<PipelineSecret>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_names(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<String>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT name FROM pipeline_secrets WHERE repo_id = ?1 ORDER BY name",
            )?;
            let mut rows = stmt.query(rusqlite::params![rid])?;
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

    async fn delete_secret(&self, tenant_id: &str, repo_id: &str, name: &str) -> Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let n = name.to_string();
        conn.call(move |c| {
            c.execute(
                "DELETE FROM pipeline_secrets WHERE repo_id = ?1 AND name = ?2",
                rusqlite::params![rid, n],
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
    use muli_core::pipeline::PipelineSecret;
    use muli_core::traits::PipelineSecretStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_secret_set_and_get() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineSecretStore::new(factory);
        let secret = PipelineSecret::new(
            "t1".into(),
            "repo-1".into(),
            "DB_URL".into(),
            "encrypted_value_here".into(),
        );
        store.set_secret(&secret).await.unwrap();
        let fetched = store.get_secret("t1", "repo-1", "DB_URL").await.unwrap().unwrap();
        assert_eq!(fetched.name, "DB_URL");
        assert_eq!(fetched.encrypted_value, "encrypted_value_here");
    }

    #[tokio::test]
    async fn test_secret_list_names_no_values() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineSecretStore::new(factory);
        let s1 = PipelineSecret::new("t1".into(), "repo-1".into(), "API_KEY".into(), "enc1".into());
        let s2 = PipelineSecret::new("t1".into(), "repo-1".into(), "DB_URL".into(), "enc2".into());
        let s3 = PipelineSecret::new("t1".into(), "repo-2".into(), "OTHER".into(), "enc3".into());
        store.set_secret(&s1).await.unwrap();
        store.set_secret(&s2).await.unwrap();
        store.set_secret(&s3).await.unwrap();

        let names = store.list_names("t1", "repo-1").await.unwrap();
        assert_eq!(names, vec!["API_KEY", "DB_URL"]);
    }

    #[tokio::test]
    async fn test_secret_delete() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineSecretStore::new(factory);
        let secret = PipelineSecret::new("t1".into(), "repo-1".into(), "DB_URL".into(), "enc".into());
        store.set_secret(&secret).await.unwrap();
        store.delete_secret("t1", "repo-1", "DB_URL").await.unwrap();
        let fetched = store.get_secret("t1", "repo-1", "DB_URL").await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_secret_upsert_updates() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineSecretStore::new(factory);
        let s1 = PipelineSecret::new("t1".into(), "repo-1".into(), "DB_URL".into(), "old_enc".into());
        store.set_secret(&s1).await.unwrap();
        let s2 = PipelineSecret::new("t1".into(), "repo-1".into(), "DB_URL".into(), "new_enc".into());
        store.set_secret(&s2).await.unwrap();
        let fetched = store.get_secret("t1", "repo-1", "DB_URL").await.unwrap().unwrap();
        assert_eq!(fetched.encrypted_value, "new_enc");
        // Should still be only one secret for that name
        let names = store.list_names("t1", "repo-1").await.unwrap();
        assert_eq!(names.len(), 1);
    }
}
