// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pipeline artifact store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use muli_core::error::Result;
use muli_core::pipeline::Artifact;
use muli_core::traits::ArtifactStore;

use super::factory::SqliteStoreFactory;
use super::util::{dt_to_ms, from_json, store_err, to_json};

pub struct SqliteArtifactStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteArtifactStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl ArtifactStore for SqliteArtifactStore {
    async fn create_artifact(&self, artifact: &Artifact) -> Result<String> {
        let conn = self.factory.tenant_conn(&artifact.tenant_id).await?;
        let a = artifact.clone();
        let id = a.id.clone();
        conn.call(move |c| {
            let json = to_json(&a)?;
            let expires_ms = a.expires_at.map(dt_to_ms);
            c.execute(
                "INSERT INTO pipeline_artifacts (id, tenant_id, run_id, step_name, name, size_bytes, expires_at, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![a.id, a.tenant_id, a.run_id, a.step_name, a.name, a.size_bytes as i64, expires_ms, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_artifact(&self, artifact_id: &str) -> Result<Option<Artifact>> {
        let aid = artifact_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let a = aid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM pipeline_artifacts WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![a])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Artifact>(&json)?))
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

    async fn list_by_run(&self, run_id: &str) -> Result<Vec<Artifact>> {
        let rid = run_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let results: Vec<Artifact> = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM pipeline_artifacts WHERE run_id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![r])?;
                    let mut result = Vec::new();
                    while let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        result.push(from_json::<Artifact>(&json)?);
                    }
                    Ok(result)
                })
                .await
                .map_err(store_err)?;
            if !results.is_empty() {
                return Ok(results);
            }
        }
        Ok(Vec::new())
    }

    async fn delete_expired(&self, before: DateTime<Utc>) -> Result<u64> {
        let before_ms = dt_to_ms(before);
        let mut total = 0u64;
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rows = conn
                .call(move |c| {
                    Ok(c.execute(
                        "DELETE FROM pipeline_artifacts WHERE expires_at IS NOT NULL AND expires_at < ?1",
                        rusqlite::params![before_ms],
                    )?)
                })
                .await
                .map_err(store_err)?;
            total += rows as u64;
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use muli_core::pipeline::Artifact;
    use muli_core::traits::ArtifactStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_artifact_create_and_list() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteArtifactStore::new(factory);
        let a1 = Artifact::new(
            "t1".into(),
            "run-1".into(),
            "build".into(),
            "binary".into(),
            1024,
            "deadbeef".into(),
            None,
        );
        let a2 = Artifact::new(
            "t1".into(),
            "run-1".into(),
            "test".into(),
            "report".into(),
            512,
            "cafebabe".into(),
            None,
        );
        let id1 = store.create_artifact(&a1).await.unwrap();
        store.create_artifact(&a2).await.unwrap();

        let fetched = store.get_artifact(&id1).await.unwrap().unwrap();
        assert_eq!(fetched.name, "binary");
        assert_eq!(fetched.size_bytes, 1024);

        let list = store.list_by_run("run-1").await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_artifact_delete_expired() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteArtifactStore::new(factory);
        let now = Utc::now();
        // Expired artifact
        let a1 = Artifact::new(
            "t1".into(),
            "run-1".into(),
            "build".into(),
            "old".into(),
            100,
            "aaa".into(),
            Some(now - Duration::hours(1)),
        );
        // Non-expired artifact
        let a2 = Artifact::new(
            "t1".into(),
            "run-1".into(),
            "build".into(),
            "new".into(),
            200,
            "bbb".into(),
            Some(now + Duration::hours(1)),
        );
        // No expiry artifact
        let a3 = Artifact::new(
            "t1".into(),
            "run-1".into(),
            "build".into(),
            "forever".into(),
            300,
            "ccc".into(),
            None,
        );
        store.create_artifact(&a1).await.unwrap();
        store.create_artifact(&a2).await.unwrap();
        store.create_artifact(&a3).await.unwrap();

        let deleted = store.delete_expired(now).await.unwrap();
        assert_eq!(deleted, 1);

        let remaining = store.list_by_run("run-1").await.unwrap();
        assert_eq!(remaining.len(), 2);
    }
}
