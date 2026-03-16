// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pipeline definition store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::pipeline::Pipeline;
use muli_core::traits::PipelineStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqlitePipelineStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqlitePipelineStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl PipelineStore for SqlitePipelineStore {
    async fn upsert_pipeline(&self, pipeline: &Pipeline) -> Result<()> {
        let conn = self.factory.tenant_conn(&pipeline.tenant_id).await?;
        let p = pipeline.clone();
        conn.call(move |c| {
            let json = to_json(&p)?;
            c.execute(
                "INSERT OR REPLACE INTO pipelines (id, tenant_id, repo_id, name, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![p.id, p.tenant_id, p.repo_id, p.name, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn get_pipeline(&self, pipeline_id: &str) -> Result<Option<Pipeline>> {
        let pid = pipeline_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let p = pid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM pipelines WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![p])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Pipeline>(&json)?))
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

    async fn get_by_repo(&self, repo_id: &str) -> Result<Vec<Pipeline>> {
        let rid = repo_id.to_string();
        let mut all = Vec::new();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let mut results: Vec<Pipeline> = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM pipelines WHERE repo_id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![r])?;
                    let mut result = Vec::new();
                    while let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        result.push(from_json::<Pipeline>(&json)?);
                    }
                    Ok(result)
                })
                .await
                .map_err(store_err)?;
            all.append(&mut results);
        }
        Ok(all)
    }

    async fn delete_pipeline(&self, pipeline_id: &str) -> Result<()> {
        let pid = pipeline_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let p = pid.clone();
            let rows = conn
                .call(move |c| {
                    Ok(c.execute("DELETE FROM pipelines WHERE id = ?1", rusqlite::params![p])?)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::traits::PipelineStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_pipeline_upsert_and_get() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineStore::new(factory);
        let p = Pipeline::new("t1".into(), "repo-1".into(), "ci".into(), "sha1".into());
        store.upsert_pipeline(&p).await.unwrap();
        let fetched = store.get_pipeline(&p.id).await.unwrap().unwrap();
        assert_eq!(fetched.name, "ci");
        assert_eq!(fetched.repo_id, "repo-1");
        assert_eq!(fetched.yaml_sha, "sha1");
    }

    #[tokio::test]
    async fn test_pipeline_get_by_repo() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineStore::new(factory);
        let p1 = Pipeline::new("t1".into(), "repo-1".into(), "ci".into(), "sha1".into());
        let p2 = Pipeline::new("t1".into(), "repo-1".into(), "deploy".into(), "sha2".into());
        let p3 = Pipeline::new("t1".into(), "repo-2".into(), "ci".into(), "sha3".into());
        store.upsert_pipeline(&p1).await.unwrap();
        store.upsert_pipeline(&p2).await.unwrap();
        store.upsert_pipeline(&p3).await.unwrap();
        let list = store.get_by_repo("repo-1").await.unwrap();
        assert_eq!(list.len(), 2);
        let names: Vec<&str> = list.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"ci"));
        assert!(names.contains(&"deploy"));
    }

    #[tokio::test]
    async fn test_pipeline_delete() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineStore::new(factory);
        let p = Pipeline::new("t1".into(), "repo-1".into(), "ci".into(), "sha1".into());
        store.upsert_pipeline(&p).await.unwrap();
        store.delete_pipeline(&p.id).await.unwrap();
        let fetched = store.get_pipeline(&p.id).await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_pipeline_upsert_updates_existing() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineStore::new(factory);
        let mut p = Pipeline::new("t1".into(), "repo-1".into(), "ci".into(), "sha1".into());
        store.upsert_pipeline(&p).await.unwrap();
        p.yaml_sha = "sha2".into();
        store.upsert_pipeline(&p).await.unwrap();
        let fetched = store.get_pipeline(&p.id).await.unwrap().unwrap();
        assert_eq!(fetched.yaml_sha, "sha2");
        // Should still be only one pipeline for the repo
        let list = store.get_by_repo("repo-1").await.unwrap();
        assert_eq!(list.len(), 1);
    }
}
