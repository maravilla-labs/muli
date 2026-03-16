// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pipeline run store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::pipeline::{PipelineRun, PipelineRunState};
use muli_core::traits::PipelineRunStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqlitePipelineRunStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqlitePipelineRunStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl PipelineRunStore for SqlitePipelineRunStore {
    async fn create_run(&self, run: &PipelineRun) -> Result<String> {
        let conn = self.factory.tenant_conn(&run.tenant_id).await?;
        let r = run.clone();
        let id = r.id.clone();
        conn.call(move |c| {
            let json = to_json(&r)?;
            c.execute(
                "INSERT INTO pipeline_runs (id, pipeline_id, tenant_id, repo_id, run_number, state, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![r.id, r.pipeline_id, r.tenant_id, r.repo_id, r.run_number as i64, r.state.as_str(), json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_run(&self, tenant_id: &str, run_id: &str) -> Result<Option<PipelineRun>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = run_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM pipeline_runs WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![rid])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<PipelineRun>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn get_run_by_number(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        run_number: u64,
    ) -> Result<Option<PipelineRun>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let pid = pipeline_id.to_string();
        let n = run_number as i64;
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM pipeline_runs WHERE pipeline_id = ?1 AND run_number = ?2",
            )?;
            let mut rows = stmt.query(rusqlite::params![pid, n])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<PipelineRun>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_by_repo(
        &self,
        tenant_id: &str,
        repo_id: &str,
        state: Option<PipelineRunState>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PipelineRun>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = repo_id.to_string();
        let state_str = state.map(|s| s.as_str());
        let lim = limit as i64;
        let off = offset as i64;
        conn.call(move |c| {
            let runs: Vec<PipelineRun> = match state_str {
                Some(state_s) => {
                    let mut stmt = c.prepare(
                        "SELECT full_json FROM pipeline_runs WHERE repo_id = ?1 AND state = ?2
                         ORDER BY run_number DESC LIMIT ?3 OFFSET ?4",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![rid, state_s, lim, off])?;
                    let mut result = Vec::new();
                    while let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        result.push(from_json::<PipelineRun>(&json)?);
                    }
                    result
                }
                None => {
                    let mut stmt = c.prepare(
                        "SELECT full_json FROM pipeline_runs WHERE repo_id = ?1
                         ORDER BY run_number DESC LIMIT ?2 OFFSET ?3",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![rid, lim, off])?;
                    let mut result = Vec::new();
                    while let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        result.push(from_json::<PipelineRun>(&json)?);
                    }
                    result
                }
            };
            Ok(runs)
        })
        .await
        .map_err(store_err)
    }

    async fn update_run(&self, run: &PipelineRun) -> Result<()> {
        let conn = self.factory.tenant_conn(&run.tenant_id).await?;
        let r = run.clone();
        conn.call(move |c| {
            let json = to_json(&r)?;
            c.execute(
                "UPDATE pipeline_runs SET state = ?1, full_json = ?2 WHERE id = ?3",
                rusqlite::params![r.state.as_str(), json, r.id],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn next_run_number(&self, tenant_id: &str, pipeline_id: &str) -> Result<u64> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let pid = pipeline_id.to_string();
        let result: Option<i64> = conn
            .call(move |c| {
                let max: Option<i64> = c.query_row(
                    "SELECT MAX(run_number) FROM pipeline_runs WHERE pipeline_id = ?1",
                    rusqlite::params![pid],
                    |row| row.get(0),
                )?;
                Ok(max)
            })
            .await
            .map_err(store_err)?;
        Ok(result.map(|m| m as u64 + 1).unwrap_or(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::pipeline::PipelineTrigger;
    use muli_core::traits::PipelineRunStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    fn make_run(tenant_id: &str, pipeline_id: &str, repo_id: &str, run_number: u64) -> PipelineRun {
        PipelineRun::new(
            pipeline_id.into(),
            tenant_id.into(),
            repo_id.into(),
            run_number,
            "abc123".into(),
            "refs/heads/main".into(),
            PipelineTrigger::Push {
                ref_name: "refs/heads/main".into(),
            },
            "name: ci\nsteps:\n  - name: test\n    image: rust:1.82".into(),
        )
    }

    #[tokio::test]
    async fn test_run_create_and_get() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineRunStore::new(factory);
        let run = make_run("t1", "pipe-1", "repo-1", 1);
        let id = store.create_run(&run).await.unwrap();
        let fetched = store.get_run("t1", &id).await.unwrap().unwrap();
        assert_eq!(fetched.run_number, 1);
        assert_eq!(fetched.pipeline_id, "pipe-1");
        assert_eq!(fetched.state, PipelineRunState::Pending);
    }

    #[tokio::test]
    async fn test_run_list_by_repo_with_state_filter() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineRunStore::new(factory);
        let run1 = make_run("t1", "pipe-1", "repo-1", 1);
        let mut run2 = make_run("t1", "pipe-1", "repo-1", 2);
        run2.state = PipelineRunState::Running;
        store.create_run(&run1).await.unwrap();
        store.create_run(&run2).await.unwrap();

        let all = store.list_by_repo("t1", "repo-1", None, 10, 0).await.unwrap();
        assert_eq!(all.len(), 2);

        let pending = store
            .list_by_repo("t1", "repo-1", Some(PipelineRunState::Pending), 10, 0)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].run_number, 1);

        let running = store
            .list_by_repo("t1", "repo-1", Some(PipelineRunState::Running), 10, 0)
            .await
            .unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].run_number, 2);
    }

    #[tokio::test]
    async fn test_run_list_by_repo_pagination() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineRunStore::new(factory);
        for i in 1..=5 {
            let run = make_run("t1", "pipe-1", "repo-1", i);
            store.create_run(&run).await.unwrap();
        }
        // Limit 2, offset 0 => runs 5, 4 (desc order)
        let page1 = store.list_by_repo("t1", "repo-1", None, 2, 0).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].run_number, 5);
        assert_eq!(page1[1].run_number, 4);

        // Limit 2, offset 2 => runs 3, 2
        let page2 = store.list_by_repo("t1", "repo-1", None, 2, 2).await.unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].run_number, 3);
        assert_eq!(page2[1].run_number, 2);
    }

    #[tokio::test]
    async fn test_run_update_state() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineRunStore::new(factory);
        let mut run = make_run("t1", "pipe-1", "repo-1", 1);
        store.create_run(&run).await.unwrap();
        run.state = PipelineRunState::Succeeded;
        store.update_run(&run).await.unwrap();
        let fetched = store.get_run("t1", &run.id).await.unwrap().unwrap();
        assert_eq!(fetched.state, PipelineRunState::Succeeded);
    }

    #[tokio::test]
    async fn test_run_next_run_number_increments() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineRunStore::new(factory);
        let run1 = make_run("t1", "pipe-1", "repo-1", 1);
        store.create_run(&run1).await.unwrap();
        let next = store.next_run_number("t1", "pipe-1").await.unwrap();
        assert_eq!(next, 2);

        let run2 = make_run("t1", "pipe-1", "repo-1", 2);
        store.create_run(&run2).await.unwrap();
        let next = store.next_run_number("t1", "pipe-1").await.unwrap();
        assert_eq!(next, 3);
    }

    #[tokio::test]
    async fn test_run_get_by_number() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePipelineRunStore::new(factory);
        let run = make_run("t1", "pipe-1", "repo-1", 42);
        store.create_run(&run).await.unwrap();
        let fetched = store.get_run_by_number("t1", "pipe-1", 42).await.unwrap().unwrap();
        assert_eq!(fetched.id, run.id);
        let missing = store.get_run_by_number("t1", "pipe-1", 99).await.unwrap();
        assert!(missing.is_none());
    }
}
