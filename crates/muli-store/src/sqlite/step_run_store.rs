// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite step run store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::pipeline::{StepRun, StepRunState};
use muli_core::traits::StepRunStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteStepRunStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteStepRunStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl StepRunStore for SqliteStepRunStore {
    async fn create_step(&self, step: &StepRun) -> Result<String> {
        let s = step.clone();
        let id = s.id.clone();
        let conn = self.factory.tenant_conn(&s.tenant_id).await?;
        let idc = id.clone();
        conn.call(move |c| {
            let json = to_json(&s)?;
            c.execute(
                "INSERT INTO step_runs (id, run_id, step_name, job_id, state, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    idc,
                    s.run_id,
                    s.step_name,
                    s.job_id,
                    s.state.as_str(),
                    json,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_step(&self, tenant_id: &str, step_id: &str) -> Result<Option<StepRun>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let sid = step_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM step_runs WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![sid])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<StepRun>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_by_run(&self, tenant_id: &str, run_id: &str) -> Result<Vec<StepRun>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = run_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM step_runs WHERE run_id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![rid])?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json::<StepRun>(&json)?);
            }
            Ok(result)
        })
        .await
        .map_err(store_err)
    }

    async fn update_step(&self, step: &StepRun) -> Result<()> {
        let conn = self.factory.tenant_conn(&step.tenant_id).await?;
        let s = step.clone();
        conn.call(move |c| {
            let json = to_json(&s)?;
            c.execute(
                "UPDATE step_runs SET state = ?1, job_id = ?2, full_json = ?3 WHERE id = ?4",
                rusqlite::params![s.state.as_str(), s.job_id, json, s.id],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn update_step_state(&self, tenant_id: &str, step_id: &str, state: StepRunState) -> Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let sid = step_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM step_runs WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![sid])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                let mut step = from_json::<StepRun>(&json)?;
                step.state = state;
                let updated_json = to_json(&step)?;
                c.execute(
                    "UPDATE step_runs SET state = ?1, full_json = ?2 WHERE id = ?3",
                    rusqlite::params![state.as_str(), updated_json, step.id],
                )?;
            }
            Ok(())
        })
        .await
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::pipeline::FailureStrategy;
    use muli_core::traits::StepRunStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    fn make_step(tenant_id: &str, run_id: &str, step_name: &str) -> StepRun {
        StepRun::new(
            run_id.into(),
            tenant_id.into(),
            step_name.into(),
            FailureStrategy::Stop,
            None,
        )
    }

    #[tokio::test]
    async fn test_step_create_and_get() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteStepRunStore::new(factory);
        let step = make_step("t1", "run-1", "build");
        let id = store.create_step(&step).await.unwrap();
        let fetched = store.get_step("t1", &id).await.unwrap().unwrap();
        assert_eq!(fetched.step_name, "build");
        assert_eq!(fetched.state, StepRunState::Pending);
    }

    #[tokio::test]
    async fn test_step_list_by_run() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteStepRunStore::new(factory);
        let s1 = make_step("t1", "run-1", "build");
        let s2 = make_step("t1", "run-1", "test");
        let s3 = make_step("t1", "run-2", "deploy");
        store.create_step(&s1).await.unwrap();
        store.create_step(&s2).await.unwrap();
        store.create_step(&s3).await.unwrap();
        let list = store.list_by_run("t1", "run-1").await.unwrap();
        assert_eq!(list.len(), 2);
        let names: Vec<&str> = list.iter().map(|s| s.step_name.as_str()).collect();
        assert!(names.contains(&"build"));
        assert!(names.contains(&"test"));
    }

    #[tokio::test]
    async fn test_step_update_state() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteStepRunStore::new(factory);
        let step = make_step("t1", "run-1", "build");
        let id = store.create_step(&step).await.unwrap();
        store
            .update_step_state("t1", &id, StepRunState::Running)
            .await
            .unwrap();
        let fetched = store.get_step("t1", &id).await.unwrap().unwrap();
        assert_eq!(fetched.state, StepRunState::Running);
    }

    #[tokio::test]
    async fn test_step_update_full() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteStepRunStore::new(factory);
        let mut step = make_step("t1", "run-1", "build");
        store.create_step(&step).await.unwrap();
        step.state = StepRunState::Succeeded;
        step.job_id = Some("job-123".into());
        store.update_step(&step).await.unwrap();
        let fetched = store.get_step("t1", &step.id).await.unwrap().unwrap();
        assert_eq!(fetched.state, StepRunState::Succeeded);
        assert_eq!(fetched.job_id, Some("job-123".into()));
    }

    #[tokio::test]
    async fn test_step_tenant_isolation() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteStepRunStore::new(factory);
        // Create step in tenant A
        let step_a = make_step("tenant-a", "run-a", "build");
        let id_a = store.create_step(&step_a).await.unwrap();
        // Create step in tenant B
        let step_b = make_step("tenant-b", "run-b", "build");
        let id_b = store.create_step(&step_b).await.unwrap();
        // Each step should be retrievable from its own tenant
        let fetched_a = store.get_step("tenant-a", &id_a).await.unwrap().unwrap();
        assert_eq!(fetched_a.tenant_id, "tenant-a");
        let fetched_b = store.get_step("tenant-b", &id_b).await.unwrap().unwrap();
        assert_eq!(fetched_b.tenant_id, "tenant-b");
        // list_by_run should only return steps from the correct tenant
        let list_a = store.list_by_run("tenant-a", "run-a").await.unwrap();
        assert_eq!(list_a.len(), 1);
        assert_eq!(list_a[0].tenant_id, "tenant-a");
        let list_b = store.list_by_run("tenant-b", "run-b").await.unwrap();
        assert_eq!(list_b.len(), 1);
        assert_eq!(list_b[0].tenant_id, "tenant-b");
    }
}
