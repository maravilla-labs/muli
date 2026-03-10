// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite job queue and execution store.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;

use muli_core::error::{MuliError, Result};
use muli_core::job::model::Job;
use muli_core::job::state_machine::JobState;
use muli_core::traits::JobStore;

use super::factory::SqliteStoreFactory;
use super::job_query;
use super::util::{dt_to_ms, from_json as job_from_json, store_err, to_json as job_to_json};

pub struct SqliteJobStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteJobStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl JobStore for SqliteJobStore {
    async fn create_job(&self, job: &Job) -> Result<String> {
        let conn = self.factory.global_conn();
        let job = job.clone();
        let id = job.id.clone();
        conn.call(move |c| {
            let full_json = job_to_json(&job)?;
            let spec_json = serde_json::to_string(&job.spec)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            c.execute(
                "INSERT INTO jobs (id, tenant_id, name, spec, state, priority_score, created_at, updated_at, full_json, idempotency_key)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    job.id,
                    job.spec.tenant_id,
                    job.name,
                    spec_json,
                    job.state.to_string(),
                    job.priority_score,
                    dt_to_ms(job.created_at),
                    dt_to_ms(job.updated_at),
                    full_json,
                    job.spec.idempotency_key.as_deref(),
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        let conn = self.factory.global_conn();
        let job_id = job_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM jobs WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![job_id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(job_from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<Job>> {
        let conn = self.factory.global_conn();
        let job_name = job_name.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM jobs WHERE name = ?1")?;
            let mut rows = stmt.query(rusqlite::params![job_name])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(job_from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn update_state(&self, job_id: &str, from: JobState, to: JobState) -> Result<()> {
        // Validate the transition is logically valid before touching the DB.
        from.transition_to(to)?;

        let conn = self.factory.global_conn();
        let job_id = job_id.to_string();
        let from_str = from.to_string();
        let to_str = to.to_string();

        let rows = conn
            .call(move |c| {
                // Read current state from DB; only proceed if it matches `from`.
                let existing: Option<String> = {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM jobs WHERE id = ?1 AND state = ?2")?;
                    let mut rows = stmt.query(rusqlite::params![job_id, from_str])?;
                    rows.next()?
                        .map(|row| row.get::<_, String>(0))
                        .transpose()?
                };

                let Some(json) = existing else {
                    return Ok(0usize);
                };

                let mut job: Job = job_from_json(&json)?;
                job.state = match to_str.as_str() {
                    "Pending" => JobState::Pending,
                    "Scheduled" => JobState::Scheduled,
                    "Pulling" => JobState::Pulling,
                    "Running" => JobState::Running,
                    "Succeeded" => JobState::Succeeded,
                    "Failed" => JobState::Failed,
                    "Cancelled" => JobState::Cancelled,
                    "TimedOut" => JobState::TimedOut,
                    _ => return Err(rusqlite::Error::InvalidQuery.into()),
                };
                job.updated_at = Utc::now();

                let new_json = job_to_json(&job)?;
                let rows = c.execute(
                    "UPDATE jobs SET state = ?1, updated_at = ?2, full_json = ?3 WHERE id = ?4",
                    rusqlite::params![to_str, dt_to_ms(job.updated_at), new_json, job.id,],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::InvalidStateTransition {
                from: from.to_string(),
                to: to.to_string(),
            });
        }
        Ok(())
    }

    async fn update_job(&self, job: &Job) -> Result<()> {
        let conn = self.factory.global_conn();
        let job = job.clone();
        let job_id_for_err = job.id.clone();
        let rows = conn
            .call(move |c| {
                let full_json = job_to_json(&job)?;
                let rows = c.execute(
                    "UPDATE jobs SET state = ?1, priority_score = ?2, updated_at = ?3, full_json = ?4 WHERE id = ?5",
                    rusqlite::params![
                        job.state.to_string(),
                        job.priority_score,
                        dt_to_ms(job.updated_at),
                        full_json,
                        job.id,
                    ],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::JobNotFound(job_id_for_err));
        }
        Ok(())
    }

    async fn list_pending(&self) -> Result<Vec<Job>> {
        let conn = self.factory.global_conn();
        conn.call(|c| Ok(job_query::list_pending(c)?))
            .await
            .map_err(store_err)
    }

    async fn list_by_tenant(&self, tenant_id: &str) -> Result<Vec<Job>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| Ok(job_query::list_by_tenant(c, &tenant_id)?))
            .await
            .map_err(store_err)
    }

    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> {
        let conn = self.factory.global_conn();
        let state_str = state.to_string();
        conn.call(move |c| Ok(job_query::list_by_state(c, &state_str)?))
            .await
            .map_err(store_err)
    }

    async fn list_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Job>> {
        let conn = self.factory.global_conn();
        let state_str = state_filter.map(|s| s.to_string());
        let tenant_id = tenant_id.map(|s| s.to_string());
        conn.call(move |c| {
            Ok(job_query::list_jobs(
                c,
                &state_str,
                &tenant_id,
                limit as i64,
                offset as i64,
            )?)
        })
        .await
        .map_err(store_err)
    }

    async fn count_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
    ) -> Result<u64> {
        let conn = self.factory.global_conn();
        let state_str = state_filter.map(|s| s.to_string());
        let tenant_id = tenant_id.map(|s| s.to_string());
        conn.call(move |c| Ok(job_query::count_jobs(c, &state_str, &tenant_id)?))
            .await
            .map_err(store_err)
    }

    async fn count_active_by_tenant(&self, tenant_id: &str) -> Result<u64> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| Ok(job_query::count_active_by_tenant(c, &tenant_id)?))
            .await
            .map_err(store_err)
    }

    async fn cleanup_old(&self, older_than: Duration) -> Result<u64> {
        let threshold = Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| MuliError::Internal(format!("Invalid duration: {e}")))?;
        let conn = self.factory.global_conn();
        let threshold_ms = dt_to_ms(threshold);
        conn.call(move |c| Ok(job_query::cleanup_old(c, threshold_ms)?))
            .await
            .map_err(store_err)
    }

    async fn delete_job(&self, job_id: &str) -> Result<()> {
        let conn = self.factory.global_conn();
        let job_id = job_id.to_string();
        conn.call(move |c| {
            c.execute("DELETE FROM jobs WHERE id = ?1", rusqlite::params![job_id])?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn find_by_idempotency_key(&self, tenant_id: &str, key: &str) -> Result<Option<Job>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        let key = key.to_string();
        conn.call(move |c| Ok(job_query::find_by_idempotency_key(c, &tenant_id, &key)?))
            .await
            .map_err(store_err)
    }
}

#[cfg(test)]
#[path = "job_store_tests.rs"]
mod tests;
