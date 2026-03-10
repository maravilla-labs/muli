// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory job queue and execution store.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::job::model::Job;
use muli_core::job::state_machine::JobState;
use muli_core::traits::JobStore;

use super::job_query;

/// In-memory implementation of JobStore for testing.
#[derive(Debug, Clone)]
pub struct MemoryJobStore {
    jobs: Arc<DashMap<String, Job>>,
}

impl MemoryJobStore {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryJobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobStore for MemoryJobStore {
    async fn create_job(&self, job: &Job) -> Result<String> {
        if self.jobs.contains_key(&job.id) {
            return Err(MuliError::Storage(format!(
                "Job with id {} already exists",
                job.id
            )));
        }
        let id = job.id.clone();
        self.jobs.insert(id.clone(), job.clone());
        Ok(id)
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        Ok(self.jobs.get(job_id).map(|entry| entry.value().clone()))
    }

    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<Job>> {
        Ok(self
            .jobs
            .iter()
            .find(|entry| entry.value().name == job_name)
            .map(|entry| entry.value().clone()))
    }

    async fn update_state(&self, job_id: &str, from: JobState, to: JobState) -> Result<()> {
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| MuliError::JobNotFound(job_id.to_string()))?;

        let job = entry.value_mut();
        if job.state != from {
            return Err(MuliError::InvalidStateTransition {
                from: job.state.to_string(),
                to: to.to_string(),
            });
        }

        from.transition_to(to)?;
        job.state = to;
        job.updated_at = Utc::now();
        Ok(())
    }

    async fn update_job(&self, job: &Job) -> Result<()> {
        if !self.jobs.contains_key(&job.id) {
            return Err(MuliError::JobNotFound(job.id.clone()));
        }
        self.jobs.insert(job.id.clone(), job.clone());
        Ok(())
    }

    async fn list_pending(&self) -> Result<Vec<Job>> {
        let mut pending: Vec<Job> = self
            .jobs
            .iter()
            .filter(|entry| entry.value().state == JobState::Pending)
            .map(|entry| entry.value().clone())
            .collect();
        pending.sort_by(|a, b| b.priority_score.total_cmp(&a.priority_score));
        Ok(pending)
    }

    async fn list_by_tenant(&self, tenant_id: &str) -> Result<Vec<Job>> {
        Ok(self
            .jobs
            .iter()
            .filter(|entry| entry.value().spec.tenant_id == tenant_id)
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> {
        Ok(self
            .jobs
            .iter()
            .filter(|entry| entry.value().state == state)
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn list_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Job>> {
        let mut jobs: Vec<Job> = self
            .jobs
            .iter()
            .filter(|entry| job_query::matches_filter(entry.value(), &state_filter, tenant_id))
            .map(|entry| entry.value().clone())
            .collect();
        jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(jobs.into_iter().skip(offset).take(limit).collect())
    }

    async fn count_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
    ) -> Result<u64> {
        let count = self
            .jobs
            .iter()
            .filter(|entry| job_query::matches_filter(entry.value(), &state_filter, tenant_id))
            .count();
        Ok(count as u64)
    }

    async fn count_active_by_tenant(&self, tenant_id: &str) -> Result<u64> {
        let count = self
            .jobs
            .iter()
            .filter(|entry| {
                let job = entry.value();
                job.spec.tenant_id == tenant_id && !job.state.is_terminal()
            })
            .count();
        Ok(count as u64)
    }

    async fn cleanup_old(&self, older_than: Duration) -> Result<u64> {
        let threshold = Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| MuliError::Internal(format!("Invalid duration for cleanup: {e}")))?;
        let to_remove: Vec<String> = self
            .jobs
            .iter()
            .filter(|entry| {
                let job = entry.value();
                job.state.is_terminal() && job.updated_at < threshold
            })
            .map(|entry| entry.key().clone())
            .collect();
        let count = to_remove.len() as u64;
        for id in to_remove {
            self.jobs.remove(&id);
        }
        Ok(count)
    }

    async fn delete_job(&self, job_id: &str) -> Result<()> {
        self.jobs.remove(job_id);
        Ok(())
    }

    async fn find_by_idempotency_key(&self, tenant_id: &str, key: &str) -> Result<Option<Job>> {
        Ok(self
            .jobs
            .iter()
            .find(|entry| {
                let job = entry.value();
                job.spec.tenant_id == tenant_id && job.spec.idempotency_key.as_deref() == Some(key)
            })
            .map(|entry| entry.value().clone()))
    }
}

#[cfg(test)]
#[path = "job_store_tests.rs"]
mod tests;
