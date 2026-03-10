// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed job queue and execution store.

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::job::model::Job;
use muli_core::job::state_machine::JobState;
use muli_core::traits::JobStore;

use super::util::chrono_to_bson;

const COLLECTION_NAME: &str = "jobs";

fn state_str(state: &JobState) -> &'static str {
    match state {
        JobState::Pending => "Pending",
        JobState::Scheduled => "Scheduled",
        JobState::Pulling => "Pulling",
        JobState::Running => "Running",
        JobState::Succeeded => "Succeeded",
        JobState::Failed => "Failed",
        JobState::Cancelled => "Cancelled",
        JobState::TimedOut => "TimedOut",
    }
}

/// MongoDB implementation of JobStore.
#[derive(Debug, Clone)]
pub struct MongoJobStore {
    collection: Collection<Job>,
}

impl MongoJobStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection::<Job>(COLLECTION_NAME),
        }
    }
}

#[async_trait]
impl JobStore for MongoJobStore {
    async fn create_job(&self, job: &Job) -> Result<String> {
        self.collection
            .insert_one(job)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create job: {e}")))?;
        Ok(job.id.clone())
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.collection
            .find_one(doc! { "id": job_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get job: {e}")))
    }

    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<Job>> {
        self.collection
            .find_one(doc! { "name": job_name })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get job by name: {e}")))
    }

    async fn update_state(&self, job_id: &str, from: JobState, to: JobState) -> Result<()> {
        // Validate transition first
        from.transition_to(to)?;

        let now = Utc::now();
        let result = self
            .collection
            .find_one_and_update(
                doc! {
                    "id": job_id,
                    "state": state_str(&from),
                },
                doc! {
                    "$set": {
                        "state": state_str(&to),
                        "updated_at": chrono_to_bson(now),
                    }
                },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update state: {e}")))?;

        match result {
            Some(_) => Ok(()),
            None => {
                // Check if job exists
                let exists = self
                    .collection
                    .find_one(doc! { "id": job_id })
                    .await
                    .map_err(|e| MuliError::Storage(e.to_string()))?;

                match exists {
                    None => Err(MuliError::JobNotFound(job_id.to_string())),
                    Some(job) => Err(MuliError::InvalidStateTransition {
                        from: job.state.to_string(),
                        to: to.to_string(),
                    }),
                }
            }
        }
    }

    async fn update_job(&self, job: &Job) -> Result<()> {
        let result = self
            .collection
            .replace_one(doc! { "id": &job.id }, job)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update job: {e}")))?;

        if result.matched_count == 0 {
            return Err(MuliError::JobNotFound(job.id.clone()));
        }
        Ok(())
    }

    async fn list_pending(&self) -> Result<Vec<Job>> {
        let opts = FindOptions::builder()
            .sort(doc! { "priority_score": -1 })
            .build();
        let cursor = self
            .collection
            .find(doc! { "state": "Pending" })
            .with_options(opts)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list pending: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect pending jobs: {e}")))
    }

    async fn list_by_tenant(&self, tenant_id: &str) -> Result<Vec<Job>> {
        let cursor = self
            .collection
            .find(doc! { "spec.tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list by tenant: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect tenant jobs: {e}")))
    }

    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> {
        let cursor = self
            .collection
            .find(doc! { "state": state_str(&state) })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list by state: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect state jobs: {e}")))
    }

    async fn list_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Job>> {
        let mut filter = doc! {};
        if let Some(state) = state_filter {
            filter.insert("state", state_str(&state));
        }
        if let Some(tid) = tenant_id {
            filter.insert("spec.tenant_id", tid);
        }

        let opts = FindOptions::builder()
            .sort(doc! { "created_at": -1 })
            .skip(offset as u64)
            .limit(limit as i64)
            .build();

        let cursor = self
            .collection
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list jobs: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect jobs: {e}")))
    }

    async fn count_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
    ) -> Result<u64> {
        let mut filter = doc! {};
        if let Some(state) = state_filter {
            filter.insert("state", state_str(&state));
        }
        if let Some(tid) = tenant_id {
            filter.insert("spec.tenant_id", tid);
        }

        self.collection
            .count_documents(filter)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to count jobs: {e}")))
    }

    async fn count_active_by_tenant(&self, tenant_id: &str) -> Result<u64> {
        let terminal_states = vec!["Succeeded", "Failed", "Cancelled", "TimedOut"];
        self.collection
            .count_documents(doc! {
                "spec.tenant_id": tenant_id,
                "state": { "$nin": terminal_states }
            })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to count active: {e}")))
    }

    async fn cleanup_old(&self, older_than: Duration) -> Result<u64> {
        let chrono_duration = chrono::Duration::from_std(older_than)
            .map_err(|e| MuliError::Storage(format!("invalid cleanup duration: {e}")))?;
        let threshold = Utc::now() - chrono_duration;
        let terminal_states = vec!["Succeeded", "Failed", "Cancelled", "TimedOut"];

        let result = self
            .collection
            .delete_many(doc! {
                "state": { "$in": terminal_states },
                "updated_at": { "$lt": chrono_to_bson(threshold) }
            })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to cleanup: {e}")))?;

        Ok(result.deleted_count)
    }

    async fn delete_job(&self, job_id: &str) -> Result<()> {
        self.collection
            .delete_one(doc! { "id": job_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete job: {e}")))?;
        Ok(())
    }

    async fn find_by_idempotency_key(&self, tenant_id: &str, key: &str) -> Result<Option<Job>> {
        self.collection
            .find_one(doc! {
                "spec.tenant_id": tenant_id,
                "spec.idempotency_key": key,
            })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to find by idempotency key: {e}")))
    }
}
