// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job, agent, and log storage traits.

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::job::model::{Job, StoredLogLine};
use crate::job::state_machine::JobState;

/// Persistent storage for jobs.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Create a new job record.
    async fn create_job(&self, job: &Job) -> Result<String>;

    /// Get a job by its ID.
    async fn get_job(&self, job_id: &str) -> Result<Option<Job>>;

    /// Get a job by its name (e.g., "muli-abcd1234").
    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<Job>>;

    /// Atomically update job state (compare-and-swap).
    async fn update_state(&self, job_id: &str, from: JobState, to: JobState) -> Result<()>;

    /// Update the full job record.
    async fn update_job(&self, job: &Job) -> Result<()>;

    /// List all pending jobs (for scheduler).
    async fn list_pending(&self) -> Result<Vec<Job>>;

    /// List jobs by tenant ID.
    async fn list_by_tenant(&self, tenant_id: &str) -> Result<Vec<Job>>;

    /// List jobs by state.
    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>>;

    /// List all jobs with optional filters.
    async fn list_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Job>>;

    /// Count jobs matching filters.
    async fn count_jobs(
        &self,
        state_filter: Option<JobState>,
        tenant_id: Option<&str>,
    ) -> Result<u64>;

    /// Count active (non-terminal) jobs for a tenant.
    async fn count_active_by_tenant(&self, tenant_id: &str) -> Result<u64>;

    /// Remove old completed jobs.
    async fn cleanup_old(&self, older_than: Duration) -> Result<u64>;

    /// Delete a job record by ID (hard delete).
    async fn delete_job(&self, job_id: &str) -> Result<()>;

    /// Find a job by its idempotency key for a given tenant.
    async fn find_by_idempotency_key(&self, tenant_id: &str, key: &str) -> Result<Option<Job>>;
}

/// Registry for managing execution agents.
#[async_trait]
pub trait AgentRegistry: Send + Sync {
    /// Register a new agent.
    async fn register(&self, agent: &AgentInfo) -> Result<String>;

    /// Update heartbeat timestamp and capabilities.
    async fn heartbeat(&self, agent_id: &str, caps: &AgentCapabilities) -> Result<()>;

    /// Get an agent by ID.
    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentInfo>>;

    /// Get all agents with Healthy status.
    async fn get_healthy_agents(&self) -> Result<Vec<AgentInfo>>;

    /// Get all registered agents regardless of health.
    async fn get_all_agents(&self) -> Result<Vec<AgentInfo>>;

    /// Mark an agent as dead (missed heartbeats).
    async fn mark_dead(&self, agent_id: &str) -> Result<()>;

    /// Remove an agent from the registry.
    async fn deregister(&self, agent_id: &str) -> Result<()>;
}

/// Persistent storage for job logs (written on job completion, read on demand).
#[async_trait]
pub trait JobLogStore: Send + Sync {
    /// Append log lines for a completed job.
    async fn append_logs(&self, job_id: &str, lines: Vec<StoredLogLine>) -> Result<()>;

    /// Retrieve the last `tail` log lines for a job.
    async fn get_logs(&self, job_id: &str, tail: usize) -> Result<Vec<StoredLogLine>>;
}

use crate::agent::model::{AgentCapabilities, AgentInfo};
