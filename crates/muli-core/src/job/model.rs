// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job data model: status, priority tiers, resources, and results.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::state_machine::JobState;
use crate::resource::limits::ResourceSpec;

/// A build job to execute in a container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub spec: JobSpec,
    pub state: JobState,
    pub result: Option<JobResult>,
    pub retry_count: u32,
    pub priority_score: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub assigned_agent: Option<String>,
}

/// Specification for what to run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub deployment_id: String,
    pub project_id: String,
    pub workspace_id: String,
    pub tenant_id: String,
    pub runner_image: String,
    pub env_vars: Vec<EnvVar>,
    pub resources: ResourceSpec,
    pub priority_tier: PriorityTier,
    pub framework: String,
    pub idempotency_key: Option<String>,
    pub registry_credentials: Option<RegistryCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PriorityTier {
    Free,
    Standard,
    Premium,
    Enterprise,
}

impl PriorityTier {
    pub fn weight(&self) -> u32 {
        match self {
            PriorityTier::Free => 1,
            PriorityTier::Standard => 10,
            PriorityTier::Premium => 100,
            PriorityTier::Enterprise => 1000,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RegistryCredentials {
    pub server: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for RegistryCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryCredentials")
            .field("server", &self.server)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// Result of a completed job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub exit_code: Option<i32>,
    pub message: String,
    pub container_id: Option<String>,
}

/// Detailed status for admin/debugging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedJobStatus {
    pub job_id: String,
    pub state: JobState,
    pub container_id: Option<String>,
    pub pod_phase: Option<String>,
    pub pod_reason: Option<String>,
    pub pod_message: Option<String>,
    pub container_state: Option<String>,
    pub container_reason: Option<String>,
    pub container_message: Option<String>,
    pub exit_code: Option<i32>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub restart_count: u32,
}

/// A persisted log line for a completed job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredLogLine {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub stream: String, // "stdout" or "stderr"
    pub message: String,
}

impl Job {
    pub fn new(spec: JobSpec) -> Self {
        let id = Uuid::new_v4().to_string();
        let name = format!("muli-{}", &id[..8]);
        let now = Utc::now();
        let priority_score = super::priority::calculate_score(spec.priority_tier, now);

        Self {
            id,
            name,
            spec,
            state: JobState::Pending,
            result: None,
            retry_count: 0,
            priority_score,
            created_at: now,
            updated_at: now,
            scheduled_at: None,
            started_at: None,
            finished_at: None,
            assigned_agent: None,
        }
    }

    /// Recalculate priority score based on wait time.
    pub fn recalculate_score(&mut self) {
        self.priority_score =
            super::priority::calculate_score(self.spec.priority_tier, self.created_at);
    }
}
