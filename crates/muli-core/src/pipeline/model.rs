// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline domain models.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::state::{FailureStrategy, PipelineRunState, StepRunState};

/// A pipeline definition linked to a repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    pub name: String,
    /// SHA-256 of the YAML content (for change detection).
    pub yaml_sha: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Pipeline {
    pub fn new(tenant_id: String, repo_id: String, name: String, yaml_sha: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            repo_id,
            name,
            yaml_sha,
            created_at: now,
            updated_at: now,
        }
    }
}

/// What triggered a pipeline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineTrigger {
    Push { ref_name: String },
    PullRequest { pr_number: u64, event: String },
    Manual { triggered_by: String },
    Schedule { cron: String },
    Retry { original_run_id: String },
}

/// A single execution of a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: String,
    pub pipeline_id: String,
    pub tenant_id: String,
    pub repo_id: String,
    pub run_number: u64,
    pub commit_sha: String,
    pub ref_name: String,
    pub trigger: PipelineTrigger,
    pub state: PipelineRunState,
    /// Snapshot of the YAML at the time of the run.
    pub yaml_content: String,
    /// Merged environment variables injected into every step Job.
    /// Vault secrets are prefixed with WUNDER_, custom env vars are raw.
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PipelineRun {
    pub fn new(
        pipeline_id: String,
        tenant_id: String,
        repo_id: String,
        run_number: u64,
        commit_sha: String,
        ref_name: String,
        trigger: PipelineTrigger,
        yaml_content: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            pipeline_id,
            tenant_id,
            repo_id,
            run_number,
            commit_sha,
            ref_name,
            trigger,
            state: PipelineRunState::Pending,
            yaml_content,
            env_vars: HashMap::new(),
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// A single step within a pipeline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRun {
    pub id: String,
    pub run_id: String,
    pub tenant_id: String,
    pub step_name: String,
    /// Links to the existing Job in the job engine (set when submitted).
    pub job_id: Option<String>,
    pub state: StepRunState,
    /// Matrix variable values for this specific expansion (e.g. {"rust_version": "1.82"}).
    pub matrix_values: Option<serde_json::Value>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub failure_strategy: FailureStrategy,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl StepRun {
    pub fn new(
        run_id: String,
        tenant_id: String,
        step_name: String,
        failure_strategy: FailureStrategy,
        matrix_values: Option<serde_json::Value>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            run_id,
            tenant_id,
            step_name,
            job_id: None,
            state: StepRunState::Pending,
            matrix_values,
            depends_on: Vec::new(),
            failure_strategy,
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// A build artifact produced by a pipeline step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub tenant_id: String,
    pub run_id: String,
    pub step_name: String,
    pub name: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl Artifact {
    pub fn new(
        tenant_id: String,
        run_id: String,
        step_name: String,
        name: String,
        size_bytes: u64,
        sha256: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            run_id,
            step_name,
            name,
            size_bytes,
            sha256,
            expires_at,
            created_at: Utc::now(),
        }
    }
}

/// A cached dependency archive for a repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    pub cache_key: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub last_used_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl CacheEntry {
    pub fn new(
        tenant_id: String,
        repo_id: String,
        cache_key: String,
        size_bytes: u64,
        sha256: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            repo_id,
            cache_key,
            size_bytes,
            sha256,
            last_used_at: now,
            created_at: now,
        }
    }
}

/// An encrypted secret for pipeline use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSecret {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    pub name: String,
    /// AES-256-GCM encrypted value (base64-encoded ciphertext + nonce).
    pub encrypted_value: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PipelineSecret {
    pub fn new(tenant_id: String, repo_id: String, name: String, encrypted_value: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            repo_id,
            name,
            encrypted_value,
            created_at: now,
            updated_at: now,
        }
    }
}
