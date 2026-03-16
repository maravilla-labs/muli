// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test data builders and setup utilities.

use chrono::Utc;
use uuid::Uuid;

use muli_core::agent::model::{AgentCapabilities, AgentInfo, AgentStatus};
use muli_core::job::model::{EnvVar, Job, JobSpec, PriorityTier};
use muli_core::resource::limits::ResourceSpec;

/// Create a basic test JobSpec for alpine:latest.
pub fn test_job_spec() -> JobSpec {
    JobSpec {
        deployment_id: "test-deployment".to_string(),
        project_id: "test-project".to_string(),
        workspace_id: "test-workspace".to_string(),
        tenant_id: "test-tenant".to_string(),
        runner_image: "alpine:latest".to_string(),
        env_vars: vec![],
        resources: test_resource_spec(),
        priority_tier: PriorityTier::Standard,
        framework: "static".to_string(),
        idempotency_key: None,
        registry_credentials: None,
        commands: vec![],
    }
}

/// Create a test JobSpec that passes a shell command via env var.
///
/// Note: The current executor does not set `cmd` on the container config,
/// so the command is stored as `MULI_CMD` env var for reference.
/// Integration tests should use images whose default entrypoint produces
/// the desired behavior.
pub fn test_job_spec_with_command(cmd: &str) -> JobSpec {
    let mut spec = test_job_spec();
    spec.env_vars = vec![EnvVar {
        name: "MULI_CMD".to_string(),
        value: cmd.to_string(),
    }];
    spec
}

/// Create a test JobSpec with a specific runner image.
pub fn test_job_spec_with_image(image: &str) -> JobSpec {
    let mut spec = test_job_spec();
    spec.runner_image = image.to_string();
    spec
}

/// Create a full Job with a test spec.
pub fn test_job() -> Job {
    Job::new(test_job_spec())
}

/// Create test AgentInfo with sensible defaults.
pub fn test_agent_info() -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        id: Uuid::new_v4().to_string(),
        name: "test-agent".to_string(),
        hostname: "localhost".to_string(),
        capabilities: AgentCapabilities {
            total_cpu_millicores: 4000,
            total_memory_bytes: 8 * 1024 * 1024 * 1024, // 8Gi
            available_cpu_millicores: 2000,
            available_memory_bytes: 4 * 1024 * 1024 * 1024, // 4Gi
            max_concurrent_jobs: 4,
            running_jobs: 0,
            pre_pulled_images: vec!["alpine:latest".to_string()],
        },
        labels: vec!["test".to_string()],
        status: AgentStatus::Healthy,
        last_heartbeat: now,
        registered_at: now,
    }
}

/// Create a test ResourceSpec with conservative limits.
pub fn test_resource_spec() -> ResourceSpec {
    ResourceSpec {
        cpu_request: "250m".to_string(),
        cpu_limit: "500m".to_string(),
        memory_request: "128Mi".to_string(),
        memory_limit: "256Mi".to_string(),
        timeout_seconds: 60,
    }
}
