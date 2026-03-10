// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tests for the in-memory job store.

use super::*;
use muli_core::job::model::{JobSpec, PriorityTier};
use muli_core::resource::limits::ResourceSpec;

fn make_job_spec(tenant_id: &str) -> JobSpec {
    JobSpec {
        deployment_id: "dep-1".to_string(),
        project_id: "proj-1".to_string(),
        workspace_id: "ws-1".to_string(),
        tenant_id: tenant_id.to_string(),
        runner_image: "node:18".to_string(),
        env_vars: vec![],
        resources: ResourceSpec::default(),
        priority_tier: PriorityTier::Standard,
        framework: "next".to_string(),
        idempotency_key: None,
        registry_credentials: None,
    }
}

#[tokio::test]
async fn test_create_and_get_job() {
    let store = MemoryJobStore::new();
    let job = Job::new(make_job_spec("tenant-1"));
    let id = store.create_job(&job).await.unwrap();
    assert_eq!(id, job.id);

    let fetched = store.get_job(&id).await.unwrap().unwrap();
    assert_eq!(fetched.id, job.id);
    assert_eq!(fetched.name, job.name);
}

#[tokio::test]
async fn test_get_job_by_name() {
    let store = MemoryJobStore::new();
    let job = Job::new(make_job_spec("tenant-1"));
    let name = job.name.clone();
    store.create_job(&job).await.unwrap();

    let fetched = store.get_job_by_name(&name).await.unwrap().unwrap();
    assert_eq!(fetched.id, job.id);
}

#[tokio::test]
async fn test_create_duplicate_fails() {
    let store = MemoryJobStore::new();
    let job = Job::new(make_job_spec("tenant-1"));
    store.create_job(&job).await.unwrap();
    assert!(store.create_job(&job).await.is_err());
}

#[tokio::test]
async fn test_update_state_cas() {
    let store = MemoryJobStore::new();
    let job = Job::new(make_job_spec("tenant-1"));
    let id = job.id.clone();
    store.create_job(&job).await.unwrap();

    // Valid transition
    store
        .update_state(&id, JobState::Pending, JobState::Scheduled)
        .await
        .unwrap();
    let fetched = store.get_job(&id).await.unwrap().unwrap();
    assert_eq!(fetched.state, JobState::Scheduled);

    // CAS failure: wrong "from" state
    assert!(
        store
            .update_state(&id, JobState::Pending, JobState::Scheduled)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_update_state_invalid_transition() {
    let store = MemoryJobStore::new();
    let job = Job::new(make_job_spec("tenant-1"));
    let id = job.id.clone();
    store.create_job(&job).await.unwrap();

    // Pending -> Running is not a valid transition
    assert!(
        store
            .update_state(&id, JobState::Pending, JobState::Running)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_list_pending_sorted_by_priority() {
    let store = MemoryJobStore::new();

    let mut spec_low = make_job_spec("tenant-1");
    spec_low.priority_tier = PriorityTier::Free;
    let job_low = Job::new(spec_low);

    let mut spec_high = make_job_spec("tenant-1");
    spec_high.priority_tier = PriorityTier::Enterprise;
    let job_high = Job::new(spec_high);

    store.create_job(&job_low).await.unwrap();
    store.create_job(&job_high).await.unwrap();

    let pending = store.list_pending().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending[0].priority_score > pending[1].priority_score);
}

#[tokio::test]
async fn test_list_by_tenant() {
    let store = MemoryJobStore::new();
    let job1 = Job::new(make_job_spec("tenant-1"));
    let job2 = Job::new(make_job_spec("tenant-2"));
    store.create_job(&job1).await.unwrap();
    store.create_job(&job2).await.unwrap();

    let tenant1_jobs = store.list_by_tenant("tenant-1").await.unwrap();
    assert_eq!(tenant1_jobs.len(), 1);
    assert_eq!(tenant1_jobs[0].spec.tenant_id, "tenant-1");
}

#[tokio::test]
async fn test_count_active_by_tenant() {
    let store = MemoryJobStore::new();
    let job1 = Job::new(make_job_spec("tenant-1"));
    let id1 = job1.id.clone();
    let job2 = Job::new(make_job_spec("tenant-1"));
    store.create_job(&job1).await.unwrap();
    store.create_job(&job2).await.unwrap();

    assert_eq!(store.count_active_by_tenant("tenant-1").await.unwrap(), 2);

    // Transition job1 to terminal
    store
        .update_state(&id1, JobState::Pending, JobState::Scheduled)
        .await
        .unwrap();
    store
        .update_state(&id1, JobState::Scheduled, JobState::Running)
        .await
        .unwrap();
    store
        .update_state(&id1, JobState::Running, JobState::Succeeded)
        .await
        .unwrap();

    assert_eq!(store.count_active_by_tenant("tenant-1").await.unwrap(), 1);
}

#[tokio::test]
async fn test_cleanup_old() {
    let store = MemoryJobStore::new();
    let mut job = Job::new(make_job_spec("tenant-1"));
    job.state = JobState::Succeeded;
    job.updated_at = Utc::now() - chrono::Duration::hours(2);
    store.jobs.insert(job.id.clone(), job);

    let removed = store.cleanup_old(Duration::from_secs(3600)).await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(store.jobs.len(), 0);
}

#[tokio::test]
async fn test_cleanup_old_keeps_recent() {
    let store = MemoryJobStore::new();
    let mut job = Job::new(make_job_spec("tenant-1"));
    job.state = JobState::Succeeded;
    // updated_at is recent (now), so it should not be cleaned up
    store.jobs.insert(job.id.clone(), job);

    let removed = store.cleanup_old(Duration::from_secs(3600)).await.unwrap();
    assert_eq!(removed, 0);
    assert_eq!(store.jobs.len(), 1);
}

#[tokio::test]
async fn test_list_jobs_with_filters() {
    let store = MemoryJobStore::new();
    let job1 = Job::new(make_job_spec("tenant-1"));
    let id1 = job1.id.clone();
    let job2 = Job::new(make_job_spec("tenant-2"));
    store.create_job(&job1).await.unwrap();
    store.create_job(&job2).await.unwrap();

    // Filter by tenant
    let jobs = store
        .list_jobs(None, Some("tenant-1"), 10, 0)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);

    // Move job1 to Scheduled and filter by state
    store
        .update_state(&id1, JobState::Pending, JobState::Scheduled)
        .await
        .unwrap();
    let jobs = store
        .list_jobs(Some(JobState::Pending), None, 10, 0)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].spec.tenant_id, "tenant-2");
}

#[tokio::test]
async fn test_update_job() {
    let store = MemoryJobStore::new();
    let mut job = Job::new(make_job_spec("tenant-1"));
    store.create_job(&job).await.unwrap();

    job.assigned_agent = Some("agent-1".to_string());
    store.update_job(&job).await.unwrap();

    let fetched = store.get_job(&job.id).await.unwrap().unwrap();
    assert_eq!(fetched.assigned_agent, Some("agent-1".to_string()));
}

#[tokio::test]
async fn test_get_nonexistent_job() {
    let store = MemoryJobStore::new();
    let result = store.get_job("nonexistent").await.unwrap();
    assert!(result.is_none());
}
