// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tests for the SQLite job store.

use super::*;
use muli_core::job::model::{JobSpec, PriorityTier};
use muli_core::resource::limits::ResourceSpec;

async fn make_store() -> (SqliteJobStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
    (SqliteJobStore::new(factory), dir)
}

fn make_spec(tenant_id: &str) -> JobSpec {
    JobSpec {
        deployment_id: "dep-1".into(),
        project_id: "proj-1".into(),
        workspace_id: "ws-1".into(),
        tenant_id: tenant_id.into(),
        runner_image: "node:18".into(),
        env_vars: vec![],
        resources: ResourceSpec::default(),
        priority_tier: PriorityTier::Standard,
        framework: "next".into(),
        idempotency_key: None,
        registry_credentials: None,
        commands: vec![],
    }
}

#[tokio::test]
async fn test_create_and_get_job() {
    let (store, _dir) = make_store().await;
    let job = Job::new(make_spec("t1"));
    let id = store.create_job(&job).await.unwrap();
    let fetched = store.get_job(&id).await.unwrap().unwrap();
    assert_eq!(fetched.id, job.id);
}

#[tokio::test]
async fn test_update_state_cas() {
    let (store, _dir) = make_store().await;
    let job = Job::new(make_spec("t1"));
    let id = job.id.clone();
    store.create_job(&job).await.unwrap();
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
    let (store, _dir) = make_store().await;
    let job = Job::new(make_spec("t1"));
    let id = job.id.clone();
    store.create_job(&job).await.unwrap();
    // Pending -> Running is not valid
    assert!(
        store
            .update_state(&id, JobState::Pending, JobState::Running)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_list_pending_sorted() {
    let (store, _dir) = make_store().await;
    let mut spec_low = make_spec("t1");
    spec_low.priority_tier = PriorityTier::Free;
    let j_low = Job::new(spec_low);
    let mut spec_high = make_spec("t1");
    spec_high.priority_tier = PriorityTier::Enterprise;
    let j_high = Job::new(spec_high);
    store.create_job(&j_low).await.unwrap();
    store.create_job(&j_high).await.unwrap();
    let pending = store.list_pending().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending[0].priority_score >= pending[1].priority_score);
}

#[tokio::test]
async fn test_count_active() {
    let (store, _dir) = make_store().await;
    let j1 = Job::new(make_spec("t1"));
    let id1 = j1.id.clone();
    let j2 = Job::new(make_spec("t1"));
    store.create_job(&j1).await.unwrap();
    store.create_job(&j2).await.unwrap();
    assert_eq!(store.count_active_by_tenant("t1").await.unwrap(), 2);
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
    assert_eq!(store.count_active_by_tenant("t1").await.unwrap(), 1);
}

#[tokio::test]
async fn test_cleanup_old() {
    let (store, _dir) = make_store().await;
    let j1 = Job::new(make_spec("t1"));
    let id1 = j1.id.clone();
    store.create_job(&j1).await.unwrap();
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
    // Only keep jobs updated within the last second
    let removed = store
        .cleanup_old(std::time::Duration::from_millis(1))
        .await
        .unwrap();
    // May or may not remove depending on timing, but shouldn't error
    let _ = removed;
}
