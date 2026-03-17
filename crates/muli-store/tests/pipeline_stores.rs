// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for all pipeline SQLite stores.

use std::sync::Arc;

use muli_core::pipeline::{
    Artifact, CacheEntry, FailureStrategy, Pipeline, PipelineRun, PipelineRunState, PipelineSecret,
    PipelineTrigger, StepRun, StepRunState,
};
use muli_core::traits::{
    ArtifactStore, CacheStore, PipelineRunStore, PipelineSecretStore, PipelineStore, StepRunStore,
};
use muli_store::sqlite::{
    SqliteArtifactStore, SqliteCacheStore, SqlitePipelineRunStore, SqlitePipelineSecretStore,
    SqlitePipelineStore, SqliteStepRunStore, SqliteStoreFactory,
};

async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
    (factory, dir)
}

fn make_run(tenant_id: &str, pipeline_id: &str, repo_id: &str, run_number: u64) -> PipelineRun {
    PipelineRun::new(
        pipeline_id.into(),
        tenant_id.into(),
        repo_id.into(),
        run_number,
        "abc123".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push {
            ref_name: "refs/heads/main".into(),
        },
        "name: ci\nsteps:\n  - name: test\n    image: rust:1.82".into(),
    )
}

// ─── Full CRUD flow ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_full_pipeline_crud_flow() {
    let (factory, _dir) = make_factory().await;

    let pipe_store = SqlitePipelineStore::new(factory.clone());
    let run_store = SqlitePipelineRunStore::new(factory.clone());
    let step_store = SqliteStepRunStore::new(factory.clone());

    // 1. Create pipeline
    let pipeline = Pipeline::new(
        "t1".into(),
        "repo-1".into(),
        "ci".into(),
        "sha256abc".into(),
    );
    pipe_store.upsert_pipeline(&pipeline).await.unwrap();
    let fetched = pipe_store
        .get_pipeline("t1", &pipeline.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.name, "ci");

    // 2. Create run
    let run = make_run("t1", &pipeline.id, "repo-1", 1);
    let run_id = run_store.create_run(&run).await.unwrap();
    let fetched_run = run_store.get_run("t1", &run_id).await.unwrap().unwrap();
    assert_eq!(fetched_run.state, PipelineRunState::Pending);

    // 3. Create steps
    let step1 = StepRun::new(
        run_id.clone(),
        "t1".into(),
        "test".into(),
        FailureStrategy::Stop,
        None,
    );
    let step2 = StepRun::new(
        run_id.clone(),
        "t1".into(),
        "build".into(),
        FailureStrategy::Stop,
        None,
    );
    let step1_id = step_store.create_step(&step1).await.unwrap();
    let step2_id = step_store.create_step(&step2).await.unwrap();

    // 4. Update step states
    step_store
        .update_step_state("t1", &step1_id, StepRunState::Running)
        .await
        .unwrap();
    step_store
        .update_step_state("t1", &step1_id, StepRunState::Succeeded)
        .await
        .unwrap();
    step_store
        .update_step_state("t1", &step2_id, StepRunState::Running)
        .await
        .unwrap();
    step_store
        .update_step_state("t1", &step2_id, StepRunState::Succeeded)
        .await
        .unwrap();

    // 5. Verify all steps succeeded
    let steps = step_store.list_by_run("t1", &run_id).await.unwrap();
    assert_eq!(steps.len(), 2);
    assert!(steps.iter().all(|s| s.state == StepRunState::Succeeded));

    // 6. Update run state
    let mut run = fetched_run;
    run.state = PipelineRunState::Succeeded;
    run_store.update_run(&run).await.unwrap();
    let final_run = run_store.get_run("t1", &run_id).await.unwrap().unwrap();
    assert_eq!(final_run.state, PipelineRunState::Succeeded);
}

// ─── Tenant isolation ───────────────────────────────────────────────────

#[tokio::test]
async fn test_tenant_isolation() {
    let (factory, _dir) = make_factory().await;
    let pipe_store = SqlitePipelineStore::new(factory.clone());
    let run_store = SqlitePipelineRunStore::new(factory.clone());
    let step_store = SqliteStepRunStore::new(factory.clone());

    // Create pipeline in tenant A
    let pipe_a = Pipeline::new(
        "tenant-a".into(),
        "repo-1".into(),
        "ci".into(),
        "sha1".into(),
    );
    pipe_store.upsert_pipeline(&pipe_a).await.unwrap();

    // Create pipeline in tenant B with same repo_id
    let pipe_b = Pipeline::new(
        "tenant-b".into(),
        "repo-1".into(),
        "ci".into(),
        "sha2".into(),
    );
    pipe_store.upsert_pipeline(&pipe_b).await.unwrap();

    // Both should be retrievable by ID from their own tenant
    assert!(
        pipe_store
            .get_pipeline("tenant-a", &pipe_a.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        pipe_store
            .get_pipeline("tenant-b", &pipe_b.id)
            .await
            .unwrap()
            .is_some()
    );

    // get_by_repo is now tenant-scoped — each tenant sees only its own
    let tenant_a_pipes = pipe_store.get_by_repo("tenant-a", "repo-1").await.unwrap();
    assert_eq!(tenant_a_pipes.len(), 1);
    let tenant_b_pipes = pipe_store.get_by_repo("tenant-b", "repo-1").await.unwrap();
    assert_eq!(tenant_b_pipes.len(), 1);

    // Create runs in different tenants
    let run_a = make_run("tenant-a", &pipe_a.id, "repo-1", 1);
    let run_b = make_run("tenant-b", &pipe_b.id, "repo-1", 1);
    run_store.create_run(&run_a).await.unwrap();
    run_store.create_run(&run_b).await.unwrap();

    // Create steps in different tenants
    let step_a = StepRun::new(
        run_a.id.clone(),
        "tenant-a".into(),
        "test".into(),
        FailureStrategy::Stop,
        None,
    );
    let step_b = StepRun::new(
        run_b.id.clone(),
        "tenant-b".into(),
        "test".into(),
        FailureStrategy::Stop,
        None,
    );
    step_store.create_step(&step_a).await.unwrap();
    step_store.create_step(&step_b).await.unwrap();

    // Steps should be isolated by tenant+run_id
    let steps_a = step_store.list_by_run("tenant-a", &run_a.id).await.unwrap();
    assert_eq!(steps_a.len(), 1);
    assert_eq!(steps_a[0].tenant_id, "tenant-a");

    let steps_b = step_store.list_by_run("tenant-b", &run_b.id).await.unwrap();
    assert_eq!(steps_b.len(), 1);
    assert_eq!(steps_b[0].tenant_id, "tenant-b");
}

// ─── Pagination ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_run_pagination() {
    let (factory, _dir) = make_factory().await;
    let run_store = SqlitePipelineRunStore::new(factory);

    // Create 10 runs
    for i in 1..=10 {
        let run = make_run("t1", "pipe-1", "repo-1", i);
        run_store.create_run(&run).await.unwrap();
    }

    // Page 1: limit 3, offset 0
    let page1 = run_store
        .list_by_repo("t1", "repo-1", None, 3, 0)
        .await
        .unwrap();
    assert_eq!(page1.len(), 3);
    assert_eq!(page1[0].run_number, 10); // Newest first
    assert_eq!(page1[1].run_number, 9);
    assert_eq!(page1[2].run_number, 8);

    // Page 2
    let page2 = run_store
        .list_by_repo("t1", "repo-1", None, 3, 3)
        .await
        .unwrap();
    assert_eq!(page2.len(), 3);
    assert_eq!(page2[0].run_number, 7);

    // Last page
    let page4 = run_store
        .list_by_repo("t1", "repo-1", None, 3, 9)
        .await
        .unwrap();
    assert_eq!(page4.len(), 1);
    assert_eq!(page4[0].run_number, 1);

    // Beyond end
    let empty = run_store
        .list_by_repo("t1", "repo-1", None, 3, 10)
        .await
        .unwrap();
    assert!(empty.is_empty());
}

// ─── Cache eviction ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_cache_eviction_flow() {
    let (factory, _dir) = make_factory().await;
    let cache_store = SqliteCacheStore::new(factory);

    // Create 5 cache entries (1000 bytes each = 5000 total)
    for i in 0..5 {
        let mut entry = CacheEntry::new(
            "t1".into(),
            "repo-1".into(),
            format!("key-{i}"),
            1000,
            format!("sha-{i}"),
        );
        // Stagger last_used_at so LRU ordering is deterministic
        entry.last_used_at = chrono::Utc::now() - chrono::Duration::hours(5 - i as i64);
        cache_store.upsert_cache(&entry).await.unwrap();
    }

    // Verify all 5 exist
    let all = cache_store.list_by_repo("t1", "repo-1").await.unwrap();
    assert_eq!(all.len(), 5);

    // Evict to 2500 bytes (should remove at least 2 oldest entries)
    let evicted = cache_store.evict_lru("t1", "repo-1", 2500).await.unwrap();
    assert!(
        evicted >= 2,
        "should evict at least 2 entries, evicted {evicted}"
    );

    let remaining = cache_store.list_by_repo("t1", "repo-1").await.unwrap();
    let total: u64 = remaining.iter().map(|e| e.size_bytes).sum();
    assert!(
        total <= 3000,
        "total after eviction should be under limit, got {total}"
    );
}

// ─── Secrets ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_secrets_cross_repo_isolation() {
    let (factory, _dir) = make_factory().await;
    let secret_store = SqlitePipelineSecretStore::new(factory);

    let s1 = PipelineSecret::new(
        "t1".into(),
        "repo-1".into(),
        "API_KEY".into(),
        "enc1".into(),
    );
    let s2 = PipelineSecret::new(
        "t1".into(),
        "repo-2".into(),
        "API_KEY".into(),
        "enc2".into(),
    );
    secret_store.set_secret(&s1).await.unwrap();
    secret_store.set_secret(&s2).await.unwrap();

    // Same name, different repos
    let fetched1 = secret_store
        .get_secret("t1", "repo-1", "API_KEY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched1.encrypted_value, "enc1");

    let fetched2 = secret_store
        .get_secret("t1", "repo-2", "API_KEY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched2.encrypted_value, "enc2");

    // list_names should be repo-scoped
    let names1 = secret_store.list_names("t1", "repo-1").await.unwrap();
    assert_eq!(names1, vec!["API_KEY"]);
    let names2 = secret_store.list_names("t1", "repo-2").await.unwrap();
    assert_eq!(names2, vec!["API_KEY"]);
}

// ─── Artifacts ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_artifacts_across_runs() {
    let (factory, _dir) = make_factory().await;
    let store = SqliteArtifactStore::new(factory);

    let a1 = Artifact::new(
        "t1".into(),
        "run-1".into(),
        "build".into(),
        "binary".into(),
        1024,
        "sha1".into(),
        None,
    );
    let a2 = Artifact::new(
        "t1".into(),
        "run-2".into(),
        "build".into(),
        "binary".into(),
        2048,
        "sha2".into(),
        None,
    );
    store.create_artifact(&a1).await.unwrap();
    store.create_artifact(&a2).await.unwrap();

    let run1_artifacts = store.list_by_run("t1", "run-1").await.unwrap();
    assert_eq!(run1_artifacts.len(), 1);
    assert_eq!(run1_artifacts[0].size_bytes, 1024);

    let run2_artifacts = store.list_by_run("t1", "run-2").await.unwrap();
    assert_eq!(run2_artifacts.len(), 1);
    assert_eq!(run2_artifacts[0].size_bytes, 2048);
}

// ─── Next run number ────────────────────────────────────────────────────

#[tokio::test]
async fn test_next_run_number_across_pipelines() {
    let (factory, _dir) = make_factory().await;
    let run_store = SqlitePipelineRunStore::new(factory);

    // Pipeline A
    let run_a1 = make_run("t1", "pipe-a", "repo-1", 1);
    let run_a2 = make_run("t1", "pipe-a", "repo-1", 2);
    run_store.create_run(&run_a1).await.unwrap();
    run_store.create_run(&run_a2).await.unwrap();

    // Pipeline B
    let run_b1 = make_run("t1", "pipe-b", "repo-1", 1);
    run_store.create_run(&run_b1).await.unwrap();

    // Each pipeline should have independent run numbers
    let next_a = run_store.next_run_number("t1", "pipe-a").await.unwrap();
    assert_eq!(next_a, 3);

    let next_b = run_store.next_run_number("t1", "pipe-b").await.unwrap();
    assert_eq!(next_b, 2);

    // Non-existent pipeline
    let next_new = run_store.next_run_number("t1", "pipe-new").await.unwrap();
    assert_eq!(next_new, 1);
}
