// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline e2e tests that execute real Docker containers.
//! These tests prove the full pipeline flow: YAML → DAG → Job → Docker → Logs.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use muli_core::job::model::Job;
use muli_core::job::state_machine::JobState;
use muli_core::pipeline::*;
use muli_core::traits::{JobLogStore, JobStore, PipelineRunStore, PipelineStore, StepRunStore};
use muli_engine::docker::logs::LogCollector;
use muli_pipeline::dag::executor::{DagExecutor, JobSubmitter};
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::validation::validate_pipeline;
use muli_queue::{ConcurrencyLimiter, PriorityQueue, Scheduler};
use muli_store::sqlite::SqliteStoreFactory;

use common::{dummy_executor, run_job};

/// Real JobSubmitter that goes through the scheduler → DockerExecutor pipeline.
struct RealJobSubmitter {
    job_store: Arc<dyn JobStore>,
    scheduler: Arc<Scheduler>,
}

#[async_trait]
impl JobSubmitter for RealJobSubmitter {
    async fn submit(&self, job: Job) -> muli_core::error::Result<String> {
        let job_id = job.id.clone();
        let tenant_id = job.spec.tenant_id.clone();
        let tier = job.spec.priority_tier;
        self.job_store.create_job(&job).await?;
        self.scheduler.enqueue(job_id.clone(), tier, tenant_id);
        Ok(job_id)
    }
}

/// Helper: set up SQLite stores + scheduler + executor for pipeline tests.
async fn setup_pipeline_infra() -> (
    Arc<dyn PipelineStore>,
    Arc<dyn PipelineRunStore>,
    Arc<dyn StepRunStore>,
    Arc<dyn JobStore>,
    Arc<dyn JobLogStore>,
    Arc<Scheduler>,
    CancellationToken,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();

    let ps: Arc<dyn PipelineStore> =
        Arc::new(muli_store::sqlite::SqlitePipelineStore::new(factory.clone()));
    let rs: Arc<dyn PipelineRunStore> =
        Arc::new(muli_store::sqlite::SqlitePipelineRunStore::new(factory.clone()));
    let ss: Arc<dyn StepRunStore> =
        Arc::new(muli_store::sqlite::SqliteStepRunStore::new(factory.clone()));
    let js: Arc<dyn JobStore> =
        Arc::new(muli_store::sqlite::SqliteJobStore::new(factory.clone()));
    let jls: Arc<dyn JobLogStore> =
        Arc::new(muli_store::sqlite::SqliteJobLogStore::new(factory.clone()));

    let notify = Arc::new(Notify::new());
    let queue = Arc::new(PriorityQueue::new(notify.clone()));
    let limiter = Arc::new(ConcurrencyLimiter::new(10, 5));
    let scheduler = Arc::new(Scheduler::new(queue, limiter, notify));

    let cancel = CancellationToken::new();

    // Start scheduler loop with real Docker executor
    {
        let sched = scheduler.clone();
        let store = js.clone();
        let log_store = jls.clone();
        let cancel_clone = cancel.clone();
        let executor = dummy_executor().await; // Uses real DockerClient
        let log_collectors: Arc<DashMap<String, Arc<LogCollector>>> = Arc::new(DashMap::new());

        tokio::spawn(async move {
            sched
                .run(cancel_clone, move |job_id, _tenant_id| {
                    let store = store.clone();
                    let executor = executor.clone();
                    let log_collectors = log_collectors.clone();
                    let log_store = log_store.clone();
                    async move {
                        run_job(job_id, store, executor, log_collectors, log_store).await;
                    }
                })
                .await;
        });
    }

    (ps, rs, ss, js, jls, scheduler, cancel, dir)
}

// ─── TESTS ──────────────────────────────────────────────────────────────────

/// Run a real Docker container via the pipeline executor and verify it succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pipeline_runs_docker_container_and_succeeds() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let (ps, rs, ss, js, jls, scheduler, cancel, _dir) = setup_pipeline_infra().await;

    let yaml = r#"
name: docker-test
steps:
  - name: hello
    image: alpine:latest
    commands:
      - echo "PIPELINE_OUTPUT_HELLO_WORLD"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&pipeline_def).unwrap();

    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "docker-test".into(), "sha".into());
    ps.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(),
        "t1".into(),
        "repo1".into(),
        1,
        "abc123".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Manual {
            triggered_by: "test".into(),
        },
        yaml.into(),
    );
    rs.create_run(&run).await.unwrap();

    let sr = StepRun::new(
        run.id.clone(),
        "t1".into(),
        "hello".into(),
        FailureStrategy::Stop,
        None,
    );
    ss.create_step(&sr).await.unwrap();

    let submitter: Arc<dyn JobSubmitter> = Arc::new(RealJobSubmitter {
        job_store: js.clone(),
        scheduler: scheduler.clone(),
    });

    let executor = DagExecutor::new(rs.clone(), ss.clone(), js.clone(), submitter);

    let result = executor
        .execute(&mut run, &pipeline_def, &[sr.clone()], None)
        .await
        .unwrap();

    cancel.cancel();

    eprintln!("Pipeline result: {result:?}");
    assert_eq!(result, PipelineRunState::Succeeded);

    // Verify the run is persisted as Succeeded
    let final_run = rs.get_run(&run.id).await.unwrap().unwrap();
    assert_eq!(final_run.state, PipelineRunState::Succeeded);
    assert!(final_run.started_at.is_some());
    assert!(final_run.finished_at.is_some());

    // Verify the step has a job_id and succeeded
    let steps = ss.list_by_run(&run.id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].state, StepRunState::Succeeded);
    let job_id = steps[0].job_id.as_ref().expect("step should have job_id");

    // Verify the Job itself succeeded
    let job = js.get_job(job_id).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(job.spec.runner_image, "alpine:latest");
    assert_eq!(job.spec.commands, vec!["echo \"PIPELINE_OUTPUT_HELLO_WORLD\""]);

    // Verify logs contain our output
    let logs = jls.get_logs(job_id, 100).await.unwrap();
    eprintln!("Logs ({} lines):", logs.len());
    for line in &logs {
        eprintln!("  [{}] {}", line.stream, line.message);
    }
    assert!(
        logs.iter().any(|l| l.message.contains("PIPELINE_OUTPUT_HELLO_WORLD")),
        "Expected 'PIPELINE_OUTPUT_HELLO_WORLD' in logs, got: {:?}",
        logs.iter().map(|l| &l.message).collect::<Vec<_>>()
    );

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

/// Run a Docker container that fails (exit code 1) and verify the pipeline fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pipeline_docker_failure_propagates() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let (ps, rs, ss, js, _jls, scheduler, cancel, _dir) = setup_pipeline_infra().await;

    let yaml = r#"
name: fail-test
steps:
  - name: fail
    image: alpine:latest
    commands:
      - echo "about to fail"
      - exit 1
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "fail-test".into(), "sha".into());
    ps.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(),
        "t1".into(),
        "repo1".into(),
        1,
        "abc".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Manual { triggered_by: "test".into() },
        yaml.into(),
    );
    rs.create_run(&run).await.unwrap();

    let sr = StepRun::new(run.id.clone(), "t1".into(), "fail".into(), FailureStrategy::Stop, None);
    ss.create_step(&sr).await.unwrap();

    let submitter: Arc<dyn JobSubmitter> = Arc::new(RealJobSubmitter {
        job_store: js.clone(),
        scheduler: scheduler.clone(),
    });
    let executor = DagExecutor::new(rs.clone(), ss.clone(), js.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &[sr], None).await.unwrap();
    cancel.cancel();

    eprintln!("Pipeline result: {result:?}");
    assert_eq!(result, PipelineRunState::Failed);

    // Verify Job has non-zero exit code
    let steps = ss.list_by_run(&run.id).await.unwrap();
    let job_id = steps[0].job_id.as_ref().unwrap();
    let job = js.get_job(job_id).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Failed);

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

/// Multi-step pipeline: test → build (depends on test). Both run in Docker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pipeline_multi_step_docker_dag() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let (ps, rs, ss, js, jls, scheduler, cancel, _dir) = setup_pipeline_infra().await;

    let yaml = r#"
name: multi-step
steps:
  - name: test
    image: alpine:latest
    commands:
      - echo "STEP_TEST_OUTPUT"
  - name: build
    image: alpine:latest
    needs: [test]
    commands:
      - echo "STEP_BUILD_OUTPUT"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&pipeline_def).unwrap();

    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "multi".into(), "sha".into());
    ps.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(), "t1".into(), "repo1".into(), 1, "abc".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Manual { triggered_by: "test".into() },
        yaml.into(),
    );
    rs.create_run(&run).await.unwrap();

    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        let sr = StepRun::new(run.id.clone(), "t1".into(), step_def.name.clone(), FailureStrategy::Stop, None);
        ss.create_step(&sr).await.unwrap();
        step_runs.push(sr);
    }

    let submitter: Arc<dyn JobSubmitter> = Arc::new(RealJobSubmitter {
        job_store: js.clone(),
        scheduler: scheduler.clone(),
    });
    let executor = DagExecutor::new(rs.clone(), ss.clone(), js.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &step_runs, None).await.unwrap();
    cancel.cancel();

    eprintln!("Pipeline result: {result:?}");
    assert_eq!(result, PipelineRunState::Succeeded);

    // Verify both steps succeeded
    let steps = ss.list_by_run(&run.id).await.unwrap();
    assert_eq!(steps.len(), 2);
    for step in &steps {
        assert_eq!(step.state, StepRunState::Succeeded, "step {} failed", step.step_name);
        assert!(step.job_id.is_some());

        let job_id = step.job_id.as_ref().unwrap();
        let logs = jls.get_logs(job_id, 100).await.unwrap();
        eprintln!("Step '{}' logs ({} lines):", step.step_name, logs.len());
        for line in &logs {
            eprintln!("  [{}] {}", line.stream, line.message);
        }
    }

    // Verify test step logs
    let test_step = steps.iter().find(|s| s.step_name == "test").unwrap();
    let test_logs = jls.get_logs(test_step.job_id.as_ref().unwrap(), 100).await.unwrap();
    assert!(test_logs.iter().any(|l| l.message.contains("STEP_TEST_OUTPUT")));

    // Verify build step logs
    let build_step = steps.iter().find(|s| s.step_name == "build").unwrap();
    let build_logs = jls.get_logs(build_step.job_id.as_ref().unwrap(), 100).await.unwrap();
    assert!(build_logs.iter().any(|l| l.message.contains("STEP_BUILD_OUTPUT")));

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}
