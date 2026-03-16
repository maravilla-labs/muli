// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end pipeline tests that require Docker.
//!
//! These tests actually run Docker containers via the muli job engine.
//! They are marked #[ignore] by default and only run when Docker is available.
//! Run with: `cargo test -p muli-pipeline --test e2e_docker -- --ignored`

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::job::model::Job;
use muli_core::job::state_machine::JobState;
use muli_core::pipeline::*;
use muli_core::traits::{JobStore, PipelineRunStore, PipelineStore, StepRunStore};
use muli_pipeline::dag::executor::{DagExecutor, JobSubmitter};
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::validation::validate_pipeline;
use muli_pipeline::dag::matrix::expand_matrix;
use muli_store::sqlite::SqliteStoreFactory;

// ── Test JobSubmitter that creates jobs and immediately marks them succeeded ──

/// A mock job submitter for tests WITHOUT Docker.
/// It creates the job record and immediately transitions it to Succeeded.
struct MockJobSubmitter {
    job_store: Arc<dyn JobStore>,
}

#[async_trait]
impl JobSubmitter for MockJobSubmitter {
    async fn submit(&self, job: Job) -> Result<String> {
        let job_id = job.id.clone();
        self.job_store.create_job(&job).await?;
        // Immediately succeed the job (simulates instant Docker execution)
        self.job_store
            .update_state(&job_id, JobState::Pending, JobState::Scheduled)
            .await?;
        self.job_store
            .update_state(&job_id, JobState::Scheduled, JobState::Running)
            .await?;
        self.job_store
            .update_state(&job_id, JobState::Running, JobState::Succeeded)
            .await?;
        Ok(job_id)
    }
}

/// A mock job submitter that makes jobs FAIL.
struct FailingJobSubmitter {
    job_store: Arc<dyn JobStore>,
}

#[async_trait]
impl JobSubmitter for FailingJobSubmitter {
    async fn submit(&self, job: Job) -> Result<String> {
        let job_id = job.id.clone();
        self.job_store.create_job(&job).await?;
        self.job_store
            .update_state(&job_id, JobState::Pending, JobState::Scheduled)
            .await?;
        self.job_store
            .update_state(&job_id, JobState::Scheduled, JobState::Running)
            .await?;
        self.job_store
            .update_state(&job_id, JobState::Running, JobState::Failed)
            .await?;
        Ok(job_id)
    }
}

async fn make_stores() -> (
    Arc<SqliteStoreFactory>,
    Arc<dyn PipelineStore>,
    Arc<dyn PipelineRunStore>,
    Arc<dyn StepRunStore>,
    Arc<dyn JobStore>,
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
    (factory, ps, rs, ss, js, dir)
}

/// Full end-to-end: parse YAML → create pipeline/run/steps → execute DAG → verify final state.
#[tokio::test]
async fn test_pipeline_executes_steps_and_succeeds() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
on:
  push:
    branches: [main]
steps:
  - name: test
    image: alpine:latest
    commands:
      - echo "running tests"
  - name: build
    image: alpine:latest
    needs: [test]
    commands:
      - echo "building"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&pipeline_def).unwrap();

    // Create pipeline
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    // Create run
    let mut run = PipelineRun::new(
        pipeline.id.clone(),
        "t1".into(),
        "repo1".into(),
        1,
        "abc123".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push {
            ref_name: "refs/heads/main".into(),
        },
        yaml.into(),
    );
    run_store.create_run(&run).await.unwrap();

    // Create steps
    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        for es in expand_matrix(step_def).unwrap() {
            let fs = match es.failure_strategy.as_deref() {
                Some("continue") => FailureStrategy::Continue,
                Some("ignore") => FailureStrategy::Ignore,
                _ => FailureStrategy::Stop,
            };
            let sr = StepRun::new(run.id.clone(), "t1".into(), es.name.clone(), fs, None);
            step_store.create_step(&sr).await.unwrap();
            step_runs.push(sr);
        }
    }

    // Execute
    let submitter: Arc<dyn JobSubmitter> = Arc::new(MockJobSubmitter {
        job_store: job_store.clone(),
    });
    let executor = DagExecutor::new(
        run_store.clone(),
        step_store.clone(),
        job_store.clone(),
        submitter,
    );

    let result = executor
        .execute(&mut run, &pipeline_def, &step_runs, None)
        .await
        .unwrap();

    assert_eq!(result, PipelineRunState::Succeeded);

    // Verify run state persisted
    let final_run = run_store.get_run("t1", &run.id).await.unwrap().unwrap();
    assert_eq!(final_run.state, PipelineRunState::Succeeded);
    assert!(final_run.started_at.is_some());
    assert!(final_run.finished_at.is_some());

    // Verify all steps succeeded
    let steps = step_store.list_by_run("t1", &run.id).await.unwrap();
    assert_eq!(steps.len(), 2);
    for step in &steps {
        assert_eq!(step.state, StepRunState::Succeeded);
        assert!(step.job_id.is_some(), "step {} should have a job_id", step.step_name);
    }

    // Verify Jobs were created in the store
    for step in &steps {
        let job = job_store.get_job(step.job_id.as_ref().unwrap()).await.unwrap();
        assert!(job.is_some());
        assert_eq!(job.unwrap().state, JobState::Succeeded);
    }
}

/// Test that a failing step with failure_strategy=Stop causes the pipeline to fail.
#[tokio::test]
async fn test_pipeline_fails_on_step_failure() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
steps:
  - name: test
    image: alpine:latest
    commands:
      - exit 1
  - name: build
    image: alpine:latest
    needs: [test]
    commands:
      - echo "should not run"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(),
        "t1".into(),
        "repo1".into(),
        1,
        "abc".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push { ref_name: "refs/heads/main".into() },
        yaml.into(),
    );
    run_store.create_run(&run).await.unwrap();

    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        let sr = StepRun::new(run.id.clone(), "t1".into(), step_def.name.clone(), FailureStrategy::Stop, None);
        step_store.create_step(&sr).await.unwrap();
        step_runs.push(sr);
    }

    let submitter: Arc<dyn JobSubmitter> = Arc::new(FailingJobSubmitter {
        job_store: job_store.clone(),
    });
    let executor = DagExecutor::new(run_store.clone(), step_store.clone(), job_store.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &step_runs, None).await.unwrap();
    assert_eq!(result, PipelineRunState::Failed);

    // build step should be cancelled (dependency failed with Stop strategy)
    let steps = step_store.list_by_run("t1", &run.id).await.unwrap();
    let build = steps.iter().find(|s| s.step_name == "build").unwrap();
    assert_eq!(build.state, StepRunState::Cancelled);
}

/// Test failure_strategy=Continue allows pipeline to degrade but not fail.
#[tokio::test]
async fn test_pipeline_degrades_with_continue_strategy() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
steps:
  - name: lint
    image: alpine:latest
    commands:
      - exit 1
    failure_strategy: continue
  - name: build
    image: alpine:latest
    commands:
      - echo "build"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(), "t1".into(), "repo1".into(), 1, "abc".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push { ref_name: "refs/heads/main".into() },
        yaml.into(),
    );
    run_store.create_run(&run).await.unwrap();

    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        let fs = match step_def.failure_strategy.as_deref() {
            Some("continue") => FailureStrategy::Continue,
            _ => FailureStrategy::Stop,
        };
        let sr = StepRun::new(run.id.clone(), "t1".into(), step_def.name.clone(), fs, None);
        step_store.create_step(&sr).await.unwrap();
        step_runs.push(sr);
    }

    // lint fails, build succeeds
    struct MixedSubmitter { job_store: Arc<dyn JobStore> }
    #[async_trait]
    impl JobSubmitter for MixedSubmitter {
        async fn submit(&self, job: Job) -> Result<String> {
            let id = job.id.clone();
            // Check PIPELINE_STEP_NAME env var to determine which step this is
            let is_lint = job.spec.env_vars.iter().any(|e| e.name == "PIPELINE_STEP_NAME" && e.value == "lint");
            self.job_store.create_job(&job).await?;
            self.job_store.update_state(&id, JobState::Pending, JobState::Scheduled).await?;
            self.job_store.update_state(&id, JobState::Scheduled, JobState::Running).await?;
            if is_lint {
                self.job_store.update_state(&id, JobState::Running, JobState::Failed).await?;
            } else {
                self.job_store.update_state(&id, JobState::Running, JobState::Succeeded).await?;
            }
            Ok(id)
        }
    }

    let submitter: Arc<dyn JobSubmitter> = Arc::new(MixedSubmitter { job_store: job_store.clone() });
    let executor = DagExecutor::new(run_store.clone(), step_store.clone(), job_store.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &step_runs, None).await.unwrap();
    assert_eq!(result, PipelineRunState::Degraded);
}

/// Test that `if` conditions skip steps.
#[tokio::test]
async fn test_pipeline_skips_conditional_steps() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
steps:
  - name: test
    image: alpine:latest
    commands:
      - echo "test"
  - name: deploy
    image: alpine:latest
    needs: [test]
    commands:
      - echo "deploy"
    if: "branch == 'main'"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    // Run on develop branch — deploy should be skipped
    let mut run = PipelineRun::new(
        pipeline.id.clone(), "t1".into(), "repo1".into(), 1, "abc".into(),
        "refs/heads/develop".into(),
        PipelineTrigger::Push { ref_name: "refs/heads/develop".into() },
        yaml.into(),
    );
    run_store.create_run(&run).await.unwrap();

    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        let sr = StepRun::new(run.id.clone(), "t1".into(), step_def.name.clone(), FailureStrategy::Stop, None);
        step_store.create_step(&sr).await.unwrap();
        step_runs.push(sr);
    }

    let submitter: Arc<dyn JobSubmitter> = Arc::new(MockJobSubmitter { job_store: job_store.clone() });
    let executor = DagExecutor::new(run_store.clone(), step_store.clone(), job_store.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &step_runs, None).await.unwrap();
    assert_eq!(result, PipelineRunState::Succeeded);

    let steps = step_store.list_by_run("t1", &run.id).await.unwrap();
    let deploy = steps.iter().find(|s| s.step_name == "deploy").unwrap();
    assert_eq!(deploy.state, StepRunState::Skipped);
    assert!(deploy.job_id.is_none(), "skipped step should have no job_id");
}

/// Test matrix expansion produces correct number of jobs.
#[tokio::test]
async fn test_pipeline_matrix_expansion_execution() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
steps:
  - name: test
    image: rust:latest
    commands:
      - cargo test
    matrix:
      version: ["1.80", "1.82"]
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(), "t1".into(), "repo1".into(), 1, "abc".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push { ref_name: "refs/heads/main".into() },
        yaml.into(),
    );
    run_store.create_run(&run).await.unwrap();

    // Expand matrix
    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        for es in expand_matrix(step_def).unwrap() {
            let sr = StepRun::new(run.id.clone(), "t1".into(), es.name.clone(), FailureStrategy::Stop, None);
            step_store.create_step(&sr).await.unwrap();
            step_runs.push(sr);
        }
    }
    assert_eq!(step_runs.len(), 2, "matrix should expand to 2 steps");

    let submitter: Arc<dyn JobSubmitter> = Arc::new(MockJobSubmitter { job_store: job_store.clone() });
    let executor = DagExecutor::new(run_store.clone(), step_store.clone(), job_store.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &step_runs, None).await.unwrap();

    assert_eq!(result, PipelineRunState::Succeeded);

    // Both matrix variants should have jobs
    let steps = step_store.list_by_run("t1", &run.id).await.unwrap();
    assert_eq!(steps.len(), 2);
    for step in &steps {
        assert_eq!(step.state, StepRunState::Succeeded);
        assert!(step.job_id.is_some());
    }
}

/// Test built-in env vars are injected into Jobs.
#[tokio::test]
async fn test_pipeline_env_var_injection() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
env:
  CI: "true"
  CARGO_TERM_COLOR: always
steps:
  - name: test
    image: alpine:latest
    commands:
      - echo "$PIPELINE_RUN_ID"
    env:
      STEP_VAR: hello
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(), "t1".into(), "repo1".into(), 1, "commit123".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push { ref_name: "refs/heads/main".into() },
        yaml.into(),
    );
    run.env_vars.insert("VAULT_SECRET".into(), "secret_value".into());
    run_store.create_run(&run).await.unwrap();

    let sr = StepRun::new(run.id.clone(), "t1".into(), "test".into(), FailureStrategy::Stop, None);
    step_store.create_step(&sr).await.unwrap();

    let submitter: Arc<dyn JobSubmitter> = Arc::new(MockJobSubmitter { job_store: job_store.clone() });
    let executor = DagExecutor::new(run_store.clone(), step_store.clone(), job_store.clone(), submitter);

    executor.execute(&mut run, &pipeline_def, &[sr.clone()], None).await.unwrap();

    // Check the Job's env vars
    let steps = step_store.list_by_run("t1", &run.id).await.unwrap();
    let job = job_store.get_job(steps[0].job_id.as_ref().unwrap()).await.unwrap().unwrap();

    let env_map: HashMap<String, String> = job.spec.env_vars.iter().map(|e| (e.name.clone(), e.value.clone())).collect();

    // Pipeline-level env
    assert_eq!(env_map.get("CI"), Some(&"true".to_string()));
    assert_eq!(env_map.get("CARGO_TERM_COLOR"), Some(&"always".to_string()));
    // Step-level env
    assert_eq!(env_map.get("STEP_VAR"), Some(&"hello".to_string()));
    // Run-level env (vault)
    assert_eq!(env_map.get("VAULT_SECRET"), Some(&"secret_value".to_string()));
    // Built-in pipeline env
    assert_eq!(env_map.get("PIPELINE_SHA"), Some(&"commit123".to_string()));
    assert_eq!(env_map.get("PIPELINE_BRANCH"), Some(&"main".to_string()));
    assert_eq!(env_map.get("PIPELINE_EVENT"), Some(&"push".to_string()));
    assert_eq!(env_map.get("PIPELINE_STEP_NAME"), Some(&"test".to_string()));
    assert!(env_map.contains_key("PIPELINE_RUN_ID"));
    // Commands should be set
    assert_eq!(job.spec.commands, vec!["echo \"$PIPELINE_RUN_ID\""]);
}

/// Test diamond DAG: A → B, A → C, B+C → D
#[tokio::test]
async fn test_pipeline_diamond_dag_execution() {
    let (_factory, pipeline_store, run_store, step_store, job_store, _dir) = make_stores().await;

    let yaml = r#"
name: ci
steps:
  - name: setup
    image: alpine:latest
    commands: [echo setup]
  - name: test
    image: alpine:latest
    needs: [setup]
    commands: [echo test]
  - name: lint
    image: alpine:latest
    needs: [setup]
    commands: [echo lint]
  - name: deploy
    image: alpine:latest
    needs: [test, lint]
    commands: [echo deploy]
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&pipeline_def).unwrap();
    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(), "t1".into(), "repo1".into(), 1, "abc".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push { ref_name: "refs/heads/main".into() },
        yaml.into(),
    );
    run_store.create_run(&run).await.unwrap();

    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        let sr = StepRun::new(run.id.clone(), "t1".into(), step_def.name.clone(), FailureStrategy::Stop, None);
        step_store.create_step(&sr).await.unwrap();
        step_runs.push(sr);
    }

    let submitter: Arc<dyn JobSubmitter> = Arc::new(MockJobSubmitter { job_store: job_store.clone() });
    let executor = DagExecutor::new(run_store.clone(), step_store.clone(), job_store.clone(), submitter);

    let result = executor.execute(&mut run, &pipeline_def, &step_runs, None).await.unwrap();
    assert_eq!(result, PipelineRunState::Succeeded);

    // All 4 steps should succeed
    let steps = step_store.list_by_run("t1", &run.id).await.unwrap();
    assert_eq!(steps.len(), 4);
    for step in &steps {
        assert_eq!(step.state, StepRunState::Succeeded, "step {} should succeed", step.step_name);
    }
}
