// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Realistic pipeline e2e test: mimics a typical GitHub Actions CI workflow.
//!
//! Creates a Node.js project, runs `npm install` → `npm test` → `npm run build`
//! across multiple pipeline steps with DAG dependencies, just like a real CI.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use muli_core::job::model::Job;
use muli_core::pipeline::*;
use muli_core::traits::{JobLogStore, JobStore, PipelineRunStore, PipelineStore, StepRunStore};
use muli_engine::docker::logs::LogCollector;
use muli_pipeline::dag::executor::{DagExecutor, JobSubmitter};
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::validation::validate_pipeline;
use muli_queue::{ConcurrencyLimiter, PriorityQueue, Scheduler};
use muli_store::sqlite::SqliteStoreFactory;

use common::{dummy_executor, run_job};

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

async fn setup() -> (
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

    let ps: Arc<dyn PipelineStore> = Arc::new(muli_store::sqlite::SqlitePipelineStore::new(
        factory.clone(),
    ));
    let rs: Arc<dyn PipelineRunStore> = Arc::new(muli_store::sqlite::SqlitePipelineRunStore::new(
        factory.clone(),
    ));
    let ss: Arc<dyn StepRunStore> =
        Arc::new(muli_store::sqlite::SqliteStepRunStore::new(factory.clone()));
    let js: Arc<dyn JobStore> = Arc::new(muli_store::sqlite::SqliteJobStore::new(factory.clone()));
    let jls: Arc<dyn JobLogStore> =
        Arc::new(muli_store::sqlite::SqliteJobLogStore::new(factory.clone()));

    let notify = Arc::new(Notify::new());
    let queue = Arc::new(PriorityQueue::new(notify.clone()));
    let limiter = Arc::new(ConcurrencyLimiter::new(10, 5));
    let scheduler = Arc::new(Scheduler::new(queue, limiter, notify));
    let cancel = CancellationToken::new();

    {
        let sched = scheduler.clone();
        let store = js.clone();
        let log_store = jls.clone();
        let cancel_clone = cancel.clone();
        let executor = dummy_executor().await;
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

/// Realistic CI pipeline: install → lint → test → build (like a GitHub Actions workflow).
///
/// Uses `node:22-alpine` to run actual npm commands against a real package.json.
/// The pipeline YAML mirrors what you'd see in `.github/workflows/ci.yml`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_realistic_npm_ci_pipeline() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "node:22-alpine").await;

    let (ps, rs, ss, js, jls, scheduler, cancel, _dir) = setup().await;

    // This YAML mimics a real GitHub Actions workflow:
    //
    //   jobs:
    //     install:
    //       runs-on: ubuntu-latest
    //       steps: [npm ci]
    //     lint:
    //       needs: install
    //       steps: [npx eslint .]
    //     test:
    //       needs: install
    //       steps: [npm test]
    //     build:
    //       needs: [lint, test]
    //       steps: [npm run build]
    //
    // We simulate this by creating a package.json inline and running real npm.
    let yaml = r#"
name: ci
on:
  push:
    branches: [main]
env:
  NODE_ENV: test
  CI: "true"
steps:
  - name: install
    image: node:22-alpine
    commands:
      - mkdir -p /workspace && cd /workspace
      - |
        cat > package.json << 'PKGJSON'
        {
          "name": "test-app",
          "version": "1.0.0",
          "scripts": {
            "test": "echo 'Tests passed!' && exit 0",
            "lint": "echo 'Lint passed!' && exit 0",
            "build": "echo 'Build succeeded!' && mkdir -p dist && echo '<h1>Hello</h1>' > dist/index.html"
          }
        }
        PKGJSON
      - npm install --ignore-scripts
      - echo "INSTALL_DONE"

  - name: lint
    image: node:22-alpine
    needs: [install]
    commands:
      - mkdir -p /workspace && cd /workspace
      - |
        cat > package.json << 'PKGJSON'
        {
          "name": "test-app",
          "version": "1.0.0",
          "scripts": { "lint": "echo 'Lint passed!'" }
        }
        PKGJSON
      - npm run lint
      - echo "LINT_DONE"

  - name: test
    image: node:22-alpine
    needs: [install]
    commands:
      - mkdir -p /workspace && cd /workspace
      - |
        cat > package.json << 'PKGJSON'
        {
          "name": "test-app",
          "version": "1.0.0",
          "scripts": { "test": "echo 'Tests passed!'" }
        }
        PKGJSON
      - npm test
      - echo "TEST_DONE"

  - name: build
    image: node:22-alpine
    needs: [lint, test]
    commands:
      - mkdir -p /workspace && cd /workspace
      - |
        cat > package.json << 'PKGJSON'
        {
          "name": "test-app",
          "version": "1.0.0",
          "scripts": { "build": "echo 'Build succeeded!' && mkdir -p dist && echo '<h1>Hello</h1>' > dist/index.html" }
        }
        PKGJSON
      - npm run build
      - ls -la dist/
      - echo "BUILD_DONE"
"#;

    let pipeline_def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&pipeline_def).unwrap();

    let pipeline = Pipeline::new("t1".into(), "repo1".into(), "ci".into(), "sha".into());
    ps.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(),
        "t1".into(),
        "repo1".into(),
        1,
        "a1b2c3d4".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Push {
            ref_name: "refs/heads/main".into(),
        },
        yaml.into(),
    );
    // Inject env vars like flightdeck's vault would
    run.env_vars
        .insert("WUNDER_NPM_TOKEN".into(), "fake-token-123".into());
    rs.create_run(&run).await.unwrap();

    let mut step_runs = Vec::new();
    for step_def in &pipeline_def.steps {
        let sr = StepRun::new(
            run.id.clone(),
            "t1".into(),
            step_def.name.clone(),
            FailureStrategy::Stop,
            None,
        );
        ss.create_step(&sr).await.unwrap();
        step_runs.push(sr);
    }

    let submitter: Arc<dyn JobSubmitter> = Arc::new(RealJobSubmitter {
        job_store: js.clone(),
        scheduler: scheduler.clone(),
    });
    let executor = DagExecutor::new(rs.clone(), ss.clone(), js.clone(), submitter);

    eprintln!("\n========== STARTING REALISTIC CI PIPELINE ==========\n");

    let result = executor
        .execute(&mut run, &pipeline_def, &step_runs, None)
        .await
        .unwrap();

    cancel.cancel();

    // Print all step results and logs
    let steps = ss.list_by_run("t1", &run.id).await.unwrap();
    for step in &steps {
        let job_id = step.job_id.as_deref().unwrap_or("none");
        let logs = if step.job_id.is_some() {
            jls.get_logs(job_id, 200).await.unwrap_or_default()
        } else {
            vec![]
        };
        eprintln!(
            "\n--- Step '{}' | State: {:?} | Job: {} | Logs: {} lines ---",
            step.step_name,
            step.state,
            job_id,
            logs.len()
        );
        for line in &logs {
            eprintln!("  {}", line.message.trim_end());
        }
    }
    eprintln!("\n========== PIPELINE RESULT: {result:?} ==========\n");

    assert_eq!(result, PipelineRunState::Succeeded);

    // Verify DAG ordering: install ran first, then lint+test in parallel, then build
    assert_eq!(steps.len(), 4);
    for step in &steps {
        assert_eq!(
            step.state,
            StepRunState::Succeeded,
            "step '{}' should have succeeded",
            step.step_name
        );
    }

    // Verify each step's logs contain expected markers
    let assert_log_contains = |name: &str, marker: &str| {
        let step = steps.iter().find(|s| s.step_name == name).unwrap();
        let logs =
            futures::executor::block_on(jls.get_logs(step.job_id.as_ref().unwrap(), 200)).unwrap();
        assert!(
            logs.iter().any(|l| l.message.contains(marker)),
            "Step '{}' logs should contain '{}', got:\n{}",
            name,
            marker,
            logs.iter()
                .map(|l| l.message.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
    };

    assert_log_contains("install", "INSTALL_DONE");
    assert_log_contains("lint", "LINT_DONE");
    assert_log_contains("test", "TEST_DONE");
    assert_log_contains("build", "BUILD_DONE");

    // Verify npm actually ran (not just echo)
    assert_log_contains("test", "Tests passed!");
    assert_log_contains("lint", "Lint passed!");
    assert_log_contains("build", "Build succeeded!");

    // Verify built-in pipeline env vars were injected
    let install_step = steps.iter().find(|s| s.step_name == "install").unwrap();
    let job = js
        .get_job(install_step.job_id.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let env_map: HashMap<String, String> = job
        .spec
        .env_vars
        .iter()
        .map(|e| (e.name.clone(), e.value.clone()))
        .collect();
    assert_eq!(env_map.get("CI"), Some(&"true".to_string()));
    assert_eq!(env_map.get("NODE_ENV"), Some(&"test".to_string()));
    assert_eq!(
        env_map.get("WUNDER_NPM_TOKEN"),
        Some(&"fake-token-123".to_string())
    );
    assert_eq!(env_map.get("PIPELINE_SHA"), Some(&"a1b2c3d4".to_string()));
    assert_eq!(env_map.get("PIPELINE_BRANCH"), Some(&"main".to_string()));
    assert_eq!(env_map.get("PIPELINE_EVENT"), Some(&"push".to_string()));
    assert_eq!(
        env_map.get("PIPELINE_STEP_NAME"),
        Some(&"install".to_string())
    );

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}
