// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! E2E tests for private-repo pipeline checkout auth and failure surfacing.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::json;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use muli_core::git::{GitPermission, GitToken, Repository};
use muli_core::job::model::Job;
use muli_core::pipeline::{
    FailureStrategy, Pipeline, PipelineRun, PipelineRunState, PipelineTrigger, StepRun,
    StepRunState,
};
use muli_core::service::RepositoryService;
use muli_core::traits::{
    ArtifactStore, CacheStore, CollaboratorStore, GitTokenStore, JobLogStore, JobStore,
    PipelineRunStore, PipelineSecretStore, PipelineStore, PullRequestStore, RepositoryStore,
    StepRunStore,
};
use muli_engine::docker::logs::LogCollector;
use muli_git::auth::{GitAuth, hash_token, token_prefix};
use muli_git::storage::FilesystemStorage;
use muli_git::tenant::TenantConfig;
use muli_git::{GitRouterConfig, git_router};
use muli_pipeline::artifact::storage::ArtifactStorage;
use muli_pipeline::dag::executor::{DagExecutor, JobSubmitter};
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::validation::validate_pipeline;
use muli_queue::{ConcurrencyLimiter, PriorityQueue, Scheduler};
use muli_server::PipelineTriggerImpl;
use muli_server::grpc::PipelineServiceImpl;
use muli_store::memory::MemoryCollaboratorStore;
use muli_store::sqlite::{
    SqliteArtifactStore, SqliteCacheStore, SqliteGitTokenStore, SqliteJobLogStore, SqliteJobStore,
    SqlitePipelineRunStore, SqlitePipelineSecretStore, SqlitePipelineStore, SqlitePrCommentStore,
    SqlitePullRequestStore, SqliteRepositoryStore, SqliteSshKeyStore, SqliteStepRunStore,
    SqliteStoreFactory, SqliteWebhookStore,
};

use muli_proto::GetPipelineRunRequest;

use common::{dummy_executor, run_job, with_tenant};

const TENANT: &str = "tenant-a";
const NAMESPACE: &str = "acme";
const OWNER_TOKEN: &str = "pipeline-checkout-owner-token";

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

struct CheckoutE2eEnv {
    addr: SocketAddr,
    cancel: CancellationToken,
    repo_store: Arc<dyn RepositoryStore>,
    token_store: Arc<dyn GitTokenStore>,
    pipeline_store: Arc<dyn PipelineStore>,
    run_store: Arc<dyn PipelineRunStore>,
    step_store: Arc<dyn StepRunStore>,
    job_store: Arc<dyn JobStore>,
    job_log_store: Arc<dyn JobLogStore>,
    artifact_store: Arc<dyn ArtifactStore>,
    cache_store: Arc<dyn CacheStore>,
    secret_store: Arc<dyn PipelineSecretStore>,
    job_submitter: Arc<dyn JobSubmitter>,
    artifact_storage: Arc<ArtifactStorage>,
    git_root: TempDir,
    _store_dir: TempDir,
    _artifact_dir: TempDir,
}

impl Drop for CheckoutE2eEnv {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl CheckoutE2eEnv {
    async fn start() -> Option<Self> {
        let git_root = TempDir::new().expect("git tempdir");
        let storage = Arc::new(
            FilesystemStorage::new(git_root.path().to_str().unwrap())
                .await
                .expect("storage"),
        );

        let store_dir = TempDir::new().expect("store tempdir");
        let factory = SqliteStoreFactory::new(store_dir.path())
            .await
            .expect("sqlite factory");

        let repo_store: Arc<dyn RepositoryStore> =
            Arc::new(SqliteRepositoryStore::new(factory.clone()));
        let token_store: Arc<dyn GitTokenStore> =
            Arc::new(SqliteGitTokenStore::new(factory.clone()));
        let webhook_store: Arc<dyn muli_core::traits::WebhookStore> =
            Arc::new(SqliteWebhookStore::new(factory.clone()));
        let ssh_key_store = Arc::new(SqliteSshKeyStore::new(factory.clone()));
        let pr_store: Arc<dyn PullRequestStore> =
            Arc::new(SqlitePullRequestStore::new(factory.clone()));
        let pr_comment_store = Arc::new(SqlitePrCommentStore::new(factory.clone()));
        let collaborator_store: Arc<dyn CollaboratorStore> =
            Arc::new(MemoryCollaboratorStore::new());

        let pipeline_store: Arc<dyn PipelineStore> =
            Arc::new(SqlitePipelineStore::new(factory.clone()));
        let run_store: Arc<dyn PipelineRunStore> =
            Arc::new(SqlitePipelineRunStore::new(factory.clone()));
        let step_store: Arc<dyn StepRunStore> = Arc::new(SqliteStepRunStore::new(factory.clone()));
        let job_store: Arc<dyn JobStore> = Arc::new(SqliteJobStore::new(factory.clone()));
        let job_log_store: Arc<dyn JobLogStore> = Arc::new(SqliteJobLogStore::new(factory.clone()));
        let artifact_store: Arc<dyn ArtifactStore> =
            Arc::new(SqliteArtifactStore::new(factory.clone()));
        let cache_store: Arc<dyn CacheStore> = Arc::new(SqliteCacheStore::new(factory.clone()));
        let secret_store: Arc<dyn PipelineSecretStore> =
            Arc::new(SqlitePipelineSecretStore::new(factory.clone()));

        let mut owner_token = GitToken::new(
            TENANT.into(),
            hash_token(OWNER_TOKEN),
            token_prefix(OWNER_TOKEN),
            vec![
                GitPermission::Pull,
                GitPermission::Push,
                GitPermission::Admin,
            ],
            "checkout e2e owner token".into(),
            None,
        );
        owner_token.user_id = Some("user-1".to_string());
        token_store
            .create_token(&owner_token)
            .await
            .expect("seed owner token");

        let notify = Arc::new(Notify::new());
        let queue = Arc::new(PriorityQueue::new(notify.clone()));
        let limiter = Arc::new(ConcurrencyLimiter::new(10, 5));
        let scheduler = Arc::new(Scheduler::new(queue, limiter, notify));
        let cancel = CancellationToken::new();

        {
            let sched = scheduler.clone();
            let store = job_store.clone();
            let log_store = job_log_store.clone();
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

        let job_submitter: Arc<dyn JobSubmitter> = Arc::new(RealJobSubmitter {
            job_store: job_store.clone(),
            scheduler: scheduler.clone(),
        });

        let listener = match tokio::net::TcpListener::bind("[::1]:0").await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("SKIP: IPv6 loopback bind failed for checkout e2e: {e}");
                cancel.cancel();
                return None;
            }
        };
        let addr = listener.local_addr().expect("local addr");

        let git_auth = GitAuth::new(token_store.clone())
            .with_repo_store(repo_store.clone())
            .with_collaborator_store(collaborator_store);
        let tenant_config = TenantConfig::new("localhost").with_default_tenant(TENANT);
        let repo_service = Arc::new(RepositoryService::new(repo_store.clone(), storage.clone()));
        let pipeline_trigger = Arc::new(PipelineTriggerImpl::new(
            storage.clone(),
            repo_store.clone(),
            pr_store.clone(),
            pipeline_store.clone(),
            run_store.clone(),
            step_store.clone(),
            job_store.clone(),
            job_submitter.clone(),
            None,
            token_store.clone(),
            webhook_store.clone(),
            true,
            format!("http://{addr}"),
        ));

        let app = git_router(GitRouterConfig {
            storage,
            repo_store: repo_store.clone(),
            token_store: token_store.clone(),
            webhook_store: webhook_store.clone(),
            ssh_key_store,
            pr_store: pr_store.clone(),
            pr_comment_store,
            auth: Some(git_auth),
            tenant_config,
            cache_store: None,
            allow_localhost_webhooks: true,
            lfs_storage: None,
            pipeline_trigger: Some(pipeline_trigger),
            repo_service,
            quota_store: None,
        });

        let cancel_srv = cancel.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(cancel_srv.cancelled_owned())
                .await
                .ok();
        });

        let artifact_dir = TempDir::new().expect("artifact tempdir");
        let artifact_storage = Arc::new(ArtifactStorage::new(artifact_dir.path()));

        Some(Self {
            addr,
            cancel,
            repo_store,
            token_store,
            pipeline_store,
            run_store,
            step_store,
            job_store,
            job_log_store,
            artifact_store,
            cache_store,
            secret_store,
            job_submitter,
            artifact_storage,
            git_root,
            _store_dir: store_dir,
            _artifact_dir: artifact_dir,
        })
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn git_url(&self, repo_name: &str, token: &str) -> String {
        format!(
            "http://x-token:{token}@{}/{NAMESPACE}/{repo_name}.git",
            self.addr
        )
    }

    async fn api_post(&self, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        let resp = reqwest::Client::new()
            .post(format!("{}{}", self.base_url(), path))
            .header("Authorization", format!("Bearer {OWNER_TOKEN}"))
            .json(&body)
            .send()
            .await
            .expect("HTTP POST");
        let status = resp.status().as_u16();
        let json = resp.json().await.unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    async fn create_private_repo(&self, repo_name: &str) -> Repository {
        let (status, _) = self
            .api_post(
                "/api/v1/repos",
                json!({
                    "namespace": NAMESPACE,
                    "name": repo_name,
                    "description": "",
                    "is_private": true
                }),
            )
            .await;
        assert_eq!(status, 201, "create repo {repo_name}");

        let mut repo = self
            .repo_store
            .get_repository_by_name(TENANT, NAMESPACE, repo_name)
            .await
            .expect("repo lookup")
            .expect("repo exists");
        repo.owner_id = "user-1".to_string();
        self.repo_store
            .update_repository(&repo)
            .await
            .expect("set owner");
        repo
    }
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::fs::write(path, contents).expect("write file");
}

async fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "advice.detachedHead")
        .env("GIT_CONFIG_VALUE_0", "false")
        .status()
        .await
        .unwrap_or_else(|e| panic!("failed to run git {args:?}: {e}"));
    assert!(
        status.success(),
        "git {} failed with {status}",
        args.join(" ")
    );
}

async fn wait_for_terminal_run(
    env: &CheckoutE2eEnv,
    repo_id: &str,
    timeout: Duration,
) -> PipelineRun {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for terminal pipeline run"
        );

        let runs = env
            .run_store
            .list_by_repo(TENANT, repo_id, None, 10, 0)
            .await
            .expect("list runs");
        if let Some(run) = runs.first()
            && run.state.is_terminal()
        {
            return run.clone();
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_terminal_run_count(
    env: &CheckoutE2eEnv,
    repo_id: &str,
    expected_count: usize,
    timeout: Duration,
) -> Vec<PipelineRun> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for {expected_count} terminal pipeline runs"
        );

        let runs = env
            .run_store
            .list_by_repo(TENANT, repo_id, None, 20, 0)
            .await
            .expect("list runs");
        if runs.len() >= expected_count
            && runs
                .iter()
                .take(expected_count)
                .all(|run| run.state.is_terminal())
        {
            return runs;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_push_trigger_private_repo_checkout_succeeds_with_repo_scoped_ci_token() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    let git_available = Command::new("git")
        .arg("--version")
        .output()
        .await
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !git_available {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let Some(env) = CheckoutE2eEnv::start().await else {
        return;
    };
    let repo = env.create_private_repo("pipeline-checkout-success").await;

    let work_dir = TempDir::new().unwrap();
    let url = env.git_url(&repo.name, OWNER_TOKEN);
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;

    write_file(
        work_dir.path().join("README.md").as_path(),
        "hello checkout\n",
    );
    write_file(
        work_dir.path().join(".maravilla/pipeline.yml").as_path(),
        r#"
name: checkout-e2e
on:
  push:
    branches: [main]
jobs:
  verify:
    image: alpine:latest
    commands:
      - test -f README.md
      - grep -q "hello checkout" README.md
      - echo "CHECKOUT_OK"
"#,
    );

    git(work_dir.path(), &["add", "."]).await;
    git(work_dir.path(), &["commit", "-m", "add pipeline"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    let run = wait_for_terminal_run(&env, &repo.id, Duration::from_secs(120)).await;
    assert_eq!(
        run.state,
        PipelineRunState::Succeeded,
        "expected successful pipeline run after push-triggered checkout"
    );

    let steps = env.step_store.list_by_run(TENANT, &run.id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].state, StepRunState::Succeeded);
    assert!(steps[0].error_message.is_none());

    let job_id = steps[0].job_id.as_ref().expect("job id");
    let job = env
        .job_store
        .get_job(job_id)
        .await
        .unwrap()
        .expect("job record");
    assert!(
        matches!(
            job.result.as_ref().and_then(|result| result.exit_code),
            Some(0)
        ),
        "expected checkout job to succeed after push-triggered clone"
    );

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pipeline_checkout_auth_failure_sets_step_error_and_api_returns_it() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let Some(env) = CheckoutE2eEnv::start().await else {
        return;
    };
    let repo = env.create_private_repo("pipeline-checkout-failure").await;

    let bootstrap_dir = TempDir::new().unwrap();
    let bootstrap_url = env.git_url(&repo.name, OWNER_TOKEN);
    git(
        bootstrap_dir.path(),
        &["clone", "--no-local", &bootstrap_url, "."],
    )
    .await;
    git(
        bootstrap_dir.path(),
        &["config", "user.email", "ci@muli.test"],
    )
    .await;
    git(bootstrap_dir.path(), &["config", "user.name", "Muli CI"]).await;
    write_file(
        bootstrap_dir.path().join("README.md").as_path(),
        "secure content\n",
    );
    git(bootstrap_dir.path(), &["add", "README.md"]).await;
    git(bootstrap_dir.path(), &["commit", "-m", "bootstrap"]).await;
    git(
        bootstrap_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    let yaml = r#"
name: checkout-failure
jobs:
  verify:
    image: alpine:latest
    commands:
      - echo "should never reach container"
"#;
    let pipeline_def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&pipeline_def).unwrap();

    let pipeline = Pipeline::new(
        TENANT.into(),
        repo.id.clone(),
        "checkout-failure".into(),
        "sha".into(),
    );
    env.pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let mut run = PipelineRun::new(
        pipeline.id.clone(),
        TENANT.into(),
        repo.id.clone(),
        1,
        "deadbeef".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Manual {
            triggered_by: "test".into(),
        },
        yaml.into(),
    );
    env.run_store.create_run(&run).await.unwrap();

    let step = StepRun::new(
        run.id.clone(),
        TENANT.into(),
        "verify".into(),
        FailureStrategy::Stop,
        None,
    );
    env.step_store.create_step(&step).await.unwrap();

    let bad_clone_url = format!(
        "http://x-pipeline-token:bad-token@{}/{NAMESPACE}/{}.git",
        env.addr, repo.name
    );
    let executor = DagExecutor::new(
        env.run_store.clone(),
        env.step_store.clone(),
        env.job_store.clone(),
        env.job_submitter.clone(),
    );
    let result = executor
        .execute(
            &mut run,
            &pipeline_def,
            std::slice::from_ref(&step),
            Some(&bad_clone_url),
        )
        .await
        .unwrap();
    assert_eq!(result, PipelineRunState::Failed);

    let stored_step = env
        .step_store
        .get_step(TENANT, &step.id)
        .await
        .unwrap()
        .expect("stored step");
    assert_eq!(stored_step.state, StepRunState::Failed);
    let error_message = stored_step
        .error_message
        .clone()
        .expect("step error message");
    assert!(
        error_message.contains("git clone failed"),
        "expected checkout failure in step error, got: {error_message}"
    );

    let service = PipelineServiceImpl {
        pipeline_store: env.pipeline_store.clone(),
        run_store: env.run_store.clone(),
        step_store: env.step_store.clone(),
        artifact_store: env.artifact_store.clone(),
        artifact_storage: env.artifact_storage.clone(),
        cache_store: env.cache_store.clone(),
        secret_store: env.secret_store.clone(),
        job_store: env.job_store.clone(),
        job_log_store: env.job_log_store.clone(),
        job_submitter: env.job_submitter.clone(),
        repo_store: env.repo_store.clone(),
        git_root: env.git_root.path().to_path_buf(),
        token_store: env.token_store.clone(),
        git_base_url: format!("http://{}", env.addr),
    };
    let response = service
        .get_pipeline_run_impl(with_tenant(
            GetPipelineRunRequest {
                tenant_id: TENANT.into(),
                repo_id: repo.id.clone(),
                run_number: 0,
                run_id: run.id.clone(),
            },
            TENANT,
        ))
        .await
        .expect("get_pipeline_run");
    let api_run = response.into_inner().run.expect("run payload");
    let api_step = api_run.steps.first().expect("step in API response");
    assert!(
        api_step.error_message.contains("git clone failed"),
        "expected API step error to include checkout failure, got: {}",
        api_step.error_message
    );

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_push_trigger_reads_pipeline_directory_and_reuses_pipeline_ids_by_name() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    let git_available = Command::new("git")
        .arg("--version")
        .output()
        .await
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !git_available {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let Some(env) = CheckoutE2eEnv::start().await else {
        return;
    };
    let repo = env.create_private_repo("pipeline-directory-trigger").await;

    let work_dir = TempDir::new().unwrap();
    let url = env.git_url(&repo.name, OWNER_TOKEN);
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;

    write_file(work_dir.path().join("README.md").as_path(), "hello multi\n");
    write_file(
        work_dir
            .path()
            .join(".maravilla/pipeline/build.yml")
            .as_path(),
        r#"
name: build-ci
on:
  push:
    branches: [main]
jobs:
  build:
    image: alpine:latest
    commands:
      - test -f README.md
      - echo "BUILD_OK"
"#,
    );
    write_file(
        work_dir
            .path()
            .join(".maravilla/pipeline/lint.yaml")
            .as_path(),
        r#"
name: lint-ci
on:
  push:
    branches: [main]
jobs:
  lint:
    image: alpine:latest
    commands:
      - grep -q "hello" README.md
      - echo "LINT_OK"
"#,
    );

    git(work_dir.path(), &["add", "."]).await;
    git(work_dir.path(), &["commit", "-m", "add pipeline directory"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    let first_runs = wait_for_terminal_run_count(&env, &repo.id, 2, Duration::from_secs(120)).await;
    assert_eq!(first_runs.len(), 2);
    assert!(
        first_runs
            .iter()
            .all(|run| run.state == PipelineRunState::Succeeded)
    );

    let pipelines = env
        .pipeline_store
        .get_by_repo(TENANT, &repo.id)
        .await
        .unwrap();
    assert_eq!(pipelines.len(), 2);
    let first_pipeline_ids: std::collections::HashMap<String, String> = pipelines
        .iter()
        .map(|pipeline| (pipeline.name.clone(), pipeline.id.clone()))
        .collect();
    assert!(first_pipeline_ids.contains_key("build-ci"));
    assert!(first_pipeline_ids.contains_key("lint-ci"));

    write_file(
        work_dir.path().join("README.md").as_path(),
        "hello multi again\n",
    );
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "second push"]).await;
    git(work_dir.path(), &["push", "origin", "main"]).await;

    let second_runs =
        wait_for_terminal_run_count(&env, &repo.id, 4, Duration::from_secs(120)).await;
    assert_eq!(second_runs.len(), 4);
    assert!(
        second_runs
            .iter()
            .take(4)
            .all(|run| run.state == PipelineRunState::Succeeded)
    );

    let pipelines_after = env
        .pipeline_store
        .get_by_repo(TENANT, &repo.id)
        .await
        .unwrap();
    assert_eq!(pipelines_after.len(), 2);
    for pipeline in pipelines_after {
        assert_eq!(
            first_pipeline_ids.get(&pipeline.name),
            Some(&pipeline.id),
            "pipeline ID should be stable for {}",
            pipeline.name
        );
    }

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}
