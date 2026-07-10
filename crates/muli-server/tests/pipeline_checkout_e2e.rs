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
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use muli_core::git::{GitPermission, GitToken, Repository};
use muli_core::job::model::{Job, StoredLogLine};
use muli_core::pipeline::{
    FailureStrategy, Pipeline, PipelineRun, PipelineRunState, PipelineTrigger, StepRun,
    StepRunState,
};
use muli_core::service::RepositoryService;
use muli_core::traits::{
    ArtifactStore, CacheStore, CollaboratorStore, GitTokenStore, JobLogStore, JobStore,
    OrgSecretStore, OrgStore, PipelineRunStore, PipelineSecretStore, PipelineStore,
    PullRequestStore, ReleaseStore, RepositoryStore, StepRunStore,
};
use muli_engine::docker::logs::{
    LogCollector, LogLine as EngineLogLine, LogStream as EngineLogStream,
};
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
use muli_server::release_storage::ReleaseAssetStorage;
use muli_store::memory::MemoryCollaboratorStore;
use muli_store::sqlite::{
    SqliteArtifactStore, SqliteCacheStore, SqliteGitTokenStore, SqliteJobLogStore, SqliteJobStore,
    SqliteOrgSecretStore, SqliteOrgStore, SqlitePipelineRunStore, SqlitePipelineSecretStore,
    SqlitePipelineStore, SqlitePrCommentStore, SqlitePullRequestStore, SqliteRegistryTokenStore,
    SqliteReleaseStore, SqliteRepositoryStore, SqliteSshKeyStore, SqliteStepRunStore,
    SqliteStoreFactory, SqliteWebhookStore,
};

use muli_proto::{GetPipelineRunRequest, ListPipelineRunsRequest, StreamStepLogsRequest};

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
    log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    artifact_store: Arc<dyn ArtifactStore>,
    cache_store: Arc<dyn CacheStore>,
    secret_store: Arc<dyn PipelineSecretStore>,
    org_secret_store: Arc<dyn OrgSecretStore>,
    _org_store: Arc<dyn OrgStore>,
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
        Self::start_with_bind_addr("[::1]:0").await
    }

    async fn start_with_bind_addr(bind_addr: &str) -> Option<Self> {
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
        let org_secret_store: Arc<dyn OrgSecretStore> =
            Arc::new(SqliteOrgSecretStore::new(factory.clone()));
        let org_store: Arc<dyn OrgStore> = Arc::new(SqliteOrgStore::new(factory.clone()));

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
        let log_collectors: Arc<DashMap<String, Arc<LogCollector>>> = Arc::new(DashMap::new());

        {
            let sched = scheduler.clone();
            let store = job_store.clone();
            let log_store = job_log_store.clone();
            let cancel_clone = cancel.clone();
            let executor = dummy_executor().await;
            let live_collectors = log_collectors.clone();

            tokio::spawn(async move {
                sched
                    .run(cancel_clone, move |job_id, _tenant_id| {
                        let store = store.clone();
                        let executor = executor.clone();
                        let log_collectors = live_collectors.clone();
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

        let listener = match tokio::net::TcpListener::bind(bind_addr).await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("SKIP: checkout e2e bind failed on {bind_addr}: {e}");
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
        // Release + artifact byte stores for the trigger's declarative `release:`
        // path. Unexercised here (no pipeline in this suite declares a release),
        // but required by the constructor.
        let release_store: Arc<dyn ReleaseStore> =
            Arc::new(SqliteReleaseStore::new(factory.clone()));
        let release_asset_dir = TempDir::new().expect("release asset tempdir");
        let release_asset_storage = Arc::new(ReleaseAssetStorage::new(release_asset_dir.path()));
        let trigger_artifact_dir = TempDir::new().expect("trigger artifact tempdir");
        let trigger_artifact_storage = Arc::new(ArtifactStorage::new(trigger_artifact_dir.path()));
        let registry_token_store = Arc::new(SqliteRegistryTokenStore::new(factory.clone()));
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
            secret_store.clone(),
            org_secret_store.clone(),
            org_store.clone(),
            None,
            artifact_store.clone(),
            release_store,
            release_asset_storage,
            trigger_artifact_storage,
            registry_token_store,
            "localhost".to_string(),
            false,
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
            log_collectors,
            artifact_store,
            cache_store,
            secret_store,
            org_secret_store,
            _org_store: org_store,
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

fn pipeline_service(env: &CheckoutE2eEnv) -> PipelineServiceImpl {
    PipelineServiceImpl {
        pipeline_store: env.pipeline_store.clone(),
        run_store: env.run_store.clone(),
        step_store: env.step_store.clone(),
        artifact_store: env.artifact_store.clone(),
        artifact_storage: env.artifact_storage.clone(),
        cache_store: env.cache_store.clone(),
        secret_store: env.secret_store.clone(),
        job_store: env.job_store.clone(),
        job_log_store: env.job_log_store.clone(),
        log_collectors: env.log_collectors.clone(),
        max_log_lines: 10_000,
        job_submitter: env.job_submitter.clone(),
        repo_store: env.repo_store.clone(),
        git_root: env.git_root.path().to_path_buf(),
        token_store: env.token_store.clone(),
        git_base_url: format!("http://{}", env.addr),
        org_secret_store: env.org_secret_store.clone(),
        encryption_key: None,
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
async fn test_push_trigger_jobs_checkout_keeps_localhost_git_base_url_for_host_clone() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let Some(env) = CheckoutE2eEnv::start_with_bind_addr("127.0.0.1:0").await else {
        return;
    };
    let repo = env
        .create_private_repo("pipeline-localhost-host-checkout")
        .await;

    let work_dir = TempDir::new().unwrap();
    let url = env.git_url(&repo.name, OWNER_TOKEN);
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    write_file(
        work_dir.path().join("README.md").as_path(),
        "hello from localhost git base url\n",
    );
    write_file(
        work_dir.path().join(".maravilla/pipeline.yml").as_path(),
        r#"
name: localhost-checkout
on:
  push:
    branches: [main]
jobs:
  verify:
    image: alpine:latest
    commands:
      - cat README.md
      - echo "HOST_CHECKOUT_OK"
"#,
    );
    git(work_dir.path(), &["add", "."]).await;
    git(
        work_dir.path(),
        &["commit", "-m", "add localhost host-checkout pipeline"],
    )
    .await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    let run = wait_for_terminal_run(&env, &repo.id, Duration::from_secs(180)).await;
    assert_eq!(run.state, PipelineRunState::Succeeded);

    let steps = env.step_store.list_by_run(TENANT, &run.id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].state, StepRunState::Succeeded);

    let job_id = steps[0].job_id.as_ref().expect("job id");
    let logs = env.job_log_store.get_logs(job_id, 200).await.unwrap();
    let log_text = logs
        .iter()
        .map(|line| line.message.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        log_text.contains("From http://127.0.0.1:")
            || log_text.contains("Cloning into")
            || log_text.contains("HEAD is now at"),
        "expected host-side checkout logs for 127.0.0.1 git base url, got: {log_text}"
    );
    assert!(
        !log_text.contains("host.docker.internal") && !log_text.contains("Could not resolve host"),
        "unexpected host.docker.internal rewrite during host checkout: {log_text}"
    );

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_push_trigger_checkout_preserves_real_exit_code_and_logs_for_fast_failing_node_job() {
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
    muli_test::docker_helpers::ensure_test_image(&docker, "node:22-alpine").await;

    let Some(env) = CheckoutE2eEnv::start().await else {
        return;
    };
    let repo = env
        .create_private_repo("pipeline-checkout-node-failure")
        .await;

    let work_dir = TempDir::new().unwrap();
    let url = env.git_url(&repo.name, OWNER_TOKEN);
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;

    write_file(
        work_dir.path().join("package.json").as_path(),
        r#"{
  "name": "checkout-node-failure",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx -y -p typescript@5.9.2 tsc --pretty false --noEmit"
  }
}
"#,
    );
    write_file(
        work_dir.path().join("package-lock.json").as_path(),
        r#"{
  "name": "checkout-node-failure",
  "version": "1.0.0",
  "lockfileVersion": 3,
  "requires": true,
  "packages": {
    "": {
      "name": "checkout-node-failure",
      "version": "1.0.0"
    }
  }
}
"#,
    );
    write_file(
        work_dir.path().join("tsconfig.json").as_path(),
        r#"{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "strict": true,
    "noEmit": true
  },
  "include": ["src/**/*.ts"]
}
"#,
    );
    write_file(
        work_dir.path().join("src/index.ts").as_path(),
        "const answer: string = 42;\nconsole.log(answer);\n",
    );
    write_file(
        work_dir.path().join(".maravilla/pipeline.yml").as_path(),
        r#"
name: fullstack-ci
on:
  push:
    branches: [main]
jobs:
  install:
    image: node:22-alpine
    commands:
      - pwd
      - ls -lh
      - npm ci
    steps:
      - name: Lint and Type Check
        commands:
          - npx eslint src/ --max-warnings 0 || true
      - name: Format
        commands:
          - npx prettier --check src/ || true
      - name: Build Node Project
        commands:
          - npm run build
"#,
    );

    git(work_dir.path(), &["add", "."]).await;
    git(
        work_dir.path(),
        &["commit", "-m", "add node pipeline that fails in build"],
    )
    .await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    let run = wait_for_terminal_run(&env, &repo.id, Duration::from_secs(180)).await;
    assert_eq!(run.state, PipelineRunState::Failed);

    let steps = env.step_store.list_by_run(TENANT, &run.id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].state, StepRunState::Failed);
    let step_error = steps[0]
        .error_message
        .clone()
        .expect("step error message should be present");
    assert!(
        step_error.contains("exit code 2"),
        "expected real build exit code in step error, got: {step_error}"
    );
    assert!(
        !step_error.contains("exit code 243"),
        "unexpected bogus wait error exit code in step error: {step_error}"
    );

    let job_id = steps[0].job_id.as_ref().expect("job id");
    let job = env
        .job_store
        .get_job(job_id)
        .await
        .unwrap()
        .expect("job record");
    let result = job.result.as_ref().expect("job result");
    assert_eq!(
        result.exit_code,
        Some(2),
        "expected real container exit code"
    );
    assert!(
        !result.message.contains("exit code 243"),
        "unexpected bogus wait error exit code in job result: {}",
        result.message
    );

    let logs = env.job_log_store.get_logs(job_id, 500).await.unwrap();
    let log_text = logs
        .iter()
        .map(|line| line.message.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        (log_text.contains("src") || log_text.contains("tsconfig.json")),
        "expected visible prep output in logs, got: {log_text}"
    );
    assert!(
        log_text.contains("found 0 vulnerabilities") || log_text.contains("up to date, audited"),
        "expected npm ci to complete before the later build failure, got: {log_text}"
    );
    assert!(
        !log_text.contains("EACCES") && !log_text.contains("permission denied"),
        "unexpected workspace permission failure in logs: {log_text}"
    );
    assert!(
        log_text.contains("src/index.ts") || log_text.contains("TS2322"),
        "expected TypeScript build failure output in logs, got: {log_text}"
    );
    assert!(
        !log_text.contains("__MULI_SUBSTEP_"),
        "hidden substep markers leaked into persisted logs: {log_text}"
    );

    let substep_events = logs
        .iter()
        .filter(|line| line.event_type.as_deref() == Some("substep_finished"))
        .map(|line| (line.substep_name.clone(), line.exit_code))
        .collect::<Vec<_>>();
    assert_eq!(
        substep_events,
        vec![
            (Some("Preparation".into()), Some(0)),
            (Some("Lint and Type Check".into()), Some(0)),
            (Some("Format".into()), Some(0)),
            (Some("Build Node Project".into()), Some(2)),
        ],
        "expected structured substep lifecycle events in persisted logs"
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
            None,
            None,
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

    let service = pipeline_service(&env);
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

    let service = pipeline_service(&env);
    let listed_runs = service
        .list_pipeline_runs_impl(with_tenant(
            ListPipelineRunsRequest {
                tenant_id: TENANT.into(),
                repo_id: repo.id.clone(),
                state_filter: 0,
                limit: 10,
                offset: 0,
            },
            TENANT,
        ))
        .await
        .expect("list_pipeline_runs")
        .into_inner()
        .runs;
    assert!(
        listed_runs
            .iter()
            .any(|run| run.pipeline_name == "build-ci" || run.pipeline_name == "lint-ci"),
        "expected list_pipeline_runs to include pipeline_name"
    );
    let fetched_run = service
        .get_pipeline_run_impl(with_tenant(
            GetPipelineRunRequest {
                tenant_id: TENANT.into(),
                repo_id: repo.id.clone(),
                run_number: 0,
                run_id: first_runs[0].id.clone(),
            },
            TENANT,
        ))
        .await
        .expect("get_pipeline_run")
        .into_inner()
        .run
        .expect("run payload");
    assert!(
        fetched_run.pipeline_name == "build-ci" || fetched_run.pipeline_name == "lint-ci",
        "expected get_pipeline_run to include pipeline_name, got: {}",
        fetched_run.pipeline_name
    );

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_push_trigger_filters_jobs_by_changed_paths_in_monorepo() {
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
    let repo = env
        .create_private_repo("pipeline-monorepo-job-filter")
        .await;

    let work_dir = TempDir::new().unwrap();
    let url = env.git_url(&repo.name, OWNER_TOKEN);
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;

    write_file(
        work_dir.path().join("frontend/app.txt").as_path(),
        "frontend-v1\n",
    );
    write_file(
        work_dir.path().join("backend/app.txt").as_path(),
        "backend-v1\n",
    );
    write_file(
        work_dir.path().join(".maravilla/pipeline.yml").as_path(),
        r#"
name: monorepo-ci
on:
  push:
    branches: [main]
jobs:
  frontend:
    image: alpine:latest
    paths: [frontend/**]
    commands:
      - test -f frontend/app.txt
      - grep -q "frontend" frontend/app.txt
  backend:
    image: alpine:latest
    paths: [backend/**]
    commands:
      - test -f backend/app.txt
      - grep -q "backend" backend/app.txt
"#,
    );

    git(work_dir.path(), &["add", "."]).await;
    git(
        work_dir.path(),
        &["commit", "-m", "bootstrap monorepo pipeline"],
    )
    .await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    let bootstrap_run = wait_for_terminal_run(&env, &repo.id, Duration::from_secs(120)).await;
    assert_eq!(bootstrap_run.state, PipelineRunState::Succeeded);

    write_file(
        work_dir.path().join("frontend/app.txt").as_path(),
        "frontend-v2\n",
    );
    git(work_dir.path(), &["add", "frontend/app.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "change frontend only"]).await;
    git(work_dir.path(), &["push", "origin", "main"]).await;

    let filtered_run = wait_for_terminal_run(&env, &repo.id, Duration::from_secs(120)).await;
    assert_ne!(
        filtered_run.id, bootstrap_run.id,
        "expected a new pipeline run"
    );
    assert_eq!(filtered_run.state, PipelineRunState::Succeeded);

    let steps = env
        .step_store
        .list_by_run(TENANT, &filtered_run.id)
        .await
        .unwrap();
    assert_eq!(steps.len(), 2);

    let step_states: std::collections::HashMap<String, StepRunState> = steps
        .iter()
        .map(|step| (step.step_name.clone(), step.state))
        .collect();
    assert_eq!(step_states.get("frontend"), Some(&StepRunState::Succeeded));
    assert_eq!(step_states.get("backend"), Some(&StepRunState::Skipped));

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

#[tokio::test]
async fn test_stream_step_logs_returns_backlog_then_live_lines_without_duplicates() {
    let Some(env) = CheckoutE2eEnv::start().await else {
        return;
    };

    let repo = env.create_private_repo("stream-logs-live").await;
    let pipeline = Pipeline::new(
        TENANT.into(),
        repo.id.clone(),
        "stream-live".into(),
        "sha-live".into(),
    );
    env.pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let run = PipelineRun::new(
        pipeline.id.clone(),
        TENANT.into(),
        repo.id.clone(),
        1,
        "sha-live".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Manual {
            triggered_by: "test".into(),
        },
        "name: stream-live".into(),
    );
    env.run_store.create_run(&run).await.unwrap();

    let mut step = StepRun::new(
        run.id.clone(),
        TENANT.into(),
        "install".into(),
        FailureStrategy::Stop,
        None,
    );
    step.job_id = Some("job-live".into());
    env.step_store.create_step(&step).await.unwrap();

    let collector = Arc::new(LogCollector::new());
    collector
        .push_line(EngineLogLine {
            sequence: 0,
            timestamp: chrono::Utc::now(),
            stream: EngineLogStream::Stdout,
            message: "backlog-1".into(),
            substep_name: None,
            event_type: "line".into(),
            exit_code: None,
        })
        .await;
    collector
        .push_line(EngineLogLine {
            sequence: 1,
            timestamp: chrono::Utc::now(),
            stream: EngineLogStream::Stderr,
            message: "backlog-2".into(),
            substep_name: None,
            event_type: "line".into(),
            exit_code: None,
        })
        .await;
    env.log_collectors
        .insert("job-live".into(), collector.clone());

    let service = pipeline_service(&env);
    let mut stream = service
        .stream_step_logs_impl(with_tenant(
            StreamStepLogsRequest {
                tenant_id: TENANT.into(),
                run_id: run.id.clone(),
                step_name: "install".into(),
            },
            TENANT,
        ))
        .await
        .unwrap()
        .into_inner();

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(first.line, "backlog-1");
    assert_eq!(second.line, "backlog-2");
    assert_eq!(second.stream, "stderr");

    collector
        .push_line(EngineLogLine {
            sequence: 2,
            timestamp: chrono::Utc::now(),
            stream: EngineLogStream::Stdout,
            message: "live-3".into(),
            substep_name: None,
            event_type: "line".into(),
            exit_code: None,
        })
        .await;

    let third = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(third.line, "live-3");

    let no_duplicate = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
    assert!(no_duplicate.is_err(), "unexpected duplicate step log line");
}

#[tokio::test]
async fn test_stream_step_logs_replays_completed_step_logs_and_closes() {
    let Some(env) = CheckoutE2eEnv::start().await else {
        return;
    };

    let repo = env.create_private_repo("stream-logs-complete").await;
    let pipeline = Pipeline::new(
        TENANT.into(),
        repo.id.clone(),
        "stream-complete".into(),
        "sha-complete".into(),
    );
    env.pipeline_store.upsert_pipeline(&pipeline).await.unwrap();

    let run = PipelineRun::new(
        pipeline.id.clone(),
        TENANT.into(),
        repo.id.clone(),
        1,
        "sha-complete".into(),
        "refs/heads/main".into(),
        PipelineTrigger::Manual {
            triggered_by: "test".into(),
        },
        "name: stream-complete".into(),
    );
    env.run_store.create_run(&run).await.unwrap();

    let mut step = StepRun::new(
        run.id.clone(),
        TENANT.into(),
        "build".into(),
        FailureStrategy::Stop,
        None,
    );
    step.job_id = Some("job-complete".into());
    env.step_store.create_step(&step).await.unwrap();

    env.job_log_store
        .append_logs(
            "job-complete",
            vec![
                StoredLogLine {
                    sequence: 0,
                    stream: "stdout".into(),
                    message: "done-1".into(),
                    timestamp: chrono::Utc::now(),
                    substep_name: None,
                    event_type: Some("line".into()),
                    exit_code: None,
                },
                StoredLogLine {
                    sequence: 1,
                    stream: "stderr".into(),
                    message: "done-2".into(),
                    timestamp: chrono::Utc::now(),
                    substep_name: None,
                    event_type: Some("line".into()),
                    exit_code: None,
                },
            ],
        )
        .await
        .unwrap();

    let service = pipeline_service(&env);
    let mut stream = service
        .stream_step_logs_impl(with_tenant(
            StreamStepLogsRequest {
                tenant_id: TENANT.into(),
                run_id: run.id.clone(),
                step_name: "build".into(),
            },
            TENANT,
        ))
        .await
        .unwrap()
        .into_inner();

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(first.line, "done-1");
    assert_eq!(second.line, "done-2");
    assert_eq!(second.stream, "stderr");

    let end = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap();
    assert!(end.is_none(), "completed step stream should close");
}
