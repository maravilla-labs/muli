// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline trigger implementation that bridges git events to pipeline runs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use chrono::Utc;
use muli_core::git::{GitPermission, GitToken};
use muli_core::pipeline::{FailureStrategy, Pipeline, PipelineRun, PipelineTrigger, StepRun};
use muli_core::token_hash;
use muli_core::traits::{
    GitTokenStore, JobStore, PipelineRunStore, PipelineStore, PullRequestStore, RepositoryStore,
    StepRunStore, TenantLimitsStore,
};
use muli_git::api::PipelineTriggerHook;
use muli_git::storage::FilesystemStorage;
use std::path::Path;

use muli_core::error::MuliError;
use muli_core::git::WebhookEvent;
use muli_git::hooks::{deliver_webhooks, webhook_http_client};
use muli_pipeline::dag::executor::{DagExecutor, JobSubmitter};
use muli_pipeline::trigger::matcher::{PipelineEvent, matches_trigger};
use muli_pipeline::trigger::reader::read_pipeline_files;
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::validation::validate_pipeline;

/// Minimum interval between pipeline triggers for the same repo (rate limit).
const MIN_TRIGGER_INTERVAL_SECS: u64 = 5;

pub struct PipelineTriggerImpl {
    git_storage: Arc<FilesystemStorage>,
    repo_store: Arc<dyn RepositoryStore>,
    pr_store: Arc<dyn PullRequestStore>,
    pipeline_store: Arc<dyn PipelineStore>,
    run_store: Arc<dyn PipelineRunStore>,
    step_store: Arc<dyn StepRunStore>,
    job_store: Arc<dyn JobStore>,
    job_submitter: Arc<dyn JobSubmitter>,
    tenant_limits_store: Option<Arc<dyn TenantLimitsStore>>,
    token_store: Arc<dyn GitTokenStore>,
    webhook_store: Arc<dyn muli_core::traits::WebhookStore>,
    webhook_http_client: Arc<reqwest::Client>,
    allow_localhost_webhooks: bool,
    /// Base URL for the git HTTP service (e.g. "http://localhost:7000").
    git_base_url: String,
    /// Per-repo last-trigger time for rate limiting.
    last_trigger: DashMap<String, Instant>,
}

impl PipelineTriggerImpl {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        git_storage: Arc<FilesystemStorage>,
        repo_store: Arc<dyn RepositoryStore>,
        pr_store: Arc<dyn PullRequestStore>,
        pipeline_store: Arc<dyn PipelineStore>,
        run_store: Arc<dyn PipelineRunStore>,
        step_store: Arc<dyn StepRunStore>,
        job_store: Arc<dyn JobStore>,
        job_submitter: Arc<dyn JobSubmitter>,
        tenant_limits_store: Option<Arc<dyn TenantLimitsStore>>,
        token_store: Arc<dyn GitTokenStore>,
        webhook_store: Arc<dyn muli_core::traits::WebhookStore>,
        allow_localhost_webhooks: bool,
        git_base_url: String,
    ) -> Self {
        Self {
            git_storage,
            repo_store,
            pr_store,
            pipeline_store,
            run_store,
            step_store,
            job_store,
            job_submitter,
            tenant_limits_store,
            token_store,
            webhook_store,
            webhook_http_client: Arc::new(webhook_http_client()),
            allow_localhost_webhooks,
            git_base_url,
            last_trigger: DashMap::new(),
        }
    }

    async fn deliver_pipeline_webhook(
        &self,
        tenant_id: &str,
        repo_id: &str,
        event: WebhookEvent,
        payload: serde_json::Value,
    ) {
        deliver_webhooks(
            self.webhook_store.clone(),
            self.webhook_http_client.clone(),
            tenant_id,
            repo_id,
            &muli_git::hooks::HookDelivery {
                repo_id: repo_id.to_string(),
                event,
                payload,
            },
            self.allow_localhost_webhooks,
        )
        .await;
    }

    async fn trigger_pipeline(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        ref_name: &str,
        event: PipelineEvent,
        trigger: PipelineTrigger,
    ) {
        // 0. Rate limit: skip if triggered too recently for this repo
        let repo_key = format!("{tenant_id}/{repo_id}");
        if let Some(last) = self.last_trigger.get(&repo_key) {
            if last.elapsed().as_secs() < MIN_TRIGGER_INTERVAL_SECS {
                warn!(
                    repo_id = %repo_id,
                    "pipeline trigger rate-limited (< {}s since last trigger)",
                    MIN_TRIGGER_INTERVAL_SECS,
                );
                return;
            }
        }
        self.last_trigger.insert(repo_key, Instant::now());

        // 0b. Tenant enforcement checks
        if let Some(ref limits_store) = self.tenant_limits_store {
            if limits_store.is_suspended(tenant_id).await.unwrap_or(false) {
                warn!(tenant_id = %tenant_id, "pipeline trigger: tenant is suspended");
                return;
            }
            if let Ok(Some(limits)) = limits_store.get_limits(tenant_id).await {
                // Check daily run limit
                if limits.max_pipeline_runs_per_day > 0 {
                    if let Ok(count) = limits_store.get_daily_run_count(tenant_id).await {
                        if count >= limits.max_pipeline_runs_per_day as u64 {
                            warn!(
                                tenant_id = %tenant_id,
                                count = count,
                                limit = limits.max_pipeline_runs_per_day,
                                "pipeline trigger: daily run limit exceeded"
                            );
                            return;
                        }
                    }
                }
                // Check concurrent pipeline limit
                if limits.max_concurrent_pipelines > 0 {
                    if let Ok(active) = self.run_store.count_active(tenant_id).await {
                        if active >= limits.max_concurrent_pipelines as u64 {
                            warn!(
                                tenant_id = %tenant_id,
                                active = active,
                                limit = limits.max_concurrent_pipelines,
                                "pipeline trigger: concurrent pipeline limit exceeded"
                            );
                            return;
                        }
                    }
                }
            }
        }

        // 1. Look up the repository to get namespace/name for the file path
        let repo = match self.repo_store.get_repository(repo_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                warn!(repo_id = %repo_id, "pipeline trigger: repository not found");
                return;
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to look up repository");
                return;
            }
        };

        // 2. Resolve the bare repo path on disk
        let repo_path = self
            .git_storage
            .repo_path(tenant_id, &repo.namespace, &repo.name);

        // 3. Read all supported pipeline configs from the commit
        let pipeline_files = match tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            let sha = commit_sha.to_string();
            move || read_pipeline_files(&repo_path, &sha)
        })
        .await
        {
            Ok(Ok(files)) if files.is_empty() => return,
            Ok(Ok(files)) => files,
            Ok(Err(e)) => {
                warn!(error = %e, "pipeline trigger: failed to read pipeline YAML");
                return;
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: spawn_blocking panicked");
                return;
            }
        };

        let mut known_pipelines = match self.pipeline_store.get_by_repo(tenant_id, repo_id).await {
            Ok(pipelines) => pipelines,
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to list pipelines for repo");
                return;
            }
        };
        let mut seen_pipeline_names = HashSet::new();

        for pipeline_file in pipeline_files {
            // 4. Enforce YAML size limit
            const MAX_YAML_SIZE: usize = 1_048_576; // 1 MB
            if pipeline_file.content.len() > MAX_YAML_SIZE {
                warn!(
                    path = %pipeline_file.path,
                    size = pipeline_file.content.len(),
                    "pipeline YAML exceeds 1MB limit"
                );
                continue;
            }

            // 5. Parse and validate the YAML
            let pipeline_def = match parse_pipeline(&pipeline_file.content) {
                Ok(def) => def,
                Err(e) => {
                    warn!(
                        error = %e,
                        path = %pipeline_file.path,
                        "pipeline trigger: invalid pipeline YAML"
                    );
                    continue;
                }
            };

            if let Err(e) = validate_pipeline(&pipeline_def) {
                warn!(
                    error = %e,
                    path = %pipeline_file.path,
                    "pipeline trigger: pipeline validation failed"
                );
                continue;
            }

            // 6. Match triggers against the event
            if !matches_trigger(&pipeline_def.on, &event) {
                info!(
                    pipeline = %pipeline_def.name,
                    path = %pipeline_file.path,
                    "pipeline trigger: event does not match trigger config"
                );
                continue;
            }

            if !seen_pipeline_names.insert(pipeline_def.name.clone()) {
                warn!(
                    pipeline = %pipeline_def.name,
                    path = %pipeline_file.path,
                    "pipeline trigger: duplicate pipeline name in commit; skipping duplicate"
                );
                continue;
            }

            // 7. Upsert the Pipeline record, reusing the existing ID for the same repo/name.
            let yaml_sha = hex::encode(Sha256::digest(pipeline_file.content.as_bytes()));
            let pipeline = if let Some(existing) = known_pipelines
                .iter()
                .find(|pipeline| pipeline.name == pipeline_def.name)
                .cloned()
            {
                let mut pipeline = existing;
                pipeline.yaml_sha = yaml_sha;
                pipeline.updated_at = Utc::now();
                pipeline
            } else {
                let pipeline = Pipeline::new(
                    tenant_id.to_string(),
                    repo_id.to_string(),
                    pipeline_def.name.clone(),
                    yaml_sha,
                );
                known_pipelines.push(pipeline.clone());
                pipeline
            };
            if let Err(e) = self.pipeline_store.upsert_pipeline(&pipeline).await {
                error!(
                    error = %e,
                    pipeline = %pipeline_def.name,
                    path = %pipeline_file.path,
                    "pipeline trigger: failed to upsert pipeline"
                );
                continue;
            }

            // 8. Create a PipelineRun
            let run_number = match self
                .run_store
                .next_run_number(tenant_id, &pipeline.id)
                .await
            {
                Ok(n) => n,
                Err(e) => {
                    error!(error = %e, "pipeline trigger: failed to get next run number");
                    continue;
                }
            };

            let mut run = PipelineRun::new(
                pipeline.id.clone(),
                tenant_id.to_string(),
                repo_id.to_string(),
                run_number,
                commit_sha.to_string(),
                ref_name.to_string(),
                trigger.clone(),
                pipeline_file.content.clone(),
            );

            if let Err(e) = self.run_store.create_run(&run).await {
                error!(error = %e, "pipeline trigger: failed to create pipeline run");
                continue;
            }

            // Increment daily run count for limit enforcement.
            if let Some(ref limits_store) = self.tenant_limits_store
                && let Err(e) = limits_store.increment_daily_run_count(tenant_id).await
            {
                warn!(error = %e, "pipeline trigger: failed to increment daily run count");
            }

            info!(
                pipeline = %pipeline_def.name,
                path = %pipeline_file.path,
                run_number = run_number,
                commit = %commit_sha,
                "pipeline run #{run_number} created"
            );

            self.deliver_pipeline_webhook(
                tenant_id,
                repo_id,
                WebhookEvent::PipelineStarted,
                serde_json::json!({
                    "run_id": run.id,
                    "pipeline_id": pipeline.id,
                    "pipeline_name": pipeline.name,
                    "run_number": run.run_number,
                    "commit_sha": run.commit_sha,
                    "ref_name": run.ref_name,
                    "state": "pending",
                }),
            )
            .await;

            // 9. Expand steps/jobs and create StepRun records
            let mut all_steps = Vec::new();

            if !pipeline_def.jobs.is_empty() {
                // Jobs mode: expand matrix job variants and record depends_on.
                for (job_name, job_def) in &pipeline_def.jobs {
                    let expanded =
                        match muli_pipeline::dag::matrix::expand_job_matrix(job_name, job_def) {
                            Ok(e) => e,
                            Err(e) => {
                                warn!(error = %e, job = %job_name, "job matrix expansion failed");
                                continue;
                            }
                        };
                    for (step_name, matrix_values) in expanded {
                        let failure_strategy = match job_def.failure_strategy.as_deref() {
                            Some("continue") => FailureStrategy::Continue,
                            Some("ignore") => FailureStrategy::Ignore,
                            _ => FailureStrategy::Stop,
                        };
                        let mut step_run = StepRun::new(
                            run.id.clone(),
                            tenant_id.to_string(),
                            step_name.clone(),
                            failure_strategy,
                            matrix_values,
                        );
                        step_run.depends_on = job_def.needs.clone();
                        if let Err(e) = self.step_store.create_step(&step_run).await {
                            error!(error = %e, step = %step_name, "failed to create job step run");
                            continue;
                        }
                        all_steps.push(step_run);
                    }
                }
            } else {
                // Steps mode (legacy): expand matrix steps.
                let mut conditions = HashMap::new();
                for step_def in &pipeline_def.steps {
                    let expanded = match muli_pipeline::dag::matrix::expand_matrix(step_def) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!(error = %e, step = %step_def.name, "matrix expansion failed");
                            continue;
                        }
                    };
                    for expanded_step in expanded {
                        let failure_strategy = match expanded_step.failure_strategy.as_deref() {
                            Some("continue") => FailureStrategy::Continue,
                            Some("ignore") => FailureStrategy::Ignore,
                            _ => FailureStrategy::Stop,
                        };
                        let mut step_run = StepRun::new(
                            run.id.clone(),
                            tenant_id.to_string(),
                            expanded_step.name.clone(),
                            failure_strategy,
                            None,
                        );
                        step_run.depends_on = expanded_step.needs.clone();
                        conditions
                            .insert(expanded_step.name.clone(), expanded_step.condition.clone());
                        if let Err(e) = self.step_store.create_step(&step_run).await {
                            error!(
                                error = %e,
                                step = %expanded_step.name,
                                "failed to create step run"
                            );
                            continue;
                        }
                        all_steps.push(step_run);
                    }
                }
                let _ = conditions; // used to evaluate conditions in executor
            }

            // 10. Generate a short-lived pull-only CI token for auto-checkout, then execute.
            let plaintext = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            let token_hash_result = {
                let pt = plaintext.clone();
                tokio::task::spawn_blocking(move || token_hash::hash_token(&pt)).await
            };
            let (ci_token_id, clone_url) = match token_hash_result {
                Ok(Ok(token_hash)) => {
                    let prefix = token_hash::token_prefix(&plaintext);
                    let ci_token = GitToken {
                        id: uuid::Uuid::new_v4().to_string(),
                        tenant_id: tenant_id.to_string(),
                        user_id: None,
                        repo_id: Some(repo.id.clone()),
                        token_hash,
                        token_prefix: prefix,
                        permissions: vec![GitPermission::Pull],
                        description: format!("CI run {}", run.id),
                        created_at: Utc::now(),
                        expires_at: Some(Utc::now() + chrono::Duration::minutes(10)),
                        revoked: false,
                    };
                    let token_id = ci_token.id.clone();
                    match self.token_store.create_token(&ci_token).await {
                        Ok(_) => {
                            let clone_url = build_ci_clone_url(
                                &self.git_base_url,
                                &plaintext,
                                &repo.namespace,
                                &repo.name,
                            );
                            (Some(token_id), Some(clone_url))
                        }
                        Err(e) => {
                            error!(error = %e, "pipeline trigger: failed to create CI token");
                            (None, None)
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!(error = %e, "pipeline trigger: token hashing failed");
                    (None, None)
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "pipeline trigger: spawn_blocking panicked during token hash"
                    );
                    (None, None)
                }
            };

            // 11. Execute via DAG executor (submits Jobs, waits for completion)
            let executor = DagExecutor::new(
                self.run_store.clone(),
                self.step_store.clone(),
                self.job_store.clone(),
                self.job_submitter.clone(),
            );
            let exec_result = executor
                .execute(&mut run, &pipeline_def, &all_steps, clone_url.as_deref())
                .await;

            // Revoke the CI token regardless of execution outcome.
            if let Some(ref id) = ci_token_id
                && let Err(e) = self.token_store.revoke_token(id).await
            {
                warn!(
                    token_id = %id,
                    error = %e,
                    "pipeline trigger: failed to revoke CI token"
                );
            }

            match exec_result {
                Ok(state) => {
                    self.deliver_pipeline_webhook(
                        tenant_id,
                        repo_id,
                        WebhookEvent::PipelineCompleted,
                        serde_json::json!({
                            "run_id": run.id,
                            "pipeline_id": pipeline.id,
                            "pipeline_name": pipeline.name,
                            "run_number": run.run_number,
                            "commit_sha": run.commit_sha,
                            "ref_name": run.ref_name,
                            "state": format!("{state:?}").to_lowercase(),
                        }),
                    )
                    .await;
                    info!(
                        run_id = %run.id,
                        state = ?state,
                        steps = all_steps.len(),
                        path = %pipeline_file.path,
                        "pipeline run #{run_number} executing"
                    );
                }
                Err(e) => {
                    self.deliver_pipeline_webhook(
                        tenant_id,
                        repo_id,
                        WebhookEvent::PipelineCompleted,
                        serde_json::json!({
                            "run_id": run.id,
                            "pipeline_id": pipeline.id,
                            "pipeline_name": pipeline.name,
                            "run_number": run.run_number,
                            "commit_sha": run.commit_sha,
                            "ref_name": run.ref_name,
                            "state": "failed",
                            "error": e.to_string(),
                        }),
                    )
                    .await;
                    error!(error = %e, "pipeline trigger: DAG execution failed");
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl PipelineTriggerHook for PipelineTriggerImpl {
    async fn on_push(&self, tenant_id: &str, repo_id: &str, commit_sha: &str, ref_name: &str) {
        let branch = ref_name
            .strip_prefix("refs/heads/")
            .unwrap_or(ref_name)
            .to_string();

        info!(
            tenant_id = %tenant_id,
            repo_id = %repo_id,
            commit_sha = %commit_sha,
            branch = %branch,
            "pipeline trigger: push event"
        );

        let event = PipelineEvent::Push {
            branch: branch.clone(),
            changed_paths: vec![], // Path filtering requires diffing old..new (future enhancement)
        };

        let trigger = PipelineTrigger::Push {
            ref_name: ref_name.to_string(),
        };

        self.trigger_pipeline(tenant_id, repo_id, commit_sha, ref_name, event, trigger)
            .await;
    }

    async fn on_pr_event(&self, tenant_id: &str, repo_id: &str, pr_number: u64, event: &str) {
        info!(
            tenant_id = %tenant_id,
            repo_id = %repo_id,
            pr_number = pr_number,
            event = %event,
            "pipeline trigger: PR event"
        );

        // Look up the PR to get the source branch and target branch
        let pr = match self.pr_store.get_pr_by_number(repo_id, pr_number).await {
            Ok(Some(pr)) => pr,
            Ok(None) => {
                warn!(pr_number = pr_number, "pipeline trigger: PR not found");
                return;
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to look up PR");
                return;
            }
        };

        // Resolve the source branch HEAD commit from the bare repo
        let repo = match self.repo_store.get_repository(repo_id).await {
            Ok(Some(r)) => r,
            Ok(None) => return,
            Err(e) => {
                error!(error = %e, "pipeline trigger: repo lookup failed");
                return;
            }
        };

        let repo_path = self
            .git_storage
            .repo_path(tenant_id, &repo.namespace, &repo.name);

        let source_branch = pr.source_branch.clone();
        let commit_sha = match tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            move || resolve_branch_head(&repo_path, &source_branch)
        })
        .await
        {
            Ok(Ok(sha)) => sha,
            Ok(Err(e)) => {
                warn!(error = %e, branch = %pr.source_branch, "failed to resolve branch HEAD");
                return;
            }
            Err(e) => {
                error!(error = %e, "spawn_blocking panicked");
                return;
            }
        };

        let ref_name = format!("refs/heads/{}", pr.source_branch);

        let pipeline_event = PipelineEvent::PullRequest {
            target_branch: pr.target_branch.clone(),
            event: event.to_string(),
        };

        let trigger = PipelineTrigger::PullRequest {
            pr_number,
            event: event.to_string(),
        };

        self.trigger_pipeline(
            tenant_id,
            repo_id,
            &commit_sha,
            &ref_name,
            pipeline_event,
            trigger,
        )
        .await;
    }
}

/// Construct a git clone URL with embedded CI token credentials.
///
/// e.g. `http://x-pipeline-token:<token>@host.docker.internal:7000/<namespace>/<repo>.git`
///
/// `localhost`/`127.0.0.1` are rewritten to `host.docker.internal` so the URL is reachable
/// from inside Docker containers on Linux (requires `extra_hosts: host.docker.internal:host-gateway`
/// in HostConfig).  For multi-tenant production, set `MULI_GIT_BASE_URL` to a public hostname
/// (e.g. `https://myorg.git.example.com`) — no substitution is applied in that case.
fn build_ci_clone_url(git_base_url: &str, token: &str, namespace: &str, name: &str) -> String {
    let base = git_base_url.trim_end_matches('/');
    let (scheme, host) = if let Some(i) = base.find("://") {
        (&base[..i], base[i + 3..].to_string())
    } else {
        ("http", base.to_string())
    };
    // Containers can't reach the host via localhost; use host.docker.internal instead.
    let host = if host.starts_with("localhost") {
        host.replacen("localhost", "host.docker.internal", 1)
    } else if host.starts_with("127.0.0.1") {
        host.replacen("127.0.0.1", "host.docker.internal", 1)
    } else {
        host
    };
    format!("{scheme}://x-pipeline-token:{token}@{host}/{namespace}/{name}.git")
}

/// Resolve the HEAD commit SHA for a branch in a bare repository.
fn resolve_branch_head(repo_path: &Path, branch: &str) -> muli_core::error::Result<String> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;

    let ref_name = format!("refs/heads/{branch}");
    let reference = repo
        .find_reference(&ref_name)
        .map_err(|e| MuliError::Pipeline(format!("branch '{branch}' not found: {e}")))?;

    let commit = reference
        .peel_to_commit()
        .map_err(|e| MuliError::Pipeline(format!("cannot resolve branch HEAD: {e}")))?;

    Ok(commit.id().to_string())
}
