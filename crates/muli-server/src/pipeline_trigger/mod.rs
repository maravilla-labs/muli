// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline trigger implementation that bridges git events to pipeline runs.
//!
//! The work is split into phase modules; [`PipelineTriggerImpl::trigger_pipeline`]
//! orchestrates them in order:
//!
//! - [`admission`] — rate limiting + tenant enforcement
//! - [`discovery`] — repo lookup, read pipeline configs, commit metadata
//! - [`plan`] — per-config parse/validate/match, upsert pipeline, build run
//! - [`expand`] — jobs/steps + matrix expansion into step runs
//! - [`run`] — mint CI token, execute the DAG, revoke token, query artifacts
//! - [`webhook`] — webhook delivery + payload construction
//! - [`git_meta`] — pure git-metadata helpers (changed paths, commit info, heads)

mod admission;
mod discovery;
mod expand;
mod git_meta;
mod plan;
mod run;
mod webhook;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tracing::{error, info, warn};

use muli_core::pipeline::PipelineTrigger;
use muli_core::traits::{
    ArtifactStore, GitTokenStore, JobStore, OrgSecretStore, OrgStore, PipelineRunStore,
    PipelineSecretStore, PipelineStore, PullRequestStore, RepositoryStore, StepRunStore,
    TenantLimitsStore,
};
use muli_git::api::PipelineTriggerHook;
use muli_git::hooks::webhook_http_client;
use muli_git::storage::FilesystemStorage;
use muli_pipeline::dag::executor::JobSubmitter;
use muli_pipeline::trigger::matcher::PipelineEvent;

use discovery::Discovery;

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
    secret_store: Arc<dyn PipelineSecretStore>,
    org_secret_store: Arc<dyn OrgSecretStore>,
    org_store: Arc<dyn OrgStore>,
    encryption_key: Option<[u8; 32]>,
    artifact_store: Arc<dyn ArtifactStore>,
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
        secret_store: Arc<dyn PipelineSecretStore>,
        org_secret_store: Arc<dyn OrgSecretStore>,
        org_store: Arc<dyn OrgStore>,
        encryption_key: Option<[u8; 32]>,
        artifact_store: Arc<dyn ArtifactStore>,
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
            secret_store,
            org_secret_store,
            org_store,
            encryption_key,
            artifact_store,
        }
    }

    async fn resolve_push_changed_paths(
        &self,
        tenant_id: &str,
        repo_id: &str,
        old_sha: &str,
        new_sha: &str,
    ) -> Vec<String> {
        let repo = match self.repo_store.get_repository(repo_id).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Vec::new(),
            Err(e) => {
                warn!(error = %e, "pipeline trigger: failed to look up repository for changed paths");
                return Vec::new();
            }
        };

        let repo_path = self
            .git_storage
            .repo_path(tenant_id, &repo.namespace, &repo.name);

        match tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            let old_sha = old_sha.to_string();
            let new_sha = new_sha.to_string();
            move || git_meta::resolve_push_changed_paths(&repo_path, &old_sha, &new_sha)
        })
        .await
        {
            Ok(Ok(paths)) => paths,
            Ok(Err(e)) => {
                warn!(error = %e, "pipeline trigger: failed to diff changed paths for push");
                Vec::new()
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: spawn_blocking panicked during push diff");
                Vec::new()
            }
        }
    }

    /// Orchestrate a trigger: admission → discovery → per-config plan/expand/run.
    async fn trigger_pipeline(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        ref_name: &str,
        event: PipelineEvent,
        trigger: PipelineTrigger,
    ) {
        // Phase 0-0b: admission control (rate limit + tenant enforcement).
        if !self.admission_allows(tenant_id, repo_id).await {
            return;
        }

        // Phase 1-3: resolve repo, read pipeline configs, gather commit metadata.
        let Some(discovery) = self.discover(tenant_id, repo_id, commit_sha).await else {
            return;
        };
        let Discovery {
            repo,
            pipeline_files,
            commit_message,
            commit_author,
            mut known_pipelines,
        } = discovery;

        let mut seen_pipeline_names = HashSet::new();

        for pipeline_file in pipeline_files {
            // Phase 4-6: size limit, parse, validate, trigger match.
            let Some(pipeline_def) = plan::parse_config(&pipeline_file, &event) else {
                continue;
            };

            // Phase 6b: dedupe pipelines by name within a single commit.
            if !seen_pipeline_names.insert(pipeline_def.name.clone()) {
                warn!(
                    pipeline = %pipeline_def.name,
                    path = %pipeline_file.path,
                    "pipeline trigger: duplicate pipeline name in commit; skipping duplicate"
                );
                continue;
            }

            // Phase 7: upsert the Pipeline record.
            let Some(pipeline) = self
                .upsert_pipeline_record(
                    tenant_id,
                    repo_id,
                    &pipeline_def,
                    &pipeline_file,
                    &mut known_pipelines,
                )
                .await
            else {
                continue;
            };

            // Phase 8: build and persist the PipelineRun.
            let Some(mut run) = self
                .build_run(
                    tenant_id,
                    repo_id,
                    commit_sha,
                    ref_name,
                    &trigger,
                    &repo,
                    &pipeline,
                    &pipeline_def,
                    &pipeline_file,
                    &commit_message,
                    &commit_author,
                )
                .await
            else {
                continue;
            };

            self.deliver_started(tenant_id, repo_id, &run, &pipeline).await;

            // Phase 9: expand steps/jobs into StepRun records.
            let all_steps = self.expand_steps(tenant_id, &pipeline_def, &run).await;

            // Phase 10-11: mint CI token, execute, revoke, deliver completion.
            self.execute_run(
                tenant_id,
                repo_id,
                &repo,
                &pipeline,
                &pipeline_def,
                &mut run,
                &all_steps,
                &pipeline_file.path,
            )
            .await;
        }
    }
}

#[async_trait::async_trait]
impl PipelineTriggerHook for PipelineTriggerImpl {
    async fn on_push(
        &self,
        tenant_id: &str,
        repo_id: &str,
        old_sha: &str,
        new_sha: &str,
        ref_name: &str,
    ) {
        let changed_paths = self
            .resolve_push_changed_paths(tenant_id, repo_id, old_sha, new_sha)
            .await;

        // A tag push (`refs/tags/*`) is a distinct event from a branch push
        // (`refs/heads/*`). The `PipelineTrigger` recorded on the run carries the
        // full ref either way; only the matching `PipelineEvent` differs.
        let event = if let Some(tag) = ref_name.strip_prefix("refs/tags/") {
            info!(
                tenant_id = %tenant_id,
                repo_id = %repo_id,
                commit_sha = %new_sha,
                tag = %tag,
                "pipeline trigger: tag push event"
            );
            PipelineEvent::Tag {
                tag: tag.to_string(),
                changed_paths: changed_paths.clone(),
            }
        } else {
            let branch = ref_name
                .strip_prefix("refs/heads/")
                .unwrap_or(ref_name)
                .to_string();
            info!(
                tenant_id = %tenant_id,
                repo_id = %repo_id,
                commit_sha = %new_sha,
                branch = %branch,
                "pipeline trigger: push event"
            );
            PipelineEvent::Push {
                branch,
                changed_paths: changed_paths.clone(),
            }
        };

        let trigger = PipelineTrigger::Push {
            ref_name: ref_name.to_string(),
            changed_paths,
        };

        self.trigger_pipeline(tenant_id, repo_id, new_sha, ref_name, event, trigger)
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
            move || git_meta::resolve_branch_head(&repo_path, &source_branch)
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
        let changed_paths = match tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            let target_branch = pr.target_branch.clone();
            let source_branch = pr.source_branch.clone();
            move || git_meta::resolve_pr_changed_paths(&repo_path, &target_branch, &source_branch)
        })
        .await
        {
            Ok(Ok(paths)) => paths,
            Ok(Err(e)) => {
                warn!(error = %e, "pipeline trigger: failed to diff changed paths for PR");
                Vec::new()
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: spawn_blocking panicked during PR diff");
                Vec::new()
            }
        };

        let pipeline_event = PipelineEvent::PullRequest {
            target_branch: pr.target_branch.clone(),
            event: event.to_string(),
        };

        let trigger = PipelineTrigger::PullRequest {
            pr_number,
            event: event.to_string(),
            changed_paths,
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
