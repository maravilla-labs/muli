// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline trigger implementation that bridges git events to pipeline runs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use muli_core::pipeline::{
    FailureStrategy, Pipeline, PipelineRun, PipelineTrigger, StepRun,
};
use muli_core::traits::{
    JobStore, PipelineRunStore, PipelineStore, PullRequestStore, RepositoryStore, StepRunStore,
};
use muli_git::api::PipelineTriggerHook;
use muli_git::storage::FilesystemStorage;
use std::path::Path;

use muli_core::error::MuliError;
use muli_pipeline::dag::executor::{DagExecutor, JobSubmitter};
use muli_pipeline::trigger::matcher::{matches_trigger, PipelineEvent};
use muli_pipeline::trigger::reader::read_pipeline_yaml;
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
    /// Per-repo last-trigger time for rate limiting.
    last_trigger: DashMap<String, Instant>,
}

impl PipelineTriggerImpl {
    pub fn new(
        git_storage: Arc<FilesystemStorage>,
        repo_store: Arc<dyn RepositoryStore>,
        pr_store: Arc<dyn PullRequestStore>,
        pipeline_store: Arc<dyn PipelineStore>,
        run_store: Arc<dyn PipelineRunStore>,
        step_store: Arc<dyn StepRunStore>,
        job_store: Arc<dyn JobStore>,
        job_submitter: Arc<dyn JobSubmitter>,
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
            last_trigger: DashMap::new(),
        }
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

        // 3. Read .maravilla/pipeline.yml from the commit
        let yaml_content = match tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            let sha = commit_sha.to_string();
            move || read_pipeline_yaml(&repo_path, &sha)
        })
        .await
        {
            Ok(Ok(Some(content))) => content,
            Ok(Ok(None)) => return, // No pipeline config — nothing to do
            Ok(Err(e)) => {
                warn!(error = %e, "pipeline trigger: failed to read pipeline YAML");
                return;
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: spawn_blocking panicked");
                return;
            }
        };

        // 4. Enforce YAML size limit
        const MAX_YAML_SIZE: usize = 1_048_576; // 1 MB
        if yaml_content.len() > MAX_YAML_SIZE {
            warn!(
                "pipeline YAML exceeds 1MB limit ({} bytes)",
                yaml_content.len()
            );
            return;
        }

        // 5. Parse and validate the YAML
        let pipeline_def = match parse_pipeline(&yaml_content) {
            Ok(def) => def,
            Err(e) => {
                warn!(error = %e, "pipeline trigger: invalid pipeline YAML");
                return;
            }
        };

        if let Err(e) = validate_pipeline(&pipeline_def) {
            warn!(error = %e, "pipeline trigger: pipeline validation failed");
            return;
        }

        // 5. Match triggers against the event
        if !matches_trigger(&pipeline_def.on, &event) {
            info!(
                pipeline = %pipeline_def.name,
                "pipeline trigger: event does not match trigger config"
            );
            return;
        }

        // 6. Upsert the Pipeline record
        let yaml_sha = hex::encode(Sha256::digest(yaml_content.as_bytes()));
        let pipeline = Pipeline::new(
            tenant_id.to_string(),
            repo_id.to_string(),
            pipeline_def.name.clone(),
            yaml_sha,
        );
        if let Err(e) = self.pipeline_store.upsert_pipeline(&pipeline).await {
            error!(error = %e, "pipeline trigger: failed to upsert pipeline");
            return;
        }

        // 7. Create a PipelineRun
        let run_number = match self.run_store.next_run_number(&pipeline.id).await {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to get next run number");
                return;
            }
        };

        let mut run = PipelineRun::new(
            pipeline.id.clone(),
            tenant_id.to_string(),
            repo_id.to_string(),
            run_number,
            commit_sha.to_string(),
            ref_name.to_string(),
            trigger,
            yaml_content,
        );

        if let Err(e) = self.run_store.create_run(&run).await {
            error!(error = %e, "pipeline trigger: failed to create pipeline run");
            return;
        }

        info!(
            pipeline = %pipeline_def.name,
            run_number = run_number,
            commit = %commit_sha,
            "pipeline run #{run_number} created"
        );

        // 8. Expand matrix steps and create StepRun records
        let mut all_steps = Vec::new();
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
                let step_run = StepRun::new(
                    run.id.clone(),
                    tenant_id.to_string(),
                    expanded_step.name.clone(),
                    failure_strategy,
                    None,
                );
                conditions.insert(
                    expanded_step.name.clone(),
                    expanded_step.condition.clone(),
                );
                if let Err(e) = self.step_store.create_step(&step_run).await {
                    error!(error = %e, step = %expanded_step.name, "failed to create step run");
                    continue;
                }
                all_steps.push(step_run);
            }
        }

        // 9. Execute via DAG executor (submits Jobs, waits for completion)
        let executor = DagExecutor::new(
            self.run_store.clone(),
            self.step_store.clone(),
            self.job_store.clone(),
            self.job_submitter.clone(),
        );
        // Build clone URL for auto-checkout
        let clone_url: Option<String> = None; // TODO: construct from git_storage + repo info
        match executor.execute(&mut run, &pipeline_def, &all_steps, clone_url.as_deref()).await {
            Ok(state) => {
                info!(
                    run_id = %run.id,
                    state = ?state,
                    steps = all_steps.len(),
                    "pipeline run #{run_number} executing"
                );
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: DAG execution failed");
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

        self.trigger_pipeline(tenant_id, repo_id, &commit_sha, &ref_name, pipeline_event, trigger)
            .await;
    }
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
