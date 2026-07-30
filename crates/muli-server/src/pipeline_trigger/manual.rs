// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Manual pipeline triggers (the `TriggerPipeline` RPC).
//!
//! Runs the same phases as a push-triggered run — discovery, parse/validate,
//! trigger match, pipeline upsert, run + step creation, DAG execution — with two
//! deliberate differences:
//!
//! - the event is [`PipelineEvent::Manual`], so only pipelines that opted in with
//!   `on: { manual: true }` are eligible, and exactly one of them runs;
//! - execution is spawned rather than awaited, so the RPC returns as soon as the
//!   run and its steps are persisted.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{error, info, warn};

use muli_core::pipeline::{Pipeline, PipelineRun, PipelineTrigger, StepRun};
use muli_pipeline::trigger::matcher::PipelineEvent;

use super::discovery::Discovery;
use super::{PipelineTriggerImpl, git_meta, plan};

/// Why a manual trigger could not produce a run.
#[derive(Debug)]
pub enum ManualTriggerError {
    /// The repository, the ref, or a pipeline config could not be found.
    NotFound(String),
    /// Pipeline configs exist but none opted into manual runs.
    NoManualPipeline(String),
    /// Admission control refused the trigger (rate limit, tenant limits).
    RateLimited(String),
    /// A store or git failure.
    Internal(String),
}

impl std::fmt::Display for ManualTriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m)
            | Self::NoManualPipeline(m)
            | Self::RateLimited(m)
            | Self::Internal(m) => f.write_str(m),
        }
    }
}

/// The outcome of a successful manual trigger.
pub struct ManualRun {
    pub pipeline: Pipeline,
    pub run: PipelineRun,
    pub steps: Vec<StepRun>,
}

impl PipelineTriggerImpl {
    /// Trigger a run by hand on `ref_input` (a branch, a tag, a fully-qualified
    /// ref, or empty for the repo's HEAD).
    ///
    /// Nothing is persisted unless a pipeline actually matches, so a rejected
    /// trigger leaves no orphaned `Pending` run behind.
    pub async fn trigger_manual(
        self: &Arc<Self>,
        tenant_id: &str,
        repo_id: &str,
        ref_input: &str,
        commit_sha: Option<&str>,
        triggered_by: &str,
        caller_env: HashMap<String, String>,
        pipeline_name: Option<&str>,
    ) -> Result<ManualRun, ManualTriggerError> {
        // 1. Resolve the repo and the ref. `repo` is re-resolved by `discover`
        //    below; here it only supplies the on-disk path.
        let repo = self
            .repo_store
            .get_repository(repo_id)
            .await
            .map_err(|e| ManualTriggerError::Internal(format!("failed to look up repo: {e}")))?
            .ok_or_else(|| ManualTriggerError::NotFound("repository not found".to_string()))?;

        if repo.tenant_id != tenant_id {
            return Err(ManualTriggerError::NotFound(
                "repository not found".to_string(),
            ));
        }

        let repo_path = self
            .git_storage
            .repo_path(tenant_id, &repo.namespace, &repo.name);

        let (ref_name, resolved_sha) = tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            let ref_input = ref_input.to_string();
            move || git_meta::resolve_ref(&repo_path, &ref_input)
        })
        .await
        .map_err(|e| ManualTriggerError::Internal(format!("spawn_blocking panicked: {e}")))?
        .map_err(|e| ManualTriggerError::NotFound(e.to_string()))?;

        // An explicit commit wins, but the ref the caller named is what the run
        // records — a manual re-run of `v1.2.3` must read as a tag run.
        let commit_sha = match commit_sha {
            Some(sha) if !sha.is_empty() => sha.to_string(),
            _ => resolved_sha,
        };

        // 2. Admission control (rate limit + tenant suspension/limits).
        if !self.admission_allows(tenant_id, repo_id, &ref_name).await {
            return Err(ManualTriggerError::RateLimited(
                "pipeline trigger refused: too many triggers for this ref, or a tenant limit was reached"
                    .to_string(),
            ));
        }

        // 3. Read the pipeline configs at that commit.
        let Some(Discovery {
            repo,
            pipeline_files,
            commit_message,
            commit_author,
            mut known_pipelines,
        }) = self.discover(tenant_id, repo_id, &commit_sha).await
        else {
            return Err(ManualTriggerError::NotFound(format!(
                "no readable pipeline config (.maravilla/pipeline.yml) at {commit_sha}"
            )));
        };

        // 4. Pick the single pipeline to run: the first one that opted into manual
        //    runs, optionally narrowed by name.
        let event = PipelineEvent::Manual;
        let matched = pipeline_files.into_iter().find_map(|file| {
            let def = plan::parse_config(&file, &event)?;
            match pipeline_name {
                Some(wanted) if def.name != wanted => None,
                _ => Some((file, def)),
            }
        });

        let Some((pipeline_file, pipeline_def)) = matched else {
            return Err(ManualTriggerError::NoManualPipeline(match pipeline_name {
                Some(name) => format!(
                    "pipeline '{name}' is not enabled for manual runs — add `on: manual: true` to its config"
                ),
                None => "no pipeline is enabled for manual runs — add `on: manual: true` to .maravilla/pipeline.yml"
                    .to_string(),
            }));
        };

        self.note_trigger(tenant_id, repo_id, &ref_name);

        // 5. Upsert the pipeline record and persist the run (with its YAML, so the
        //    executor and any later retry have something to work from).
        let pipeline = self
            .upsert_pipeline_record(
                tenant_id,
                repo_id,
                &pipeline_def,
                &pipeline_file,
                &mut known_pipelines,
            )
            .await
            .ok_or_else(|| {
                ManualTriggerError::Internal("failed to upsert pipeline record".to_string())
            })?;

        let trigger = PipelineTrigger::Manual {
            triggered_by: triggered_by.to_string(),
        };
        let run = self
            .build_run(
                tenant_id,
                repo_id,
                &commit_sha,
                &ref_name,
                &trigger,
                &repo,
                &pipeline,
                &pipeline_def,
                &pipeline_file,
                &commit_message,
                &commit_author,
                caller_env,
            )
            .await
            .ok_or_else(|| {
                ManualTriggerError::Internal("failed to create pipeline run".to_string())
            })?;

        self.deliver_started(tenant_id, repo_id, &run, &pipeline)
            .await;

        // 6. Expand jobs/steps into StepRun rows — what the UI renders as jobs.
        let steps = self.expand_steps(tenant_id, &pipeline_def, &run).await;

        info!(
            operation = "trigger_manual",
            tenant_id = %tenant_id,
            repo_id = %repo_id,
            pipeline = %pipeline_def.name,
            run_number = run.run_number,
            ref_name = %ref_name,
            commit = %commit_sha,
            steps = steps.len(),
            "audit: pipeline triggered manually"
        );

        if steps.is_empty() {
            warn!(
                run_id = %run.id,
                pipeline = %pipeline_def.name,
                "manual trigger produced no steps; check the pipeline's jobs/steps"
            );
        }

        // 7. Execute in the background so the RPC can return the created run.
        {
            let this = Arc::clone(self);
            let tenant_id = tenant_id.to_string();
            let repo_id = repo_id.to_string();
            let config_path = pipeline_file.path.clone();
            let pipeline_bg = pipeline.clone();
            let mut run_bg = run.clone();
            let steps_bg = steps.clone();
            tokio::spawn(async move {
                this.execute_run(
                    &tenant_id,
                    &repo_id,
                    &repo,
                    &pipeline_bg,
                    &pipeline_def,
                    &mut run_bg,
                    &steps_bg,
                    &config_path,
                )
                .await;
            });
        }

        // The returned run is the just-created (Pending) snapshot; the spawned
        // executor advances it from there.
        Ok(ManualRun {
            pipeline,
            run,
            steps,
        })
    }
}

impl From<ManualTriggerError> for tonic::Status {
    fn from(err: ManualTriggerError) -> Self {
        match err {
            ManualTriggerError::NotFound(m) => tonic::Status::not_found(m),
            ManualTriggerError::NoManualPipeline(m) => tonic::Status::failed_precondition(m),
            ManualTriggerError::RateLimited(m) => tonic::Status::resource_exhausted(m),
            ManualTriggerError::Internal(m) => {
                error!(error = %m, "manual pipeline trigger failed");
                tonic::Status::internal(m)
            }
        }
    }
}
