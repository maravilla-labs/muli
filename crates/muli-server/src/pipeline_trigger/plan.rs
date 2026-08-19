// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-config planning: parse + validate + trigger-match a pipeline YAML file,
//! upsert the `Pipeline` record, and build the `PipelineRun`.

use std::collections::HashMap;

use chrono::Utc;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use muli_core::git::Repository;
use muli_core::pipeline::{Pipeline, PipelineRun, PipelineTrigger};
use muli_pipeline::trigger::matcher::{PipelineEvent, matches_trigger};
use muli_pipeline::trigger::reader::PipelineFile;
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::schema::PipelineDef;
use muli_pipeline::yaml::validation::validate_pipeline;

use super::PipelineTriggerImpl;

/// Maximum accepted pipeline YAML size.
const MAX_YAML_SIZE: usize = 1_048_576; // 1 MB

/// Short label for a trigger, used in the resolved-env log line so a
/// "secret missing" report can be pinned to push vs manual immediately.
fn trigger_kind(trigger: &PipelineTrigger) -> &'static str {
    match trigger {
        PipelineTrigger::Push { .. } => "push",
        PipelineTrigger::PullRequest { .. } => "pull_request",
        PipelineTrigger::Manual { .. } => "manual",
        PipelineTrigger::Schedule { .. } => "schedule",
        PipelineTrigger::Retry { .. } => "retry",
    }
}

/// Parse + validate a single pipeline file and check its triggers against the
/// event. Returns the parsed `PipelineDef` when the pipeline should run, or
/// `None` (skip this file) on size limit, parse/validation error, or no match.
pub(crate) fn parse_config(
    pipeline_file: &PipelineFile,
    event: &PipelineEvent,
) -> Option<PipelineDef> {
    // 4. Enforce YAML size limit
    if pipeline_file.content.len() > MAX_YAML_SIZE {
        warn!(
            path = %pipeline_file.path,
            size = pipeline_file.content.len(),
            "pipeline YAML exceeds 1MB limit"
        );
        return None;
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
            return None;
        }
    };

    if let Err(e) = validate_pipeline(&pipeline_def) {
        warn!(
            error = %e,
            path = %pipeline_file.path,
            "pipeline trigger: pipeline validation failed"
        );
        return None;
    }

    // 6. Match triggers against the event
    if !matches_trigger(&pipeline_def.on, event) {
        info!(
            pipeline = %pipeline_def.name,
            path = %pipeline_file.path,
            "pipeline trigger: event does not match trigger config"
        );
        return None;
    }

    Some(pipeline_def)
}

impl PipelineTriggerImpl {
    /// 7. Upsert the `Pipeline` record, reusing the existing ID for the same
    /// repo/name. Returns `None` on store error (skip this file).
    pub(crate) async fn upsert_pipeline_record(
        &self,
        tenant_id: &str,
        repo_id: &str,
        pipeline_def: &PipelineDef,
        pipeline_file: &PipelineFile,
        known_pipelines: &mut Vec<Pipeline>,
    ) -> Option<Pipeline> {
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
            return None;
        }
        Some(pipeline)
    }

    /// 8. Build and persist a `PipelineRun` (with resolved secrets), incrementing
    /// the tenant's daily run count. Returns `None` on store error (skip this file).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn build_run(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        ref_name: &str,
        trigger: &PipelineTrigger,
        repo: &Repository,
        pipeline: &Pipeline,
        pipeline_def: &PipelineDef,
        pipeline_file: &PipelineFile,
        commit_message: &str,
        commit_author: &str,
        caller_env: HashMap<String, String>,
    ) -> Option<PipelineRun> {
        let run_number = match self
            .run_store
            .next_run_number(tenant_id, &pipeline.id)
            .await
        {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to get next run number");
                return None;
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
        run.commit_message = commit_message.to_string();
        run.commit_author = commit_author.to_string();
        run.webhook_data = pipeline_def.webhook.clone();
        run.triggered_by = match trigger {
            PipelineTrigger::Manual { triggered_by } => triggered_by.clone(),
            _ => tenant_id.to_string(),
        };

        // Resolve secrets. `caller_env` carries variables supplied by the caller
        // of a manual trigger (e.g. an external control plane's build variables
        // and vault values); it is empty for push/PR triggers, which have no
        // caller to supply them.
        let org_id = match self
            .org_store
            .get_org_by_handle(tenant_id, &repo.namespace)
            .await
        {
            Ok(Some(org)) => Some(org.id),
            Ok(None) => {
                warn!(
                    namespace = %repo.namespace,
                    repo_id = %repo_id,
                    "pipeline trigger: namespace is not an org handle; org-level secrets will be skipped"
                );
                None
            }
            Err(e) => {
                warn!(
                    namespace = %repo.namespace,
                    repo_id = %repo_id,
                    error = %e,
                    "pipeline trigger: org lookup failed; org-level secrets will be skipped"
                );
                None
            }
        };

        match crate::secret_resolver::resolve_env_vars(
            &self.secret_store,
            &self.org_secret_store,
            tenant_id,
            repo_id,
            org_id.as_deref(),
            self.encryption_key.as_ref(),
            caller_env,
        )
        .await
        {
            Ok(env) => {
                // Names only, never values -- pipeline log output is not redacted.
                let mut names: Vec<&str> = env.keys().map(String::as_str).collect();
                names.sort_unstable();
                info!(
                    repo_id = %repo_id,
                    trigger = trigger_kind(trigger),
                    count = names.len(),
                    secrets = %names.join(","),
                    "pipeline trigger: resolved run env"
                );
                run.env_vars = env;
            }
            Err(e) => {
                warn!(error = %e, "pipeline trigger: failed to resolve secrets; continuing without");
            }
        }

        if let Err(e) = self.run_store.create_run(&run).await {
            error!(error = %e, "pipeline trigger: failed to create pipeline run");
            return None;
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

        Some(run)
    }
}
