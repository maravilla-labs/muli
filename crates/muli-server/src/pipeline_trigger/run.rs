// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Execution phase: mint a short-lived CI token, run the DAG executor, revoke
//! the token, and deliver the completion webhook.

use chrono::Utc;
use tracing::{error, info, warn};

use muli_core::git::{GitPermission, GitToken, Repository};
use muli_core::pipeline::{Pipeline, PipelineRun, PipelineRunState, StepRun};
use muli_core::token_hash;
use muli_pipeline::dag::executor::DagExecutor;
use muli_pipeline::yaml::schema::PipelineDef;

use super::PipelineTriggerImpl;
use super::git_meta::ci_clone_url_target;
use crate::pipeline_clone_url::build_ci_clone_url;

impl PipelineTriggerImpl {
    /// 10-11. Mint a pull-only CI token, execute the run via the DAG executor,
    /// revoke the token, and deliver the completion webhook (with artifacts).
    pub(crate) async fn execute_run(
        &self,
        tenant_id: &str,
        repo_id: &str,
        repo: &Repository,
        pipeline: &Pipeline,
        pipeline_def: &PipelineDef,
        run: &mut PipelineRun,
        all_steps: &[StepRun],
        config_path: &str,
    ) {
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
                            ci_clone_url_target(pipeline_def),
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
            .execute(run, pipeline_def, all_steps, clone_url.as_deref())
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

        // Query artifacts produced by this run for inclusion in the webhook payload.
        let artifacts_json: serde_json::Value =
            match self.artifact_store.list_by_run(tenant_id, &run.id).await {
                Ok(artifacts) => serde_json::json!(
                    artifacts.iter().map(|a| serde_json::json!({
                        "id": a.id,
                        "name": a.name,
                        "step_name": a.step_name,
                        "size_bytes": a.size_bytes,
                    })).collect::<Vec<_>>()
                ),
                Err(e) => {
                    warn!(error = %e, "pipeline trigger: failed to list artifacts for webhook");
                    serde_json::json!([])
                }
            };

        match exec_result {
            Ok(state) => {
                // Declarative `release:` blocks run in-process, only on a fully
                // successful run, before the completion webhook so their summary
                // rides along in the payload.
                let releases = if state == PipelineRunState::Succeeded {
                    self.run_releases(tenant_id, repo, run, pipeline_def).await
                } else {
                    Vec::new()
                };
                self.deliver_completed(
                    tenant_id,
                    repo_id,
                    run,
                    pipeline,
                    &format!("{state:?}").to_lowercase(),
                    None,
                    &artifacts_json,
                    releases.first(),
                )
                .await;
                info!(
                    run_id = %run.id,
                    state = ?state,
                    steps = all_steps.len(),
                    releases = releases.len(),
                    path = %config_path,
                    "pipeline run #{} executing",
                    run.run_number,
                );
            }
            Err(e) => {
                self.deliver_completed(
                    tenant_id,
                    repo_id,
                    run,
                    pipeline,
                    "failed",
                    Some(&e.to_string()),
                    &artifacts_json,
                    None,
                )
                .await;
                error!(error = %e, "pipeline trigger: DAG execution failed");
            }
        }
    }
}
