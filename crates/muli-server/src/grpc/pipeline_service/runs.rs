// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline run RPCs: trigger, get, list, cancel, retry.

use std::collections::HashMap;

use chrono::Utc;
use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::git::{GitPermission, GitToken};
use muli_core::pipeline::{
    Pipeline, PipelineRun as DomainRun, PipelineRunState as DomainRunState, PipelineTrigger,
};
use muli_core::token_hash;
use muli_proto::{
    CancelPipelineRequest, CancelPipelineResponse, GetPipelineRunRequest, ListPipelineRunsRequest,
    ListPipelineRunsResponse, PipelineRunResponse, RetryPipelineRequest, TriggerPipelineRequest,
};

use muli_core::pipeline::StepRunState as DomainStepState;

use super::PipelineServiceImpl;
use super::conversions::{proto_to_run_state, run_to_proto};
use super::helpers::{create_steps_from_yaml, resolve_env_vars};
use crate::grpc::util::validate_tenant;
use crate::pipeline_clone_url::{CiCloneUrlTarget, build_ci_clone_url};

impl PipelineServiceImpl {
    pub async fn trigger_pipeline_impl(
        &self,
        request: Request<TriggerPipelineRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if req.branch.is_empty() && req.commit_sha.is_empty() {
            return Err(Status::invalid_argument("branch or commit_sha is required"));
        }

        // Find or create pipeline for this repo
        let pipelines = self
            .pipeline_store
            .get_by_repo(&caller_tenant, &req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get pipelines: {e}")))?;

        let pipeline = if let Some(p) = pipelines.first() {
            p.clone()
        } else {
            // Create a default pipeline record
            let p = Pipeline::new(
                req.tenant_id.clone(),
                req.repo_id.clone(),
                "default".to_string(),
                String::new(),
            );
            self.pipeline_store
                .upsert_pipeline(&p)
                .await
                .map_err(|e| Status::internal(format!("Failed to create pipeline: {e}")))?;
            p
        };

        // Look up the repo's org for org-level secret scoping
        let org_id =
            resolve_org_id_for_repo(self.repo_store.as_ref(), &caller_tenant, &req.repo_id).await;

        // Resolve env vars (muli secrets + caller env_vars)
        let env_vars = resolve_env_vars(
            &self.secret_store,
            &self.org_secret_store,
            &caller_tenant,
            &req.repo_id,
            org_id.as_deref(),
            self.encryption_key.as_ref(),
            req.env_vars,
        )
        .await?;

        // Get next run number
        let run_number = self
            .run_store
            .next_run_number(&caller_tenant, &pipeline.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run number: {e}")))?;

        // Create the pipeline run
        let ref_name = format!("refs/heads/{}", req.branch);
        let trigger = PipelineTrigger::Manual {
            triggered_by: caller_tenant.clone(),
        };
        let mut run = DomainRun::new(
            pipeline.id.clone(),
            req.tenant_id.clone(),
            req.repo_id.clone(),
            run_number,
            req.commit_sha.clone(),
            ref_name,
            trigger,
            String::new(), // yaml_content will be populated by trigger hook
        );
        run.env_vars = env_vars;
        run.triggered_by = caller_tenant.clone();

        self.run_store
            .create_run(&run)
            .await
            .map_err(|e| Status::internal(format!("Failed to create run: {e}")))?;

        info!(
            operation = "trigger_pipeline",
            tenant_id = %caller_tenant,
            repo_id = %req.repo_id,
            run_number = run_number,
            "audit: pipeline triggered via gRPC"
        );

        Ok(Response::new(PipelineRunResponse {
            run: Some(run_to_proto(&run, Some(&pipeline.name), &[])),
        }))
    }

    pub async fn get_pipeline_run_impl(
        &self,
        request: Request<GetPipelineRunRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        // Support lookup by run_id (UUID) or run_number
        let run = if !req.run_id.is_empty() {
            // Direct UUID lookup
            let r = self
                .run_store
                .get_run(&caller_tenant, &req.run_id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
                .ok_or_else(|| Status::not_found(format!("run {} not found", req.run_id)))?;
            // Verify run belongs to the requested repo
            if r.repo_id != req.repo_id {
                return Err(Status::not_found(format!("run {} not found", req.run_id)));
            }
            r
        } else {
            // Legacy run_number lookup (already repo-scoped via pipeline)
            let pipelines = self
                .pipeline_store
                .get_by_repo(&caller_tenant, &req.repo_id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get pipelines: {e}")))?;

            let pipeline = pipelines
                .first()
                .ok_or_else(|| Status::not_found("no pipeline found for this repo"))?;

            self.run_store
                .get_run_by_number(&caller_tenant, &pipeline.id, req.run_number)
                .await
                .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
                .ok_or_else(|| Status::not_found(format!("run #{} not found", req.run_number)))?
        };

        let run = &run;

        let steps = self
            .step_store
            .list_by_run(&caller_tenant, &run.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list steps: {e}")))?;

        let pipeline_name = self
            .pipeline_store
            .get_pipeline(&caller_tenant, &run.pipeline_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get pipeline: {e}")))?
            .map(|pipeline| pipeline.name);

        Ok(Response::new(PipelineRunResponse {
            run: Some(run_to_proto(run, pipeline_name.as_deref(), &steps)),
        }))
    }

    pub async fn list_pipeline_runs_impl(
        &self,
        request: Request<ListPipelineRunsRequest>,
    ) -> Result<Response<ListPipelineRunsResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        let state_filter = proto_to_run_state(req.state_filter);
        let limit = if req.limit == 0 {
            50
        } else {
            (req.limit as usize).min(100)
        };
        let offset = req.offset as usize;

        let runs = self
            .run_store
            .list_by_repo(&caller_tenant, &req.repo_id, state_filter, limit, offset)
            .await
            .map_err(|e| Status::internal(format!("Failed to list runs: {e}")))?;

        // For list operations, omit per-run steps to avoid N+1 queries.
        // Steps are only included on get_pipeline_run (single run lookup).
        let pipeline_names: HashMap<String, String> = self
            .pipeline_store
            .get_by_repo(&caller_tenant, &req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get pipelines: {e}")))?
            .into_iter()
            .map(|pipeline| (pipeline.id, pipeline.name))
            .collect();

        let proto_runs: Vec<muli_proto::PipelineRun> = runs
            .iter()
            .map(|run| {
                run_to_proto(
                    run,
                    pipeline_names.get(&run.pipeline_id).map(String::as_str),
                    &[],
                )
            })
            .collect();

        Ok(Response::new(ListPipelineRunsResponse {
            total: proto_runs.len() as u32,
            runs: proto_runs,
        }))
    }

    pub async fn cancel_pipeline_impl(
        &self,
        request: Request<CancelPipelineRequest>,
    ) -> Result<Response<CancelPipelineResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        let mut run = self
            .run_store
            .get_run(&caller_tenant, &req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
            .ok_or_else(|| Status::not_found(format!("run {} not found", req.run_id)))?;

        if run.tenant_id != caller_tenant {
            return Err(Status::not_found(format!("run {} not found", req.run_id)));
        }

        // Verify run belongs to the requested repo
        if !req.repo_id.is_empty() && run.repo_id != req.repo_id {
            return Err(Status::not_found(format!("run {} not found", req.run_id)));
        }

        if run.state.is_terminal() {
            return Err(Status::failed_precondition(format!(
                "run is already in terminal state: {}",
                run.state.as_str()
            )));
        }

        // Cancel all non-terminal steps
        let steps = self
            .step_store
            .list_by_run(&caller_tenant, &req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list steps: {e}")))?;

        for step in &steps {
            if !step.state.is_terminal() {
                self.step_store
                    .update_step_state(&caller_tenant, &step.id, DomainStepState::Cancelled)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to cancel step {}: {e}", step.step_name))
                    })?;
            }
        }

        // Cancel the run
        run.state = DomainRunState::Cancelled;
        run.finished_at = Some(Utc::now());
        run.updated_at = Utc::now();

        self.run_store
            .update_run(&run)
            .await
            .map_err(|e| Status::internal(format!("Failed to update run: {e}")))?;

        info!(
            operation = "cancel_pipeline",
            tenant_id = %caller_tenant,
            run_id = %req.run_id,
            cancelled_steps = steps.iter().filter(|s| !s.state.is_terminal()).count(),
            "audit: pipeline run cancelled"
        );

        Ok(Response::new(CancelPipelineResponse {}))
    }

    pub async fn retry_pipeline_impl(
        &self,
        request: Request<RetryPipelineRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        // Look up original run
        let original = self
            .run_store
            .get_run(&caller_tenant, &req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
            .ok_or_else(|| Status::not_found(format!("run {} not found", req.run_id)))?;

        if original.tenant_id != caller_tenant {
            return Err(Status::not_found(format!("run {} not found", req.run_id)));
        }

        // Verify run belongs to the requested repo
        if !req.repo_id.is_empty() && original.repo_id != req.repo_id {
            return Err(Status::not_found(format!("run {} not found", req.run_id)));
        }

        if !original.state.is_terminal() {
            return Err(Status::failed_precondition(format!(
                "can only retry terminal runs (current state: {})",
                original.state.as_str()
            )));
        }

        // Get next run number
        let run_number = self
            .run_store
            .next_run_number(&caller_tenant, &original.pipeline_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run number: {e}")))?;

        // Create new run
        let trigger = PipelineTrigger::Retry {
            original_run_id: original.id.clone(),
        };
        let mut new_run = DomainRun::new(
            original.pipeline_id.clone(),
            original.tenant_id.clone(),
            original.repo_id.clone(),
            run_number,
            original.commit_sha.clone(),
            original.ref_name.clone(),
            trigger,
            original.yaml_content.clone(),
        );
        new_run.env_vars = original.env_vars.clone();

        self.run_store
            .create_run(&new_run)
            .await
            .map_err(|e| Status::internal(format!("Failed to create retry run: {e}")))?;

        // Re-create steps from the original YAML
        let steps = if !original.yaml_content.is_empty() {
            create_steps_from_yaml(
                &original.yaml_content,
                &new_run.id,
                &caller_tenant,
                &self.step_store,
            )
            .await?
        } else {
            vec![]
        };

        // Spawn DAG execution in background (don't block the gRPC response)
        {
            let run_store = self.run_store.clone();
            let step_store = self.step_store.clone();
            let job_store = self.job_store.clone();
            let job_submitter = self.job_submitter.clone();
            let repo_store = self.repo_store.clone();
            let token_store = self.token_store.clone();
            let git_base_url = self.git_base_url.clone();
            let bg_repo_id = original.repo_id.clone();
            let bg_tenant_id = caller_tenant.clone();
            let mut bg_run = new_run.clone();
            let bg_steps = steps.clone();
            tokio::spawn(async move {
                let Ok(pipeline_def) =
                    muli_pipeline::yaml::parser::parse_pipeline(&bg_run.yaml_content)
                else {
                    tracing::warn!("Failed to parse retry pipeline YAML for DAG execution");
                    return;
                };

                // Generate a short-lived pull-only CI token for auto-checkout.
                let clone_url = build_ci_clone_url_for_repo(
                    repo_store.as_ref(),
                    token_store.as_ref(),
                    &git_base_url,
                    &bg_tenant_id,
                    &bg_repo_id,
                    &bg_run.id,
                    ci_clone_url_target(&pipeline_def),
                )
                .await;
                let ci_token_id = clone_url.as_ref().and_then(|(_, id)| id.clone());
                let clone_url_str = clone_url.map(|(url, _)| url);

                let executor = muli_pipeline::dag::executor::DagExecutor::new(
                    run_store,
                    step_store,
                    job_store,
                    job_submitter,
                );
                if let Err(e) = executor
                    .execute(
                        &mut bg_run,
                        &pipeline_def,
                        &bg_steps,
                        clone_url_str.as_deref(),
                    )
                    .await
                {
                    tracing::warn!(error = %e, "DAG executor failed for retry run");
                }

                // Revoke the CI token regardless of execution outcome.
                if let Some(id) = ci_token_id {
                    let _ = token_store.revoke_token(&id).await;
                }
            });
        }

        info!(
            operation = "retry_pipeline",
            tenant_id = %caller_tenant,
            original_run_id = %req.run_id,
            new_run_number = run_number,
            "audit: pipeline run retried"
        );

        let final_steps = self
            .step_store
            .list_by_run(&caller_tenant, &new_run.id)
            .await
            .unwrap_or_default();
        let pipeline_name = self
            .pipeline_store
            .get_pipeline(&caller_tenant, &new_run.pipeline_id)
            .await
            .ok()
            .flatten()
            .map(|pipeline| pipeline.name);

        Ok(Response::new(PipelineRunResponse {
            run: Some(run_to_proto(
                &new_run,
                pipeline_name.as_deref(),
                &final_steps,
            )),
        }))
    }
}

/// Generate a short-lived pull-only CI token and return `(clone_url, token_id)`.
/// Returns `None` on any failure (non-fatal; execution continues without checkout).
async fn build_ci_clone_url_for_repo(
    repo_store: &dyn muli_core::traits::RepositoryStore,
    token_store: &dyn muli_core::traits::GitTokenStore,
    git_base_url: &str,
    tenant_id: &str,
    repo_id: &str,
    run_id: &str,
    target: CiCloneUrlTarget,
) -> Option<(String, Option<String>)> {
    let repo = repo_store.get_repository(repo_id).await.ok()??;

    let plaintext = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let token_hash = tokio::task::spawn_blocking({
        let pt = plaintext.clone();
        move || token_hash::hash_token(&pt)
    })
    .await
    .ok()?
    .ok()?;

    let prefix = token_hash::token_prefix(&plaintext);
    let ci_token = GitToken {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant_id.to_string(),
        user_id: None,
        repo_id: Some(repo.id.clone()),
        token_hash,
        token_prefix: prefix,
        permissions: vec![GitPermission::Pull],
        description: format!("CI run {run_id}"),
        created_at: Utc::now(),
        expires_at: Some(Utc::now() + chrono::Duration::minutes(10)),
        revoked: false,
    };
    let token_id = ci_token.id.clone();
    token_store.create_token(&ci_token).await.ok()?;

    let clone_url = build_ci_clone_url(
        git_base_url,
        &plaintext,
        &repo.namespace,
        &repo.name,
        target,
    );

    Some((clone_url, Some(token_id)))
}

/// Look up the org ID for a repo's namespace. Returns `None` if the namespace
/// doesn't correspond to an organization (e.g. it's a user handle).
async fn resolve_org_id_for_repo(
    repo_store: &dyn muli_core::traits::RepositoryStore,
    _tenant_id: &str,
    repo_id: &str,
) -> Option<String> {
    // Return the repo namespace as org handle; the caller uses this for org secret lookup.
    let repo = repo_store.get_repository(repo_id).await.ok()??;
    Some(repo.namespace.clone())
}

fn ci_clone_url_target(
    pipeline_def: &muli_pipeline::yaml::schema::PipelineDef,
) -> CiCloneUrlTarget {
    if pipeline_def.jobs.is_empty() {
        CiCloneUrlTarget::ContainerClone
    } else {
        CiCloneUrlTarget::HostCheckout
    }
}
