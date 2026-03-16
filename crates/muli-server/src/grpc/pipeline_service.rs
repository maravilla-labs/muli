// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline CI/CD gRPC service.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use chrono::Utc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use muli_core::pipeline::{
    Artifact, CacheEntry, FailureStrategy as DomainFailureStrategy, Pipeline,
    PipelineRun as DomainRun, PipelineRunState as DomainRunState, PipelineTrigger,
    StepRun as DomainStep, StepRunState as DomainStepState,
};
use muli_core::traits::{
    ArtifactStore, CacheStore, JobLogStore, JobStore, PipelineRunStore, PipelineSecretStore,
    PipelineStore, StepRunStore,
};
use muli_pipeline::dag::executor::JobSubmitter;

use muli_proto::pipeline_service_server::PipelineService;
use muli_proto::{
    ArtifactChunk, CancelPipelineRequest, CancelPipelineResponse, DeleteCacheRequest,
    DeleteCacheResponse, DownloadArtifactRequest, FailureStrategy as ProtoFailureStrategy,
    GetPipelineConfigRequest, GetPipelineConfigResponse, GetPipelineRunRequest,
    GetStepLogsRequest, GetStepLogsResponse, ListArtifactsRequest, ListArtifactsResponse,
    ListCachesRequest, ListCachesResponse, ListPipelineRunsRequest, ListPipelineRunsResponse,
    LogLine, PipelineArtifact, PipelineCache, PipelineRun, PipelineRunEvent,
    PipelineRunResponse, PipelineRunState, PipelineStepState, PipelineTriggerType,
    RetryPipelineRequest, StepRun, StreamStepLogsRequest, TriggerPipelineRequest,
    WatchPipelineRunRequest,
};

use super::util::{datetime_to_proto, extract_tenant_id};

pub struct PipelineServiceImpl {
    pub pipeline_store: Arc<dyn PipelineStore>,
    pub run_store: Arc<dyn PipelineRunStore>,
    pub step_store: Arc<dyn StepRunStore>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub cache_store: Arc<dyn CacheStore>,
    pub secret_store: Arc<dyn PipelineSecretStore>,
    pub job_store: Arc<dyn JobStore>,
    pub job_log_store: Arc<dyn JobLogStore>,
    pub job_submitter: Arc<dyn JobSubmitter>,
}

// --- Domain → Proto conversions ---

fn run_state_to_proto(s: DomainRunState) -> i32 {
    match s {
        DomainRunState::Pending => PipelineRunState::Pending as i32,
        DomainRunState::Running => PipelineRunState::Running as i32,
        DomainRunState::Succeeded => PipelineRunState::Succeeded as i32,
        DomainRunState::Failed => PipelineRunState::Failed as i32,
        DomainRunState::Cancelled => PipelineRunState::Cancelled as i32,
        DomainRunState::Degraded => PipelineRunState::Degraded as i32,
    }
}

fn proto_to_run_state(v: i32) -> Option<DomainRunState> {
    match PipelineRunState::try_from(v) {
        Ok(PipelineRunState::Pending) => Some(DomainRunState::Pending),
        Ok(PipelineRunState::Running) => Some(DomainRunState::Running),
        Ok(PipelineRunState::Succeeded) => Some(DomainRunState::Succeeded),
        Ok(PipelineRunState::Failed) => Some(DomainRunState::Failed),
        Ok(PipelineRunState::Cancelled) => Some(DomainRunState::Cancelled),
        Ok(PipelineRunState::Degraded) => Some(DomainRunState::Degraded),
        _ => None,
    }
}

fn step_state_to_proto(s: DomainStepState) -> i32 {
    match s {
        DomainStepState::Pending => PipelineStepState::Pending as i32,
        DomainStepState::Ready => PipelineStepState::Ready as i32,
        DomainStepState::Running => PipelineStepState::Running as i32,
        DomainStepState::Succeeded => PipelineStepState::Succeeded as i32,
        DomainStepState::Failed => PipelineStepState::Failed as i32,
        DomainStepState::Skipped => PipelineStepState::Skipped as i32,
        DomainStepState::Cancelled => PipelineStepState::Cancelled as i32,
    }
}

fn trigger_to_proto(t: &PipelineTrigger) -> i32 {
    match t {
        PipelineTrigger::Push { .. } => PipelineTriggerType::Push as i32,
        PipelineTrigger::PullRequest { .. } => PipelineTriggerType::PullRequest as i32,
        PipelineTrigger::Manual { .. } => PipelineTriggerType::Manual as i32,
        PipelineTrigger::Schedule { .. } => PipelineTriggerType::Schedule as i32,
        PipelineTrigger::Retry { .. } => PipelineTriggerType::Retry as i32,
    }
}

fn failure_strategy_to_proto(s: DomainFailureStrategy) -> i32 {
    match s {
        DomainFailureStrategy::Stop => ProtoFailureStrategy::Stop as i32,
        DomainFailureStrategy::Continue => ProtoFailureStrategy::Continue as i32,
        DomainFailureStrategy::Ignore => ProtoFailureStrategy::Ignore as i32,
    }
}

fn step_to_proto(s: &DomainStep) -> StepRun {
    StepRun {
        id: s.id.clone(),
        run_id: s.run_id.clone(),
        step_name: s.step_name.clone(),
        job_id: s.job_id.clone().unwrap_or_default(),
        state: step_state_to_proto(s.state),
        matrix_values: s
            .matrix_values
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default(),
        failure_strategy: failure_strategy_to_proto(s.failure_strategy),
        started_at: s.started_at.map(datetime_to_proto),
        finished_at: s.finished_at.map(datetime_to_proto),
    }
}

fn run_to_proto(r: &DomainRun, steps: &[DomainStep]) -> PipelineRun {
    PipelineRun {
        id: r.id.clone(),
        pipeline_id: r.pipeline_id.clone(),
        tenant_id: r.tenant_id.clone(),
        repo_id: r.repo_id.clone(),
        run_number: r.run_number,
        commit_sha: r.commit_sha.clone(),
        ref_name: r.ref_name.clone(),
        trigger_type: trigger_to_proto(&r.trigger),
        state: run_state_to_proto(r.state),
        steps: steps.iter().map(step_to_proto).collect(),
        created_at: Some(datetime_to_proto(r.created_at)),
        started_at: r.started_at.map(datetime_to_proto),
        finished_at: r.finished_at.map(datetime_to_proto),
    }
}

fn artifact_to_proto(a: &Artifact) -> PipelineArtifact {
    PipelineArtifact {
        id: a.id.clone(),
        run_id: a.run_id.clone(),
        step_name: a.step_name.clone(),
        name: a.name.clone(),
        size_bytes: a.size_bytes,
        sha256: a.sha256.clone(),
        expires_at: a.expires_at.map(datetime_to_proto),
        created_at: Some(datetime_to_proto(a.created_at)),
    }
}

fn cache_to_proto(c: &CacheEntry) -> PipelineCache {
    PipelineCache {
        id: c.id.clone(),
        repo_id: c.repo_id.clone(),
        cache_key: c.cache_key.clone(),
        size_bytes: c.size_bytes,
        last_used_at: Some(datetime_to_proto(c.last_used_at)),
        created_at: Some(datetime_to_proto(c.created_at)),
    }
}

/// Parse YAML, expand matrix, create StepRun records for a run.
async fn create_steps_from_yaml(
    yaml_content: &str,
    run_id: &str,
    tenant_id: &str,
    step_store: &Arc<dyn StepRunStore>,
) -> Result<Vec<DomainStep>, Status> {
    let pipeline_def = muli_pipeline::yaml::parser::parse_pipeline(yaml_content)
        .map_err(|e| Status::invalid_argument(format!("Invalid pipeline YAML: {e}")))?;

    muli_pipeline::yaml::validation::validate_pipeline(&pipeline_def)
        .map_err(|e| Status::invalid_argument(format!("Pipeline validation failed: {e}")))?;

    let mut all_steps = Vec::new();
    for step_def in &pipeline_def.steps {
        let expanded = muli_pipeline::dag::matrix::expand_matrix(step_def)
            .map_err(|e| Status::invalid_argument(format!("Matrix expansion failed: {e}")))?;

        for es in expanded {
            let failure_strategy = match es.failure_strategy.as_deref() {
                Some("continue") => DomainFailureStrategy::Continue,
                Some("ignore") => DomainFailureStrategy::Ignore,
                _ => DomainFailureStrategy::Stop,
            };
            let step = DomainStep::new(
                run_id.to_string(),
                tenant_id.to_string(),
                es.name.clone(),
                failure_strategy,
                None,
            );
            step_store
                .create_step(&step)
                .await
                .map_err(|e| Status::internal(format!("Failed to create step: {e}")))?;
            all_steps.push(step);
        }
    }
    Ok(all_steps)
}

/// Resolve muli-native pipeline secrets and merge with caller env_vars.
async fn resolve_env_vars(
    secret_store: &Arc<dyn PipelineSecretStore>,
    repo_id: &str,
    caller_env_vars: HashMap<String, String>,
) -> Result<HashMap<String, String>, Status> {
    let mut env = HashMap::new();

    // Muli's own pipeline secrets (from PipelineSecretStore)
    let secret_names = secret_store
        .list_names(repo_id)
        .await
        .map_err(|e| Status::internal(format!("Failed to list secrets: {e}")))?;

    for name in secret_names {
        if let Ok(Some(_secret)) = secret_store.get_secret(repo_id, &name).await {
            // SECURITY: muli-native secrets are AES-256-GCM encrypted at rest.
            // Decryption requires `pipeline_secret_encryption_key` from server config,
            // which is not yet wired into this code path. Skip these secrets rather
            // than injecting raw ciphertext into step environments.
            warn!(
                secret_name = %name,
                repo_id = %repo_id,
                "skipping muli-native secret: decryption not yet implemented"
            );
        }
    }

    // Caller-provided env_vars override muli-native secrets
    env.extend(caller_env_vars);
    Ok(env)
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl PipelineService for PipelineServiceImpl {
    type StreamStepLogsStream = BoxStream<LogLine>;
    type DownloadArtifactStream = BoxStream<ArtifactChunk>;
    type WatchPipelineRunStream = BoxStream<PipelineRunEvent>;

    async fn trigger_pipeline(
        &self,
        request: Request<TriggerPipelineRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if req.branch.is_empty() && req.commit_sha.is_empty() {
            return Err(Status::invalid_argument(
                "branch or commit_sha is required",
            ));
        }

        // Find or create pipeline for this repo
        let pipelines = self
            .pipeline_store
            .get_by_repo(&req.repo_id)
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

        // Resolve env vars (muli secrets + caller env_vars)
        let env_vars =
            resolve_env_vars(&self.secret_store, &req.repo_id, req.env_vars).await?;

        // Get next run number
        let run_number = self
            .run_store
            .next_run_number(&pipeline.id)
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
            run: Some(run_to_proto(&run, &[])),
        }))
    }

    async fn get_pipeline_run(
        &self,
        request: Request<GetPipelineRunRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        // Look up the pipeline for this repo to get the pipeline_id
        let pipelines = self
            .pipeline_store
            .get_by_repo(&req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get pipelines: {e}")))?;

        let pipeline = pipelines
            .first()
            .ok_or_else(|| Status::not_found("no pipeline found for this repo"))?;

        let run = self
            .run_store
            .get_run_by_number(&pipeline.id, req.run_number)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
            .ok_or_else(|| Status::not_found(format!("run #{} not found", req.run_number)))?;

        if run.tenant_id != caller_tenant {
            return Err(Status::not_found(format!("run #{} not found", req.run_number)));
        }

        let run = &run;

        let steps = self
            .step_store
            .list_by_run(&run.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list steps: {e}")))?;

        Ok(Response::new(PipelineRunResponse {
            run: Some(run_to_proto(run, &steps)),
        }))
    }

    async fn list_pipeline_runs(
        &self,
        request: Request<ListPipelineRunsRequest>,
    ) -> Result<Response<ListPipelineRunsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        let state_filter = proto_to_run_state(req.state_filter);
        let limit = if req.limit == 0 { 50 } else { (req.limit as usize).min(100) };
        let offset = req.offset as usize;

        let runs = self
            .run_store
            .list_by_repo(&req.repo_id, state_filter, limit, offset)
            .await
            .map_err(|e| Status::internal(format!("Failed to list runs: {e}")))?;

        // For list operations, omit per-run steps to avoid N+1 queries.
        // Steps are only included on get_pipeline_run (single run lookup).
        let proto_runs: Vec<PipelineRun> = runs.iter().map(|run| run_to_proto(run, &[])).collect();

        Ok(Response::new(ListPipelineRunsResponse {
            total: proto_runs.len() as u32,
            runs: proto_runs,
        }))
    }

    async fn cancel_pipeline(
        &self,
        request: Request<CancelPipelineRequest>,
    ) -> Result<Response<CancelPipelineResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        let mut run = self
            .run_store
            .get_run(&req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
            .ok_or_else(|| Status::not_found(format!("run {} not found", req.run_id)))?;

        if run.tenant_id != caller_tenant {
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
            .list_by_run(&req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list steps: {e}")))?;

        for step in &steps {
            if !step.state.is_terminal() {
                self.step_store
                    .update_step_state(&step.id, DomainStepState::Cancelled)
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

    async fn retry_pipeline(
        &self,
        request: Request<RetryPipelineRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        // Look up original run
        let original = self
            .run_store
            .get_run(&req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
            .ok_or_else(|| Status::not_found(format!("run {} not found", req.run_id)))?;

        if original.tenant_id != caller_tenant {
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
            .next_run_number(&original.pipeline_id)
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
            create_steps_from_yaml(&original.yaml_content, &new_run.id, &caller_tenant, &self.step_store).await?
        } else {
            vec![]
        };

        // Spawn DAG execution in background (don't block the gRPC response)
        {
            let run_store = self.run_store.clone();
            let step_store = self.step_store.clone();
            let job_store = self.job_store.clone();
            let job_submitter = self.job_submitter.clone();
            let mut bg_run = new_run.clone();
            let bg_steps = steps.clone();
            tokio::spawn(async move {
                let executor = muli_pipeline::dag::executor::DagExecutor::new(
                    run_store, step_store, job_store, job_submitter,
                );
                // Re-parse YAML for the executor
                if let Ok(pipeline_def) =
                    muli_pipeline::yaml::parser::parse_pipeline(&bg_run.yaml_content)
                {
                    if let Err(e) = executor
                        .execute(&mut bg_run, &pipeline_def, &bg_steps, None)
                        .await
                    {
                        tracing::warn!(error = %e, "DAG executor failed for retry run");
                    }
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
            .list_by_run(&new_run.id)
            .await
            .unwrap_or_default();

        Ok(Response::new(PipelineRunResponse {
            run: Some(run_to_proto(&new_run, &final_steps)),
        }))
    }

    async fn get_step_logs(
        &self,
        request: Request<GetStepLogsRequest>,
    ) -> Result<Response<GetStepLogsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        // Find the step by run_id + step_name
        let steps = self
            .step_store
            .list_by_run(&req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list steps: {e}")))?;

        let step = steps
            .iter()
            .find(|s| s.step_name == req.step_name)
            .ok_or_else(|| {
                Status::not_found(format!("step '{}' not found in run", req.step_name))
            })?;

        // Get the job_id from the step
        let job_id = step.job_id.as_ref().ok_or_else(|| {
            Status::not_found(format!(
                "step '{}' has not been submitted as a job yet",
                req.step_name
            ))
        })?;

        // Fetch logs from the job log store
        let tail = if req.tail == 0 { 1000 } else { req.tail as usize };
        let stored_logs = self
            .job_log_store
            .get_logs(job_id, tail)
            .await
            .map_err(|e| Status::internal(format!("Failed to get logs: {e}")))?;

        let lines = stored_logs
            .into_iter()
            .map(|l| LogLine {
                line: l.message,
                stream: l.stream,
                timestamp_ms: l.timestamp.timestamp_millis(),
            })
            .collect();

        Ok(Response::new(GetStepLogsResponse { lines }))
    }

    async fn stream_step_logs(
        &self,
        _request: Request<StreamStepLogsRequest>,
    ) -> Result<Response<Self::StreamStepLogsStream>, Status> {
        Err(Status::unimplemented(
            "StreamStepLogs requires log streaming integration",
        ))
    }

    async fn list_artifacts(
        &self,
        request: Request<ListArtifactsRequest>,
    ) -> Result<Response<ListArtifactsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        let artifacts = self
            .artifact_store
            .list_by_run(&req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list artifacts: {e}")))?;

        Ok(Response::new(ListArtifactsResponse {
            artifacts: artifacts.iter().map(artifact_to_proto).collect(),
        }))
    }

    async fn download_artifact(
        &self,
        _request: Request<DownloadArtifactRequest>,
    ) -> Result<Response<Self::DownloadArtifactStream>, Status> {
        Err(Status::unimplemented(
            "DownloadArtifact requires blob storage integration",
        ))
    }

    async fn list_caches(
        &self,
        request: Request<ListCachesRequest>,
    ) -> Result<Response<ListCachesResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        let caches = self
            .cache_store
            .list_by_repo(&req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list caches: {e}")))?;

        Ok(Response::new(ListCachesResponse {
            caches: caches.iter().map(cache_to_proto).collect(),
        }))
    }

    async fn delete_cache(
        &self,
        request: Request<DeleteCacheRequest>,
    ) -> Result<Response<DeleteCacheResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if req.cache_key.is_empty() {
            return Err(Status::invalid_argument("cache_key is required"));
        }

        self.cache_store
            .delete_cache(&req.repo_id, &req.cache_key)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete cache: {e}")))?;

        info!(
            operation = "delete_cache",
            tenant_id = %caller_tenant,
            repo_id = %req.repo_id,
            cache_key = %req.cache_key,
            "audit: pipeline cache deleted"
        );

        Ok(Response::new(DeleteCacheResponse {}))
    }

    async fn watch_pipeline_run(
        &self,
        _request: Request<WatchPipelineRunRequest>,
    ) -> Result<Response<Self::WatchPipelineRunStream>, Status> {
        Err(Status::unimplemented(
            "WatchPipelineRun requires event streaming integration",
        ))
    }

    async fn get_pipeline_config(
        &self,
        _request: Request<GetPipelineConfigRequest>,
    ) -> Result<Response<GetPipelineConfigResponse>, Status> {
        Err(Status::unimplemented(
            "GetPipelineConfig requires git repository access",
        ))
    }
}
