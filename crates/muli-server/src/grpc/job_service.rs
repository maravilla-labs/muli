// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job management gRPC service.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::job::model::{self as core_model, Job, JobSpec};
use muli_core::job::state_machine::JobState;
use muli_core::resource::limits::ResourceSpec;
use muli_core::traits::{JobStore, TenantLimitsStore};
use muli_core::validation;
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_queue::Scheduler;

use muli_proto::{
    CancelJobRequest, CancelJobResponse, DeleteJobRequest, DeleteJobResponse,
    GetDetailedJobStatusRequest, GetDetailedJobStatusResponse, GetJobStatusRequest,
    GetJobStatusResponse, SubmitJobRequest, SubmitJobResponse, Timestamp,
};

use super::conversions::core_state_to_proto;
use super::util::{datetime_to_proto, extract_tenant_id, validate_tenant};

/// Maximum duration a watch_job_status stream can stay open (1 hour).
pub(crate) const MAX_WATCH_DURATION: Duration = Duration::from_secs(3600);

pub struct JobServiceImpl {
    pub store: Arc<dyn JobStore>,
    pub scheduler: Arc<Scheduler>,
    pub executor: Arc<DockerExecutor>,
    pub log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    pub tenant_limits_store: Option<Arc<dyn TenantLimitsStore>>,
    pub max_jobs_per_tenant: usize,
}

/// Verify that a job belongs to the requesting tenant.
pub(crate) fn verify_job_ownership(job: &Job, tenant_id: &str) -> Result<(), Status> {
    if job.spec.tenant_id != tenant_id {
        return Err(Status::permission_denied(format!(
            "job {} does not belong to tenant {}",
            job.id, tenant_id
        )));
    }
    Ok(())
}

pub(crate) fn opt_datetime_to_proto(dt: Option<DateTime<Utc>>) -> Option<Timestamp> {
    dt.map(datetime_to_proto)
}

/// Command handlers: submit, get_status, get_detailed_status, cancel, delete.
impl JobServiceImpl {
    pub(crate) async fn handle_submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        // Enforcement checks
        crate::enforcement::check_not_suspended(
            self.tenant_limits_store.as_deref(),
            &req.tenant_id,
        )
        .await?;
        crate::enforcement::check_job_limit(
            self.tenant_limits_store.as_deref(),
            &*self.store,
            &req.tenant_id,
            self.max_jobs_per_tenant,
        )
        .await?;

        validation::validate_identifier("tenant_id", &req.tenant_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        validation::validate_identifier("project_id", &req.project_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        validation::validate_identifier("workspace_id", &req.workspace_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        validation::validate_identifier("deployment_id", &req.deployment_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        validation::validate_docker_image("runner_image", &req.runner_image)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        for ev in &req.env_vars {
            validation::validate_env_var_name(&ev.name)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        validation::validate_max_length("framework", &req.framework, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        if !req.idempotency_key.is_empty() {
            validation::validate_max_length("idempotency_key", &req.idempotency_key, 256)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        if let Some(ref r) = req.resources {
            validation::validate_resource_spec("cpu_request", &r.cpu_request)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            validation::validate_resource_spec("cpu_limit", &r.cpu_limit)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            validation::validate_resource_spec("memory_request", &r.memory_request)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            validation::validate_resource_spec("memory_limit", &r.memory_limit)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        let tier = crate::enforcement::resolve_effective_tier(
            self.tenant_limits_store.as_deref(),
            &req.tenant_id,
            req.priority_tier,
        )
        .await;

        if !req.idempotency_key.is_empty() {
            match self
                .store
                .find_by_idempotency_key(&req.tenant_id, &req.idempotency_key)
                .await
            {
                Ok(Some(existing)) => {
                    return Ok(Response::new(SubmitJobResponse {
                        job_id: existing.id,
                        job_name: existing.name,
                        state: core_state_to_proto(existing.state),
                    }));
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(Status::internal(format!("Idempotency check failed: {e}")));
                }
            }
        }

        let env_vars: Vec<core_model::EnvVar> = req
            .env_vars
            .into_iter()
            .map(|e| core_model::EnvVar {
                name: e.name,
                value: e.value,
            })
            .collect();

        let resources = match req.resources {
            Some(r) => ResourceSpec {
                cpu_request: r.cpu_request,
                cpu_limit: r.cpu_limit,
                memory_request: r.memory_request,
                memory_limit: r.memory_limit,
                timeout_seconds: r.timeout_seconds,
            },
            None => ResourceSpec {
                cpu_request: "500m".to_string(),
                cpu_limit: "1000m".to_string(),
                memory_request: "512Mi".to_string(),
                memory_limit: "1Gi".to_string(),
                timeout_seconds: 1800,
            },
        };

        let registry_credentials =
            req.registry_credentials
                .map(|rc| core_model::RegistryCredentials {
                    server: rc.server,
                    username: rc.username,
                    password: rc.password,
                });

        let spec = JobSpec {
            deployment_id: req.deployment_id,
            project_id: req.project_id,
            workspace_id: req.workspace_id,
            tenant_id: req.tenant_id.clone(),
            runner_image: req.runner_image,
            env_vars,
            resources,
            priority_tier: tier,
            framework: req.framework,
            idempotency_key: if req.idempotency_key.is_empty() {
                None
            } else {
                Some(req.idempotency_key)
            },
            registry_credentials,
            commands: vec![],
            substeps: vec![],
            checkout: None,
            artifact_downloads: vec![],
            artifact_upload_paths: vec![],
            artifact_upload_key: None,
            pipeline_step_run_id: None,
        };

        let job = Job::new(spec);
        let job_id = job.id.clone();
        let job_name = job.name.clone();
        let tenant_id = req.tenant_id;

        if let Err(create_err) = self.store.create_job(&job).await {
            // If we have an idempotency key, a concurrent request may have inserted
            // the same job between our check and this insert. Re-check the key and
            // return the winning job if it now exists, making the operation idempotent.
            if job.spec.idempotency_key.is_some() {
                if let Ok(Some(existing)) = self
                    .store
                    .find_by_idempotency_key(
                        &tenant_id,
                        job.spec.idempotency_key.as_deref().unwrap(),
                    )
                    .await
                {
                    return Ok(Response::new(SubmitJobResponse {
                        job_id: existing.id,
                        job_name: existing.name,
                        state: core_state_to_proto(existing.state),
                    }));
                }
            }
            return Err(Status::internal(format!(
                "Failed to create job: {create_err}"
            )));
        }

        self.scheduler
            .enqueue(job_id.clone(), tier, tenant_id.clone());

        crate::metrics::JOBS_SUBMITTED_TOTAL
            .with_label_values(&[&tenant_id, &format!("{tier:?}")])
            .inc();

        info!(
            operation = "submit_job",
            job_id = %job_id,
            job_name = %job_name,
            tenant_id = %tenant_id,
            result = "success",
            "audit: job submitted"
        );

        Ok(Response::new(SubmitJobResponse {
            job_id,
            job_name,
            state: core_state_to_proto(JobState::Pending),
        }))
    }

    pub(crate) async fn handle_get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let job_id = request.into_inner().job_id;

        validation::validate_non_empty("job_id", &job_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let job = self
            .store
            .get_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?
            .ok_or_else(|| Status::not_found(format!("Job not found: {job_id}")))?;

        verify_job_ownership(&job, &caller_tenant)?;

        let message = job
            .result
            .as_ref()
            .map(|r| r.message.clone())
            .unwrap_or_default();

        Ok(Response::new(GetJobStatusResponse {
            job_id: job.id,
            state: core_state_to_proto(job.state),
            message,
        }))
    }

    pub(crate) async fn handle_get_detailed_job_status(
        &self,
        request: Request<GetDetailedJobStatusRequest>,
    ) -> Result<Response<GetDetailedJobStatusResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let job_id = request.into_inner().job_id;

        validation::validate_non_empty("job_id", &job_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let job = self
            .store
            .get_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?
            .ok_or_else(|| Status::not_found(format!("Job not found: {job_id}")))?;

        verify_job_ownership(&job, &caller_tenant)?;

        let (container_id, exit_code) = match &job.result {
            Some(r) => (r.container_id.clone().unwrap_or_default(), r.exit_code),
            None => (String::new(), None),
        };

        let (pod_phase, container_state) = match job.state {
            JobState::Pending | JobState::Scheduled => {
                ("Pending".to_string(), "waiting".to_string())
            }
            JobState::Pulling => ("Pending".to_string(), "waiting".to_string()),
            JobState::Running => ("Running".to_string(), "running".to_string()),
            JobState::Succeeded => ("Succeeded".to_string(), "terminated".to_string()),
            JobState::Failed => ("Failed".to_string(), "terminated".to_string()),
            JobState::Cancelled => ("Failed".to_string(), "terminated".to_string()),
            JobState::TimedOut => ("Failed".to_string(), "terminated".to_string()),
        };

        Ok(Response::new(GetDetailedJobStatusResponse {
            job_id: job.id,
            state: core_state_to_proto(job.state),
            container_id,
            pod_phase,
            pod_reason: String::new(),
            pod_message: String::new(),
            container_state,
            container_reason: String::new(),
            container_message: job
                .result
                .as_ref()
                .map(|r| r.message.clone())
                .unwrap_or_default(),
            exit_code,
            started_at: opt_datetime_to_proto(job.started_at),
            finished_at: opt_datetime_to_proto(job.finished_at),
            restart_count: job.retry_count,
        }))
    }

    pub(crate) async fn handle_cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();
        let job_id = req.job_id;

        validation::validate_non_empty("job_id", &job_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let job = self
            .store
            .get_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?
            .ok_or_else(|| Status::not_found(format!("Job not found: {job_id}")))?;

        verify_job_ownership(&job, &caller_tenant)?;

        if job.state.is_terminal() {
            return Ok(Response::new(CancelJobResponse {
                success: false,
                message: format!("Job is already in terminal state: {}", job.state),
            }));
        }

        self.scheduler.cancel(&job_id);

        self.store
            .update_state(&job_id, job.state, JobState::Cancelled)
            .await
            .map_err(|e| Status::internal(format!("Failed to cancel job: {e}")))?;

        info!(
            operation = "cancel_job",
            job_id = %job_id,
            tenant_id = %caller_tenant,
            result = "success",
            "audit: job cancelled"
        );

        Ok(Response::new(CancelJobResponse {
            success: true,
            message: "Job cancelled".to_string(),
        }))
    }

    pub(crate) async fn handle_delete_job(
        &self,
        request: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let job_id = request.into_inner().job_id;

        validation::validate_non_empty("job_id", &job_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let job = self
            .store
            .get_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?;

        match &job {
            Some(j) => verify_job_ownership(j, &caller_tenant)?,
            None => return Ok(Response::new(DeleteJobResponse { success: true })),
        }

        self.log_collectors.remove(&job_id);

        self.store
            .delete_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete job: {e}")))?;

        info!(
            operation = "delete_job",
            job_id = %job_id,
            tenant_id = %caller_tenant,
            result = "success",
            "audit: job deleted"
        );

        Ok(Response::new(DeleteJobResponse { success: true }))
    }
}
