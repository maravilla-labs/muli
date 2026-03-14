// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job query and streaming gRPC handlers.

use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::error;

use muli_core::job::state_machine::JobState;
use muli_core::validation;

use muli_proto::job_service_server::JobService;
use muli_proto::{
    CancelJobRequest, CancelJobResponse, DeleteJobRequest, DeleteJobResponse,
    GetDetailedJobStatusRequest, GetDetailedJobStatusResponse, GetJobStatusRequest,
    GetJobStatusResponse, JobStatusEvent, JobSummary, ListJobsRequest, ListJobsResponse,
    SubmitJobRequest, SubmitJobResponse, WatchJobStatusRequest,
};

use super::conversions::{core_state_to_proto, core_tier_to_proto, proto_state_to_core};
use super::job_service::{
    JobServiceImpl, MAX_WATCH_DURATION, opt_datetime_to_proto, verify_job_ownership,
};
use super::util::{datetime_to_proto, extract_tenant_id};

#[tonic::async_trait]
impl JobService for JobServiceImpl {
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        self.handle_submit_job(request).await
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        self.handle_get_job_status(request).await
    }

    async fn get_detailed_job_status(
        &self,
        request: Request<GetDetailedJobStatusRequest>,
    ) -> Result<Response<GetDetailedJobStatusResponse>, Status> {
        self.handle_get_detailed_job_status(request).await
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        self.handle_cancel_job(request).await
    }

    async fn delete_job(
        &self,
        request: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        self.handle_delete_job(request).await
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if let Some(ref tid) = req.tenant_id
            && !tid.is_empty()
        {
            validation::validate_identifier("tenant_id", tid)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            if *tid != caller_tenant {
                return Err(Status::permission_denied(
                    "cannot list jobs for a different tenant",
                ));
            }
        }

        let state_filter = if req.state_filter.is_some() {
            proto_state_to_core(req.state_filter.unwrap())
        } else {
            None
        };

        let tenant_id = Some(caller_tenant.as_str());
        let limit = validation::cap_limit(req.limit, 1000) as usize;
        let offset = req.offset as usize;

        let jobs = self
            .store
            .list_jobs(state_filter, tenant_id, limit, offset)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?;

        let total_count = self
            .store
            .count_jobs(state_filter, tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?;

        let summaries: Vec<JobSummary> = jobs
            .into_iter()
            .map(|j| JobSummary {
                job_id: j.id,
                job_name: j.name,
                deployment_id: j.spec.deployment_id,
                project_id: j.spec.project_id,
                tenant_id: j.spec.tenant_id,
                state: core_state_to_proto(j.state),
                priority_tier: core_tier_to_proto(j.spec.priority_tier),
                framework: j.spec.framework,
                created_at: Some(datetime_to_proto(j.created_at)),
                started_at: opt_datetime_to_proto(j.started_at),
                finished_at: opt_datetime_to_proto(j.finished_at),
            })
            .collect();

        Ok(Response::new(ListJobsResponse {
            jobs: summaries,
            total_count: total_count as u32,
        }))
    }

    type WatchJobStatusStream = ReceiverStream<Result<JobStatusEvent, Status>>;

    async fn watch_job_status(
        &self,
        request: Request<WatchJobStatusRequest>,
    ) -> Result<Response<Self::WatchJobStatusStream>, Status> {
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

        let store = self.store.clone();
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut last_state: Option<JobState> = None;
            let deadline = tokio::time::Instant::now() + MAX_WATCH_DURATION;
            let mut consecutive_errors: u32 = 0;
            const MAX_CONSECUTIVE_ERRORS: u32 = 3;

            loop {
                if tokio::time::Instant::now() >= deadline {
                    let _ = tx
                        .send(Ok(JobStatusEvent {
                            job_id: job_id.clone(),
                            previous_state: last_state.map(core_state_to_proto).unwrap_or(0),
                            current_state: last_state.map(core_state_to_proto).unwrap_or(0),
                            message: "Watch stream closed: max duration reached".to_string(),
                            timestamp: Some(datetime_to_proto(Utc::now())),
                        }))
                        .await;
                    break;
                }

                match store.get_job(&job_id).await {
                    Ok(Some(job)) => {
                        consecutive_errors = 0;
                        let current_state = job.state;
                        let should_send = match last_state {
                            Some(prev) => prev != current_state,
                            None => true,
                        };

                        if should_send {
                            let event = JobStatusEvent {
                                job_id: job_id.clone(),
                                previous_state: last_state.map(core_state_to_proto).unwrap_or(0),
                                current_state: core_state_to_proto(current_state),
                                message: job
                                    .result
                                    .as_ref()
                                    .map(|r| r.message.clone())
                                    .unwrap_or_default(),
                                timestamp: Some(datetime_to_proto(Utc::now())),
                            };

                            if tx.send(Ok(event)).await.is_err() {
                                break;
                            }

                            last_state = Some(current_state);

                            if current_state.is_terminal() {
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        let _ = tx
                            .send(Err(Status::not_found(format!("Job not found: {job_id}"))))
                            .await;
                        break;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        error!(
                            job_id = %job_id,
                            error = %e,
                            consecutive_errors,
                            "Error watching job status"
                        );
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            let _ = tx
                                .send(Err(Status::internal(format!(
                                    "Store unavailable after {consecutive_errors} consecutive errors: {e}"
                                ))))
                                .await;
                            break;
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
