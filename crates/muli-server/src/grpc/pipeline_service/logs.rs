// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Step log RPCs: get_step_logs, stream_step_logs.

use tonic::{Request, Response, Status};

use muli_proto::{GetStepLogsRequest, GetStepLogsResponse, LogLine, StreamStepLogsRequest};

use super::PipelineServiceImpl;
use crate::grpc::util::validate_tenant;

impl PipelineServiceImpl {
    pub async fn get_step_logs_impl(
        &self,
        request: Request<GetStepLogsRequest>,
    ) -> Result<Response<GetStepLogsResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        // Verify run belongs to the requested repo
        if !req.repo_id.is_empty() {
            let run = self
                .run_store
                .get_run(&caller_tenant, &req.run_id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
                .ok_or_else(|| Status::not_found(format!("run {} not found", req.run_id)))?;
            if run.repo_id != req.repo_id {
                return Err(Status::not_found(format!("run {} not found", req.run_id)));
            }
        }

        // Find the step by run_id + step_name
        let steps = self
            .step_store
            .list_by_run(&caller_tenant, &req.run_id)
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
        let tail = if req.tail == 0 {
            1000
        } else {
            req.tail as usize
        };
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

    pub async fn stream_step_logs_impl(
        &self,
        _request: Request<StreamStepLogsRequest>,
    ) -> Result<Response<super::BoxStream<LogLine>>, Status> {
        Err(Status::unimplemented(
            "StreamStepLogs requires log streaming integration",
        ))
    }
}
