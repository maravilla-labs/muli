// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Step log RPCs: get_step_logs, stream_step_logs.

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use muli_proto::{GetStepLogsRequest, GetStepLogsResponse, LogLine, StreamStepLogsRequest};

use super::PipelineServiceImpl;
use crate::grpc::util::validate_tenant;

fn proto_log_line(
    line: String,
    stream: String,
    timestamp_ms: i64,
    substep_name: Option<String>,
    event_type: Option<String>,
    exit_code: Option<i32>,
) -> LogLine {
    LogLine {
        line,
        stream,
        timestamp_ms,
        substep_name: substep_name.unwrap_or_default(),
        event_type: event_type.unwrap_or_else(|| "line".to_string()),
        exit_code: exit_code.unwrap_or_default(),
    }
}

impl PipelineServiceImpl {
    async fn resolve_step_job_id(
        &self,
        tenant_id: &str,
        run_id: &str,
        step_name: &str,
    ) -> Result<String, Status> {
        let _run = self
            .run_store
            .get_run(tenant_id, run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get run: {e}")))?
            .ok_or_else(|| Status::not_found(format!("run {run_id} not found")))?;

        let steps = self
            .step_store
            .list_by_run(tenant_id, run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list steps: {e}")))?;

        let step = steps
            .iter()
            .find(|s| s.step_name == step_name)
            .ok_or_else(|| Status::not_found(format!("step '{step_name}' not found in run")))?;

        step.job_id.as_ref().cloned().ok_or_else(|| {
            Status::not_found(format!(
                "step '{step_name}' has not been submitted as a job yet"
            ))
        })
    }
}

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
        let job_id = self
            .resolve_step_job_id(&caller_tenant, &req.run_id, &req.step_name)
            .await?;

        // Fetch logs from the job log store
        let tail = if req.tail == 0 {
            1000
        } else {
            req.tail as usize
        };
        let stored_logs = self
            .job_log_store
            .get_logs(&job_id, tail)
            .await
            .map_err(|e| Status::internal(format!("Failed to get logs: {e}")))?;

        let lines = stored_logs
            .into_iter()
            .map(|l| {
                proto_log_line(
                    l.message,
                    l.stream,
                    l.timestamp.timestamp_millis(),
                    l.substep_name,
                    l.event_type,
                    l.exit_code,
                )
            })
            .collect();

        Ok(Response::new(GetStepLogsResponse { lines }))
    }

    pub async fn stream_step_logs_impl(
        &self,
        request: Request<StreamStepLogsRequest>,
    ) -> Result<Response<super::BoxStream<LogLine>>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        if req.step_name.is_empty() {
            return Err(Status::invalid_argument("step_name is required"));
        }

        let job_id = self
            .resolve_step_job_id(&caller_tenant, &req.run_id, &req.step_name)
            .await?;

        let (tx, rx) = mpsc::channel(256);

        if let Some(collector) = self.log_collectors.get(&job_id).map(|c| c.value().clone()) {
            let job_id_clone = job_id.clone();
            let max_log_lines = self.max_log_lines;

            tokio::spawn(async move {
                let mut receiver = collector.subscribe();
                let historical = collector.get_historical(max_log_lines).await;
                drop(collector);
                let mut last_sequence = None;

                for line in historical {
                    last_sequence = Some(line.sequence);
                    let payload = proto_log_line(
                        line.message,
                        match line.stream {
                            muli_engine::docker::logs::LogStream::Stdout => "stdout".to_string(),
                            muli_engine::docker::logs::LogStream::Stderr => "stderr".to_string(),
                        },
                        line.timestamp.timestamp_millis(),
                        line.substep_name,
                        Some(line.event_type),
                        line.exit_code,
                    );
                    if tx.send(Ok(payload)).await.is_err() {
                        return;
                    }
                }

                loop {
                    match receiver.recv().await {
                        Ok(line) => {
                            if last_sequence.is_some_and(|seq| line.sequence <= seq) {
                                continue;
                            }
                            last_sequence = Some(line.sequence);
                            let payload = proto_log_line(
                                line.message,
                                match line.stream {
                                    muli_engine::docker::logs::LogStream::Stdout => {
                                        "stdout".to_string()
                                    }
                                    muli_engine::docker::logs::LogStream::Stderr => {
                                        "stderr".to_string()
                                    }
                                },
                                line.timestamp.timestamp_millis(),
                                line.substep_name,
                                Some(line.event_type),
                                line.exit_code,
                            );
                            if tx.send(Ok(payload)).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                job_id = %job_id_clone,
                                lagged = n,
                                "step log subscriber lagged"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        } else {
            let job_log_store = self.job_log_store.clone();
            let job_store = self.job_store.clone();
            let job_id_clone = job_id.clone();
            let max_log_lines = self.max_log_lines;

            tokio::spawn(async move {
                match job_log_store.get_logs(&job_id_clone, max_log_lines).await {
                    Ok(lines) if !lines.is_empty() => {
                        for line in lines {
                            let payload = proto_log_line(
                                line.message,
                                line.stream,
                                line.timestamp.timestamp_millis(),
                                line.substep_name,
                                line.event_type,
                                line.exit_code,
                            );
                            if tx.send(Ok(payload)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(_empty) => match job_store.get_job(&job_id_clone).await {
                        Ok(None) => {
                            let _ = tx
                                .send(Err(Status::not_found(format!(
                                    "job {job_id_clone} not found"
                                ))))
                                .await;
                        }
                        Ok(Some(_)) => {}
                        Err(e) => {
                            let _ = tx
                                .send(Err(Status::internal(format!("Failed to get job: {e}"))))
                                .await;
                        }
                    },
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("Failed to get logs: {e}"))))
                            .await;
                    }
                }
            });
        }

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
