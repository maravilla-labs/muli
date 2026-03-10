// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Log streaming gRPC service.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use muli_core::traits::{JobLogStore, JobStore};
use muli_engine::docker::logs::{LogCollector, LogLine, LogStream as EngineLogStream};
use muli_proto::log_service_server::LogService;
use muli_proto::{
    GetLogsRequest, GetLogsResponse, LogEntry, LogStream, StreamLogsRequest, Timestamp,
};

use super::util::extract_tenant_id;

pub struct LogServiceImpl {
    pub log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    pub max_log_lines: usize,
    pub job_log_store: Arc<dyn JobLogStore>,
    pub job_store: Arc<dyn JobStore>,
}

fn log_line_to_entry(job_id: &str, line: &LogLine) -> LogEntry {
    LogEntry {
        job_id: job_id.to_string(),
        sequence: line.sequence,
        timestamp: Some(Timestamp {
            seconds: line.timestamp.timestamp(),
            nanos: line.timestamp.timestamp_subsec_nanos() as i32,
        }),
        line: line.message.clone(),
        stream: match line.stream {
            EngineLogStream::Stdout => LogStream::Stdout as i32,
            EngineLogStream::Stderr => LogStream::Stderr as i32,
        },
    }
}

#[tonic::async_trait]
impl LogService for LogServiceImpl {
    type StreamLogsStream = ReceiverStream<Result<LogEntry, Status>>;

    async fn stream_logs(
        &self,
        request: Request<StreamLogsRequest>,
    ) -> Result<Response<Self::StreamLogsStream>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();
        let job_id = req.job_id.clone();

        // Verify job belongs to the caller's tenant
        let job = self
            .job_store
            .get_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to fetch job: {e}")))?
            .ok_or_else(|| Status::not_found("Job not found"))?;
        if job.spec.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "job does not belong to your tenant",
            ));
        }
        let follow = req.follow;

        let collector = self.log_collectors.get(&job_id).map(|c| c.value().clone());

        let (tx, rx) = mpsc::channel(256);

        if let Some(collector) = collector {
            let job_id_clone = job_id.clone();
            let max_log_lines = self.max_log_lines;
            let since_sequence = req.since_sequence;

            // Extract what we need from Arc<LogCollector> BEFORE spawning, then drop the Arc.
            // Holding the Arc inside the spawned task creates a cycle: the task keeps the sender
            // alive (inside the Arc), so RecvError::Closed never fires, so the task never exits.
            let historical = collector.get_historical(max_log_lines).await;
            let receiver = if follow {
                Some(collector.subscribe())
            } else {
                None
            };
            drop(collector); // Release Arc — only DashMap + execute_job hold it now

            tokio::spawn(async move {
                // Send historical logs
                for line in &historical {
                    if let Some(since) = since_sequence
                        && line.sequence <= since
                    {
                        continue;
                    }
                    let entry = log_line_to_entry(&job_id_clone, line);
                    if tx.send(Ok(entry)).await.is_err() {
                        return;
                    }
                }

                if !follow {
                    return;
                }

                let mut receiver = receiver.unwrap(); // safe: follow == true
                loop {
                    match receiver.recv().await {
                        Ok(line) => {
                            let entry = log_line_to_entry(&job_id_clone, &line);
                            if tx.send(Ok(entry)).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                job_id = %job_id_clone,
                                lagged = n,
                                "Log subscriber lagged"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        } else {
            // No live collector — job has completed (or never existed); stream persisted logs
            let job_log_store = self.job_log_store.clone();
            let job_store = self.job_store.clone();
            let job_id_clone = job_id.clone();
            let max_log_lines = self.max_log_lines;
            tokio::spawn(async move {
                match job_log_store.get_logs(&job_id_clone, max_log_lines).await {
                    Ok(lines) if !lines.is_empty() => {
                        for l in &lines {
                            if let Some(since) = req.since_sequence
                                && l.sequence <= since
                            {
                                continue;
                            }
                            let entry = LogEntry {
                                job_id: job_id_clone.clone(),
                                sequence: l.sequence,
                                timestamp: Some(Timestamp {
                                    seconds: l.timestamp.timestamp(),
                                    nanos: l.timestamp.timestamp_subsec_nanos() as i32,
                                }),
                                line: l.message.clone(),
                                stream: if l.stream == "stdout" {
                                    LogStream::Stdout as i32
                                } else {
                                    LogStream::Stderr as i32
                                },
                            };
                            if tx.send(Ok(entry)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(_empty) => {
                        // No persisted logs — check whether the job exists at all
                        match job_store.get_job(&job_id_clone).await {
                            Ok(None) => {
                                tracing::warn!(job_id = %job_id_clone, "stream_logs: job not found");
                                let _ = tx
                                    .send(Err(Status::not_found(format!(
                                        "job {job_id_clone} not found"
                                    ))))
                                    .await;
                            }
                            Ok(Some(_)) => {
                                // Job exists but produced no logs — stream ends normally
                            }
                            Err(e) => {
                                tracing::warn!(job_id = %job_id_clone, error = %e, "Failed to look up job for stream_logs");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(job_id = %job_id_clone, error = %e, "Failed to load persisted logs for stream");
                    }
                }
            });
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_logs(
        &self,
        request: Request<GetLogsRequest>,
    ) -> Result<Response<GetLogsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();
        let job_id = req.job_id;

        // Verify job belongs to the caller's tenant
        let job = self
            .job_store
            .get_job(&job_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to fetch job: {e}")))?
            .ok_or_else(|| Status::not_found("Job not found"))?;
        if job.spec.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "job does not belong to your tenant",
            ));
        }
        let tail = if req.tail == 0 {
            100
        } else {
            (req.tail as usize).min(self.max_log_lines)
        };

        let collector = self.log_collectors.get(&job_id).map(|c| c.value().clone());

        let is_complete = collector.is_none();

        let entries = if let Some(collector) = collector {
            let lines = collector.get_historical(tail).await;
            lines
                .iter()
                .map(|l| log_line_to_entry(&job_id, l))
                .collect()
        } else {
            // Collector is gone — job has completed (or never existed); load from persistent store
            match self.job_log_store.get_logs(&job_id, tail).await {
                Ok(lines) if !lines.is_empty() => lines
                    .iter()
                    .map(|l| LogEntry {
                        job_id: job_id.clone(),
                        sequence: l.sequence,
                        timestamp: Some(Timestamp {
                            seconds: l.timestamp.timestamp(),
                            nanos: l.timestamp.timestamp_subsec_nanos() as i32,
                        }),
                        line: l.message.clone(),
                        stream: if l.stream == "stdout" {
                            LogStream::Stdout as i32
                        } else {
                            LogStream::Stderr as i32
                        },
                    })
                    .collect(),
                Ok(_empty) => {
                    // No persisted logs — check whether the job exists at all
                    match self.job_store.get_job(&job_id).await {
                        Ok(None) => {
                            tracing::warn!(job_id = %job_id, "get_logs: job not found");
                            return Err(Status::not_found(format!("job {job_id} not found")));
                        }
                        Ok(Some(_)) => Vec::new(), // Job exists but produced no logs
                        Err(e) => {
                            tracing::warn!(job_id = %job_id, error = %e, "Failed to look up job for get_logs");
                            Vec::new()
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(job_id = %job_id, error = %e, "Failed to load persisted logs");
                    Vec::new()
                }
            }
        };

        Ok(Response::new(GetLogsResponse {
            entries,
            is_complete,
        }))
    }
}
