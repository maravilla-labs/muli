// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job execution worker with log streaming.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use muli_core::error::MuliError;
use muli_core::job::model::{
    EnvVar as CoreEnvVar, Job, JobSpec, PriorityTier,
    RegistryCredentials as CoreRegistryCredentials,
};
use muli_core::job::state_machine::JobState as CoreJobState;
use muli_core::resource::limits::ResourceSpec as CoreResourceSpec;
use muli_engine::docker::logs::{LogCollector, LogStream as EngineLogStream};
use muli_engine::executor::DockerExecutor;
use muli_proto::{JobAssignment, JobState, LogEntry, LogStream, ReportJobResultRequest, Timestamp};

use crate::AgentClient;

/// Convert a proto `JobAssignment` into a core `Job` for the `DockerExecutor`.
fn assignment_to_job(assignment: &JobAssignment) -> Job {
    let resources = assignment
        .resources
        .as_ref()
        .map(|r| CoreResourceSpec {
            cpu_request: r.cpu_request.clone(),
            cpu_limit: r.cpu_limit.clone(),
            memory_request: r.memory_request.clone(),
            memory_limit: r.memory_limit.clone(),
            timeout_seconds: r.timeout_seconds,
        })
        .unwrap_or_default();

    let env_vars = assignment
        .env_vars
        .iter()
        .map(|e| CoreEnvVar {
            name: e.name.clone(),
            value: e.value.clone(),
        })
        .collect();

    let registry_credentials =
        assignment
            .registry_credentials
            .as_ref()
            .map(|c| CoreRegistryCredentials {
                server: c.server.clone(),
                username: c.username.clone(),
                password: c.password.clone(),
            });

    let now = Utc::now();
    let id_prefix_len = assignment.job_id.len().min(8);

    let spec = JobSpec {
        deployment_id: String::new(),
        project_id: String::new(),
        workspace_id: String::new(),
        tenant_id: String::new(),
        runner_image: assignment.runner_image.clone(),
        env_vars,
        resources,
        priority_tier: PriorityTier::Standard,
        framework: String::new(),
        idempotency_key: None,
        registry_credentials,
        commands: vec![],
    };

    Job {
        id: assignment.job_id.clone(),
        name: format!("muli-{}", &assignment.job_id[..id_prefix_len]),
        spec,
        state: CoreJobState::Running,
        result: None,
        retry_count: 0,
        priority_score: PriorityTier::Standard.weight() as f64,
        created_at: now,
        updated_at: now,
        scheduled_at: Some(now),
        started_at: Some(now),
        finished_at: None,
        assigned_agent: None,
    }
}

fn to_proto_timestamp(dt: chrono::DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// Run the worker loop. Receives job assignments and executes them via Docker.
pub async fn worker_loop(
    mut client: AgentClient,
    agent_id: String,
    executor: Arc<DockerExecutor>,
    running_jobs: Arc<AtomicU32>,
    mut assignment_rx: mpsc::Receiver<JobAssignment>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            Some(assignment) = assignment_rx.recv() => {
                let job_id = assignment.job_id.clone();
                info!(job_id = %job_id, image = %assignment.runner_image, "executing job");

                running_jobs.fetch_add(1, Ordering::Relaxed);

                let started_at = Utc::now();
                let job = assignment_to_job(&assignment);

                // Create a log collector and subscribe BEFORE execution starts so we
                // don't miss lines emitted in the first milliseconds of the container.
                let log_collector = Arc::new(LogCollector::new());
                let mut log_receiver = log_collector.subscribe();

                // Bridge: broadcast receiver → mpsc channel → gRPC stream
                let (entry_tx, entry_rx) = mpsc::channel::<LogEntry>(256);
                let job_id_fwd = job_id.clone();
                let fwd_task = tokio::spawn(async move {
                    loop {
                        match log_receiver.recv().await {
                            Ok(line) => {
                                let entry = LogEntry {
                                    job_id: job_id_fwd.clone(),
                                    sequence: line.sequence,
                                    timestamp: Some(Timestamp {
                                        seconds: line.timestamp.timestamp(),
                                        nanos: line.timestamp.timestamp_subsec_nanos() as i32,
                                    }),
                                    line: line.message,
                                    stream: match line.stream {
                                        EngineLogStream::Stdout => LogStream::Stdout as i32,
                                        EngineLogStream::Stderr => LogStream::Stderr as i32,
                                    },
                                };
                                if entry_tx.send(entry).await.is_err() {
                                    break; // gRPC stream dropped
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(job_id = %job_id_fwd, lagged = n, "Log forwarder lagged");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    // entry_tx drops here → ReceiverStream ends → stream_job_logs RPC closes
                });

                // Task that streams log entries to the server via gRPC.
                // Any network error is logged and ignored; job result is still reported.
                let mut log_client = client.clone();
                let job_id_stream = job_id.clone();
                let stream_task = tokio::spawn(async move {
                    let stream = ReceiverStream::new(entry_rx);
                    if let Err(e) = log_client.stream_job_logs(stream).await {
                        warn!(job_id = %job_id_stream, error = %e, "stream_job_logs to server failed");
                    }
                });

                let (final_state, exit_code, message) = match executor
                    .execute_job(&job, log_collector.clone())
                    .await
                {
                    Ok(result) => {
                        let state = if result.exit_code == Some(0) {
                            JobState::Succeeded
                        } else {
                            JobState::Failed
                        };
                        (state, result.exit_code, result.message)
                    }
                    Err(e) => {
                        let state = if matches!(e, MuliError::Timeout(_)) {
                            JobState::TimedOut
                        } else {
                            JobState::Failed
                        };
                        (state, None, format!("Execution error: {e}"))
                    }
                };

                let finished_at = Utc::now();

                // Drop our Arc so RecvError::Closed fires in fwd_task, which closes the
                // mpsc channel, which ends the gRPC stream, which triggers log persistence
                // on the server — all before we report the final job state.
                drop(log_collector);

                // Wait for both forwarding tasks so logs are persisted server-side
                // before report_job_result transitions the job to a terminal state.
                let _ = tokio::join!(fwd_task, stream_task);

                let result = client
                    .report_job_result(ReportJobResultRequest {
                        agent_id: agent_id.clone(),
                        job_id: job_id.clone(),
                        final_state: final_state.into(),
                        exit_code,
                        message,
                        started_at: Some(to_proto_timestamp(started_at)),
                        finished_at: Some(to_proto_timestamp(finished_at)),
                    })
                    .await;

                running_jobs.fetch_sub(1, Ordering::Relaxed);

                match result {
                    Ok(_) => info!(job_id = %job_id, "reported job result"),
                    Err(e) => error!(job_id = %job_id, error = %e, "failed to report job result"),
                }
            }
            _ = shutdown.recv() => {
                info!("worker loop shutting down");
                break;
            }
        }
    }

    Ok(())
}
