// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Agent management gRPC service.

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;
use uuid::Uuid;

use muli_core::agent::model::{AgentCapabilities as CoreCapabilities, AgentInfo, AgentStatus};
use muli_core::job::model::{Job, StoredLogLine};
use muli_core::job::state_machine::JobState;
use muli_core::traits::{AgentRegistry, JobLogStore, JobStore};
use muli_core::validation;
use muli_engine::docker::logs::{LogCollector, LogLine, LogStream as EngineLogStream};
use muli_queue::PriorityQueue;

use muli_proto::agent_service_server::AgentService;
use muli_proto::{
    DeregisterAgentRequest, DeregisterAgentResponse, HeartbeatRequest, HeartbeatResponse,
    JobAssignment, LogEntry, LogStream, RegisterAgentRequest, RegisterAgentResponse,
    ReportJobResultRequest, ReportJobResultResponse, StreamJobLogsResponse,
};

use super::conversions::proto_state_to_core;
use super::util::extract_tenant_id;

pub struct AgentServiceImpl {
    pub agent_registry: Arc<dyn AgentRegistry>,
    pub job_store: Arc<dyn JobStore>,
    pub queue: Arc<PriorityQueue>,
    pub log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    pub job_log_store: Arc<dyn JobLogStore>,
}

fn proto_to_core_capabilities(c: muli_proto::AgentCapabilities) -> CoreCapabilities {
    CoreCapabilities {
        total_cpu_millicores: c.total_cpu_millicores,
        total_memory_bytes: c.total_memory_bytes,
        available_cpu_millicores: c.available_cpu_millicores,
        available_memory_bytes: c.available_memory_bytes,
        max_concurrent_jobs: c.max_concurrent_jobs,
        running_jobs: c.running_jobs,
        pre_pulled_images: c.pre_pulled_images,
    }
}

impl AgentServiceImpl {
    /// Pop jobs from the queue and assign them to an agent.
    async fn assign_jobs(&self, agent_id: &str, caps: &CoreCapabilities) -> Vec<JobAssignment> {
        let available = caps.max_concurrent_jobs.saturating_sub(caps.running_jobs);
        let mut assignments = Vec::new();

        for _ in 0..available {
            let Some(scored_job) = self.queue.pop() else {
                break;
            };
            let job_id = scored_job.job_id.clone();

            let Ok(Some(job)) = self.job_store.get_job(&job_id).await else {
                tracing::warn!(job_id = %job_id, "Job not found in store during assignment; discarding");
                continue;
            };

            match self
                .job_store
                .update_state(&job_id, JobState::Pending, JobState::Scheduled)
                .await
            {
                Ok(()) => {
                    let mut updated = job.clone();
                    updated.assigned_agent = Some(agent_id.to_string());
                    updated.state = JobState::Scheduled;
                    let _ = self.job_store.update_job(&updated).await;
                    assignments.push(job_to_assignment(&job));
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %e,
                        "Failed to mark job as Scheduled; re-enqueueing"
                    );
                    self.queue.requeue(scored_job);
                }
            }
        }

        assignments
    }
}

#[tonic::async_trait]
impl AgentService for AgentServiceImpl {
    async fn register_agent(
        &self,
        request: Request<RegisterAgentRequest>,
    ) -> Result<Response<RegisterAgentResponse>, Status> {
        let req = request.into_inner();

        validation::validate_agent_name(&req.name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        validation::validate_non_empty("hostname", &req.hostname, 255)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        for label in &req.labels {
            validation::validate_label(label)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        let capabilities = match req.capabilities {
            Some(c) => proto_to_core_capabilities(c),
            None => CoreCapabilities {
                total_cpu_millicores: 0,
                total_memory_bytes: 0,
                available_cpu_millicores: 0,
                available_memory_bytes: 0,
                max_concurrent_jobs: 0,
                running_jobs: 0,
                pre_pulled_images: vec![],
            },
        };

        let agent = AgentInfo {
            id: Uuid::new_v4().to_string(),
            name: req.name.clone(),
            hostname: req.hostname,
            status: AgentStatus::Healthy,
            capabilities,
            labels: req.labels,
            registered_at: Utc::now(),
            last_heartbeat: Utc::now(),
        };

        let agent_id = self
            .agent_registry
            .register(&agent)
            .await
            .map_err(|e| Status::internal(format!("Failed to register agent: {e}")))?;

        info!(
            operation = "register_agent",
            agent_id = %agent_id,
            agent_name = %req.name,
            result = "success",
            "audit: agent registered"
        );

        Ok(Response::new(RegisterAgentResponse {
            agent_id,
            heartbeat_interval_seconds: 10,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        validation::validate_non_empty("agent_id", &req.agent_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let capabilities = req.capabilities.map(proto_to_core_capabilities);

        if let Some(caps) = &capabilities {
            self.agent_registry
                .heartbeat(&req.agent_id, caps)
                .await
                .map_err(|e| Status::internal(format!("Heartbeat failed: {e}")))?;
        }

        let assignments = match &capabilities {
            Some(caps) => self.assign_jobs(&req.agent_id, caps).await,
            None => Vec::new(),
        };

        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
            assignments,
        }))
    }

    async fn report_job_result(
        &self,
        request: Request<ReportJobResultRequest>,
    ) -> Result<Response<ReportJobResultResponse>, Status> {
        let req = request.into_inner();

        validation::validate_non_empty("job_id", &req.job_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        validation::validate_non_empty("agent_id", &req.agent_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let final_state = proto_state_to_core(req.final_state)
            .ok_or_else(|| Status::invalid_argument("Invalid final state"))?;

        let job = self
            .job_store
            .get_job(&req.job_id)
            .await
            .map_err(|e| Status::internal(format!("Store error: {e}")))?
            .ok_or_else(|| Status::not_found(format!("Job not found: {}", req.job_id)))?;

        if let Some(ref assigned) = job.assigned_agent
            && *assigned != req.agent_id
        {
            return Err(Status::permission_denied(format!(
                "agent {} is not assigned to job {}",
                req.agent_id, req.job_id
            )));
        }

        if job.state.is_terminal() {
            return Err(Status::failed_precondition(format!(
                "job {} is already in terminal state: {}",
                req.job_id, job.state
            )));
        }

        // Transition through intermediate states if needed.
        let mut current = job.state;
        if !current.can_transition_to(&final_state)
            && current == JobState::Scheduled
            && matches!(
                final_state,
                JobState::Succeeded | JobState::Failed | JobState::TimedOut
            )
        {
            self.job_store
                .update_state(&req.job_id, current, JobState::Running)
                .await
                .map_err(|e| Status::internal(format!("Failed to update state: {e}")))?;
            current = JobState::Running;
        }

        self.job_store
            .update_state(&req.job_id, current, final_state)
            .await
            .map_err(|e| Status::internal(format!("Failed to update state: {e}")))?;

        if let Ok(Some(mut updated_job)) = self.job_store.get_job(&req.job_id).await {
            updated_job.result = Some(muli_core::job::model::JobResult {
                exit_code: req.exit_code,
                message: req.message.clone(),
                container_id: None,
            });
            if let Some(ts) = req.started_at {
                updated_job.started_at =
                    chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32);
            }
            if let Some(ts) = req.finished_at {
                updated_job.finished_at =
                    chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32);
            }
            let _ = self.job_store.update_job(&updated_job).await;
        }

        info!(
            operation = "report_job_result",
            job_id = %req.job_id,
            agent_id = %req.agent_id,
            final_state = ?final_state,
            result = "success",
            "audit: agent reported job result"
        );

        Ok(Response::new(ReportJobResultResponse {
            acknowledged: true,
        }))
    }

    async fn stream_job_logs(
        &self,
        request: Request<Streaming<LogEntry>>,
    ) -> Result<Response<StreamJobLogsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let mut stream = request.into_inner();
        let log_collectors = self.log_collectors.clone();
        let job_log_store = self.job_log_store.clone();
        let job_store = self.job_store.clone();

        let collector = Arc::new(LogCollector::new());
        let mut job_id: Option<String> = None;
        let mut count: u64 = 0;

        while let Some(entry) = stream.message().await? {
            count += 1;

            if job_id.is_none() {
                let jid = entry.job_id.clone();

                // Verify job belongs to the caller's tenant
                match job_store.get_job(&jid).await {
                    Ok(Some(job)) if job.spec.tenant_id != caller_tenant => {
                        return Err(Status::permission_denied(
                            "job does not belong to your tenant",
                        ));
                    }
                    Ok(None) => {
                        return Err(Status::not_found(format!("job {jid} not found")));
                    }
                    Err(e) => {
                        return Err(Status::internal(format!("Failed to look up job: {e}")));
                    }
                    Ok(Some(_)) => {} // ownership verified
                }

                tracing::debug!(job_id = %jid, "Registering agent log collector");
                log_collectors.insert(jid.clone(), collector.clone());
                job_id = Some(jid);
            }

            let timestamp = entry
                .timestamp
                .and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32))
                .unwrap_or_else(Utc::now);

            let stream_type = if entry.stream == LogStream::Stdout as i32 {
                EngineLogStream::Stdout
            } else {
                EngineLogStream::Stderr
            };

            collector
                .push_line(LogLine {
                    sequence: entry.sequence,
                    timestamp,
                    stream: stream_type,
                    message: entry.line,
                    substep_name: None,
                    event_type: "line".to_string(),
                    exit_code: None,
                })
                .await;
        }

        if let Some(ref jid) = job_id {
            let lines = collector.drain().await;
            if !lines.is_empty() {
                let stored: Vec<StoredLogLine> = lines
                    .iter()
                    .map(|l| StoredLogLine {
                        sequence: l.sequence,
                        timestamp: l.timestamp,
                        stream: match l.stream {
                            EngineLogStream::Stdout => "stdout".to_string(),
                            EngineLogStream::Stderr => "stderr".to_string(),
                        },
                        message: l.message.clone(),
                        substep_name: l.substep_name.clone(),
                        event_type: Some(l.event_type.clone()),
                        exit_code: l.exit_code,
                    })
                    .collect();
                if let Err(e) = job_log_store.append_logs(jid, stored).await {
                    tracing::warn!(job_id = %jid, error = %e, "Failed to persist agent logs");
                }
            }
            log_collectors.remove(jid);
            tracing::debug!(job_id = %jid, entries = count, "Agent log stream complete");
        }

        drop(collector);

        Ok(Response::new(StreamJobLogsResponse {
            entries_received: count,
        }))
    }

    async fn deregister_agent(
        &self,
        request: Request<DeregisterAgentRequest>,
    ) -> Result<Response<DeregisterAgentResponse>, Status> {
        let req = request.into_inner();

        validation::validate_non_empty("agent_id", &req.agent_id, 256)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.agent_registry
            .deregister(&req.agent_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to deregister: {e}")))?;

        info!(
            operation = "deregister_agent",
            agent_id = %req.agent_id,
            reason = %req.reason,
            result = "success",
            "audit: agent deregistered"
        );

        Ok(Response::new(DeregisterAgentResponse {
            acknowledged: true,
        }))
    }
}

/// Convert a core `Job` to a proto `JobAssignment`.
fn job_to_assignment(job: &Job) -> JobAssignment {
    JobAssignment {
        job_id: job.id.clone(),
        runner_image: job.spec.runner_image.clone(),
        env_vars: job
            .spec
            .env_vars
            .iter()
            .map(|e| muli_proto::EnvVar {
                name: e.name.clone(),
                value: e.value.clone(),
            })
            .collect(),
        resources: Some(muli_proto::ResourceSpec {
            cpu_request: job.spec.resources.cpu_request.clone(),
            cpu_limit: job.spec.resources.cpu_limit.clone(),
            memory_request: job.spec.resources.memory_request.clone(),
            memory_limit: job.spec.resources.memory_limit.clone(),
            timeout_seconds: job.spec.resources.timeout_seconds,
        }),
        registry_credentials: job.spec.registry_credentials.as_ref().map(|c| {
            muli_proto::RegistryCredentials {
                server: c.server.clone(),
                username: c.username.clone(),
                password: c.password.clone(),
            }
        }),
    }
}
