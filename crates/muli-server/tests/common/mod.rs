// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared test harness and helpers for muli-server integration tests.
#![allow(dead_code)]

pub mod e2e;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::metadata::MetadataValue;

use muli_core::job::state_machine::JobState;
use muli_core::traits::{AgentRegistry, JobLogStore, JobStore};
use muli_engine::docker::logs::LogCollector;
use muli_queue::{ConcurrencyLimiter, PriorityQueue, Scheduler};
use muli_store::memory::{MemoryAgentStore, MemoryJobLogStore, MemoryJobStore};

use muli_proto::AgentCapabilities as ProtoCapabilities;
use muli_proto::agent_service_server::AgentServiceServer;
use muli_proto::job_service_server::JobServiceServer;
use muli_proto::log_service_server::LogServiceServer;

use muli_server::grpc::{AgentServiceImpl, AuthInterceptor, JobServiceImpl, LogServiceImpl};

use muli_test::grpc_helpers::agent_client;

// ---------------------------------------------------------------------------
// Test server harness
// ---------------------------------------------------------------------------

pub struct TestGrpcServer {
    pub port: u16,
    _cancel: CancellationToken,
    pub job_store: Arc<dyn JobStore>,
    #[allow(dead_code)]
    pub agent_store: Arc<dyn AgentRegistry>,
    pub scheduler: Arc<Scheduler>,
    pub log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    pub job_log_store: Arc<dyn JobLogStore>,
}

impl TestGrpcServer {
    /// Start a gRPC server with no auth on a random port.
    pub async fn start() -> Self {
        Self::start_with_options(None).await
    }

    /// Start a gRPC server with optional API key auth.
    pub async fn start_with_options(api_key: Option<String>) -> Self {
        let job_store: Arc<dyn JobStore> = Arc::new(MemoryJobStore::new());
        let agent_store: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentStore::new());
        let log_collectors: Arc<DashMap<String, Arc<LogCollector>>> = Arc::new(DashMap::new());

        let notify = Arc::new(Notify::new());
        let queue = Arc::new(PriorityQueue::new(notify.clone()));
        let limiter = Arc::new(ConcurrencyLimiter::new(10, 3));
        let scheduler = Arc::new(Scheduler::new(queue.clone(), limiter, notify));

        let cancel = CancellationToken::new();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().unwrap().port();

        let job_log_store: Arc<dyn JobLogStore> = Arc::new(MemoryJobLogStore::new());

        let job_service = JobServiceImpl {
            store: job_store.clone(),
            scheduler: scheduler.clone(),
            executor: dummy_executor().await,
            log_collectors: log_collectors.clone(),
            tenant_limits_store: None,
            max_jobs_per_tenant: 3,
        };

        let log_service = LogServiceImpl {
            log_collectors: log_collectors.clone(),
            max_log_lines: 10000,
            job_log_store: job_log_store.clone(),
            job_store: job_store.clone(),
        };

        let agent_service = AgentServiceImpl {
            agent_registry: agent_store.clone(),
            job_store: job_store.clone(),
            queue,
            log_collectors: log_collectors.clone(),
            job_log_store: job_log_store.clone(),
        };

        let auth = AuthInterceptor::new(api_key, Some("test-tenant".to_string()));

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(JobServiceServer::with_interceptor(
                    job_service,
                    auth.clone(),
                ))
                .add_service(LogServiceServer::with_interceptor(
                    log_service,
                    auth.clone(),
                ))
                .add_service(AgentServiceServer::with_interceptor(agent_service, auth))
                .serve_with_incoming_shutdown(incoming, cancel_clone.cancelled_owned())
                .await
                .expect("gRPC server failed");
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        TestGrpcServer {
            port,
            _cancel: cancel,
            job_store,
            agent_store,
            scheduler,
            log_collectors,
            job_log_store,
        }
    }
}

/// Create a dummy DockerExecutor for tests that don't execute Docker jobs.
pub async fn dummy_executor() -> Arc<muli_engine::executor::DockerExecutor> {
    use muli_engine::docker::client::DockerClient;
    use muli_engine::resource_manager::ResourceManager;

    let docker =
        DockerClient::new().expect("DockerClient::new should not fail even if daemon is down");
    let rm = Arc::new(ResourceManager::new(8000, 17_179_869_184, 10));
    Arc::new(muli_engine::executor::DockerExecutor::new(docker, rm))
}

/// Inject `x-tenant-id` metadata into a gRPC request.
pub fn with_tenant<T>(inner: T, tenant_id: &str) -> Request<T> {
    let mut req = Request::new(inner);
    req.metadata_mut()
        .insert("x-tenant-id", MetadataValue::try_from(tenant_id).unwrap());
    req
}

/// Inject both `x-tenant-id` and `authorization` metadata.
pub fn with_tenant_and_auth<T>(inner: T, tenant_id: &str, api_key: &str) -> Request<T> {
    let mut req = Request::new(inner);
    req.metadata_mut()
        .insert("x-tenant-id", MetadataValue::try_from(tenant_id).unwrap());
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {api_key}")).unwrap(),
    );
    req
}

pub fn test_capabilities() -> Option<ProtoCapabilities> {
    Some(ProtoCapabilities {
        total_cpu_millicores: 4000,
        total_memory_bytes: 8 * 1024 * 1024 * 1024,
        available_cpu_millicores: 2000,
        available_memory_bytes: 4 * 1024 * 1024 * 1024,
        max_concurrent_jobs: 4,
        running_jobs: 0,
        pre_pulled_images: vec!["alpine:latest".to_string()],
    })
}

/// Poll the job store until the job reaches a terminal state, with timeout.
pub async fn wait_for_terminal_state(
    server: &TestGrpcServer,
    job_id: &str,
    timeout: Duration,
) -> JobState {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("Timeout waiting for job {job_id} to reach terminal state");
        }

        if let Ok(Some(job)) = server.job_store.get_job(job_id).await
            && job.state.is_terminal()
        {
            return job.state;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Execute a job via DockerExecutor and report the result back to the server.
pub async fn execute_and_report(
    store: &Arc<dyn JobStore>,
    executor: &Arc<muli_engine::executor::DockerExecutor>,
    job_id: &str,
    agent_id: &str,
    port: u16,
) {
    use muli_proto::ReportJobResultRequest;

    let job = store.get_job(job_id).await.unwrap().unwrap();

    let _ = store
        .update_state(job_id, JobState::Scheduled, JobState::Running)
        .await;

    let result = executor
        .execute_job(&job, Arc::new(LogCollector::new()))
        .await;

    let (final_state, exit_code, msg) = match result {
        Ok(r) => {
            let state = if r.exit_code == Some(0) { 5 } else { 6 };
            (state, r.exit_code, r.message)
        }
        Err(e) => (6, None, format!("Error: {e}")),
    };

    let mut client = agent_client(port).await;
    client
        .report_job_result(Request::new(ReportJobResultRequest {
            agent_id: agent_id.to_string(),
            job_id: job_id.to_string(),
            final_state,
            exit_code,
            message: msg,
            started_at: None,
            finished_at: None,
        }))
        .await
        .unwrap();
}

/// Run a job end-to-end (used as scheduler callback).
///
/// Mirrors the production `execute_job_inner` flow: creates a log collector,
/// executes the job, drains and persists logs, then removes the collector so
/// `LogService::get_logs` sees the job as complete.
pub async fn run_job(
    job_id: String,
    store: Arc<dyn JobStore>,
    executor: Arc<muli_engine::executor::DockerExecutor>,
    log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    job_log_store: Arc<dyn JobLogStore>,
) {
    let job = match store.get_job(&job_id).await {
        Ok(Some(j)) => j,
        _ => return,
    };

    let _ = store
        .update_state(&job_id, JobState::Pending, JobState::Scheduled)
        .await;

    let log_collector = Arc::new(LogCollector::new());
    log_collectors.insert(job_id.clone(), log_collector.clone());

    let _ = store
        .update_state(&job_id, JobState::Scheduled, JobState::Running)
        .await;

    match executor.execute_job(&job, log_collector.clone()).await {
        Ok(result) => {
            let final_state = if result.exit_code == Some(0) {
                JobState::Succeeded
            } else {
                JobState::Failed
            };
            let mut updated = job.clone();
            updated.state = final_state;
            updated.result = Some(result);
            updated.finished_at = Some(chrono::Utc::now());
            let _ = store.update_job(&updated).await;
        }
        Err(e) => {
            let final_state = match &e {
                muli_core::error::MuliError::Timeout(_) => JobState::TimedOut,
                muli_core::error::MuliError::Cancelled(_) => JobState::Cancelled,
                _ => JobState::Failed,
            };
            let mut updated = job.clone();
            updated.state = final_state;
            updated.result = Some(muli_core::job::model::JobResult {
                exit_code: None,
                message: format!("Error: {e}"),
                container_id: None,
            });
            updated.finished_at = Some(chrono::Utc::now());
            let _ = store.update_job(&updated).await;
        }
    }

    // Persist logs before removing collector (mirrors production flow)
    let lines = log_collector.drain().await;
    if !lines.is_empty() {
        let stored: Vec<_> = lines
            .into_iter()
            .map(|l| muli_core::job::model::StoredLogLine {
                sequence: l.sequence,
                timestamp: l.timestamp,
                stream: match l.stream {
                    muli_engine::docker::logs::LogStream::Stdout => "stdout".to_string(),
                    muli_engine::docker::logs::LogStream::Stderr => "stderr".to_string(),
                },
                message: l.message,
            })
            .collect();
        let _ = job_log_store.append_logs(&job_id, stored).await;
    }

    // Remove from live map so LogService sees the job as complete
    log_collectors.remove(&job_id);
}
