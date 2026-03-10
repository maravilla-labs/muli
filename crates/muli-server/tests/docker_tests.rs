// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests: Docker Execution (Docker required, auto-skip).

mod common;

use std::sync::Arc;
use std::time::Duration;

use muli_core::job::state_machine::JobState;
use muli_engine::docker::logs::LogCollector;
use muli_proto::{
    GetJobStatusRequest, HeartbeatRequest, RegisterAgentRequest, ReportJobResultRequest,
};
use tokio_util::sync::CancellationToken;
use tonic::Request;

use muli_test::grpc_helpers::{agent_client, job_client, test_submit_request};

use common::{
    TestGrpcServer, dummy_executor, execute_and_report, run_job, test_capabilities,
    wait_for_terminal_state, with_tenant,
};

#[tokio::test]
async fn test_embedded_execution_roundtrip() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let cancel = CancellationToken::new();
    let sched = server.scheduler.clone();
    let store = server.job_store.clone();
    let log_collectors = server.log_collectors.clone();
    let cancel_clone = cancel.clone();
    let executor = dummy_executor().await;

    let _sched_handle = tokio::spawn(async move {
        sched
            .run(cancel_clone, move |jid, _tid| {
                let store = store.clone();
                let executor = executor.clone();
                let log_collectors = log_collectors.clone();
                async move {
                    run_job(jid, store, executor, log_collectors).await;
                }
            })
            .await;
    });

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(60)).await;
    cancel.cancel();

    assert!(
        terminal == JobState::Succeeded || terminal == JobState::Failed,
        "Expected Succeeded or Failed, got {terminal:?}"
    );
}

#[tokio::test]
async fn test_failed_job_bad_image() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let mut req = test_submit_request();
    req.runner_image = "nonexistent-image-xyz:never".to_string();

    let resp = client
        .submit_job(with_tenant(req, "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let cancel = CancellationToken::new();
    let sched = server.scheduler.clone();
    let store = server.job_store.clone();
    let log_collectors = server.log_collectors.clone();
    let cancel_clone = cancel.clone();
    let executor = dummy_executor().await;

    let _sched_handle = tokio::spawn(async move {
        sched
            .run(cancel_clone, move |jid, _tid| {
                let store = store.clone();
                let executor = executor.clone();
                let log_collectors = log_collectors.clone();
                async move {
                    run_job(jid, store, executor, log_collectors).await;
                }
            })
            .await;
    });

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(60)).await;
    cancel.cancel();

    assert_eq!(terminal, JobState::Failed);
}

#[tokio::test]
async fn test_agent_execution_roundtrip() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    let resp = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let reg = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "exec-agent".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_id = reg.into_inner().agent_id;

    let hb = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: agent_id.clone(),
            capabilities: test_capabilities(),
        }))
        .await
        .unwrap();
    let assignments = hb.into_inner().assignments;
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].job_id, job_id);

    let executor = dummy_executor().await;
    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();

    server
        .job_store
        .update_state(&job_id, JobState::Scheduled, JobState::Running)
        .await
        .unwrap();

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

    agent_cl
        .report_job_result(Request::new(ReportJobResultRequest {
            agent_id: agent_id.clone(),
            job_id: job_id.clone(),
            final_state,
            exit_code,
            message: msg,
            started_at: None,
            finished_at: None,
        }))
        .await
        .unwrap();

    let status = job_cl
        .get_job_status(with_tenant(
            GetJobStatusRequest {
                job_id: job_id.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(status.into_inner().state, 5); // Succeeded

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

#[tokio::test]
async fn test_two_agents_parallel_execution() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    let resp1 = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id_1 = resp1.into_inner().job_id;

    let resp2 = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id_2 = resp2.into_inner().job_id;

    let mut caps = test_capabilities().unwrap();
    caps.max_concurrent_jobs = 1;

    let reg1 = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "par-agent-1".to_string(),
            hostname: "localhost".to_string(),
            capabilities: Some(caps.clone()),
            labels: vec![],
        }))
        .await
        .unwrap();
    let aid1 = reg1.into_inner().agent_id;

    let reg2 = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "par-agent-2".to_string(),
            hostname: "localhost".to_string(),
            capabilities: Some(caps.clone()),
            labels: vec![],
        }))
        .await
        .unwrap();
    let aid2 = reg2.into_inner().agent_id;

    let hb1 = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: aid1.clone(),
            capabilities: Some(caps.clone()),
        }))
        .await
        .unwrap();
    let a1_jobs = hb1.into_inner().assignments;
    assert_eq!(a1_jobs.len(), 1);

    let hb2 = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: aid2.clone(),
            capabilities: Some(caps),
        }))
        .await
        .unwrap();
    let a2_jobs = hb2.into_inner().assignments;
    assert_eq!(a2_jobs.len(), 1);

    let executor = dummy_executor().await;

    let exec1 = {
        let store = server.job_store.clone();
        let ex = executor.clone();
        let jid = a1_jobs[0].job_id.clone();
        let aid = aid1.clone();
        let port = server.port;
        tokio::spawn(async move {
            execute_and_report(&store, &ex, &jid, &aid, port).await;
        })
    };

    let exec2 = {
        let store = server.job_store.clone();
        let ex = executor.clone();
        let jid = a2_jobs[0].job_id.clone();
        let aid = aid2.clone();
        let port = server.port;
        tokio::spawn(async move {
            execute_and_report(&store, &ex, &jid, &aid, port).await;
        })
    };

    let _ = tokio::join!(exec1, exec2);

    let s1 = job_cl
        .get_job_status(with_tenant(
            GetJobStatusRequest {
                job_id: job_id_1.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    let s2 = job_cl
        .get_job_status(with_tenant(
            GetJobStatusRequest {
                job_id: job_id_2.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();

    let states = vec![s1.into_inner().state, s2.into_inner().state];
    for s in &states {
        assert!(
            *s == 5 || *s == 6,
            "Expected Succeeded(5) or Failed(6), got {s}"
        );
    }

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}
