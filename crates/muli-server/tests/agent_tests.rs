// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests: Agent Lifecycle (no Docker required).

mod common;

use std::time::Duration;

use muli_proto::{
    DeregisterAgentRequest, HeartbeatRequest, LogEntry, RegisterAgentRequest,
    ReportJobResultRequest, StreamLogsRequest,
};
use tonic::Request;

use muli_test::grpc_helpers::{agent_client, job_client, log_client, test_submit_request};

use common::{TestGrpcServer, test_capabilities, with_tenant};

// ===========================================================================
// Agent Lifecycle
// ===========================================================================

#[tokio::test]
async fn test_agent_register_heartbeat_deregister() {
    let server = TestGrpcServer::start().await;
    let mut client = agent_client(server.port).await;

    let reg = client
        .register_agent(Request::new(RegisterAgentRequest {
            name: "test-agent-1".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec!["env=test".to_string()],
        }))
        .await
        .unwrap();
    let agent_id = reg.into_inner().agent_id;
    assert!(!agent_id.is_empty());

    let hb = client
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: agent_id.clone(),
            capabilities: test_capabilities(),
        }))
        .await
        .unwrap();
    assert!(hb.into_inner().acknowledged);

    let dereg = client
        .deregister_agent(Request::new(DeregisterAgentRequest {
            agent_id: agent_id.clone(),
            reason: "test cleanup".to_string(),
        }))
        .await
        .unwrap();
    assert!(dereg.into_inner().acknowledged);
}

#[tokio::test]
async fn test_agent_register_invalid_name() {
    let server = TestGrpcServer::start().await;
    let mut client = agent_client(server.port).await;

    let result = client
        .register_agent(Request::new(RegisterAgentRequest {
            name: "".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_report_job_result_wrong_agent() {
    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    let resp = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let reg_a = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "agent-a".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_a_id = reg_a.into_inner().agent_id;

    agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: agent_a_id.clone(),
            capabilities: test_capabilities(),
        }))
        .await
        .unwrap();

    let reg_b = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "agent-b".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_b_id = reg_b.into_inner().agent_id;

    let result = agent_cl
        .report_job_result(Request::new(ReportJobResultRequest {
            agent_id: agent_b_id,
            job_id: job_id.clone(),
            final_state: 5,
            exit_code: Some(0),
            message: "done".to_string(),
            started_at: None,
            finished_at: None,
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_stream_job_logs_from_agent() {
    let server = TestGrpcServer::start().await;
    let mut client = agent_client(server.port).await;
    let mut job_cl = job_client(server.port).await;

    // Create a real job so the server can find it
    let job_id = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let entries = vec![
        LogEntry {
            job_id: job_id.clone(),
            sequence: 0,
            timestamp: None,
            line: "line 1".to_string(),
            stream: 1,
        },
        LogEntry {
            job_id: job_id.clone(),
            sequence: 1,
            timestamp: None,
            line: "line 2".to_string(),
            stream: 1,
        },
        LogEntry {
            job_id: job_id.clone(),
            sequence: 2,
            timestamp: None,
            line: "line 3".to_string(),
            stream: 2,
        },
    ];

    let stream = tokio_stream::iter(entries);
    let resp = client.stream_job_logs(stream).await.unwrap();
    assert_eq!(resp.into_inner().entries_received, 3);

    let stored = server.job_log_store.get_logs(&job_id, 100).await.unwrap();
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[0].message, "line 1");
    assert_eq!(stored[1].message, "line 2");
    assert_eq!(stored[2].message, "line 3");
    assert_eq!(stored[2].stream, "stderr");

    assert!(!server.log_collectors.contains_key(&job_id));
}

#[tokio::test]
async fn test_agent_logs_readable_via_get_logs() {
    let server = TestGrpcServer::start().await;
    let mut agent_cl = agent_client(server.port).await;
    let mut log_cl = log_client(server.port).await;
    let mut job_cl = job_client(server.port).await;

    // Create a real job so the server can find it
    let job_id = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let entries: Vec<LogEntry> = (0u64..5)
        .map(|i| LogEntry {
            job_id: job_id.clone(),
            sequence: i,
            timestamp: None,
            line: format!("output line {i}"),
            stream: 1,
        })
        .collect();
    agent_cl
        .stream_job_logs(tokio_stream::iter(entries))
        .await
        .unwrap();

    let resp = log_cl
        .get_logs(Request::new(muli_proto::GetLogsRequest {
            job_id: job_id.clone(),
            tail: 100,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.entries.len(), 5);
    for (i, entry) in resp.entries.iter().enumerate() {
        assert_eq!(entry.line, format!("output line {i}"));
    }
    assert!(resp.is_complete);
}

#[tokio::test]
async fn test_stream_logs_receives_live_agent_logs_and_closes() {
    let server = TestGrpcServer::start().await;
    let mut agent_cl = agent_client(server.port).await;
    let mut log_cl = log_client(server.port).await;
    let mut job_cl = job_client(server.port).await;

    // Create a real job so the server can find it
    let job_id = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let (entry_tx, entry_rx) = tokio::sync::mpsc::channel::<LogEntry>(16);
    let stream = tokio_stream::wrappers::ReceiverStream::new(entry_rx);

    let agent_job_id = job_id.clone();
    let agent_task = tokio::spawn(async move {
        agent_cl
            .stream_job_logs(stream)
            .await
            .expect("stream_job_logs failed")
    });

    entry_tx
        .send(LogEntry {
            job_id: agent_job_id.clone(),
            sequence: 0,
            timestamp: None,
            line: "first line".to_string(),
            stream: 1,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut log_stream = log_cl
        .stream_logs(Request::new(StreamLogsRequest {
            job_id: job_id.clone(),
            follow: true,
            since_sequence: None,
        }))
        .await
        .unwrap()
        .into_inner();

    entry_tx
        .send(LogEntry {
            job_id: agent_job_id.clone(),
            sequence: 1,
            timestamp: None,
            line: "second line".to_string(),
            stream: 1,
        })
        .await
        .unwrap();
    entry_tx
        .send(LogEntry {
            job_id: agent_job_id.clone(),
            sequence: 2,
            timestamp: None,
            line: "third line".to_string(),
            stream: 2,
        })
        .await
        .unwrap();

    drop(entry_tx);

    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, log_stream.message()).await {
            Ok(Ok(Some(entry))) => received.push(entry),
            Ok(Ok(None)) | Ok(Err(_)) => break,
            Err(_) => panic!("stream_logs did not close within timeout (deadlock?)"),
        }
    }

    assert!(
        received.len() >= 2,
        "expected ≥2 live log entries, got {}",
        received.len()
    );

    agent_task.await.unwrap();

    let stored = server.job_log_store.get_logs(&job_id, 100).await.unwrap();
    assert_eq!(stored.len(), 3, "all 3 lines should be persisted");
}
