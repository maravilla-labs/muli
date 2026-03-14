// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests: Log Streaming (Docker required, auto-skip).

mod common;

use std::time::Duration;

use muli_proto::StreamLogsRequest;
use tokio_util::sync::CancellationToken;
use muli_test::grpc_helpers::{job_client, log_client, test_submit_request};

use common::{TestGrpcServer, dummy_executor, run_job, wait_for_terminal_state, with_tenant};

#[tokio::test]
async fn test_stream_logs_follow() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut log_cl = log_client(server.port).await;

    let resp = job_cl
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

    tokio::spawn(async move {
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

    let mut stream = log_cl
        .stream_logs(with_tenant(
            StreamLogsRequest {
                job_id: job_id.clone(),
                follow: true,
                since_sequence: None,
            },
            "test-tenant",
        ))
        .await
        .unwrap()
        .into_inner();

    let mut entries = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let result = tokio::time::timeout_at(deadline, stream.message()).await;
        match result {
            Ok(Ok(Some(entry))) => entries.push(entry),
            Ok(Ok(None)) => break,
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    assert!(!entries.is_empty(), "expected at least one log entry");

    // Verify sequence numbers are monotonically increasing
    for window in entries.windows(2) {
        assert!(
            window[1].sequence >= window[0].sequence,
            "log entries should have non-decreasing sequence numbers: {} < {}",
            window[1].sequence,
            window[0].sequence,
        );
    }

    // Verify all entries reference the correct job
    for entry in &entries {
        assert_eq!(entry.job_id, job_id, "log entry should reference the submitted job");
    }

    cancel.cancel();
    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}

#[tokio::test]
async fn test_get_logs_after_completion() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }

    let docker = muli_test::docker_helpers::require_docker().await;
    muli_test::docker_helpers::ensure_test_image(&docker, "alpine:latest").await;

    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut log_cl = log_client(server.port).await;

    let resp = job_cl
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

    tokio::spawn(async move {
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

    wait_for_terminal_state(&server, &job_id, Duration::from_secs(60)).await;
    cancel.cancel();

    let logs_resp = log_cl
        .get_logs(with_tenant(
            muli_proto::GetLogsRequest {
                job_id: job_id.clone(),
                tail: 100,
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    let body = logs_resp.into_inner();

    assert!(body.is_complete, "logs for a completed job should be marked complete");
    assert!(!body.entries.is_empty(), "completed job should have at least one log entry");

    // Verify sequence ordering
    for window in body.entries.windows(2) {
        assert!(
            window[1].sequence >= window[0].sequence,
            "log entries should have non-decreasing sequence numbers: {} < {}",
            window[1].sequence,
            window[0].sequence,
        );
    }

    // Verify all entries reference the correct job
    for entry in &body.entries {
        assert_eq!(entry.job_id, job_id, "log entry should reference the submitted job");
    }

    muli_test::docker_helpers::cleanup_test_containers(&docker).await;
}
