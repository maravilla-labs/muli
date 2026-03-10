// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests: gRPC Job API (no Docker required).

mod common;

use std::time::Duration;

use muli_proto::{
    CancelJobRequest, DeleteJobRequest, GetJobStatusRequest, ListJobsRequest, WatchJobStatusRequest,
};

use muli_test::grpc_helpers::{job_client, test_submit_request};

use common::{TestGrpcServer, with_tenant};

#[tokio::test]
async fn test_submit_and_get_status() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;
    assert!(!job_id.is_empty());

    let status = client
        .get_job_status(with_tenant(
            GetJobStatusRequest {
                job_id: job_id.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    let inner = status.into_inner();
    assert_eq!(inner.job_id, job_id);
    assert_eq!(inner.state, 1); // Pending
}

#[tokio::test]
async fn test_cancel_pending_job() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let cancel_resp = client
        .cancel_job(with_tenant(
            CancelJobRequest {
                job_id: job_id.clone(),
                reason: String::new(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert!(cancel_resp.into_inner().success);

    let status = client
        .get_job_status(with_tenant(GetJobStatusRequest { job_id }, "test-tenant"))
        .await
        .unwrap();
    assert_eq!(status.into_inner().state, 7); // Cancelled
}

#[tokio::test]
async fn test_cancel_terminal_job_noop() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    client
        .cancel_job(with_tenant(
            CancelJobRequest {
                job_id: job_id.clone(),
                reason: String::new(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();

    let cancel_resp = client
        .cancel_job(with_tenant(
            CancelJobRequest {
                job_id: job_id.clone(),
                reason: String::new(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert!(!cancel_resp.into_inner().success);
}

#[tokio::test]
async fn test_list_jobs_with_filter() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    for _ in 0..3 {
        client
            .submit_job(with_tenant(test_submit_request(), "test-tenant"))
            .await
            .unwrap();
    }

    let list = client
        .list_jobs(with_tenant(
            ListJobsRequest {
                state_filter: None,
                tenant_id: Some("test-tenant".to_string()),
                priority_filter: None,
                limit: 100,
                offset: 0,
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    let jobs = list.into_inner().jobs;
    assert_eq!(jobs.len(), 3);

    client
        .cancel_job(with_tenant(
            CancelJobRequest {
                job_id: jobs[0].job_id.clone(),
                reason: String::new(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();

    let pending = client
        .list_jobs(with_tenant(
            ListJobsRequest {
                state_filter: Some(1),
                tenant_id: Some("test-tenant".to_string()),
                priority_filter: None,
                limit: 100,
                offset: 0,
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(pending.into_inner().jobs.len(), 2);

    let cancelled = client
        .list_jobs(with_tenant(
            ListJobsRequest {
                state_filter: Some(7),
                tenant_id: Some("test-tenant".to_string()),
                priority_filter: None,
                limit: 100,
                offset: 0,
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.into_inner().jobs.len(), 1);
}

#[tokio::test]
async fn test_delete_job() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let del = client
        .delete_job(with_tenant(
            DeleteJobRequest {
                job_id: job_id.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert!(del.into_inner().success);
}

#[tokio::test]
async fn test_watch_job_status_initial_event() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let mut stream = client
        .watch_job_status(with_tenant(
            WatchJobStatusRequest {
                job_id: job_id.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap()
        .into_inner();

    let first_event = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("timeout waiting for first event")
        .expect("stream error")
        .expect("stream ended without event");

    assert_eq!(first_event.job_id, job_id);
    assert_eq!(first_event.current_state, 1); // Pending
}

#[tokio::test]
async fn test_tenant_isolation() {
    let server = TestGrpcServer::start().await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let result = client
        .get_job_status(with_tenant(GetJobStatusRequest { job_id }, "other-tenant"))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
}
