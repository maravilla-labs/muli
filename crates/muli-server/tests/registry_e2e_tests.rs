// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! E2E tests: Registry auth, cancellation, and error-reporting edge cases.

mod common;

use std::time::Duration;

use muli_core::job::state_machine::JobState;
use muli_proto::{CancelJobRequest, ReportJobResultRequest};
use tonic::Request;

use muli_test::grpc_helpers::test_submit_request;

use common::e2e::{
    build_and_push_image, e2e_cleanup_image, e2e_skip_guards, start_e2e_registry, start_real_agent,
    submit_e2e_job,
};
use common::{TestGrpcServer, wait_for_terminal_state, with_tenant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_wrong_registry_credentials() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    if e2e_skip_guards() {
        return;
    }

    let registry = start_e2e_registry().await;
    let image_ref = build_and_push_image(
        &registry,
        "e2e-auth-test:latest",
        "FROM alpine:latest\nCMD [\"echo\", \"hello\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;

    let mut job_cl = muli_test::grpc_helpers::job_client(server.port).await;
    let mut req = test_submit_request();
    req.runner_image = image_ref.clone();
    req.registry_credentials = Some(muli_proto::RegistryCredentials {
        server: format!("http://{}:{}", registry.docker_host, registry.port),
        username: "user".into(),
        password: "wrong-password-definitely-bad".into(),
    });

    let resp = job_cl
        .submit_job(with_tenant(req, "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    agent.shutdown().await;

    assert_eq!(
        terminal,
        JobState::Failed,
        "Expected Failed for wrong credentials, got {terminal:?}"
    );

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert!(job.result.is_some(), "job should have a result");
    let msg = &job.result.as_ref().unwrap().message;
    assert!(
        msg.to_lowercase().contains("auth")
            || msg.to_lowercase().contains("unauthorized")
            || msg.to_lowercase().contains("401")
            || msg.to_lowercase().contains("pull")
            || msg.to_lowercase().contains("error"),
        "Expected auth-related error, got: {msg}"
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_cancel_pending_job() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    if e2e_skip_guards() {
        return;
    }

    let registry = start_e2e_registry().await;
    let image_ref = build_and_push_image(
        &registry,
        "e2e-cancel-test:latest",
        "FROM alpine:latest\nCMD [\"echo\", \"should not run\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let job_id = submit_e2e_job(server.port, &image_ref, &registry, 60).await;

    let mut job_cl = muli_test::grpc_helpers::job_client(server.port).await;
    let cancel_resp = job_cl
        .cancel_job(with_tenant(
            CancelJobRequest {
                job_id: job_id.clone(),
                reason: "test cancellation".to_string(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert!(cancel_resp.into_inner().success);

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Cancelled);

    let agent = start_real_agent(server.port).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    agent.shutdown().await;

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert_eq!(
        job.state,
        JobState::Cancelled,
        "Cancelled job should remain Cancelled after agent start"
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_report_on_terminal_job_rejected() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    if e2e_skip_guards() {
        return;
    }

    let registry = start_e2e_registry().await;
    let image_ref = build_and_push_image(
        &registry,
        "e2e-terminal-test:latest",
        "FROM alpine:latest\nCMD [\"echo\", \"hello terminal\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let job_id = submit_e2e_job(server.port, &image_ref, &registry, 60).await;
    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    assert_eq!(terminal, JobState::Succeeded);

    let agent_id = agent.agent_id.clone();
    agent.shutdown().await;

    let mut agent_cl = muli_test::grpc_helpers::agent_client(server.port).await;
    let result = agent_cl
        .report_job_result(Request::new(ReportJobResultRequest {
            agent_id: agent_id.clone(),
            job_id: job_id.clone(),
            final_state: 6,
            exit_code: Some(1),
            message: "bogus report".to_string(),
            started_at: None,
            finished_at: None,
        }))
        .await;

    assert!(result.is_err(), "Expected error for report on terminal job");
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "Expected FailedPrecondition error"
    );

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert_eq!(
        job.state,
        JobState::Succeeded,
        "Job should still be Succeeded"
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_double_report_fails() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    if e2e_skip_guards() {
        return;
    }

    let registry = start_e2e_registry().await;
    let image_ref = build_and_push_image(
        &registry,
        "e2e-double-report:latest",
        "FROM alpine:latest\nCMD [\"echo\", \"hello double\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let job_id = submit_e2e_job(server.port, &image_ref, &registry, 60).await;
    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    assert_eq!(terminal, JobState::Succeeded);

    let agent_id = agent.agent_id.clone();
    agent.shutdown().await;

    let mut agent_cl = muli_test::grpc_helpers::agent_client(server.port).await;
    let result = agent_cl
        .report_job_result(Request::new(ReportJobResultRequest {
            agent_id,
            job_id: job_id.clone(),
            final_state: 5,
            exit_code: Some(0),
            message: "duplicate report".to_string(),
            started_at: None,
            finished_at: None,
        }))
        .await;

    assert!(result.is_err(), "Expected error for double report");
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "Expected FailedPrecondition error for double report"
    );

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert_eq!(
        job.state,
        JobState::Succeeded,
        "Job should still be Succeeded after double report attempt"
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}
