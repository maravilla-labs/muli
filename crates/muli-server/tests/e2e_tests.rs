// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! E2E tests: Core execution scenarios (success, bad image, nonzero exit, timeout).

mod common;

use std::time::Duration;

use muli_core::job::state_machine::JobState;

use common::e2e::{
    build_and_push_image, e2e_cleanup_image, e2e_skip_guards, start_e2e_registry, start_real_agent,
    submit_e2e_job,
};
use common::{TestGrpcServer, wait_for_terminal_state};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_full_e2e_registry_agent_execution() {
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
        "e2e-test-job:latest",
        "FROM alpine:latest\nCMD [\"echo\", \"hello muli e2e\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let job_id = submit_e2e_job(server.port, &image_ref, &registry, 60).await;
    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    agent.shutdown().await;

    assert_eq!(
        terminal,
        JobState::Succeeded,
        "E2E test: expected Succeeded, got {terminal:?}"
    );

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert!(job.result.is_some(), "job should have a result");
    assert_eq!(
        job.result.as_ref().unwrap().exit_code,
        Some(0),
        "exit code should be 0"
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_bad_image_job_fails() {
    if !muli_test::docker_helpers::docker_available().await {
        eprintln!("SKIP: Docker not available");
        return;
    }
    if e2e_skip_guards() {
        return;
    }

    let registry = start_e2e_registry().await;
    let _valid_image = build_and_push_image(
        &registry,
        "e2e-valid:latest",
        "FROM alpine:latest\nCMD [\"echo\", \"ok\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let bad_image = format!("{}/non-existent-image:latest", registry.registry_addr());
    let job_id = submit_e2e_job(server.port, &bad_image, &registry, 60).await;
    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    agent.shutdown().await;

    assert_eq!(
        terminal,
        JobState::Failed,
        "Expected Failed for non-existent image, got {terminal:?}"
    );

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert!(job.result.is_some(), "job should have a result");
    let msg = &job.result.as_ref().unwrap().message;
    assert!(
        msg.to_lowercase().contains("pull")
            || msg.to_lowercase().contains("not found")
            || msg.to_lowercase().contains("error"),
        "Expected image pull error message, got: {msg}"
    );

    e2e_cleanup_image(&_valid_image, &registry.docker_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_job_nonzero_exit_code() {
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
        "e2e-exit42:latest",
        "FROM alpine:latest\nCMD [\"sh\", \"-c\", \"sleep 1; exit 42\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let job_id = submit_e2e_job(server.port, &image_ref, &registry, 60).await;
    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    agent.shutdown().await;

    assert_eq!(
        terminal,
        JobState::Failed,
        "Expected Failed for non-zero exit code, got {terminal:?}"
    );

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert!(job.result.is_some(), "job should have a result");
    assert_eq!(
        job.result.as_ref().unwrap().exit_code,
        Some(42),
        "exit code should be 42, message: {}",
        job.result.as_ref().unwrap().message
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_job_timeout() {
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
        "e2e-sleep:latest",
        "FROM alpine:latest\nCMD [\"sleep\", \"300\"]\n",
    )
    .await;

    let server = TestGrpcServer::start().await;
    let job_id = submit_e2e_job(server.port, &image_ref, &registry, 5).await;
    let agent = start_real_agent(server.port).await;

    let terminal = wait_for_terminal_state(&server, &job_id, Duration::from_secs(120)).await;
    agent.shutdown().await;

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert_eq!(
        terminal,
        JobState::TimedOut,
        "Expected TimedOut for long-running container, got {:?}. Result: {:?}",
        terminal,
        job.result
    );

    assert!(job.result.is_some(), "job should have a result");
    let msg = job.result.as_ref().unwrap().message.to_lowercase();
    assert!(
        msg.contains("timeout") || msg.contains("timed out"),
        "Expected timeout message, got: {msg}"
    );

    e2e_cleanup_image(&image_ref, &registry.docker_config).await;
}
