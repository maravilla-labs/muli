// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use muli_core::job::model::Job;
use muli_engine::docker::client::DockerClient;
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_engine::resource_manager::ResourceManager;
use muli_test::docker_helpers::docker_available;
use muli_test::fixtures::{test_job_spec, test_job_spec_with_image};

async fn setup_executor() -> DockerExecutor {
    let docker = DockerClient::new().expect("DockerClient creation failed");
    let resource_manager = Arc::new(ResourceManager::new(4000, 4 * 1024 * 1024 * 1024, 4));
    DockerExecutor::new(docker, resource_manager)
}

/// Submit a simple alpine job that exits immediately with code 0.
#[tokio::test]
#[ignore]
async fn test_simple_job_succeeds() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let executor = setup_executor().await;
    let job = Job::new(test_job_spec());

    let log_collector = Arc::new(LogCollector::new());
    let result = executor
        .execute_job(&job, log_collector)
        .await
        .expect("Job execution failed");

    assert_eq!(result.exit_code, Some(0), "Expected exit code 0");
    assert!(result.container_id.is_some(), "Expected container_id");
}

/// Submit a job with a non-existent image to trigger a pull failure.
#[tokio::test]
#[ignore]
async fn test_failing_job() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let executor = setup_executor().await;
    let spec = test_job_spec_with_image("alpine:nonexistent-tag-999");
    let job = Job::new(spec);

    let log_collector = Arc::new(LogCollector::new());
    let result = executor.execute_job(&job, log_collector).await;
    assert!(
        result.is_err(),
        "Expected job to fail with non-existent image"
    );
}

/// Submit a job and verify the result structure.
#[tokio::test]
#[ignore]
async fn test_job_result_structure() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let executor = setup_executor().await;
    let job = Job::new(test_job_spec());

    let log_collector = Arc::new(LogCollector::new());
    let result = executor
        .execute_job(&job, log_collector)
        .await
        .expect("Job execution failed");

    // Verify result has all expected fields
    assert!(result.exit_code.is_some());
    assert!(!result.message.is_empty());
    assert!(result.container_id.is_some());
}
