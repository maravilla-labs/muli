// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use muli_core::job::model::Job;
use muli_engine::docker::client::DockerClient;
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_engine::resource_manager::ResourceManager;
use muli_test::docker_helpers::docker_available;
use muli_test::fixtures::test_job_spec;

/// Submit a job and verify that log collection infrastructure works.
/// Since alpine's default CMD exits quickly, we verify the executor
/// completes without log collection errors.
#[tokio::test]
#[ignore]
async fn test_log_collection_completes() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let docker = DockerClient::new().expect("DockerClient creation failed");
    let resource_manager = Arc::new(ResourceManager::new(4000, 4 * 1024 * 1024 * 1024, 4));
    let executor = DockerExecutor::new(docker, resource_manager);

    let job = Job::new(test_job_spec());
    let log_collector = Arc::new(LogCollector::new());
    let result = executor
        .execute_job(&job, log_collector)
        .await
        .expect("Job execution failed");

    assert_eq!(result.exit_code, Some(0));
}
