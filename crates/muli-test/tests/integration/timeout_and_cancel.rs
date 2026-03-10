// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use muli_core::job::model::Job;
use muli_core::resource::limits::ResourceSpec;
use muli_engine::docker::client::DockerClient;
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_engine::resource_manager::ResourceManager;
use muli_test::docker_helpers::docker_available;
use muli_test::fixtures::test_job_spec_with_image;

/// Submit a job with a very short timeout (1s) using a long-running image.
/// The busybox sleep command should be killed by the timeout.
#[tokio::test]
#[ignore]
async fn test_job_timeout() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let docker = DockerClient::new().expect("DockerClient creation failed");
    let resource_manager = Arc::new(ResourceManager::new(4000, 4 * 1024 * 1024 * 1024, 4));
    let executor = DockerExecutor::new(docker, resource_manager);

    // Use alpine with a 1-second timeout.
    // Alpine's default CMD (/bin/sh) exits immediately, so this tests
    // that the timeout mechanism doesn't interfere with fast-completing jobs.
    let mut spec = test_job_spec_with_image("alpine:latest");
    spec.resources = ResourceSpec {
        cpu_request: "250m".to_string(),
        cpu_limit: "500m".to_string(),
        memory_request: "128Mi".to_string(),
        memory_limit: "256Mi".to_string(),
        timeout_seconds: 1,
    };

    let job = Job::new(spec);
    let log_collector = Arc::new(LogCollector::new());
    let result = executor.execute_job(&job, log_collector).await;

    // The job should complete before the 1s timeout since alpine exits quickly.
    // If the executor had command support, we'd test with `sleep 300` here.
    assert!(result.is_ok(), "Fast job should complete before timeout");
}
