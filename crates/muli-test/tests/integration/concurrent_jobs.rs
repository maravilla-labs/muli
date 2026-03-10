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

/// Submit multiple jobs concurrently and verify all complete.
#[tokio::test]
#[ignore]
async fn test_concurrent_jobs_complete() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let docker = DockerClient::new().expect("DockerClient creation failed");
    // Enough resources for 3 concurrent jobs (500m cpu each = 1500m needed)
    let resource_manager = Arc::new(ResourceManager::new(4000, 4 * 1024 * 1024 * 1024, 4));
    let executor = Arc::new(DockerExecutor::new(docker, resource_manager));

    let mut handles = vec![];
    for _ in 0..3 {
        let exec = executor.clone();
        let job = Job::new(test_job_spec());
        handles.push(tokio::spawn(async move {
            let log_collector = Arc::new(LogCollector::new());
            exec.execute_job(&job, log_collector).await
        }));
    }

    let mut success_count = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(result)) => {
                assert_eq!(result.exit_code, Some(0));
                success_count += 1;
            }
            Ok(Err(e)) => {
                panic!("Job failed: {e}");
            }
            Err(e) => {
                panic!("Task join error: {e}");
            }
        }
    }

    assert_eq!(
        success_count, 3,
        "Expected all 3 concurrent jobs to succeed"
    );
}
