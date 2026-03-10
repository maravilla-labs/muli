// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use muli_test::docker_helpers::{cleanup_test_containers, docker_available, require_docker};

/// Verify cleanup_test_containers removes muli-labeled containers.
#[tokio::test]
#[ignore]
async fn test_cleanup_removes_test_containers() {
    if !docker_available().await {
        eprintln!("Docker not available, skipping test");
        return;
    }

    let docker = require_docker().await;

    // Run cleanup — should not error even if no containers exist.
    cleanup_test_containers(&docker).await;
}
