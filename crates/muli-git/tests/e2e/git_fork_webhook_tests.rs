// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_fork_repo() {
    if !git_available() {
        eprintln!("SKIP test_fork_repo: git binary not found");
        return;
    }

    let srv = start_server().await;
    let src_name = "fork-source";
    let fork_name = "fork-dest";

    // Create and populate the source repo
    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": src_name, "description": "", "is_private": false}),
    )
    .await;

    let src_url = git_url(&srv, NAMESPACE, src_name);
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &src_url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("shared.txt"), "shared content\n").unwrap();
    git(work_dir.path(), &["add", "shared.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "shared commit"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Fork the repo
    let (status, fork_body) = api_post(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/{src_name}/forks"),
        json!({"dest_namespace": NAMESPACE, "dest_name": fork_name}),
    )
    .await;
    assert_eq!(status, 201, "fork failed: {fork_body}");
    assert_eq!(fork_body["name"], fork_name);
    assert!(
        fork_body["fork_of"].as_str().is_some(),
        "fork_of should be set"
    );

    // Clone the fork and verify the shared commit is there
    let fork_url = git_url(&srv, NAMESPACE, fork_name);
    let fork_dir = TempDir::new().unwrap();
    git(fork_dir.path(), &["clone", "--no-local", &fork_url, "."]).await;

    let content = std::fs::read_to_string(fork_dir.path().join("shared.txt")).unwrap();
    assert_eq!(content, "shared content\n");

    // Push a new commit to the fork (should not affect source)
    git(fork_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(fork_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(fork_dir.path().join("fork-only.txt"), "fork-only\n").unwrap();
    git(fork_dir.path(), &["add", "fork-only.txt"]).await;
    git(fork_dir.path(), &["commit", "-m", "fork-only commit"]).await;
    git(fork_dir.path(), &["push", "origin", "main"]).await;

    // Source should still only have the shared commit (no fork-only.txt)
    let src_verify = TempDir::new().unwrap();
    git(src_verify.path(), &["clone", "--no-local", &src_url, "."]).await;
    assert!(!src_verify.path().join("fork-only.txt").exists());
}

#[tokio::test]
async fn test_webhook_delivery() {
    if !git_available() {
        eprintln!("SKIP test_webhook_delivery: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "webhook-repo";

    // Create repo
    let (_, _repo) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    // Start a simple mock HTTP server to receive webhooks
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    let received = Arc::new(tokio::sync::Mutex::new(Vec::<Vec<u8>>::new()));
    let received_clone = received.clone();

    tokio::spawn(async move {
        // Accept up to 5 connections for this test
        for _ in 0..5 {
            if let Ok((mut stream, _)) = mock_listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 8192];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                buf.truncate(n);
                received_clone.lock().await.push(buf);
                // Send minimal HTTP 200 response
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            }
        }
    });

    // Register a webhook pointing at the mock server
    let hook_url = format!("http://127.0.0.1:{}/webhook", mock_addr.port());
    let (status, hook) = api_post(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/hooks"),
        json!({
            "url": hook_url,
            "secret": "test-secret",
            "events": ["push"]
        }),
    )
    .await;
    assert_eq!(status, 201, "webhook create: {hook}");

    // Push a commit — this should trigger the webhook
    let url = git_url(&srv, NAMESPACE, repo_name);
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("trigger.txt"), "trigger\n").unwrap();
    git(work_dir.path(), &["add", "trigger.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "trigger webhook"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Allow time for async webhook delivery
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let deliveries = received.lock().await;
    assert!(
        !deliveries.is_empty(),
        "expected at least one webhook delivery"
    );
    // Verify the request contains the HMAC signature header (case-insensitive)
    let request_str = String::from_utf8_lossy(&deliveries[0]);
    assert!(
        request_str
            .to_ascii_lowercase()
            .contains("x-hub-signature-256"),
        "expected HMAC signature header in: {}",
        &request_str[..request_str.len().min(500)]
    );
}
