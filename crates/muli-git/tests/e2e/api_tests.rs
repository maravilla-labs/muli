// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;

// ---------------------------------------------------------------------------
// REST API tests (no git binary required)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_check() {
    let srv = start_server().await;
    let resp = reqwest::get(format!("{}/-/health", base_url(&srv)))
        .await
        .expect("request");
    assert_eq!(resp.status().as_u16(), 200);
}

#[tokio::test]
async fn test_create_and_list_repo() {
    let srv = start_server().await;

    // Create a repository
    let (status, body) = api_post(
        &srv,
        "/api/v1/repos",
        json!({
            "namespace": NAMESPACE,
            "name": "hello-world",
            "description": "A test repo",
            "is_private": false
        }),
    )
    .await;
    assert_eq!(status, 201, "create failed: {body}");
    assert_eq!(body["name"], "hello-world");
    assert_eq!(body["namespace"], NAMESPACE);
    assert_eq!(body["tenant_id"], TENANT);

    // List repositories — should return one
    let (status, body) = api_get(&srv, "/api/v1/repos").await;
    assert_eq!(status, 200);
    let repos = body.as_array().expect("array");
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0]["name"], "hello-world");
}

#[tokio::test]
async fn test_create_duplicate_repo_fails() {
    let srv = start_server().await;

    let payload = json!({
        "namespace": NAMESPACE,
        "name": "dup-repo",
        "description": "",
        "is_private": false
    });
    let (s1, _) = api_post(&srv, "/api/v1/repos", payload.clone()).await;
    assert_eq!(s1, 201);

    // Second create with the same name should fail
    let (s2, body) = api_post(&srv, "/api/v1/repos", payload).await;
    assert_eq!(s2, 409, "expected conflict: {body}");
}

#[tokio::test]
async fn test_delete_repo() {
    let srv = start_server().await;

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "to-delete", "description": "", "is_private": false}),
    )
    .await;

    let status = api_delete(&srv, &format!("/api/v1/repos/{NAMESPACE}/to-delete")).await;
    assert_eq!(status, 204);

    // Verify gone
    let (list_status, body) = api_get(&srv, "/api/v1/repos").await;
    assert_eq!(list_status, 200);
    assert_eq!(body.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_delete_nonexistent_repo_returns_404() {
    let srv = start_server().await;
    let status = api_delete(&srv, &format!("/api/v1/repos/{NAMESPACE}/ghost")).await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn test_create_and_list_webhook() {
    let srv = start_server().await;

    // Create repo first
    let (_, repo) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "hook-repo", "description": "", "is_private": false}),
    )
    .await;
    let repo_id = repo["id"].as_str().expect("id");

    // Create webhook
    let (status, hook) = api_post(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/hook-repo/hooks"),
        json!({
            "url": "https://example.com/hook",
            "secret": "my-secret",
            "events": ["push"]
        }),
    )
    .await;
    assert_eq!(status, 201, "webhook create failed: {hook}");
    assert_eq!(hook["url"], "https://example.com/hook");
    assert_eq!(hook["repo_id"], repo_id);

    // List webhooks
    let (status, list) = api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/hook-repo/hooks")).await;
    assert_eq!(status, 200);
    assert_eq!(list.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_auth_required() {
    let srv = start_server().await;
    // Request without any Authorization header
    let resp = reqwest::Client::new()
        .get(format!("{}/api/v1/repos", base_url(&srv)))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn test_auth_wrong_token() {
    let srv = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/api/v1/repos", base_url(&srv)))
        .header("Authorization", "Bearer wrong-token")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status().as_u16(), 401);
}
