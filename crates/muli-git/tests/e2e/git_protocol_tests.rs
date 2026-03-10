// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use base64::Engine as _;
use serde_json::{Value, json};
use tempfile::TempDir;

#[tokio::test]
async fn test_git_clone_empty_repo() {
    if !git_available() {
        eprintln!("SKIP test_git_clone_empty_repo: git binary not found");
        return;
    }

    let srv = start_server().await;

    // Create repository via REST API
    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "clone-empty", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let clone_dir = TempDir::new().unwrap();
    let url = git_url(&srv, NAMESPACE, "clone-empty");

    git(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;

    // The cloned directory should be a git repo
    assert!(clone_dir.path().join(".git").exists());
}

#[tokio::test]
async fn test_git_push_and_clone() {
    if !git_available() {
        eprintln!("SKIP test_git_push_and_clone: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "push-test";

    // Create the repository
    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let url = git_url(&srv, NAMESPACE, repo_name);

    // --- Clone the empty repo ---
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;

    // --- Configure identity (required for commits) ---
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;

    // --- Add a file and commit ---
    std::fs::write(work_dir.path().join("README.md"), "# hello\n").unwrap();
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "initial commit"]).await;

    // --- Push to the server ---
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // --- Fresh clone and verify ---
    let verify_dir = TempDir::new().unwrap();
    git(verify_dir.path(), &["clone", "--no-local", &url, "."]).await;

    let readme = std::fs::read_to_string(verify_dir.path().join("README.md")).unwrap();
    assert_eq!(readme, "# hello\n");
}

#[tokio::test]
async fn test_git_refs_after_push() {
    if !git_available() {
        eprintln!("SKIP test_git_refs_after_push: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "refs-test";

    // Create the repo
    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    // Clone, commit, push
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("file.txt"), "content\n").unwrap();
    git(work_dir.path(), &["add", "file.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "add file"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // List refs via REST API — should see 'main'
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200, "refs failed: {body}");
    let refs = body.as_array().expect("array");
    // The refs API returns full ref names (e.g. "refs/heads/main") in `name`
    // and short names (e.g. "main") in `shorthand`.
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        shortnames.contains(&"main"),
        "expected 'main' in shorthand refs, got {shortnames:?}"
    );
}

#[tokio::test]
async fn test_git_commits_after_push() {
    if !git_available() {
        eprintln!("SKIP test_git_commits_after_push: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "commits-test";

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;

    // Two commits
    std::fs::write(work_dir.path().join("a.txt"), "a").unwrap();
    git(work_dir.path(), &["add", "a.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "commit A"]).await;

    std::fs::write(work_dir.path().join("b.txt"), "b").unwrap();
    git(work_dir.path(), &["add", "b.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "commit B"]).await;

    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // REST API: list commits
    let (status, body) = api_get(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/commits"),
    )
    .await;
    assert_eq!(status, 200, "commits failed: {body}");
    let commits = body.as_array().expect("array");
    assert!(
        commits.len() >= 2,
        "expected >= 2 commits, got {}",
        commits.len()
    );
    // Most recent commit should be "commit B"
    assert_eq!(
        commits[0]["message"].as_str().unwrap_or("").trim(),
        "commit B"
    );
}

#[tokio::test]
async fn test_git_blob_content() {
    if !git_available() {
        eprintln!("SKIP test_git_blob_content: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "blob-test";

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("hello.txt"), "hello from muli\n").unwrap();
    git(work_dir.path(), &["add", "hello.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "add hello"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Fetch file content via REST API
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/api/v1/repos/{}/{}/contents/hello.txt",
            base_url(&srv),
            NAMESPACE,
            repo_name
        ))
        .header("Authorization", auth_header(&srv))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    // Content is base64-encoded in the response
    let encoded = body["content"].as_str().expect("content field");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .unwrap();
    assert_eq!(String::from_utf8(decoded).unwrap(), "hello from muli\n");
}
