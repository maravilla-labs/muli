// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_push_branch() {
    if !git_available() {
        eprintln!("SKIP test_push_branch: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "test-branch";

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    // Clone, commit, push to main
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("file.txt"), "content\n").unwrap();
    git(work_dir.path(), &["add", "file.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "initial"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Create and push a feature branch
    git(work_dir.path(), &["checkout", "-b", "feature"]).await;
    std::fs::write(work_dir.path().join("feature.txt"), "feature\n").unwrap();
    git(work_dir.path(), &["add", "feature.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "feature commit"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feature"],
    )
    .await;

    // Verify both branches appear in the refs API
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200, "refs failed: {body}");
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        shortnames.contains(&"main"),
        "expected 'main' in refs, got {shortnames:?}"
    );
    assert!(
        shortnames.contains(&"feature"),
        "expected 'feature' in refs, got {shortnames:?}"
    );
}

#[tokio::test]
async fn test_push_and_delete_tag() {
    if !git_available() {
        eprintln!("SKIP test_push_and_delete_tag: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "test-tags";

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    // Clone, commit, push to main
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("file.txt"), "content\n").unwrap();
    git(work_dir.path(), &["add", "file.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "initial"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Create and push a tag
    git(work_dir.path(), &["tag", "v1.0"]).await;
    git(work_dir.path(), &["push", "origin", "v1.0"]).await;

    // Verify the tag appears in refs
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200, "refs failed: {body}");
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        shortnames.contains(&"v1.0"),
        "expected 'v1.0' tag in refs, got {shortnames:?}"
    );

    // Delete the tag via git push
    git(work_dir.path(), &["push", "origin", ":refs/tags/v1.0"]).await;

    // Verify the tag is gone
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200);
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        !shortnames.contains(&"v1.0"),
        "expected 'v1.0' to be deleted, but refs still contain: {shortnames:?}"
    );
}

#[tokio::test]
async fn test_push_annotated_tag() {
    if !git_available() {
        eprintln!("SKIP test_push_annotated_tag: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "test-annotated-tag";

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    // Clone, commit, push to main
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("file.txt"), "content\n").unwrap();
    git(work_dir.path(), &["add", "file.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "initial"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Create an annotated tag (includes tagger name/email and a message)
    git(
        work_dir.path(),
        &["tag", "-a", "v1.0", "-m", "Release v1.0"],
    )
    .await;
    git(work_dir.path(), &["push", "origin", "v1.0"]).await;

    // Verify the tag appears in refs
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200, "refs failed: {body}");
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        shortnames.contains(&"v1.0"),
        "expected annotated tag 'v1.0' in refs, got {shortnames:?}"
    );

    // Delete the annotated tag via git push refspec
    git(work_dir.path(), &["push", "origin", ":refs/tags/v1.0"]).await;

    // Verify the tag is gone
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200);
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        !shortnames.contains(&"v1.0"),
        "expected annotated tag 'v1.0' to be deleted, refs: {shortnames:?}"
    );
}

#[tokio::test]
async fn test_tag_rest_api() {
    if !git_available() {
        eprintln!("SKIP test_tag_rest_api: git binary not found");
        return;
    }

    let srv = start_server().await;
    let repo_name = "test-tag-rest";

    api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;

    let url = git_url(&srv, NAMESPACE, repo_name);

    // Clone, commit, push to main
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("file.txt"), "content\n").unwrap();
    git(work_dir.path(), &["add", "file.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "initial"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Get HEAD commit SHA
    let sha = git_output(work_dir.path(), &["log", "--format=%H", "-1"])
        .await
        .trim()
        .to_string();
    assert!(!sha.is_empty(), "expected a commit SHA");

    // Create a tag via REST API
    let (status, body) = api_post(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/tags"),
        json!({ "name": "v2.0", "target": sha }),
    )
    .await;
    assert_eq!(status, 201, "create tag failed: {body}");
    assert_eq!(body["name"], "v2.0");

    // Verify the tag appears in refs
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200);
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        shortnames.contains(&"v2.0"),
        "expected 'v2.0' in refs, got {shortnames:?}"
    );

    // Delete the tag via REST API
    let status = api_delete(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/tags/v2.0"),
    )
    .await;
    assert_eq!(status, 204, "delete tag expected 204");

    // Verify the tag is gone
    let (status, body) =
        api_get(&srv, &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs")).await;
    assert_eq!(status, 200);
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        !shortnames.contains(&"v2.0"),
        "expected 'v2.0' to be deleted, refs: {shortnames:?}"
    );
}
