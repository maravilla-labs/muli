// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;

/// PR-1: Create a pull request → 201, verify all fields present and correct.
#[tokio::test]
async fn test_pr_create() {
    if !git_available() {
        eprintln!("SKIP test_pr_create: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-create").await;

    // Create a feature branch with one commit
    git(work_dir.path(), &["checkout", "-b", "feature/hello"]).await;
    std::fs::write(work_dir.path().join("feature.txt"), "feature content\n").unwrap();
    git(work_dir.path(), &["add", "feature.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "add feature"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feature/hello"],
    )
    .await;

    let (status, body) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-create/pulls",
        json!({
            "source_branch": "feature/hello",
            "target_branch": "main",
            "title": "My first PR",
            "description": "Adds feature",
            "author_user_id": "user-1"
        }),
    )
    .await;
    assert_eq!(status, 201, "create PR failed: {body}");
    assert_eq!(body["number"], 1, "first PR should be #1");
    assert_eq!(body["title"], "My first PR");
    assert_eq!(body["state"], "open");
    assert_eq!(body["source_branch"], "feature/hello");
    assert_eq!(body["target_branch"], "main");
    assert_eq!(body["author_user_id"], "user-1");
    assert!(body["id"].as_str().is_some(), "id must be present");
    assert!(
        body["repo_id"].as_str().is_some(),
        "repo_id must be present"
    );
    assert!(
        body["created_at"].as_str().is_some(),
        "created_at must be present"
    );
    assert!(
        body["updated_at"].as_str().is_some(),
        "updated_at must be present"
    );
    assert!(
        body["merge_commit_sha"].is_null(),
        "merge_commit_sha should be null on open PR"
    );
}

/// PR-2: Sequential PR numbers increment correctly.
#[tokio::test]
async fn test_pr_sequential_numbers() {
    if !git_available() {
        eprintln!("SKIP test_pr_sequential_numbers: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-numbers").await;

    for i in 1u64..=3 {
        let branch = format!("feature/f{i}");
        git(work_dir.path(), &["checkout", "-b", &branch]).await;
        let fname = format!("f{i}.txt");
        std::fs::write(work_dir.path().join(&fname), format!("content {i}\n")).unwrap();
        git(work_dir.path(), &["add", &fname]).await;
        git(work_dir.path(), &["commit", "-m", &format!("feature {i}")]).await;
        git(
            work_dir.path(),
            &["push", "--set-upstream", "origin", &branch],
        )
        .await;
        // Return to main so subsequent branches are independent
        git(work_dir.path(), &["checkout", "main"]).await;

        let (status, body) = api_post(
            &srv,
            "/api/v1/repos/acme/pr-numbers/pulls",
            json!({
                "source_branch": branch,
                "target_branch": "main",
                "title": format!("PR {}", i),
                "author_user_id": "user-1"
            }),
        )
        .await;
        assert_eq!(status, 201, "create PR {i} failed: {body}");
        assert_eq!(body["number"], i, "PR number should be {i}");
    }
}

/// PR-3: List PRs with and without state filter.
#[tokio::test]
async fn test_pr_list() {
    if !git_available() {
        eprintln!("SKIP test_pr_list: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-list").await;

    // Create two branches
    for branch in &["feat-a", "feat-b"] {
        git(work_dir.path(), &["checkout", "-b", branch]).await;
        std::fs::write(work_dir.path().join(format!("{branch}.txt")), "x").unwrap();
        git(work_dir.path(), &["add", "."]).await;
        git(work_dir.path(), &["commit", "-m", branch]).await;
        git(
            work_dir.path(),
            &["push", "--set-upstream", "origin", branch],
        )
        .await;
        git(work_dir.path(), &["checkout", "main"]).await;

        api_post(
            &srv,
            "/api/v1/repos/acme/pr-list/pulls",
            json!({
                "source_branch": branch,
                "target_branch": "main",
                "title": format!("PR for {}", branch),
                "author_user_id": "user-1"
            }),
        )
        .await;
    }

    // List all PRs — should return 2
    let (status, body) = api_get(&srv, "/api/v1/repos/acme/pr-list/pulls").await;
    assert_eq!(status, 200, "list PRs failed: {body}");
    let prs = body.as_array().expect("expected JSON array");
    assert_eq!(prs.len(), 2, "expected 2 open PRs");

    // List filtered to open — should also return 2
    let (status, body) = api_get(&srv, "/api/v1/repos/acme/pr-list/pulls?state=open").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().unwrap().len(), 2);

    // List filtered to merged — should return 0
    let (status, body) = api_get(&srv, "/api/v1/repos/acme/pr-list/pulls?state=merged").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().unwrap().len(), 0, "no merged PRs yet");

    // List filtered to closed — should return 0
    let (status, body) = api_get(&srv, "/api/v1/repos/acme/pr-list/pulls?state=closed").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().unwrap().len(), 0, "no closed PRs yet");
}

/// PR-4: Get a specific PR by number.
#[tokio::test]
async fn test_pr_get() {
    if !git_available() {
        eprintln!("SKIP test_pr_get: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-get").await;

    git(work_dir.path(), &["checkout", "-b", "feat"]).await;
    std::fs::write(work_dir.path().join("feat.txt"), "feat\n").unwrap();
    git(work_dir.path(), &["add", "feat.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "feat"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feat"],
    )
    .await;

    let (s, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-get/pulls",
        json!({
            "source_branch": "feat",
            "target_branch": "main",
            "title": "Get test PR",
            "author_user_id": "user-42"
        }),
    )
    .await;
    assert_eq!(s, 201);
    let pr_number = created["number"].as_u64().unwrap();

    // Fetch by number
    let (status, body) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-get/pulls/{pr_number}"),
    )
    .await;
    assert_eq!(status, 200, "get PR failed: {body}");
    assert_eq!(body["number"], pr_number);
    assert_eq!(body["title"], "Get test PR");
    assert_eq!(body["state"], "open");

    // Get non-existent PR number → 404
    let (status, _) = api_get(&srv, "/api/v1/repos/acme/pr-get/pulls/9999").await;
    assert_eq!(status, 404, "expected 404 for non-existent PR");
}
