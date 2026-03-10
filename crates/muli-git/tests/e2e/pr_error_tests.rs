// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;

/// PR-11: Merge conflict detection → 409 Conflict.
#[tokio::test]
async fn test_pr_merge_conflict() {
    if !git_available() {
        eprintln!("SKIP test_pr_merge_conflict: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-conflict").await;

    // Both main and feature branch modify the same line in README.md
    // Feature branch: edit README with conflicting content
    git(work_dir.path(), &["checkout", "-b", "conflict-branch"]).await;
    std::fs::write(
        work_dir.path().join("README.md"),
        "# conflict version from branch\n",
    )
    .unwrap();
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "branch edit"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "conflict-branch"],
    )
    .await;

    // Also modify README.md on main (independently)
    git(work_dir.path(), &["checkout", "main"]).await;
    std::fs::write(
        work_dir.path().join("README.md"),
        "# conflict version from main\n",
    )
    .unwrap();
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "main edit"]).await;
    git(work_dir.path(), &["push", "origin", "main"]).await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-conflict/pulls",
        json!({
            "source_branch": "conflict-branch",
            "target_branch": "main",
            "title": "Conflicting PR",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    let (status, body) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-conflict/pulls/{pr_number}"),
        json!({"action": "merge"}),
    )
    .await;
    assert_eq!(status, 409, "expected 409 Conflict: {body}");
    assert!(
        body["error"].as_str().is_some(),
        "error field should be present: {body}"
    );

    // PR should still be open after conflict
    let (s, pr) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-conflict/pulls/{pr_number}"),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(
        pr["state"], "open",
        "PR should still be open after conflict: {pr}"
    );
}

/// PR-12: Invalid action in PATCH → 400.
#[tokio::test]
async fn test_pr_invalid_patch_action() {
    if !git_available() {
        eprintln!("SKIP test_pr_invalid_patch_action: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-invalid-action").await;

    git(work_dir.path(), &["checkout", "-b", "feat-z"]).await;
    std::fs::write(work_dir.path().join("z.txt"), "z\n").unwrap();
    git(work_dir.path(), &["add", "z.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "z"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feat-z"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-invalid-action/pulls",
        json!({
            "source_branch": "feat-z",
            "target_branch": "main",
            "title": "Invalid action test",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    let (status, body) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-invalid-action/pulls/{pr_number}"),
        json!({"action": "rebase"}),
    )
    .await;
    assert_eq!(status, 400, "unknown action should return 400: {body}");
}

/// PR-13: Create PR on non-existent repository → 404.
#[tokio::test]
async fn test_pr_on_nonexistent_repo() {
    let srv = start_server().await;
    let (status, body) = api_post(
        &srv,
        "/api/v1/repos/acme/ghost-repo/pulls",
        json!({
            "source_branch": "feature",
            "target_branch": "main",
            "title": "Ghost PR",
            "author_user_id": "user-1"
        }),
    )
    .await;
    assert_eq!(status, 404, "should 404 for non-existent repo: {body}");
}

/// PR-14: Create PR with missing required fields → 400.
#[tokio::test]
async fn test_pr_create_missing_fields() {
    if !git_available() {
        eprintln!("SKIP test_pr_create_missing_fields: git binary not found");
        return;
    }
    let srv = start_server().await;
    let _work_dir = setup_repo_with_commit(&srv, "pr-missing-fields").await;

    // Missing title
    let (status, _) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-missing-fields/pulls",
        json!({
            "source_branch": "feature",
            "target_branch": "main",
            "title": "",
            "author_user_id": "user-1"
        }),
    )
    .await;
    assert_eq!(status, 400, "empty title should be rejected");

    // Missing source_branch
    let (status, _) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-missing-fields/pulls",
        json!({
            "source_branch": "",
            "target_branch": "main",
            "title": "Some title",
            "author_user_id": "user-1"
        }),
    )
    .await;
    assert_eq!(status, 400, "empty source_branch should be rejected");
}
