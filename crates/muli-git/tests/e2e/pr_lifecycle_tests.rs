// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;
use tempfile::TempDir;

/// PR-5: Close a PR (without merging).
#[tokio::test]
async fn test_pr_close() {
    if !git_available() {
        eprintln!("SKIP test_pr_close: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-close").await;

    git(work_dir.path(), &["checkout", "-b", "wip"]).await;
    std::fs::write(work_dir.path().join("wip.txt"), "wip\n").unwrap();
    git(work_dir.path(), &["add", "wip.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "wip"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "wip"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-close/pulls",
        json!({
            "source_branch": "wip",
            "target_branch": "main",
            "title": "WIP PR",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    // Close the PR
    let (status, body) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-close/pulls/{pr_number}"),
        json!({"action": "close"}),
    )
    .await;
    assert_eq!(status, 200, "close PR failed: {body}");
    assert_eq!(body["state"], "closed");

    // Verify closed state via GET
    let (status, body) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-close/pulls/{pr_number}"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["state"], "closed");

    // Attempting to close again → 422 (already closed)
    let (status, body) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-close/pulls/{pr_number}"),
        json!({"action": "close"}),
    )
    .await;
    assert_eq!(
        status, 422,
        "expected 422 closing already-closed PR: {body}"
    );

    // List with ?state=closed should return 1
    let (s, list) = api_get(&srv, "/api/v1/repos/acme/pr-close/pulls?state=closed").await;
    assert_eq!(s, 200);
    assert_eq!(list.as_array().unwrap().len(), 1);
}

/// PR-6: Merge a pull request — fast-forward-compatible change.
#[tokio::test]
async fn test_pr_merge() {
    if !git_available() {
        eprintln!("SKIP test_pr_merge: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-merge").await;

    // feature branch adds a new file (no conflict with main)
    git(work_dir.path(), &["checkout", "-b", "feature/add-service"]).await;
    std::fs::write(work_dir.path().join("service.rs"), "pub fn run() {}\n").unwrap();
    git(work_dir.path(), &["add", "service.rs"]).await;
    git(work_dir.path(), &["commit", "-m", "add service"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feature/add-service"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-merge/pulls",
        json!({
            "source_branch": "feature/add-service",
            "target_branch": "main",
            "title": "Add service module",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    // Merge the PR
    let (status, body) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-merge/pulls/{pr_number}"),
        json!({"action": "merge"}),
    )
    .await;
    assert_eq!(status, 200, "merge PR failed: {body}");
    assert_eq!(body["state"], "merged");
    assert!(
        body["merge_commit_sha"].as_str().is_some(),
        "merge_commit_sha should be set after merge: {body}"
    );

    // Verify merged state via GET
    let (status, body) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-merge/pulls/{pr_number}"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["state"], "merged");

    // After merge, merged file should appear on target branch
    let verify_dir = TempDir::new().unwrap();
    let url = git_url(&srv, NAMESPACE, "pr-merge");
    git(
        verify_dir.path(),
        &["clone", "--no-local", "-b", "main", &url, "."],
    )
    .await;
    assert!(
        verify_dir.path().join("service.rs").exists(),
        "service.rs should be on main after merge"
    );

    // Attempting to merge again → 422
    let (status, body) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-merge/pulls/{pr_number}"),
        json!({"action": "merge"}),
    )
    .await;
    assert_eq!(status, 422, "expected 422 re-merging: {body}");

    // Merged PR should appear in ?state=merged list
    let (s, list) = api_get(&srv, "/api/v1/repos/acme/pr-merge/pulls?state=merged").await;
    assert_eq!(s, 200);
    assert_eq!(list.as_array().unwrap().len(), 1);
}

/// PR-15: Full lifecycle — open, comment, merge, verify in list.
#[tokio::test]
async fn test_pr_full_lifecycle() {
    if !git_available() {
        eprintln!("SKIP test_pr_full_lifecycle: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-lifecycle").await;

    // Push a feature branch
    git(work_dir.path(), &["checkout", "-b", "lifecycle-feat"]).await;
    std::fs::write(work_dir.path().join("handler.rs"), "pub fn handle() {}\n").unwrap();
    git(work_dir.path(), &["add", "handler.rs"]).await;
    git(work_dir.path(), &["commit", "-m", "add handler"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "lifecycle-feat"],
    )
    .await;

    // 1. Create PR
    let (s, pr) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-lifecycle/pulls",
        json!({
            "source_branch": "lifecycle-feat",
            "target_branch": "main",
            "title": "Lifecycle PR",
            "description": "Full lifecycle test",
            "author_user_id": "alice"
        }),
    )
    .await;
    assert_eq!(s, 201, "create PR failed: {pr}");
    let pr_number = pr["number"].as_u64().unwrap();
    let pr_id = pr["id"].as_str().unwrap().to_string();
    assert_eq!(pr["state"], "open");

    // 2. Add a comment
    let (s, comment) = api_post(
        &srv,
        &format!("/api/v1/repos/acme/pr-lifecycle/pulls/{pr_number}/comments"),
        json!({"user_id": "bob", "body": "Looks good to me"}),
    )
    .await;
    assert_eq!(s, 201, "add comment failed: {comment}");

    // 3. Get diff
    let (s, diff) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-lifecycle/pulls/{pr_number}/diff"),
    )
    .await;
    assert_eq!(s, 200, "get diff failed: {diff}");
    assert!(!diff.as_array().unwrap().is_empty());

    // 4. Merge
    let (s, merged) = api_patch(
        &srv,
        &format!("/api/v1/repos/acme/pr-lifecycle/pulls/{pr_number}"),
        json!({"action": "merge"}),
    )
    .await;
    assert_eq!(s, 200, "merge failed: {merged}");
    assert_eq!(merged["state"], "merged");
    assert!(merged["merge_commit_sha"].as_str().is_some());

    // 5. Verify via list: 0 open, 1 merged
    let (s, open_list) = api_get(&srv, "/api/v1/repos/acme/pr-lifecycle/pulls?state=open").await;
    assert_eq!(s, 200);
    assert_eq!(open_list.as_array().unwrap().len(), 0);

    let (s, merged_list) =
        api_get(&srv, "/api/v1/repos/acme/pr-lifecycle/pulls?state=merged").await;
    assert_eq!(s, 200);
    assert_eq!(merged_list.as_array().unwrap().len(), 1);

    // 6. After merge, comments are still accessible
    let (s, comments) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-lifecycle/pulls/{pr_number}/comments"),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(comments.as_array().unwrap().len(), 1);
    assert_eq!(
        comments.as_array().unwrap()[0]["pr_id"].as_str(),
        Some(pr_id.as_str())
    );

    // 7. handler.rs must appear on main after merge
    let verify = TempDir::new().unwrap();
    let url = git_url(&srv, NAMESPACE, "pr-lifecycle");
    git(
        verify.path(),
        &["clone", "--no-local", "-b", "main", &url, "."],
    )
    .await;
    assert!(verify.path().join("handler.rs").exists());
}
