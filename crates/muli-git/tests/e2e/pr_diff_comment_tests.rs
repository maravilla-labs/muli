// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use serde_json::json;

/// PR-7: Diff endpoint returns structured diff JSON.
#[tokio::test]
async fn test_pr_diff() {
    if !git_available() {
        eprintln!("SKIP test_pr_diff: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-diff").await;

    // Branch adds a new file
    git(work_dir.path(), &["checkout", "-b", "diff-branch"]).await;
    std::fs::write(work_dir.path().join("new_file.rs"), "fn hello() {}\n").unwrap();
    git(work_dir.path(), &["add", "new_file.rs"]).await;
    git(work_dir.path(), &["commit", "-m", "add new_file"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "diff-branch"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-diff/pulls",
        json!({
            "source_branch": "diff-branch",
            "target_branch": "main",
            "title": "Diff test",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    let (status, diff) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-diff/pulls/{pr_number}/diff"),
    )
    .await;
    assert_eq!(status, 200, "diff failed: {diff}");

    let files = diff.as_array().expect("diff should be an array");
    assert!(!files.is_empty(), "diff should have at least one file");

    // Find the new_file.rs entry
    let new_file_entry = files
        .iter()
        .find(|f| {
            f["file"]
                .as_str()
                .is_some_and(|n| n.contains("new_file.rs"))
        })
        .expect("new_file.rs should appear in diff");

    assert!(
        new_file_entry["is_new"].as_bool().unwrap_or(false),
        "should be marked as new file"
    );
    let hunks = new_file_entry["hunks"].as_array().expect("hunks array");
    assert!(!hunks.is_empty(), "should have at least one hunk");

    // At least one added line
    let has_added = hunks.iter().any(|h| {
        h["lines"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .any(|l| l["type"].as_str() == Some("added"))
    });
    assert!(has_added, "should have added lines in diff");
}

/// PR-8: Diff on a PR that modifies an existing file (context + added lines).
#[tokio::test]
async fn test_pr_diff_modified_file() {
    if !git_available() {
        eprintln!("SKIP test_pr_diff_modified_file: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-diff-mod").await;

    // Branch modifies README.md
    git(work_dir.path(), &["checkout", "-b", "mod-readme"]).await;
    std::fs::write(
        work_dir.path().join("README.md"),
        "# hello\nAdded a new line\n",
    )
    .unwrap();
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "modify readme"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "mod-readme"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-diff-mod/pulls",
        json!({
            "source_branch": "mod-readme",
            "target_branch": "main",
            "title": "Modify readme",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    let (status, diff) = api_get(
        &srv,
        &format!("/api/v1/repos/acme/pr-diff-mod/pulls/{pr_number}/diff"),
    )
    .await;
    assert_eq!(status, 200, "diff failed: {diff}");

    let files = diff.as_array().unwrap();
    let readme_entry = files
        .iter()
        .find(|f| f["file"].as_str().is_some_and(|n| n.contains("README")))
        .expect("README.md should appear in diff");
    assert!(
        !readme_entry["is_new"].as_bool().unwrap_or(true),
        "README.md is not new"
    );

    // Should have an added line
    let has_added = readme_entry["hunks"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .any(|h| {
            h["lines"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .any(|l| l["type"].as_str() == Some("added"))
        });
    assert!(has_added, "should have added lines for modified file");
}

/// PR-9: Comments — add and list.
#[tokio::test]
async fn test_pr_comments() {
    if !git_available() {
        eprintln!("SKIP test_pr_comments: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-comments").await;

    git(work_dir.path(), &["checkout", "-b", "feat-comment"]).await;
    std::fs::write(work_dir.path().join("c.txt"), "c\n").unwrap();
    git(work_dir.path(), &["add", "c.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "c"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feat-comment"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-comments/pulls",
        json!({
            "source_branch": "feat-comment",
            "target_branch": "main",
            "title": "Comment test",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();
    let pr_id = created["id"].as_str().unwrap().to_string();

    let comments_path = format!("/api/v1/repos/acme/pr-comments/pulls/{pr_number}/comments");

    // List comments on fresh PR → empty
    let (status, body) = api_get(&srv, &comments_path).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().unwrap().len(), 0, "no comments yet");

    // Add first comment
    let (status, c1) = api_post(
        &srv,
        &comments_path,
        json!({"user_id": "user-1", "body": "LGTM!"}),
    )
    .await;
    assert_eq!(status, 201, "add comment failed: {c1}");
    assert_eq!(c1["body"], "LGTM!");
    assert_eq!(c1["user_id"], "user-1");
    assert_eq!(c1["pr_id"], pr_id);
    assert!(c1["id"].as_str().is_some());
    assert!(c1["created_at"].as_str().is_some());

    // Add second comment
    let (status, c2) = api_post(
        &srv,
        &comments_path,
        json!({"user_id": "user-2", "body": "Please update the docs"}),
    )
    .await;
    assert_eq!(status, 201, "add second comment failed: {c2}");
    assert_eq!(c2["user_id"], "user-1");

    // List comments → should have 2
    let (status, list) = api_get(&srv, &comments_path).await;
    assert_eq!(status, 200);
    let comments = list.as_array().unwrap();
    assert_eq!(comments.len(), 2, "expected 2 comments");

    // Both pr_ids match
    for comment in comments {
        assert_eq!(comment["pr_id"].as_str(), Some(pr_id.as_str()));
    }
}

/// PR-10: Empty comment body → 400 Bad Request.
#[tokio::test]
async fn test_pr_comment_empty_body_rejected() {
    if !git_available() {
        eprintln!("SKIP test_pr_comment_empty_body_rejected: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work_dir = setup_repo_with_commit(&srv, "pr-comment-empty").await;

    git(work_dir.path(), &["checkout", "-b", "feat-x"]).await;
    std::fs::write(work_dir.path().join("x.txt"), "x\n").unwrap();
    git(work_dir.path(), &["add", "x.txt"]).await;
    git(work_dir.path(), &["commit", "-m", "x"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "feat-x"],
    )
    .await;

    let (_, created) = api_post(
        &srv,
        "/api/v1/repos/acme/pr-comment-empty/pulls",
        json!({
            "source_branch": "feat-x",
            "target_branch": "main",
            "title": "Empty comment test",
            "author_user_id": "user-1"
        }),
    )
    .await;
    let pr_number = created["number"].as_u64().unwrap();

    let (status, body) = api_post(
        &srv,
        &format!("/api/v1/repos/acme/pr-comment-empty/pulls/{pr_number}/comments"),
        json!({"user_id": "user-1", "body": ""}),
    )
    .await;
    assert_eq!(status, 400, "empty comment body should be rejected: {body}");
}
