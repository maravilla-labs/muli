// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end HTTP ACL security tests.
//!
//! These tests verify that `check_repo_access` is properly enforced in the
//! HTTP auth middleware when `repo_store` and `collaborator_store` are wired
//! into `GitAuth`.

use super::harness::*;
use muli_core::git::{GitPermission, RepositoryCollaborator};
use serde_json::json;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a repo (public or private) via REST, push an initial commit using
/// user-1's token, and optionally set `owner_id`.
///
/// user-1 is temporarily added as a collaborator with full permissions so the
/// initial push succeeds even when ACL is enforced. The collaborator entry is
/// removed after the push so tests start with a clean slate.
async fn create_http_repo(
    srv: &TestServerWithAcl,
    repo_name: &str,
    is_private: bool,
    owner_id: Option<&str>,
) {
    let (status, _) = api_post(
        &srv.http,
        "/api/v1/repos",
        json!({
            "namespace": NAMESPACE,
            "name": repo_name,
            "description": "",
            "is_private": is_private
        }),
    )
    .await;
    assert_eq!(status, 201, "create repo {repo_name}");

    // Look up repo to get its id
    let mut repo = srv
        .http
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, repo_name)
        .await
        .expect("repo lookup")
        .expect("repo must exist");

    // Set owner_id if requested
    if let Some(oid) = owner_id {
        repo.owner_id = oid.to_string();
        srv.http
            .repo_store
            .update_repository(&repo)
            .await
            .expect("set owner_id");
    }

    // Add user-1 as collaborator with full permissions for the initial push
    let setup_collab = RepositoryCollaborator {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: TENANT.to_string(),
        repo_id: repo.id.clone(),
        user_id: "user-1".to_string(),
        permissions: vec![
            GitPermission::Pull,
            GitPermission::Push,
            GitPermission::Admin,
        ],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&setup_collab)
        .await
        .expect("add setup collaborator");

    // Push an initial commit so the repo is not bare-empty
    let url = git_url(&srv.http, NAMESPACE, repo_name);
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("README.md"), "# init\n").unwrap();
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "initial commit"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;

    // Remove the setup collaborator so tests start clean
    srv.collaborator_store
        .remove_collaborator(&repo.id, "user-1")
        .await
        .expect("remove setup collaborator");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Public repo: an HTTP GET to info/refs should succeed without any auth header
/// when `check_repo_access` allows public reads.
///
/// NOTE: We test at the HTTP level (reqwest) because git always probes
/// without credentials first and expects a 401 challenge. A dedicated server
/// with `anonymous_pull` enabled is used, and the repo is created via the
/// REST API only (no git push needed — the info/refs probe works on an empty
/// repo too).
#[tokio::test]
async fn test_http_public_repo_read_no_auth() {
    let srv = start_server_with_acl_anonymous().await;
    let repo_name = "pub-anon-read";

    // Create a public repo via REST (authenticated). No need to push content —
    // info/refs responds even for empty repos.
    let (status, _) = api_post(
        &srv.http,
        "/api/v1/repos",
        json!({
            "namespace": NAMESPACE,
            "name": repo_name,
            "description": "",
            "is_private": false
        }),
    )
    .await;
    assert_eq!(status, 201);

    // Direct HTTP GET without auth — should succeed for public repo reads
    let client = reqwest::Client::new();
    let url = format!(
        "http://127.0.0.1:{}/{}/{}.git/info/refs?service=git-upload-pack",
        srv.http.addr.port(),
        NAMESPACE,
        repo_name
    );
    let resp = client.get(&url).send().await.expect("HTTP GET");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "anonymous GET on public repo should return 200"
    );
}

/// Public repo: authenticated user (user-2) who is NOT a collaborator — push should fail.
#[tokio::test]
async fn test_http_public_repo_push_denied_for_non_collaborator() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "pub-no-collab-push";

    create_http_repo(&srv, repo_name, false, Some("user-1")).await;

    // Clone as user-2 (public, so clone works)
    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, USER2_TOKEN);
    let clone_dir = TempDir::new().unwrap();
    let clone_st = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        clone_st.success(),
        "clone of public repo should succeed for any authenticated user: {clone_st}"
    );

    // Commit and try to push as user-2 — should be denied
    git(clone_dir.path(), &["config", "user.email", "u2@muli.test"]).await;
    git(clone_dir.path(), &["config", "user.name", "User Two"]).await;
    std::fs::write(clone_dir.path().join("bad.txt"), "nope\n").unwrap();
    git(clone_dir.path(), &["add", "bad.txt"]).await;
    git(clone_dir.path(), &["commit", "-m", "unauthorized push"]).await;

    let push_st = git_status(
        clone_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;
    assert!(
        !push_st.success(),
        "push to public repo should fail for non-collaborator user-2"
    );
}

/// Private repo: authenticated user-2 who is NOT a collaborator — read should fail.
#[tokio::test]
async fn test_http_private_repo_read_denied_for_non_collaborator() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-no-collab-read";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    // Try to clone as user-2 (not collaborator on this private repo)
    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, USER2_TOKEN);
    let clone_dir = TempDir::new().unwrap();
    let status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        !status.success(),
        "clone of private repo should fail for non-collaborator user-2"
    );
}

/// Private repo: user-2 is added as collaborator with Pull — clone should succeed.
#[tokio::test]
async fn test_http_private_repo_read_allowed_for_collaborator() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-collab-read";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    // Add user-2 as collaborator with Pull
    let repo = srv
        .http
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, repo_name)
        .await
        .expect("repo lookup")
        .expect("repo must exist");
    let collab = RepositoryCollaborator {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: TENANT.to_string(),
        repo_id: repo.id.clone(),
        user_id: "user-2".to_string(),
        permissions: vec![GitPermission::Pull],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&collab)
        .await
        .expect("add collaborator");

    // Clone as user-2 — should succeed
    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, USER2_TOKEN);
    let clone_dir = TempDir::new().unwrap();
    let status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        status.success(),
        "clone of private repo should succeed for collaborator with Pull: {status}"
    );
}

/// Private repo: owner (user-1) can push without being an explicit collaborator.
#[tokio::test]
async fn test_http_private_repo_push_allowed_for_owner() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-owner-push";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    // user-1 is the owner — push should succeed without explicit collaborator entry
    let url = git_url(&srv.http, NAMESPACE, repo_name);
    let clone_dir = TempDir::new().unwrap();
    git(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(clone_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(clone_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(clone_dir.path().join("owner.txt"), "owner push\n").unwrap();
    git(clone_dir.path(), &["add", "owner.txt"]).await;
    git(clone_dir.path(), &["commit", "-m", "owner push"]).await;

    let push_st = git_status(
        clone_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;
    assert!(
        push_st.success(),
        "owner should be able to push to their private repo: {push_st}"
    );
}
