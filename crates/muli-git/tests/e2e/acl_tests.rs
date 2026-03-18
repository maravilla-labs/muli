// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end HTTP ACL security tests.
//!
//! These tests verify that `check_repo_access` is properly enforced in the
//! HTTP auth middleware when `repo_store` and `collaborator_store` are wired
//! into `GitAuth`.

use super::harness::*;
use muli_core::git::{GitPermission, GitToken, RepositoryCollaborator};
use muli_git::auth::{hash_token, token_prefix};
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

async fn create_repo_scoped_token(
    srv: &TestServerWithAcl,
    repo_name: &str,
    permissions: Vec<GitPermission>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
) -> (String, String) {
    let repo = srv
        .http
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, repo_name)
        .await
        .expect("repo lookup")
        .expect("repo must exist");

    let plaintext = format!("repo-scope-{}", uuid::Uuid::new_v4().simple());
    let mut token = GitToken::new(
        TENANT.into(),
        hash_token(&plaintext),
        token_prefix(&plaintext),
        permissions,
        format!("repo-scoped token for {repo_name}"),
        expires_at,
    );
    token.repo_id = Some(repo.id.clone());
    let token_id = token.id.clone();
    srv.http
        .token_store
        .create_token(&token)
        .await
        .expect("create repo-scoped token");
    (plaintext, token_id)
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

/// Private repo: a repo-scoped pull token without a user_id can clone the matching repo.
#[tokio::test]
async fn test_http_private_repo_read_allowed_for_matching_repo_scoped_token() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-repo-scoped-read";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    let (token, _) =
        create_repo_scoped_token(&srv, repo_name, vec![GitPermission::Pull], None).await;

    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, &token);
    let clone_dir = TempDir::new().unwrap();
    let status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        status.success(),
        "clone of matching private repo should succeed for repo-scoped token: {status}"
    );
}

/// Private repo: a repo-scoped token cannot be reused to read a different private repo.
#[tokio::test]
async fn test_http_private_repo_read_denied_for_wrong_repo_scoped_token() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let source_repo = "priv-repo-scoped-source";
    let other_repo = "priv-repo-scoped-other";

    create_http_repo(&srv, source_repo, true, Some("user-1")).await;
    create_http_repo(&srv, other_repo, true, Some("user-1")).await;

    let (token, _) =
        create_repo_scoped_token(&srv, source_repo, vec![GitPermission::Pull], None).await;

    let url = git_url_with_token(&srv.http, NAMESPACE, other_repo, &token);
    let clone_dir = TempDir::new().unwrap();
    let status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        !status.success(),
        "clone of different private repo should fail for repo-scoped token"
    );
}

/// Private repo: a repo-scoped token must not bypass write ACL even if the token carries Push.
#[tokio::test]
async fn test_http_private_repo_push_denied_for_repo_scoped_token_without_user() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-repo-scoped-push";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    let (token, _) = create_repo_scoped_token(
        &srv,
        repo_name,
        vec![GitPermission::Pull, GitPermission::Push],
        None,
    )
    .await;

    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, &token);
    let clone_dir = TempDir::new().unwrap();
    let clone_status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        clone_status.success(),
        "clone should succeed before push is denied: {clone_status}"
    );

    git(clone_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(clone_dir.path(), &["config", "user.name", "Repo Scoped"]).await;
    std::fs::write(clone_dir.path().join("deny.txt"), "no push\n").unwrap();
    git(clone_dir.path(), &["add", "deny.txt"]).await;
    git(clone_dir.path(), &["commit", "-m", "attempt push"]).await;

    let push_status = git_status(
        clone_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;
    assert!(
        !push_status.success(),
        "push should fail for repo-scoped token without user identity"
    );
}

/// Private repo: a revoked repo-scoped token must be denied for clone.
#[tokio::test]
async fn test_http_private_repo_read_denied_for_revoked_repo_scoped_token() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-repo-scoped-revoked";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    let (token, token_id) =
        create_repo_scoped_token(&srv, repo_name, vec![GitPermission::Pull], None).await;
    srv.http
        .token_store
        .revoke_token(&token_id)
        .await
        .expect("revoke token");

    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, &token);
    let clone_dir = TempDir::new().unwrap();
    let status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        !status.success(),
        "clone should fail for revoked repo-scoped token"
    );
}

/// Private repo: an expired repo-scoped token must be denied for clone.
#[tokio::test]
async fn test_http_private_repo_read_denied_for_expired_repo_scoped_token() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-repo-scoped-expired";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    let (token, _) = create_repo_scoped_token(
        &srv,
        repo_name,
        vec![GitPermission::Pull],
        Some(chrono::Utc::now() - chrono::Duration::minutes(1)),
    )
    .await;

    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, &token);
    let clone_dir = TempDir::new().unwrap();
    let status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        !status.success(),
        "clone should fail for expired repo-scoped token"
    );
}
