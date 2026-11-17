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

/// Private repo: a repo-scoped token WITHOUT Push cannot push to its repo.
#[tokio::test]
async fn test_http_private_repo_push_denied_for_pull_only_repo_scoped_token() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    let repo_name = "priv-repo-scoped-push";

    create_http_repo(&srv, repo_name, true, Some("user-1")).await;

    let (token, _) =
        create_repo_scoped_token(&srv, repo_name, vec![GitPermission::Pull], None).await;

    let url = git_url_with_token(&srv.http, NAMESPACE, repo_name, &token);
    let clone_dir = TempDir::new().unwrap();
    let clone_status = git_status(clone_dir.path(), &["clone", "--no-local", &url, "."]).await;
    assert!(
        clone_status.success(),
        "clone should succeed: {clone_status}"
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
        "push should fail for a pull-only repo-scoped token"
    );
}

/// Private repo: a repo-scoped Pull+Push token pushes to its own repository
/// (no user), and nowhere else.
#[tokio::test]
async fn test_http_repo_scoped_push_token_pushes_to_its_repository_only() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }

    let srv = start_server_with_acl().await;
    create_http_repo(&srv, "scoped-push-own", true, Some("user-1")).await;
    create_http_repo(&srv, "scoped-push-other", true, Some("user-1")).await;

    let (token, _) = create_repo_scoped_token(
        &srv,
        "scoped-push-own",
        vec![GitPermission::Pull, GitPermission::Push],
        None,
    )
    .await;

    // Push to its own repository.
    let url = git_url_with_token(&srv.http, NAMESPACE, "scoped-push-own", &token);
    let dir = TempDir::new().unwrap();
    assert!(
        git_status(dir.path(), &["clone", "--no-local", &url, "."])
            .await
            .success()
    );
    git(dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(dir.path(), &["config", "user.name", "Repo Scoped"]).await;
    std::fs::write(dir.path().join("ok.txt"), "pushed\n").unwrap();
    git(dir.path(), &["add", "ok.txt"]).await;
    git(dir.path(), &["commit", "-m", "scoped push"]).await;
    let pushed = git_status(dir.path(), &["push", "origin", "main"]).await;
    assert!(pushed.success(), "push to its own repo: {pushed}");

    // The same commit to another repository: refused.
    let other = git_url_with_token(&srv.http, NAMESPACE, "scoped-push-other", &token);
    let refused = git_status(dir.path(), &["push", "--force", &other, "main"]).await;
    assert!(!refused.success(), "push to another repository must fail");

    // Neither the REST API (read or write) nor admin on its own repository.
    for path in [
        format!("/api/v1/repos/{NAMESPACE}/scoped-push-own/refs"),
        format!("/api/v1/repos/{NAMESPACE}/scoped-push-own/commits"),
        "/api/v1/repos".to_string(),
    ] {
        assert_eq!(get_with(&srv, &path, &token).await, 403, "{path}");
    }
    let status = reqwest::Client::new()
        .delete(format!(
            "http://127.0.0.1:{}/api/v1/repos/{NAMESPACE}/scoped-push-own",
            srv.http.addr.port()
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("HTTP request")
        .status()
        .as_u16();
    assert_eq!(status, 403, "delete (admin)");
}

/// Expired, revoked and other-tenant Pull+Push scoped tokens cannot push.
#[tokio::test]
async fn test_invalid_repo_scoped_push_tokens_are_refused() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }
    let srv = start_server_with_acl().await;
    let repo = "scoped-push-invalid";
    create_http_repo(&srv, repo, true, Some("user-1")).await;
    let receive = format!("/{NAMESPACE}/{repo}.git/info/refs?service=git-receive-pack");
    let past = Some(chrono::Utc::now() - chrono::Duration::minutes(1));
    let both = vec![GitPermission::Pull, GitPermission::Push];
    let cases = [
        (
            "expired",
            create_scoped_token_with(&srv, TENANT, repo, both.clone(), past, false).await,
        ),
        (
            "revoked",
            create_scoped_token_with(&srv, TENANT, repo, both.clone(), None, true).await,
        ),
        (
            "other tenant",
            create_scoped_token_with(&srv, "tenant-b", repo, both.clone(), None, false).await,
        ),
    ];
    for (what, token) in cases {
        let status = get_with(&srv, &receive, &token).await;
        assert!(status == 401 || status == 403, "{what}: {status}");
    }
    let ok = create_scoped_token_with(&srv, TENANT, repo, both, None, false).await;
    assert_eq!(
        get_with(&srv, &receive, &ok).await,
        200,
        "a valid one advertises receive-pack"
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

// ---------------------------------------------------------------------------
// Repository-scoped tokens: enforced scope, never fail-open
// ---------------------------------------------------------------------------

/// A repo-scoped token stored for `tenant` (which may differ from the server's).
async fn create_scoped_token_in(
    srv: &TestServerWithAcl,
    tenant: &str,
    repo_name: &str,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    revoked: bool,
) -> String {
    create_scoped_token_with(
        srv,
        tenant,
        repo_name,
        vec![GitPermission::Pull],
        expires_at,
        revoked,
    )
    .await
}

async fn create_scoped_token_with(
    srv: &TestServerWithAcl,
    tenant: &str,
    repo_name: &str,
    permissions: Vec<GitPermission>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    revoked: bool,
) -> String {
    let repo = srv
        .http
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, repo_name)
        .await
        .expect("repo lookup")
        .expect("repo must exist");
    let plaintext = format!("repo-scope-{}", uuid::Uuid::new_v4().simple());
    let mut token = GitToken::new(
        tenant.into(),
        hash_token(&plaintext),
        token_prefix(&plaintext),
        permissions,
        format!("repo-scoped token for {repo_name}"),
        expires_at,
    );
    token.repo_id = Some(repo.id.clone());
    token.revoked = revoked;
    srv.http
        .token_store
        .create_token(&token)
        .await
        .expect("create token");
    plaintext
}

/// Status of a GET with `token` as a bearer token.
async fn get_with(srv: &TestServerWithAcl, path: &str, token: &str) -> u16 {
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}{}", srv.http.addr.port(), path))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("HTTP request")
        .status()
        .as_u16()
}

fn upload_refs(repo: &str) -> String {
    format!("/{NAMESPACE}/{repo}.git/info/refs?service=git-upload-pack")
}

/// Valid scoped pull: its repo only. Other private or PUBLIC repos, a path
/// naming no repository, an unknown repository and push are all refused.
#[tokio::test]
async fn test_repo_scoped_token_reaches_exactly_its_repository() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }
    let srv = start_server_with_acl().await;
    create_http_repo(&srv, "scope-own", true, Some("user-1")).await;
    create_http_repo(&srv, "scope-other-private", true, Some("user-1")).await;
    create_http_repo(&srv, "scope-other-public", false, Some("user-1")).await;
    let token = create_scoped_token_in(&srv, TENANT, "scope-own", None, false).await;

    assert_eq!(
        get_with(&srv, &upload_refs("scope-own"), &token).await,
        200,
        "own repo"
    );
    assert_eq!(
        get_with(&srv, &upload_refs("scope-other-private"), &token).await,
        403,
        "other private repo"
    );
    assert_eq!(
        get_with(&srv, &upload_refs("scope-other-public"), &token).await,
        403,
        "other public repo"
    );
    assert_eq!(
        get_with(&srv, "/api/v1/repos", &token).await,
        403,
        "non-repo path"
    );
    assert_eq!(
        get_with(&srv, &upload_refs("does-not-exist"), &token).await,
        403,
        "unresolvable path"
    );
    assert_eq!(
        get_with(
            &srv,
            &format!("/{NAMESPACE}/scope-own.git/info/refs?service=git-receive-pack"),
            &token
        )
        .await,
        403,
        "push"
    );
}

/// Expired, revoked and other-tenant scoped tokens are refused, with and
/// without anonymous pull (where a rejected token must count as no token).
#[tokio::test]
async fn test_invalid_repo_scoped_tokens_are_refused() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }
    for anonymous in [false, true] {
        let srv = if anonymous {
            start_server_with_acl_anonymous().await
        } else {
            start_server_with_acl().await
        };
        let repo = "scope-invalid";
        // With anonymous pull git sends credentials only after a 401, so the
        // setup push cannot run: the repository stays empty (info/refs still
        // answers) and is created private through the API.
        let (status, _) = api_post(
            &srv.http,
            "/api/v1/repos",
            json!({ "namespace": NAMESPACE, "name": repo, "description": "", "is_private": true }),
        )
        .await;
        assert_eq!(status, 201, "create repo");
        let past = Some(chrono::Utc::now() - chrono::Duration::minutes(1));
        let cases = [
            (
                "expired",
                create_scoped_token_in(&srv, TENANT, repo, past, false).await,
            ),
            (
                "revoked",
                create_scoped_token_in(&srv, TENANT, repo, None, true).await,
            ),
            (
                "other tenant",
                create_scoped_token_in(&srv, "tenant-b", repo, None, false).await,
            ),
        ];
        for (what, token) in cases {
            let status = get_with(&srv, &upload_refs(repo), &token).await;
            assert!(
                status == 401 || status == 403,
                "{what} token (anonymous_pull={anonymous}) must be refused, got {status}"
            );
        }
        // Sanity: a valid one is accepted on the same server.
        let ok = create_scoped_token_in(&srv, TENANT, repo, None, false).await;
        assert_eq!(
            get_with(&srv, &upload_refs(repo), &ok).await,
            200,
            "anonymous_pull={anonymous}"
        );
    }
}
