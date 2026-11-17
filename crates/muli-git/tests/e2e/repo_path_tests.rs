// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The repository a request is authorized against is the repository it is
//! served from.
//!
//! Before, the auth middleware and the handlers each derived the repository
//! from the path with different rules (all `.git` suffixes vs one, raw vs
//! percent-decoded, REST verbatim vs stripped), and a path the middleware
//! could not resolve skipped the per-repository check. These tests send
//! tricky paths over a raw TCP connection (an HTTP client library would
//! normalize `..` and `%2e%2e` before sending) and check that every request
//! is either refused or served from the repository it was authorized for.
//!
//! Setup: user-1 owns private `x`, private `x.git` and private `y`; user-2
//! may read and write `x` only.

use muli_core::git::{GitPermission, GitToken, RepositoryCollaborator};
use muli_git::auth::{hash_token, token_prefix};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::harness::*;

const SECRET: &[u8] = b"x.git's private LFS object";

/// Send one request with exactly this request-target; return (status, body).
async fn raw(
    srv: &TestServer,
    method: &str,
    target: &str,
    token: Option<&str>,
    body: &str,
) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(srv.addr).await.unwrap();
    let mut req = format!("{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("Authorization: Bearer {t}\r\n"));
    }
    if !body.is_empty() {
        req.push_str("Content-Type: application/vnd.git-lfs+json\r\n");
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    let text = String::from_utf8_lossy(&resp).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

async fn private_repo(srv: &TestServerWithAcl, name: &str) -> String {
    let (status, body) = api_post(
        &srv.http,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": name, "description": "", "is_private": true}),
    )
    .await;
    assert_eq!(status, 201, "create {name}: {body}");
    let mut repo = srv
        .http
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, name)
        .await
        .unwrap()
        .unwrap();
    repo.owner_id = "user-1".to_string();
    srv.http.repo_store.update_repository(&repo).await.unwrap();
    repo.id
}

/// Push one commit with `marker` in it to repository NAMED `name` as user-1
/// (git URL `/{ns}/{name}.git`); return the commit SHA.
async fn push_commit(srv: &TestServer, name: &str, marker: &str) -> String {
    let work = TempDir::new().unwrap();
    git(work.path(), &["init", "-q", "-b", "main"]).await;
    git(work.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work.path().join("which.txt"), marker).unwrap();
    git(work.path(), &["add", "which.txt"]).await;
    git(work.path(), &["commit", "-q", "-m", marker]).await;
    git(
        work.path(),
        &["push", "-q", &git_url(srv, NAMESPACE, name), "main"],
    )
    .await;
    git_output(work.path(), &["rev-parse", "HEAD"])
        .await
        .trim()
        .to_string()
}

struct Fixture {
    srv: TestServerWithAcl,
    x_sha: String,
    xgit_sha: String,
    secret_oid: String,
    x_id: String,
}

async fn fixture() -> Option<Fixture> {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return None;
    }
    let srv = start_server_with_acl().await;
    let x_id = private_repo(&srv, "x").await;
    private_repo(&srv, "x.git").await;
    private_repo(&srv, "y").await;
    srv.collaborator_store
        .upsert_collaborator(&RepositoryCollaborator {
            id: uuid::Uuid::new_v4().to_string(),
            tenant_id: TENANT.to_string(),
            repo_id: x_id.clone(),
            user_id: "user-2".to_string(),
            permissions: vec![GitPermission::Pull, GitPermission::Push],
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let x_sha = push_commit(&srv.http, "x", "repo x").await;
    let xgit_sha = push_commit(&srv.http, "x.git", "repo x.git").await;
    assert_ne!(x_sha, xgit_sha);

    // x.git owns SECRET (uploaded through x.git's canonical transport path).
    let secret_oid = hex::encode(Sha256::digest(SECRET));
    let resp = reqwest::Client::new()
        .put(format!(
            "{}/{NAMESPACE}/x.git.git/info/lfs/objects/{secret_oid}",
            base_url(&srv.http)
        ))
        .header("Authorization", auth_header(&srv.http))
        .body(SECRET.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    Some(Fixture {
        srv,
        x_sha,
        xgit_sha,
        secret_oid,
        x_id,
    })
}

fn refs(repo_segment: &str) -> String {
    format!("/{NAMESPACE}/{repo_segment}/info/refs?service=git-upload-pack")
}

#[tokio::test]
async fn double_git_suffix_is_authorized_and_served_as_the_same_repository() {
    let Some(f) = fixture().await else { return };
    let srv = &f.srv.http;

    // `x.git.git` IS repository `x.git`: the owner gets x.git's history...
    let (s, body) = raw(srv, "GET", &refs("x.git.git"), Some(TEST_TOKEN), "").await;
    assert_eq!(s, 200);
    assert!(
        body.contains(&f.xgit_sha) && !body.contains(&f.x_sha),
        "{body}"
    );
    // ... and `x.git` / `x` are repository `x`.
    for seg in ["x.git", "x"] {
        let (s, body) = raw(srv, "GET", &refs(seg), Some(TEST_TOKEN), "").await;
        assert_eq!(s, 200, "{seg}");
        assert!(
            body.contains(&f.x_sha) && !body.contains(&f.xgit_sha),
            "{seg}: {body}"
        );
    }

    // user-2 (may read `x` only) is authorized against x.git for `x.git.git`,
    // so refused: smart HTTP, LFS GET, LFS batch, LFS verify.
    let (s, _) = raw(srv, "GET", &refs("x.git.git"), Some(USER2_TOKEN), "").await;
    assert_eq!(s, 403, "upload-pack refs of x.git");
    let lfs = |rest: &str| format!("/{NAMESPACE}/x.git.git/info/lfs/objects{rest}");
    let (s, _) = raw(
        srv,
        "GET",
        &lfs(&format!("/{}", f.secret_oid)),
        Some(USER2_TOKEN),
        "",
    )
    .await;
    assert_eq!(s, 403, "LFS GET of x.git");
    let batch =
        json!({"operation": "download", "objects": [{"oid": f.secret_oid, "size": SECRET.len()}]})
            .to_string();
    let (s, _) = raw(srv, "POST", &lfs("/batch"), Some(USER2_TOKEN), &batch).await;
    assert_eq!(s, 403, "LFS batch of x.git");
    let verify = json!({"oid": f.secret_oid, "size": SECRET.len()}).to_string();
    let (s, _) = raw(srv, "POST", &lfs("/verify"), Some(USER2_TOKEN), &verify).await;
    assert_eq!(s, 403, "LFS verify of x.git");

    // Through x (`x.git` segment) user-2 reaches x, which does not own SECRET.
    let (s, body) = raw(
        srv,
        "GET",
        &format!("/{NAMESPACE}/x.git/info/lfs/objects/{}", f.secret_oid),
        Some(USER2_TOKEN),
        "",
    )
    .await;
    assert_eq!(s, 404, "{body}");

    // A token scoped to x is refused for x.git.git, as for any other repository.
    let scoped = scoped_token(srv, &f.x_id).await;
    let (s, _) = raw(srv, "GET", &refs("x.git.git"), Some(&scoped), "").await;
    assert_eq!(s, 403);
    let (s, _) = raw(
        srv,
        "GET",
        &lfs(&format!("/{}", f.secret_oid)),
        Some(&scoped),
        "",
    )
    .await;
    assert_eq!(s, 403);
    let (s, body) = raw(srv, "GET", &refs("x.git"), Some(&scoped), "").await;
    assert_eq!(s, 200);
    assert!(body.contains(&f.x_sha), "{body}");
}

async fn scoped_token(srv: &TestServer, repo_id: &str) -> String {
    let plaintext = format!("repo-scope-{}", uuid::Uuid::new_v4().simple());
    let mut token = GitToken::new(
        TENANT.into(),
        hash_token(&plaintext),
        token_prefix(&plaintext),
        vec![GitPermission::Pull],
        "scoped".into(),
        None,
    );
    token.repo_id = Some(repo_id.to_string());
    srv.token_store.create_token(&token).await.unwrap();
    plaintext
}

/// Paths that are not exactly a repository are refused (never served, and
/// never let through without the per-repository check). user-2 may read `x`
/// only; `y` is private.
#[tokio::test]
async fn encoded_dotted_cased_and_slashed_paths_are_refused() {
    let Some(f) = fixture().await else { return };
    let srv = &f.srv.http;
    let lfs_get = |seg: &str| format!("/{NAMESPACE}/{seg}/info/lfs/objects/{}", f.secret_oid);

    for target in [
        // percent-encoding: never decoded into another repository
        refs("%79"),         // y
        refs("x%2Egit.git"), // x.git
        refs("x.git%2Egit"), // x.git
        refs("%2e%2e/acme/y"),
        "/%61cme/y/info/refs?service=git-upload-pack".to_string(),
        lfs_get("x%2Egit.git"),
        lfs_get("%2e%2e"),
        // dot segments, sent verbatim
        format!("/{NAMESPACE}/../{NAMESPACE}/y/info/refs?service=git-upload-pack"),
        refs(".."),
        lfs_get(".."),
        format!("/../{NAMESPACE}/y/info/refs?service=git-upload-pack"),
        // empty and suffix-only names
        refs(""),
        refs(".git"),
        // case: `Y` and `X` are not `y` and `x`
        refs("Y"),
        refs("X.git"),
        lfs_get("X.GIT.git"),
        // trailing slashes
        format!("/{NAMESPACE}/y/info/refs/?service=git-upload-pack"),
        format!("/{NAMESPACE}/y.git/git-upload-pack/"),
        format!("/{NAMESPACE}/x.git.git/info/lfs/objects/{}/", f.secret_oid),
        format!("/api/v1/repos/{NAMESPACE}/y/"),
        // REST names a repository verbatim: `y.git` is not `y`
        format!("/api/v1/repos/{NAMESPACE}/y.git/pulls"),
        format!("/api/v1/repos/{NAMESPACE}/y.git/commits"),
        format!("/api/v1/repos/{NAMESPACE}/y.git/tree"),
        format!("/api/v1/repos/{NAMESPACE}/%79/pulls"),
        format!("/api/v1/repos/{NAMESPACE}/x.git/tree"),
    ] {
        let (s, body) = raw(srv, "GET", &target, Some(USER2_TOKEN), "").await;
        assert!(
            matches!(s, 400 | 403 | 404),
            "user-2 GET {target} -> {s}: {body}"
        );
        assert!(
            !body.contains(&f.xgit_sha) && !body.contains("x.git's private"),
            "user-2 GET {target} leaked x.git: {body}"
        );
    }

    // The owner gets no other repository through them either: the encoded,
    // dotted and cased forms resolve to nothing.
    for target in [
        refs("%79"),
        refs("x%2Egit.git"),
        refs("%2e%2e/acme/y"),
        refs("Y"),
        format!("/api/v1/repos/{NAMESPACE}/y.git/pulls"),
        lfs_get("x%2Egit.git"),
    ] {
        let (s, _) = raw(srv, "GET", &target, Some(TEST_TOKEN), "").await;
        assert!(matches!(s, 400 | 404), "owner GET {target} -> {s}");
    }

    // Control: the canonical forms work for whoever may read them.
    assert_eq!(
        raw(srv, "GET", &refs("y"), Some(TEST_TOKEN), "").await.0,
        200
    );
    assert_eq!(
        raw(srv, "GET", &refs("y"), Some(USER2_TOKEN), "").await.0,
        403
    );
    assert_eq!(
        raw(srv, "GET", &refs("x"), Some(USER2_TOKEN), "").await.0,
        200
    );
    let (s, body) = raw(srv, "GET", &lfs_get("x.git.git"), Some(TEST_TOKEN), "").await;
    assert_eq!(s, 200);
    assert!(body.contains("x.git's private"));
}

/// Without credentials nothing tells an unresolvable repository from a
/// private one.
#[tokio::test]
async fn anonymous_caller_learns_nothing_from_repository_paths() {
    let Some(f) = fixture().await else { return };
    let srv = &f.srv.http;
    for target in [
        refs("x"),
        refs("does-not-exist"),
        refs("%79"),
        refs("x.git.git"),
    ] {
        assert_eq!(raw(srv, "GET", &target, None, "").await.0, 401, "{target}");
    }
}
