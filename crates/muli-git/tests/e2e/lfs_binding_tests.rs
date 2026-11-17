// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! LFS objects belong to repositories.
//!
//! Bytes are stored once per tenant, but the LFS API answers for an oid only
//! through a repository that owns it (uploaded there, inherited by a fork, or
//! backfilled from that repository's own history). Reading access to repo A
//! must never reach an object of repo B by naming its oid on A's path, and A's
//! answers must not reveal whether B holds it.

use std::sync::Arc;

use muli_core::git::{GitPermission, GitToken, RepositoryCollaborator};
use muli_core::traits::RepositoryStore;
use muli_git::auth::{hash_token, token_prefix};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::harness::*;

const SECRET: &[u8] = b"org B's confidential design file";

fn oid_of(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn lfs_url(srv: &TestServer, repo: &str, rest: &str) -> String {
    format!(
        "{}/{NAMESPACE}/{repo}.git/info/lfs/objects{rest}",
        base_url(srv)
    )
}

/// Create a private repository owned by user-1 (REST only, no history).
async fn create_private_repo(srv: &TestServer, name: &str) -> String {
    let (status, body) = api_post(
        srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": name, "description": "", "is_private": true}),
    )
    .await;
    assert_eq!(status, 201, "create {name}: {body}");
    let mut repo = srv
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, name)
        .await
        .unwrap()
        .unwrap();
    repo.owner_id = "user-1".to_string();
    srv.repo_store.update_repository(&repo).await.unwrap();
    repo.id
}

async fn add_collaborator(
    srv: &TestServerWithAcl,
    repo_id: &str,
    user: &str,
    perms: Vec<GitPermission>,
) {
    srv.collaborator_store
        .upsert_collaborator(&RepositoryCollaborator {
            id: uuid::Uuid::new_v4().to_string(),
            tenant_id: TENANT.to_string(),
            repo_id: repo_id.to_string(),
            user_id: user.to_string(),
            permissions: perms,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
}

async fn scoped_token(srv: &TestServer, repo_id: &str, perms: Vec<GitPermission>) -> String {
    let plaintext = format!("repo-scope-{}", uuid::Uuid::new_v4().simple());
    let mut token = GitToken::new(
        TENANT.into(),
        hash_token(&plaintext),
        token_prefix(&plaintext),
        perms,
        "scoped".into(),
        None,
    );
    token.repo_id = Some(repo_id.to_string());
    srv.token_store.create_token(&token).await.unwrap();
    plaintext
}

async fn put(srv: &TestServer, repo: &str, token: &str, oid: &str, data: &[u8]) -> u16 {
    reqwest::Client::new()
        .put(lfs_url(srv, repo, &format!("/{oid}")))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(data.to_vec())
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn get(srv: &TestServer, repo: &str, token: &str, oid: &str) -> (u16, Vec<u8>) {
    let resp = reqwest::Client::new()
        .get(lfs_url(srv, repo, &format!("/{oid}")))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.bytes().await.unwrap().to_vec())
}

async fn batch(
    srv: &TestServer,
    repo: &str,
    token: &str,
    op: &str,
    oid: &str,
    size: usize,
) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(lfs_url(srv, repo, "/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({"operation": op, "objects": [{"oid": oid, "size": size}]}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

async fn verify(srv: &TestServer, repo: &str, token: &str, oid: &str, size: usize) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(lfs_url(srv, repo, "/verify"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({"oid": oid, "size": size}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

/// Repo B owns SECRET (uploaded by user-1 through B). user-2 may read and
/// write repo A only.
async fn two_repos() -> (TestServerWithAcl, String, String) {
    let srv = start_server_with_acl().await;
    let a = create_private_repo(&srv.http, "lfs-a").await;
    let b = create_private_repo(&srv.http, "lfs-b").await;
    add_collaborator(
        &srv,
        &a,
        "user-2",
        vec![GitPermission::Pull, GitPermission::Push],
    )
    .await;
    let oid = oid_of(SECRET);
    assert_eq!(put(&srv.http, "lfs-b", TEST_TOKEN, &oid, SECRET).await, 200);
    assert_eq!(
        get(&srv.http, "lfs-b", TEST_TOKEN, &oid).await.0,
        200,
        "owner reads B"
    );
    (srv, a, b)
}

/// The answers A gives for B's oid equal the answers it gives for an oid
/// stored nowhere: no fetch, no existence oracle.
async fn assert_not_reachable_through(srv: &TestServer, repo: &str, token: &str) {
    let oid = oid_of(SECRET);
    let ghost = oid_of(b"stored nowhere at all");
    let n = SECRET.len();

    let (s, body) = get(srv, repo, token, &oid).await;
    let (gs, gbody) = get(srv, repo, token, &ghost).await;
    assert_eq!(s, 404, "GET of B's oid via {repo}");
    assert_eq!((s, body), (gs, gbody), "GET: same answer as a missing oid");

    let (s, body) = batch(srv, repo, token, "download", &oid, n).await;
    assert_eq!(s, 200);
    let (_, gbody) = batch(srv, repo, token, "download", &ghost, n).await;
    assert_eq!(
        body["objects"][0]["error"]["code"], 404,
        "batch download: {body}"
    );
    assert!(body["objects"][0]["actions"].is_null());
    assert_eq!(body["objects"][0]["error"], gbody["objects"][0]["error"]);

    let (s, body) = verify(srv, repo, token, &oid, n).await;
    let (gs, gbody) = verify(srv, repo, token, &ghost, n).await;
    assert_eq!(s, 404, "verify of B's oid via {repo}");
    assert_eq!(
        (s, body),
        (gs, gbody),
        "verify: same answer as a missing oid"
    );
}

#[tokio::test]
async fn reader_of_a_cannot_fetch_b_objects_through_a() {
    let (srv, _, _) = two_repos().await;
    assert_not_reachable_through(&srv.http, "lfs-a", USER2_TOKEN).await;
    // B itself stays closed to user-2 (ordinary ACL).
    assert_eq!(
        get(&srv.http, "lfs-b", USER2_TOKEN, &oid_of(SECRET))
            .await
            .0,
        403
    );
}

#[tokio::test]
async fn repo_scoped_token_of_a_cannot_fetch_b_objects() {
    let (srv, a, _) = two_repos().await;
    let token = scoped_token(
        &srv.http,
        &a,
        vec![GitPermission::Pull, GitPermission::Push],
    )
    .await;
    assert_not_reachable_through(&srv.http, "lfs-a", &token).await;
}

#[tokio::test]
async fn unknown_repository_path_serves_nothing() {
    let (srv, _, _) = two_repos().await;
    // user-1's full token, a path naming no repository: no ACL applies, so
    // the index is what refuses.
    let (s, _) = get(&srv.http, "no-such-repo", TEST_TOKEN, &oid_of(SECRET)).await;
    assert_eq!(s, 404);
}

#[tokio::test]
async fn batch_upload_on_a_asks_for_b_object_and_only_real_bytes_bind_it() {
    let (srv, _, _) = two_repos().await;
    let oid = oid_of(SECRET);

    // Upload of an oid B owns: A must still be asked to send it.
    let (s, body) = batch(
        &srv.http,
        "lfs-a",
        USER2_TOKEN,
        "upload",
        &oid,
        SECRET.len(),
    )
    .await;
    assert_eq!(s, 200);
    assert!(
        body["objects"][0]["actions"]["upload"]["href"].is_string(),
        "upload action expected, got {body}"
    );

    // Claiming it with other bytes fails and binds nothing.
    assert_eq!(
        put(&srv.http, "lfs-a", USER2_TOKEN, &oid, b"guess").await,
        422
    );
    assert_eq!(get(&srv.http, "lfs-a", USER2_TOKEN, &oid).await.0, 404);
    // ... and B's stored copy is intact.
    assert_eq!(get(&srv.http, "lfs-b", TEST_TOKEN, &oid).await.1, SECRET);

    // Whoever really holds the bytes may add them to A.
    assert_eq!(
        put(&srv.http, "lfs-a", USER2_TOKEN, &oid, SECRET).await,
        200
    );
    let (s, bytes) = get(&srv.http, "lfs-a", USER2_TOKEN, &oid).await;
    assert_eq!((s, bytes.as_slice()), (200, SECRET));
    assert_eq!(
        verify(&srv.http, "lfs-a", USER2_TOKEN, &oid, SECRET.len())
            .await
            .0,
        200
    );
    let (_, body) = batch(
        &srv.http,
        "lfs-a",
        USER2_TOKEN,
        "upload",
        &oid,
        SECRET.len(),
    )
    .await;
    assert!(body["objects"][0]["actions"].is_null(), "now owned: {body}");
}

#[tokio::test]
async fn upload_then_fetch_through_the_same_repository() {
    let (srv, a, _) = two_repos().await;
    let token = scoped_token(
        &srv.http,
        &a,
        vec![GitPermission::Pull, GitPermission::Push],
    )
    .await;
    let data = b"A's own asset";
    let oid = oid_of(data);
    assert_eq!(put(&srv.http, "lfs-a", &token, &oid, data).await, 200);
    assert_eq!(get(&srv.http, "lfs-a", &token, &oid).await.1, data);
    let (_, body) = batch(&srv.http, "lfs-a", &token, "download", &oid, data.len()).await;
    assert!(
        body["objects"][0]["actions"]["download"]["href"].is_string(),
        "{body}"
    );
    // B does not see A's object either.
    assert_eq!(get(&srv.http, "lfs-b", TEST_TOKEN, &oid).await.0, 404);
}

#[tokio::test]
async fn fork_inherits_lfs_objects_and_delete_drops_them() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }
    let srv = start_server().await;
    let _work = setup_repo_with_commit(&srv, "lfs-parent").await;
    let oid = oid_of(SECRET);
    assert_eq!(put(&srv, "lfs-parent", TEST_TOKEN, &oid, SECRET).await, 200);

    let (status, body) = api_post(
        &srv,
        &format!("/api/v1/repos/{NAMESPACE}/lfs-parent/forks"),
        json!({"dest_namespace": NAMESPACE, "dest_name": "lfs-fork"}),
    )
    .await;
    assert_eq!(status, 201, "fork: {body}");
    assert_eq!(get(&srv, "lfs-fork", TEST_TOKEN, &oid).await.1, SECRET);

    let fork = srv
        .repo_store
        .get_repository_by_name(TENANT, NAMESPACE, "lfs-fork")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        api_delete(&srv, &format!("/api/v1/repos/{NAMESPACE}/lfs-fork")).await,
        204
    );
    assert!(
        !srv.lfs_objects
            .has_lfs_object(TENANT, &fork.id, &oid)
            .await
            .unwrap()
    );
    // The parent keeps its object.
    assert_eq!(get(&srv, "lfs-parent", TEST_TOKEN, &oid).await.0, 200);
}

/// Commit an LFS pointer for `data` into `work` and push it.
async fn push_pointer(work: &std::path::Path, file: &str, data: &[u8]) {
    let pointer = format!(
        "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize {}\n",
        oid_of(data),
        data.len()
    );
    std::fs::write(work.join(file), pointer).unwrap();
    git(work, &["add", file]).await;
    git(work, &["commit", "-m", &format!("add {file}")]).await;
    git(work, &["push", "origin", "main"]).await;
}

#[tokio::test]
async fn backfill_indexes_pointers_from_history_once() {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return;
    }
    let srv = start_server().await;
    let work = setup_repo_with_commit(&srv, "lfs-legacy").await;
    let other = setup_repo_with_commit(&srv, "lfs-other").await;

    // State of an installation from before the index: bytes stored for the
    // tenant, pointers in history, no index rows.
    let stored = b"legacy asset";
    let not_stored = b"pointer whose object was never uploaded";
    for data in [&stored[..], SECRET] {
        srv.lfs_storage
            .put_object(
                TENANT,
                &oid_of(data),
                data.len() as u64,
                axum::body::Body::from(data.to_vec()),
            )
            .await
            .unwrap();
    }
    push_pointer(work.path(), "asset.bin", stored).await;
    push_pointer(work.path(), "gone.bin", not_stored).await;
    // Overwrite it on main: the pointer stays in history.
    std::fs::write(work.path().join("asset.bin"), "plain now\n").unwrap();
    git(work.path(), &["commit", "-am", "drop lfs"]).await;
    git(work.path(), &["push", "origin", "main"]).await;

    assert_eq!(
        get(&srv, "lfs-legacy", TEST_TOKEN, &oid_of(stored)).await.0,
        404
    );

    let repo_store: Arc<dyn RepositoryStore> = srv.repo_store.clone();
    let report = muli_git::lfs::backfill::run(
        &[TENANT.to_string()],
        &repo_store,
        &srv.storage,
        &srv.lfs_storage,
        &srv.lfs_objects,
    )
    .await
    .unwrap();
    assert!(!report.skipped);
    assert_eq!(report.rows, 1, "{report:?}");
    assert_eq!(report.missing, 1, "{report:?}");

    assert_eq!(
        get(&srv, "lfs-legacy", TEST_TOKEN, &oid_of(stored)).await.1,
        stored
    );
    // Nothing for the repository without the pointer, nothing for SECRET.
    assert_eq!(
        get(&srv, "lfs-other", TEST_TOKEN, &oid_of(stored)).await.0,
        404
    );
    assert_eq!(
        get(&srv, "lfs-legacy", TEST_TOKEN, &oid_of(SECRET)).await.0,
        404
    );

    // After the migration a pushed pointer claims nothing, even if the
    // backfill is started again.
    push_pointer(other.path(), "claim.bin", SECRET).await;
    let again = muli_git::lfs::backfill::run(
        &[TENANT.to_string()],
        &repo_store,
        &srv.storage,
        &srv.lfs_storage,
        &srv.lfs_objects,
    )
    .await
    .unwrap();
    assert!(again.skipped);
    assert_eq!(
        get(&srv, "lfs-other", TEST_TOKEN, &oid_of(SECRET)).await.0,
        404
    );
}
