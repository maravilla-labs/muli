// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use muli_core::git::{GitPermission, GitToken};
use muli_core::traits::{GitTokenStore, OrgMemberStore, OrgStore, SshKeyStore};
use muli_git::auth::{hash_token, token_prefix};

use muli_git::storage::FilesystemStorage;
use muli_git::tenant::TenantConfig;
use muli_git::{GitAuth, GitRouterConfig, SshServer, git_router};
use muli_store::memory::{MemoryOrgMemberStore, MemoryOrgStore};
use muli_store::sqlite::{
    SqliteGitTokenStore, SqlitePrCommentStore, SqlitePullRequestStore, SqliteRepositoryStore,
    SqliteSshKeyStore, SqliteStoreFactory, SqliteWebhookStore,
};

pub const TENANT: &str = "tenant-a";
pub const NAMESPACE: &str = "acme";
pub const TEST_TOKEN: &str = "test-token-e2e-abc123";

// ---------------------------------------------------------------------------
// Test server harness
// ---------------------------------------------------------------------------

pub struct TestServer {
    pub addr: SocketAddr,
    pub token: String,
    pub _git_root: TempDir,
    /// Keeps the SQLite data directory alive for the duration of the test.
    pub _store_dir: TempDir,
    /// Shared SSH key store (used by both HTTP router state and SSH server).
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    /// Shared repository store (used by both HTTP router state and SSH server).
    pub repo_store: Arc<dyn muli_core::traits::RepositoryStore>,
    /// Filesystem storage reference for the SSH server to share.
    pub storage: Arc<FilesystemStorage>,
    pub cancel: CancellationToken,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub async fn start_server() -> TestServer {
    let git_root = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        FilesystemStorage::new(git_root.path().to_str().unwrap())
            .await
            .expect("storage"),
    );

    let store_dir = TempDir::new().expect("store tempdir");
    let factory = SqliteStoreFactory::new(store_dir.path())
        .await
        .expect("sqlite factory");

    // Seed a token that has all permissions
    let token_store: Arc<dyn GitTokenStore> = Arc::new(SqliteGitTokenStore::new(factory.clone()));
    let mut git_token = GitToken::new(
        TENANT.into(),
        hash_token(TEST_TOKEN),
        token_prefix(TEST_TOKEN),
        vec![
            GitPermission::Pull,
            GitPermission::Push,
            GitPermission::Admin,
        ],
        "e2e test token".into(),
        None,
    );
    git_token.user_id = Some("user-1".to_string());
    token_store
        .create_token(&git_token)
        .await
        .expect("seed token");

    let repo_store: Arc<dyn muli_core::traits::RepositoryStore> =
        Arc::new(SqliteRepositoryStore::new(factory.clone()));
    let webhook_store = Arc::new(SqliteWebhookStore::new(factory.clone()));
    let ssh_key_store: Arc<dyn SshKeyStore> = Arc::new(SqliteSshKeyStore::new(factory.clone()));

    let git_auth = GitAuth::new(token_store.clone());
    // Use `with_default_tenant` so requests to 127.0.0.1 (no subdomain) fall
    // through to TENANT instead of returning 400.
    let tenant_config = TenantConfig::new("localhost").with_default_tenant(TENANT);

    let pr_store = Arc::new(SqlitePullRequestStore::new(factory.clone()));
    let pr_comment_store = Arc::new(SqlitePrCommentStore::new(factory.clone()));

    let app = git_router(GitRouterConfig {
        storage: storage.clone(),
        repo_store: repo_store.clone(),
        token_store,
        webhook_store: webhook_store as Arc<dyn muli_core::traits::WebhookStore>,
        ssh_key_store: ssh_key_store.clone(),
        pr_store,
        pr_comment_store,
        auth: Some(git_auth),
        tenant_config,
        cache_store: None,
        allow_localhost_webhooks: true,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let cancel = CancellationToken::new();
    let cancel_srv = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(cancel_srv.cancelled_owned())
            .await
            .ok();
    });

    TestServer {
        addr,
        token: TEST_TOKEN.to_string(),
        _git_root: git_root,
        _store_dir: store_dir,
        ssh_key_store,
        repo_store,
        storage,
        cancel,
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

pub fn base_url(srv: &TestServer) -> String {
    format!("http://127.0.0.1:{}", srv.addr.port())
}

pub fn auth_header(srv: &TestServer) -> String {
    format!("Bearer {}", srv.token)
}

/// POST JSON to the REST API and return the parsed response body.
pub async fn api_post(srv: &TestServer, path: &str, body: Value) -> (u16, Value) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}{}", base_url(srv), path))
        .header("Authorization", auth_header(srv))
        .json(&body)
        .send()
        .await
        .expect("HTTP request");
    let status = resp.status().as_u16();
    let json: Value = resp.json().await.unwrap_or(Value::Null);
    (status, json)
}

/// GET the REST API and return the parsed response body.
pub async fn api_get(srv: &TestServer, path: &str) -> (u16, Value) {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}{}", base_url(srv), path))
        .header("Authorization", auth_header(srv))
        .send()
        .await
        .expect("HTTP request");
    let status = resp.status().as_u16();
    let json: Value = resp.json().await.unwrap_or(Value::Null);
    (status, json)
}

pub async fn api_delete(srv: &TestServer, path: &str) -> u16 {
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}{}", base_url(srv), path))
        .header("Authorization", auth_header(srv))
        .send()
        .await
        .expect("HTTP request");
    resp.status().as_u16()
}

/// PATCH JSON to the REST API and return (status, body).
pub async fn api_patch(srv: &TestServer, path: &str, body: Value) -> (u16, Value) {
    let client = reqwest::Client::new();
    let resp = client
        .patch(format!("{}{}", base_url(srv), path))
        .header("Authorization", auth_header(srv))
        .json(&body)
        .send()
        .await
        .expect("HTTP PATCH request");
    let status = resp.status().as_u16();
    let json: Value = resp.json().await.unwrap_or(Value::Null);
    (status, json)
}

// ---------------------------------------------------------------------------
// Git CLI helpers
// ---------------------------------------------------------------------------

/// Returns true if the `git` binary is available.
pub fn git_available() -> bool {
    std::process::Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run a git command inside `dir`, failing the test on non-zero exit.
///
/// Uses `tokio::process::Command` so the call is non-blocking and the
/// async test server can continue processing requests while git runs.
pub async fn git(dir: &Path, args: &[&str]) {
    let status = tokio::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        // Suppress "advice" noise in test output
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "advice.detachedHead")
        .env("GIT_CONFIG_VALUE_0", "false")
        .status()
        .await
        .unwrap_or_else(|e| panic!("failed to run git {args:?}: {e}"));
    assert!(
        status.success(),
        "git {} failed with {}",
        args.join(" "),
        status
    );
}

/// Run a git command and capture stdout as a UTF-8 string.
pub async fn git_output(dir: &Path, args: &[&str]) -> String {
    let out = tokio::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .unwrap_or_else(|e| panic!("failed to run git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git {} failed with {}\nstderr: {}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8 git output")
}

/// Build a git remote URL with Basic auth credentials embedded.
/// git encodes `http://user:pass@host/path` as
/// `Authorization: Basic base64(user:pass)`.
/// Our `extract_basic_auth_token` treats the *password* part as the token.
pub fn git_url(srv: &TestServer, namespace: &str, repo: &str) -> String {
    format!(
        "http://x-token:{}@127.0.0.1:{}/{}/{}.git",
        srv.token,
        srv.addr.port(),
        namespace,
        repo
    )
}

// ---------------------------------------------------------------------------
// SSH server harness
// ---------------------------------------------------------------------------

/// Start both an HTTP server and an SSH server, returning their addresses and
/// a shared SSH key store so tests can register keys.
pub struct TestServerWithSsh {
    pub http: TestServer,
    pub ssh_addr: SocketAddr,
}

pub async fn start_server_with_ssh() -> TestServerWithSsh {
    let srv = start_server().await;

    // Generate a temporary host key for this test run
    let host_key = russh_keys::key::KeyPair::generate_ed25519();

    let org_store: Arc<dyn OrgStore> = Arc::new(MemoryOrgStore::new());
    let org_member_store: Arc<dyn OrgMemberStore> = Arc::new(MemoryOrgMemberStore::new());

    let ssh_server = SshServer {
        ssh_key_store: srv.ssh_key_store.clone(),
        // Share the same repo_store and storage as the HTTP server so that
        // repos created via the REST API are visible to the SSH server.
        repo_store: srv.repo_store.clone(),
        storage: srv.storage.clone(),
        default_tenant_id: Some(TENANT.to_string()),
        org_store,
        org_member_store,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("SSH bind");
    let ssh_addr = listener.local_addr().expect("ssh local addr");
    let cancel = srv.cancel.clone();
    tokio::spawn(async move {
        ssh_server.run_on(listener, host_key, cancel).await.ok();
    });

    TestServerWithSsh {
        http: srv,
        ssh_addr,
    }
}

// ---------------------------------------------------------------------------
// Pull Request helpers
// ---------------------------------------------------------------------------

/// Create a repo, clone it, configure git identity, push an initial commit on
/// `main`, and return the work dir so the caller can create more branches.
pub async fn setup_repo_with_commit(srv: &TestServer, repo_name: &str) -> tempfile::TempDir {
    let (status, _) = api_post(
        srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201, "create repo {repo_name}");

    let url = git_url(srv, NAMESPACE, repo_name);
    let work_dir = TempDir::new().unwrap();
    git(work_dir.path(), &["clone", "--no-local", &url, "."]).await;
    git(work_dir.path(), &["config", "user.email", "ci@muli.test"]).await;
    git(work_dir.path(), &["config", "user.name", "Muli CI"]).await;
    std::fs::write(work_dir.path().join("README.md"), "# hello\n").unwrap();
    git(work_dir.path(), &["add", "README.md"]).await;
    git(work_dir.path(), &["commit", "-m", "initial commit"]).await;
    git(
        work_dir.path(),
        &["push", "--set-upstream", "origin", "main"],
    )
    .await;
    work_dir
}
