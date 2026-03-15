// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use muli_core::git::{GitPermission, RepositoryCollaborator, SshKey};
use serde_json::json;
use std::process::Stdio;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// SSH clone + push test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ssh_clone_and_push() {
    if !git_available() {
        eprintln!("SKIP test_ssh_clone_and_push: git binary not found");
        return;
    }
    // Also require ssh-keygen for key generation
    let has_keygen = std::process::Command::new("ssh-keygen")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|_| true)
        .unwrap_or(false);
    if !has_keygen {
        eprintln!("SKIP test_ssh_clone_and_push: ssh-keygen binary not found");
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "ssh-test-repo";

    // Create repo via REST
    let (status, _) = api_post(
        &srv.http,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    // Generate an Ed25519 key pair in a temp directory
    let key_dir = TempDir::new().unwrap();
    let key_path = key_dir.path().join("id_ed25519");
    let key_pub_path = key_dir.path().join("id_ed25519.pub");

    let keygen_status = tokio::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-N", "", "-f", key_path.to_str().unwrap()])
        .status()
        .await
        .expect("ssh-keygen");
    assert!(keygen_status.success(), "ssh-keygen failed");

    // Read the public key
    let pub_key_str = std::fs::read_to_string(&key_pub_path).expect("read pub key");

    // Compute the fingerprint using ssh-keygen -l -E sha256
    let fp_output = tokio::process::Command::new("ssh-keygen")
        .args(["-l", "-E", "sha256", "-f", key_pub_path.to_str().unwrap()])
        .output()
        .await
        .expect("ssh-keygen -l");
    assert!(fp_output.status.success(), "ssh-keygen -l failed");

    let fp_line = String::from_utf8_lossy(&fp_output.stdout);
    // Format: "256 SHA256:xxx user@host (ED25519)"
    let fingerprint = fp_line
        .split_whitespace()
        .nth(1)
        .expect("fingerprint field")
        .to_string();
    assert!(
        fingerprint.starts_with("SHA256:"),
        "unexpected fingerprint format: {fingerprint}"
    );

    // Register the key in the SSH key store (shared between HTTP and SSH servers)
    let ssh_key = SshKey {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: TENANT.to_string(),
        user_id: Some("user-1".to_string()),
        fingerprint: fingerprint.clone(),
        public_key: pub_key_str.trim().to_string(),
        title: "test key".to_string(),
        permissions: vec![GitPermission::Pull, GitPermission::Push],
        created_at: chrono::Utc::now(),
    };
    srv.http
        .ssh_key_store
        .add_key(&ssh_key)
        .await
        .expect("add SSH key");

    // Register user-1 as a collaborator with push permission so ACL allows the push
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
        user_id: "user-1".to_string(),
        permissions: vec![GitPermission::Pull, GitPermission::Push],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&collab)
        .await
        .expect("add collaborator");

    // Clone over SSH
    let ssh_url = format!(
        "ssh://git@127.0.0.1:{}/{}/{}.git",
        srv.ssh_addr.port(),
        NAMESPACE,
        repo_name
    );
    let git_ssh_cmd = format!(
        "ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
        key_path.display()
    );

    let clone_dir = TempDir::new().unwrap();
    let status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(status.success(), "git clone over SSH failed: {status}");

    // Commit and push over SSH
    tokio::process::Command::new("git")
        .args(["config", "user.email", "ci@muli.test"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["config", "user.name", "Muli CI"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    std::fs::write(clone_dir.path().join("ssh.txt"), "hello via ssh\n").unwrap();
    tokio::process::Command::new("git")
        .args(["add", "ssh.txt"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["commit", "-m", "ssh push test"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();

    let push_status = tokio::process::Command::new("git")
        .args(["push", "--set-upstream", "origin", "main"])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git push over SSH");
    assert!(
        push_status.success(),
        "git push over SSH failed: {push_status}"
    );

    // Verify via HTTP REST API that 'main' branch is present
    let (status, body) = api_get(
        &srv.http,
        &format!("/api/v1/repos/{NAMESPACE}/{repo_name}/refs"),
    )
    .await;
    assert_eq!(status, 200, "refs failed: {body}");
    let refs = body.as_array().expect("array");
    let shortnames: Vec<&str> = refs
        .iter()
        .filter_map(|r| r["shorthand"].as_str())
        .collect();
    assert!(
        shortnames.contains(&"main"),
        "expected 'main' in refs after SSH push, got {shortnames:?}"
    );
}

// ---------------------------------------------------------------------------
// SSH permission tests
// ---------------------------------------------------------------------------

/// Helper: generate a key pair in `key_dir` and return (key_path, pub_key_str, fingerprint).
async fn generate_key_pair(key_dir: &TempDir) -> (std::path::PathBuf, String, String) {
    let key_path = key_dir.path().join("id_ed25519");
    let key_pub_path = key_dir.path().join("id_ed25519.pub");

    let status = tokio::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-N", "", "-f", key_path.to_str().unwrap()])
        .status()
        .await
        .expect("ssh-keygen");
    assert!(status.success(), "ssh-keygen failed");

    let pub_key_str = std::fs::read_to_string(&key_pub_path).expect("read pub key");

    let fp_output = tokio::process::Command::new("ssh-keygen")
        .args(["-l", "-E", "sha256", "-f", key_pub_path.to_str().unwrap()])
        .output()
        .await
        .expect("ssh-keygen -l");
    assert!(fp_output.status.success(), "ssh-keygen -l failed");

    let fp_line = String::from_utf8_lossy(&fp_output.stdout);
    let fingerprint = fp_line
        .split_whitespace()
        .nth(1)
        .expect("fingerprint field")
        .to_string();

    (key_path, pub_key_str.trim().to_string(), fingerprint)
}

/// A pull-only key must be able to clone (read) but must not be able to push.
#[tokio::test]
async fn test_ssh_push_rejected_for_pull_only_key() {
    if !git_available() {
        eprintln!("SKIP test_ssh_push_rejected_for_pull_only_key: git binary not found");
        return;
    }
    let has_keygen = std::process::Command::new("ssh-keygen")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|_| true)
        .unwrap_or(false);
    if !has_keygen {
        eprintln!("SKIP test_ssh_push_rejected_for_pull_only_key: ssh-keygen binary not found");
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "pull-only-repo";

    let (status, _) = api_post(
        &srv.http,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let key_dir = TempDir::new().unwrap();
    let (key_path, pub_key_str, fingerprint) = generate_key_pair(&key_dir).await;

    // Register key with Pull-only permission
    let ssh_key = SshKey {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: TENANT.to_string(),
        user_id: Some("user-1".to_string()),
        fingerprint: fingerprint.clone(),
        public_key: pub_key_str,
        title: "pull-only key".to_string(),
        permissions: vec![GitPermission::Pull],
        created_at: chrono::Utc::now(),
    };
    srv.http
        .ssh_key_store
        .add_key(&ssh_key)
        .await
        .expect("add SSH key");

    let ssh_url = format!(
        "ssh://git@127.0.0.1:{}/{}/{}.git",
        srv.ssh_addr.port(),
        NAMESPACE,
        repo_name
    );
    let git_ssh_cmd = format!(
        "ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
        key_path.display()
    );

    // Clone should succeed (read access)
    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        clone_status.success(),
        "git clone should succeed for pull-only key: {clone_status}"
    );

    // Commit something and try to push — should fail
    tokio::process::Command::new("git")
        .args(["config", "user.email", "ci@muli.test"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["config", "user.name", "Muli CI"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    std::fs::write(clone_dir.path().join("file.txt"), "data\n").unwrap();
    tokio::process::Command::new("git")
        .args(["add", "file.txt"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["commit", "-m", "attempt push"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();

    let push_status = tokio::process::Command::new("git")
        .args(["push", "--set-upstream", "origin", "main"])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git push over SSH");
    assert!(
        !push_status.success(),
        "git push should fail for pull-only key but succeeded"
    );
}

/// A key with Pull+Push permissions can clone and push successfully.
#[tokio::test]
async fn test_ssh_clone_with_pull_only_key() {
    if !git_available() {
        eprintln!("SKIP test_ssh_clone_with_pull_only_key: git binary not found");
        return;
    }
    let has_keygen = std::process::Command::new("ssh-keygen")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|_| true)
        .unwrap_or(false);
    if !has_keygen {
        eprintln!("SKIP test_ssh_clone_with_pull_only_key: ssh-keygen binary not found");
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "pull-only-clone-repo";

    let (status, _) = api_post(
        &srv.http,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": repo_name, "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let key_dir = TempDir::new().unwrap();
    let (key_path, pub_key_str, fingerprint) = generate_key_pair(&key_dir).await;

    // Register key with Pull-only permission
    let ssh_key = SshKey {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: TENANT.to_string(),
        user_id: Some("user-1".to_string()),
        fingerprint: fingerprint.clone(),
        public_key: pub_key_str,
        title: "pull-only clone key".to_string(),
        permissions: vec![GitPermission::Pull],
        created_at: chrono::Utc::now(),
    };
    srv.http
        .ssh_key_store
        .add_key(&ssh_key)
        .await
        .expect("add SSH key");

    let ssh_url = format!(
        "ssh://git@127.0.0.1:{}/{}/{}.git",
        srv.ssh_addr.port(),
        NAMESPACE,
        repo_name
    );
    let git_ssh_cmd = format!(
        "ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
        key_path.display()
    );

    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        clone_status.success(),
        "git clone should succeed for pull-only key: {clone_status}"
    );
}

// ---------------------------------------------------------------------------
// SSH ACL security tests
// ---------------------------------------------------------------------------

/// Helper: skip test if git or ssh-keygen binaries are not available.
fn skip_if_no_git_ssh() -> bool {
    if !git_available() {
        eprintln!("SKIP: git binary not found");
        return true;
    }
    let has_keygen = std::process::Command::new("ssh-keygen")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|_| true)
        .unwrap_or(false);
    if !has_keygen {
        eprintln!("SKIP: ssh-keygen binary not found");
        return true;
    }
    false
}

/// Helper: register an SSH key for a given user and return (key_path, ssh_url, git_ssh_cmd).
async fn register_ssh_key(
    srv: &TestServerWithSsh,
    user_id: &str,
    permissions: Vec<GitPermission>,
    repo_name: &str,
) -> (std::path::PathBuf, String, String, TempDir) {
    let key_dir = TempDir::new().unwrap();
    let (key_path, pub_key_str, fingerprint) = generate_key_pair(&key_dir).await;

    let ssh_key = SshKey {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: TENANT.to_string(),
        user_id: Some(user_id.to_string()),
        fingerprint,
        public_key: pub_key_str,
        title: format!("{user_id} key"),
        permissions,
        created_at: chrono::Utc::now(),
    };
    srv.http
        .ssh_key_store
        .add_key(&ssh_key)
        .await
        .expect("add SSH key");

    let ssh_url = format!(
        "ssh://git@127.0.0.1:{}/{}/{}.git",
        srv.ssh_addr.port(),
        NAMESPACE,
        repo_name
    );
    let git_ssh_cmd = format!(
        "ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
        key_path.display()
    );

    (key_path, ssh_url, git_ssh_cmd, key_dir)
}

/// Helper: create a repo (public or private), push an initial commit via HTTP,
/// and optionally set `owner_id`.
async fn create_repo_with_commit(
    srv: &TestServerWithSsh,
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

    // Set owner_id if requested
    if let Some(oid) = owner_id {
        let mut repo = srv
            .http
            .repo_store
            .get_repository_by_name(TENANT, NAMESPACE, repo_name)
            .await
            .expect("repo lookup")
            .expect("repo must exist");
        repo.owner_id = oid.to_string();
        srv.http
            .repo_store
            .update_repository(&repo)
            .await
            .expect("set owner_id");
    }

    // Push an initial commit so the repo is not empty (use HTTP auth)
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
}

/// Private repo: user has SSH key but is NOT owner or collaborator — clone should fail.
#[tokio::test]
async fn test_ssh_private_repo_clone_denied_for_non_collaborator() {
    if skip_if_no_git_ssh() {
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "priv-no-collab-clone";

    // Create private repo owned by "owner-user" (not user-1)
    create_repo_with_commit(&srv, repo_name, true, Some("owner-user")).await;

    // Register SSH key for user-1 with Pull+Push at key level
    let (_key_path, ssh_url, git_ssh_cmd, _key_dir) = register_ssh_key(
        &srv,
        "user-1",
        vec![GitPermission::Pull, GitPermission::Push],
        repo_name,
    )
    .await;
    // user-1 is NOT added as collaborator

    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        !clone_status.success(),
        "clone of private repo should fail for non-collaborator"
    );
}

/// Private repo: user is collaborator with Pull permission — clone should succeed.
#[tokio::test]
async fn test_ssh_private_repo_clone_allowed_for_collaborator() {
    if skip_if_no_git_ssh() {
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "priv-collab-clone";

    create_repo_with_commit(&srv, repo_name, true, Some("owner-user")).await;

    let (_key_path, ssh_url, git_ssh_cmd, _key_dir) = register_ssh_key(
        &srv,
        "user-1",
        vec![GitPermission::Pull, GitPermission::Push],
        repo_name,
    )
    .await;

    // Add user-1 as collaborator with Pull
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
        user_id: "user-1".to_string(),
        permissions: vec![GitPermission::Pull],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&collab)
        .await
        .expect("add collaborator");

    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        clone_status.success(),
        "clone of private repo should succeed for collaborator with Pull: {clone_status}"
    );
}

/// Private repo: user is collaborator with Pull only — push should fail.
#[tokio::test]
async fn test_ssh_private_repo_push_denied_for_pull_only_collaborator() {
    if skip_if_no_git_ssh() {
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "priv-pull-only-push";

    create_repo_with_commit(&srv, repo_name, true, Some("owner-user")).await;

    let (_key_path, ssh_url, git_ssh_cmd, _key_dir) = register_ssh_key(
        &srv,
        "user-1",
        vec![GitPermission::Pull, GitPermission::Push],
        repo_name,
    )
    .await;

    // Add user-1 as collaborator with Pull only
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
        user_id: "user-1".to_string(),
        permissions: vec![GitPermission::Pull],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&collab)
        .await
        .expect("add collaborator");

    // Clone should succeed
    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        clone_status.success(),
        "clone should succeed for pull collaborator: {clone_status}"
    );

    // Commit and try to push — should fail
    tokio::process::Command::new("git")
        .args(["config", "user.email", "ci@muli.test"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["config", "user.name", "Muli CI"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    std::fs::write(clone_dir.path().join("newfile.txt"), "data\n").unwrap();
    tokio::process::Command::new("git")
        .args(["add", "newfile.txt"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["commit", "-m", "attempt push"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();

    let push_status = tokio::process::Command::new("git")
        .args(["push", "--set-upstream", "origin", "main"])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git push over SSH");
    assert!(
        !push_status.success(),
        "push should fail for pull-only collaborator on private repo"
    );
}

/// Private repo: user is collaborator with Push permission — push should succeed.
#[tokio::test]
async fn test_ssh_private_repo_push_allowed_for_push_collaborator() {
    if skip_if_no_git_ssh() {
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "priv-push-collab";

    create_repo_with_commit(&srv, repo_name, true, Some("owner-user")).await;

    let (_key_path, ssh_url, git_ssh_cmd, _key_dir) = register_ssh_key(
        &srv,
        "user-1",
        vec![GitPermission::Pull, GitPermission::Push],
        repo_name,
    )
    .await;

    // Add user-1 as collaborator with Pull + Push
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
        user_id: "user-1".to_string(),
        permissions: vec![GitPermission::Pull, GitPermission::Push],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&collab)
        .await
        .expect("add collaborator");

    // Clone
    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        clone_status.success(),
        "clone should succeed: {clone_status}"
    );

    // Commit and push — should succeed
    tokio::process::Command::new("git")
        .args(["config", "user.email", "ci@muli.test"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["config", "user.name", "Muli CI"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    std::fs::write(clone_dir.path().join("newfile.txt"), "push data\n").unwrap();
    tokio::process::Command::new("git")
        .args(["add", "newfile.txt"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["commit", "-m", "push with permission"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();

    let push_status = tokio::process::Command::new("git")
        .args(["push", "--set-upstream", "origin", "main"])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git push over SSH");
    assert!(
        push_status.success(),
        "push should succeed for collaborator with Push permission: {push_status}"
    );
}

/// SSH server must stay responsive under concurrent sessions.
/// This verifies the accept loop continues on errors and the connection
/// semaphore correctly limits and releases permits.
#[tokio::test]
async fn test_ssh_concurrent_sessions_stability() {
    if skip_if_no_git_ssh() {
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "ssh-concurrent-test";

    create_repo_with_commit(&srv, repo_name, false, Some("user-1")).await;

    let (_key_path, ssh_url, git_ssh_cmd, _key_dir) = register_ssh_key(
        &srv,
        "user-1",
        vec![GitPermission::Pull, GitPermission::Push],
        repo_name,
    )
    .await;

    // Add user-1 as collaborator
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
        user_id: "user-1".to_string(),
        permissions: vec![GitPermission::Pull, GitPermission::Push],
        created_at: chrono::Utc::now(),
    };
    srv.collaborator_store
        .upsert_collaborator(&collab)
        .await
        .expect("add collaborator");

    // Launch 5 concurrent SSH clones
    let mut handles = Vec::new();
    for i in 0..5 {
        let ssh_url = ssh_url.clone();
        let git_ssh_cmd = git_ssh_cmd.clone();
        handles.push(tokio::spawn(async move {
            let clone_dir = TempDir::new().unwrap();
            let status = tokio::process::Command::new("git")
                .args(["clone", "--no-local", &ssh_url, "."])
                .current_dir(clone_dir.path())
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GIT_SSH_COMMAND", &git_ssh_cmd)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .unwrap_or_else(|e| panic!("concurrent clone {i} failed to run: {e}"));
            (i, status.success())
        }));
    }

    let mut success_count = 0;
    for handle in handles {
        let (i, ok) = handle.await.expect("join concurrent clone task");
        if ok {
            success_count += 1;
        } else {
            eprintln!("concurrent clone {i} returned non-zero (may be expected under load)");
        }
    }

    // All 5 should succeed (well within the 128 session limit)
    assert_eq!(
        success_count, 5,
        "all 5 concurrent SSH clones should succeed, got {success_count}/5"
    );

    // After all sessions end, the server should still be accepting new connections.
    // Verify by doing one more clone.
    let final_dir = TempDir::new().unwrap();
    let final_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(final_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("final clone after concurrent sessions");
    assert!(
        final_status.success(),
        "SSH server should still accept connections after concurrent sessions"
    );
}

/// Public repo: user has SSH key but is NOT collaborator — push should fail (clone works).
#[tokio::test]
async fn test_ssh_public_repo_push_denied_for_non_collaborator() {
    if skip_if_no_git_ssh() {
        return;
    }

    let srv = start_server_with_ssh().await;
    let repo_name = "pub-no-collab-push";

    // Public repo owned by someone else
    create_repo_with_commit(&srv, repo_name, false, Some("owner-user")).await;

    let (_key_path, ssh_url, git_ssh_cmd, _key_dir) = register_ssh_key(
        &srv,
        "user-1",
        vec![GitPermission::Pull, GitPermission::Push],
        repo_name,
    )
    .await;
    // user-1 is NOT added as collaborator

    // Clone should succeed (public repo, read access)
    let clone_dir = TempDir::new().unwrap();
    let clone_status = tokio::process::Command::new("git")
        .args(["clone", "--no-local", &ssh_url, "."])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git clone over SSH");
    assert!(
        clone_status.success(),
        "clone of public repo should succeed for anyone: {clone_status}"
    );

    // Commit and try to push — should fail (not collaborator)
    tokio::process::Command::new("git")
        .args(["config", "user.email", "ci@muli.test"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["config", "user.name", "Muli CI"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    std::fs::write(clone_dir.path().join("newfile.txt"), "sneaky\n").unwrap();
    tokio::process::Command::new("git")
        .args(["add", "newfile.txt"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();
    tokio::process::Command::new("git")
        .args(["commit", "-m", "attempt unauthorized push"])
        .current_dir(clone_dir.path())
        .status()
        .await
        .unwrap();

    let push_status = tokio::process::Command::new("git")
        .args(["push", "--set-upstream", "origin", "main"])
        .current_dir(clone_dir.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_SSH_COMMAND", &git_ssh_cmd)
        .status()
        .await
        .expect("git push over SSH");
    assert!(
        !push_status.success(),
        "push to public repo should fail for non-collaborator"
    );
}
