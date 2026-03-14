// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::harness::*;
use muli_core::git::{GitPermission, SshKey};
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
