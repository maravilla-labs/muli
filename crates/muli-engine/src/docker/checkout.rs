// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Host-side git checkout: clone + checkout + optional submodules.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::info;

use muli_core::error::{MuliError, Result};
use muli_core::job::model::CheckoutSpec;

use crate::docker::logs::{LogCollector, LogLine, LogStream};

/// Perform a host-side git checkout into `workspace`:
/// 1. `git clone --depth 1 <url> <workspace>`
/// 2. `git fetch --depth 1 origin <sha>` + `git checkout <sha>` (handles non-tip SHAs)
/// 3. If `submodules`: `git submodule update --init --recursive --depth 1`
///
/// All output is forwarded to the log collector with monotone sequence numbers.
pub async fn perform_checkout(
    checkout: &CheckoutSpec,
    workspace: &Path,
    log_collector: &Arc<LogCollector>,
) -> Result<()> {
    let workspace_str = workspace.to_string_lossy().to_string();
    let seq = Arc::new(AtomicU64::new(0));

    // Step 1: clone — URL is kept separate to avoid accidental token logging.
    run_git_clone(&checkout.clone_url, &workspace_str, log_collector, &seq).await?;

    // Step 2: fetch the specific SHA (handles commits not at a branch tip)
    run_git(
        &[
            "-C",
            &workspace_str,
            "fetch",
            "--depth",
            "1",
            "origin",
            &checkout.sha,
        ],
        log_collector,
        &seq,
    )
    .await?;

    run_git(
        &[
            "-c",
            "advice.detachedHead=false",
            "-C",
            &workspace_str,
            "checkout",
            &checkout.sha,
        ],
        log_collector,
        &seq,
    )
    .await?;

    // Step 3: optional submodules
    if checkout.submodules {
        run_git(
            &[
                "-C",
                &workspace_str,
                "submodule",
                "update",
                "--init",
                "--recursive",
                "--depth",
                "1",
            ],
            log_collector,
            &seq,
        )
        .await?;
    }

    info!(sha = %checkout.sha, workspace = %workspace_str, "checkout complete");
    Ok(())
}

/// Run `git clone <url> <dest>`.  The URL is passed separately so it is never
/// part of a joined string that gets included in error messages or logs
/// (the URL may contain an auth token).
async fn run_git_clone(
    url: &str,
    dest: &str,
    log_collector: &Arc<LogCollector>,
    seq: &Arc<AtomicU64>,
) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.args(["clone", "--depth", "1", url, dest]);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| MuliError::Internal(format!("failed to spawn git clone: {e}")))?;

    spawn_log_streamers(&mut child, log_collector, seq);

    let status = child
        .wait()
        .await
        .map_err(|e| MuliError::Internal(format!("git clone wait error: {e}")))?;

    if !status.success() {
        // Intentionally omit URL from error to prevent auth token leakage.
        return Err(MuliError::Internal(format!(
            "git clone failed with exit code {:?}",
            status.code()
        )));
    }
    Ok(())
}

/// Run an arbitrary git command (no URLs — args are safe to echo in errors).
async fn run_git(
    args: &[&str],
    log_collector: &Arc<LogCollector>,
    seq: &Arc<AtomicU64>,
) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.args(args);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| MuliError::Internal(format!("failed to spawn git {}: {e}", args.join(" "))))?;

    spawn_log_streamers(&mut child, log_collector, seq);

    let status = child
        .wait()
        .await
        .map_err(|e| MuliError::Internal(format!("git wait error: {e}")))?;

    if !status.success() {
        return Err(MuliError::Internal(format!(
            "git {} exited with code {:?}",
            args.join(" "),
            status.code()
        )));
    }
    Ok(())
}

/// Spawn background tasks draining stdout and stderr into the log collector.
fn spawn_log_streamers(
    child: &mut tokio::process::Child,
    log_collector: &Arc<LogCollector>,
    seq: &Arc<AtomicU64>,
) {
    spawn_pipe(
        child.stdout.take(),
        log_collector.clone(),
        seq.clone(),
        LogStream::Stdout,
    );
    spawn_pipe(
        child.stderr.take(),
        log_collector.clone(),
        seq.clone(),
        LogStream::Stderr,
    );
}

fn spawn_pipe<R>(
    pipe: Option<R>,
    log_collector: Arc<LogCollector>,
    seq: Arc<AtomicU64>,
    stream: LogStream,
) where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
{
    if let Some(reader) = pipe {
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let s = seq.fetch_add(1, Ordering::Relaxed);
                log_collector
                    .push_line(LogLine {
                        sequence: s,
                        timestamp: chrono::Utc::now(),
                        stream,
                        message: line,
                    })
                    .await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::logs::LogCollector;
    use muli_core::job::model::CheckoutSpec;

    /// Helper: create a local bare git repo with one commit at `dir`.
    /// Returns the file:// URL and the commit SHA.
    fn make_local_repo(dir: &std::path::Path) -> (String, String) {
        use std::process::Command;

        let repo_path = dir.join("origin.git");
        std::fs::create_dir_all(&repo_path).unwrap();

        // Init bare repo
        Command::new("git")
            .args(["init", "--bare"])
            .current_dir(&repo_path)
            .output()
            .unwrap();

        // Create a non-bare working copy, make a commit, push to bare
        let work = dir.join("work");
        std::fs::create_dir_all(&work).unwrap();
        Command::new("git")
            .args(["init"])
            .current_dir(&work)
            .output()
            .unwrap();
        Command::new("git")
            .args([
                "-C",
                work.to_str().unwrap(),
                "config",
                "user.email",
                "test@test.com",
            ])
            .output()
            .unwrap();
        Command::new("git")
            .args(["-C", work.to_str().unwrap(), "config", "user.name", "Test"])
            .output()
            .unwrap();
        std::fs::write(work.join("hello.txt"), b"hello checkout").unwrap();
        Command::new("git")
            .args(["-C", work.to_str().unwrap(), "add", "."])
            .output()
            .unwrap();
        Command::new("git")
            .args(["-C", work.to_str().unwrap(), "commit", "-m", "init"])
            .output()
            .unwrap();
        let sha_out = Command::new("git")
            .args(["-C", work.to_str().unwrap(), "rev-parse", "HEAD"])
            .output()
            .unwrap();
        let sha = String::from_utf8(sha_out.stdout)
            .unwrap()
            .trim()
            .to_string();

        let remote = format!("file://{}", repo_path.display());
        Command::new("git")
            .args([
                "-C",
                work.to_str().unwrap(),
                "remote",
                "add",
                "origin",
                &remote,
            ])
            .output()
            .unwrap();
        Command::new("git")
            .args([
                "-C",
                work.to_str().unwrap(),
                "push",
                "origin",
                "HEAD:refs/heads/main",
            ])
            .output()
            .unwrap();

        (remote, sha)
    }

    /// perform_checkout clones a local repo and checks out the commit.
    #[tokio::test]
    async fn test_perform_checkout_clones_and_checks_out() {
        let tmp = tempfile::tempdir().unwrap();
        let (clone_url, sha) = make_local_repo(tmp.path());

        let workspace = tmp.path().join("ws");
        std::fs::create_dir_all(&workspace).unwrap();

        let log_collector = Arc::new(LogCollector::new());
        let spec = CheckoutSpec {
            clone_url,
            sha: sha.clone(),
            submodules: false,
        };

        perform_checkout(&spec, &workspace, &log_collector)
            .await
            .unwrap();

        // hello.txt should be present in workspace
        assert!(
            workspace.join("hello.txt").exists(),
            "hello.txt should be checked out"
        );
        let content = std::fs::read(workspace.join("hello.txt")).unwrap();
        assert_eq!(content, b"hello checkout");

        // Verify we're at the expected commit
        let head = std::process::Command::new("git")
            .args(["-C", workspace.to_str().unwrap(), "rev-parse", "HEAD"])
            .output()
            .unwrap();
        let head_sha = String::from_utf8(head.stdout).unwrap().trim().to_string();
        assert_eq!(head_sha, sha);
    }

    /// Log collector receives lines from checkout (at least one line emitted).
    #[tokio::test]
    async fn test_checkout_logs_forwarded_to_collector() {
        let tmp = tempfile::tempdir().unwrap();
        let (clone_url, sha) = make_local_repo(tmp.path());

        let workspace = tmp.path().join("ws2");
        std::fs::create_dir_all(&workspace).unwrap();

        let log_collector = Arc::new(LogCollector::new());
        let spec = CheckoutSpec {
            clone_url,
            sha,
            submodules: false,
        };

        perform_checkout(&spec, &workspace, &log_collector)
            .await
            .unwrap();

        // Give the background streamers a moment to flush
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let lines = log_collector.peek_unflushed().await;
        assert!(
            !lines.is_empty(),
            "at least one log line should be emitted during checkout"
        );
    }

    /// perform_checkout returns an error for a non-existent clone URL.
    #[tokio::test]
    async fn test_checkout_invalid_url_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().join("ws");
        std::fs::create_dir_all(&workspace).unwrap();

        let log_collector = Arc::new(LogCollector::new());
        let spec = CheckoutSpec {
            clone_url: "file:///nonexistent/repo.git".to_string(),
            sha: "abc123".to_string(),
            submodules: false,
        };

        let result = perform_checkout(&spec, &workspace, &log_collector).await;
        assert!(result.is_err(), "checkout of nonexistent repo should fail");
        // Error message must NOT contain the URL (auth token leak prevention)
        let err_msg = result.unwrap_err().to_string();
        assert!(
            !err_msg.contains("nonexistent/repo.git"),
            "error must not contain clone URL: {err_msg}"
        );
    }
}
