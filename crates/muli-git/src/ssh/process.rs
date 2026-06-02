// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git subprocess spawning for SSH sessions.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use russh::ChannelId;
use russh::server::Session;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::ref_tracking::{SshPrePushSnapshot, compute_ref_updates, read_refs};
use super::session::ProcessHandle;

/// Maximum time to wait for a git SSH subprocess to complete (10 minutes).
const GIT_SSH_TIMEOUT: Duration = Duration::from_secs(600);

pub(super) async fn spawn_git_process(
    git_cmd: String,
    repo_path: PathBuf,
    tenant_id: &str,
    channel: ChannelId,
    session: &mut Session,
    processes: &mut HashMap<ChannelId, ProcessHandle>,
    post_push: Option<SshPrePushSnapshot>,
) -> Result<(), anyhow::Error> {
    let mut cmd = tokio::process::Command::new(&git_cmd);
    cmd.arg(&repo_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("REMOTE_USER", tenant_id);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, cmd = %git_cmd, "failed to spawn git process");
            let _ = session.channel_failure(channel);
            return Ok(());
        }
    };

    let mut stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(32);

    tokio::spawn(async move {
        while let Some(data) = stdin_rx.recv().await {
            if stdin.write_all(&data).await.is_err() {
                break;
            }
        }
    });

    let git_cmd_clone = git_cmd.clone();
    let handle = session.handle();
    tokio::spawn(async move {
        let mut stdout = stdout;
        let mut buf = vec![0u8; 16384];
        loop {
            match stdout.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    // russh 0.50+: Handle::data takes `impl Into<bytes::Bytes>`.
                    if handle.data(channel, buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        // Use 128 as default exit code (signal-killed) since 0 would falsely indicate success.
        let exit_code = match tokio::time::timeout(GIT_SSH_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) => status.code().unwrap_or(128) as u32,
            Ok(Err(e)) => {
                tracing::error!(error = %e, "failed to wait for git SSH process");
                128
            }
            Err(_) => {
                tracing::error!("git SSH process timed out after {:?}", GIT_SSH_TIMEOUT);
                let _ = child.kill().await;
                128
            }
        };

        // Fire post-push hooks (pipelines, webhooks, cache) on success.
        if exit_code == 0 {
            if let Some(snap) = post_push {
                tracing::debug!(
                    tenant_id = %snap.tenant_id,
                    repo_id = %snap.repo_id,
                    "SSH receive-pack succeeded, firing post-push hooks"
                );
                tokio::spawn(async move {
                    let new_refs = read_refs(&snap.repo_path).await;
                    let ref_updates = compute_ref_updates(&snap.old_refs, &new_refs);
                    snap.hooks.fire(
                        snap.tenant_id,
                        snap.repo_id,
                        snap.repo_name,
                        ref_updates,
                        snap.repo_size_before,
                        snap.repo_path,
                    );
                });
            }
        }

        // Capture and log stderr output from the git process
        let mut stderr = stderr;
        let mut stderr_buf = Vec::new();
        let _ = stderr.read_to_end(&mut stderr_buf).await;
        if !stderr_buf.is_empty() {
            let stderr_str = String::from_utf8_lossy(&stderr_buf);
            tracing::warn!(cmd = %git_cmd_clone, stderr = %stderr_str, "git SSH process stderr");
        }

        let _ = handle.exit_status_request(channel, exit_code).await;
        let _ = handle.eof(channel).await;
        let _ = handle.close(channel).await;
    });

    let _ = session.channel_success(channel);
    processes.insert(channel, ProcessHandle { stdin_tx });
    Ok(())
}
