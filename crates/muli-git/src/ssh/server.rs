// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH server implementation and session handling.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use russh::server::{Auth, Handler, Msg, Session};
use russh::{Channel, ChannelId, CryptoVec};
use russh_keys::key::{KeyPair, PublicKey};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use muli_core::git::{GitPermission, HasPermissions, SshKey};
use muli_core::traits::{RepositoryStore, SshKeyStore};

use crate::ssh::auth::{parse_git_ssh_command, parse_repo_path};
use crate::storage::FilesystemStorage;

/// Maximum time to wait for a git SSH subprocess to complete (10 minutes).
const GIT_SSH_TIMEOUT: Duration = Duration::from_secs(600);

/// Configuration used when starting the SSH server.
pub struct SshConfig {
    pub host_key_path: PathBuf,
    pub bind_addr: SocketAddr,
}

/// Multi-tenant SSH git server.
pub struct SshServer {
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub storage: Arc<FilesystemStorage>,
}

impl SshServer {
    pub async fn run_on(
        self,
        listener: TcpListener,
        host_key: KeyPair,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let config = Arc::new(russh::server::Config {
            keys: vec![host_key],
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            ..Default::default()
        });

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    let (stream, addr) = match result {
                        Ok(pair) => pair,
                        Err(e) => {
                            tracing::error!(error = %e, "SSH accept error");
                            break;
                        }
                    };

                    let handler = SshSessionHandler {
                        ssh_key_store: self.ssh_key_store.clone(),
                        repo_store: self.repo_store.clone(),
                        storage: self.storage.clone(),
                        authenticated_key: None,
                        processes: HashMap::new(),
                    };

                    let config = config.clone();
                    tokio::spawn(async move {
                        tracing::debug!(%addr, "new SSH connection");
                        if let Err(e) = russh::server::run_stream(config, stream, handler).await {
                            tracing::debug!(%addr, error = %e, "SSH session ended");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// Load an Ed25519 host key from `path`, or generate one.
pub async fn load_or_generate_host_key(path: &std::path::Path) -> anyhow::Result<KeyPair> {
    if path.exists()
        && let Ok(key) = russh_keys::load_secret_key(path, None)
    {
        return Ok(key);
    }

    let key = russh_keys::key::KeyPair::generate_ed25519();

    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let path_str = path.to_string_lossy().to_string();
    let _ = tokio::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-N", "", "-f", &path_str])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;

    if path.exists()
        && let Ok(loaded) = russh_keys::load_secret_key(path, None)
    {
        return Ok(loaded);
    }

    tracing::warn!(path = %path.display(), "could not persist SSH host key");
    Ok(key)
}

// ---------------------------------------------------------------------------
// Per-process handle
// ---------------------------------------------------------------------------

struct ProcessHandle {
    stdin_tx: mpsc::Sender<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// SSH session handler
// ---------------------------------------------------------------------------

struct SshSessionHandler {
    ssh_key_store: Arc<dyn SshKeyStore>,
    repo_store: Arc<dyn RepositoryStore>,
    storage: Arc<FilesystemStorage>,
    authenticated_key: Option<SshKey>,
    processes: HashMap<ChannelId, ProcessHandle>,
}

#[async_trait]
impl Handler for SshSessionHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        public_key: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        let raw = public_key.fingerprint();
        let fingerprint = if raw.starts_with("SHA256:") {
            raw
        } else {
            format!("SHA256:{raw}")
        };
        match self.ssh_key_store.find_by_fingerprint(&fingerprint).await {
            Ok(Some(ssh_key)) => {
                tracing::debug!(%fingerprint, tenant_id = %ssh_key.tenant_id, "SSH key accepted");
                self.authenticated_key = Some(ssh_key);
                Ok(Auth::Accept)
            }
            Ok(None) => {
                tracing::debug!(%fingerprint, "SSH key not found – rejecting");
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                })
            }
            Err(e) => {
                tracing::error!(error = %e, "SSH key store error during auth");
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                })
            }
        }
    }

    async fn channel_open_session(
        &mut self,
        _channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let command = std::str::from_utf8(data).unwrap_or("").trim().to_string();
        tracing::debug!(%command, "SSH exec request");

        let tenant_id = match self.authenticated_key.as_ref().map(|k| k.tenant_id.clone()) {
            Some(t) => t,
            None => {
                tracing::warn!("exec on unauthenticated SSH session");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        let (git_cmd, path) = match parse_git_ssh_command(&command) {
            Some(v) => v,
            None => {
                tracing::debug!(%command, "unrecognised SSH command");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        let (namespace, repo_name) = match parse_repo_path(&path) {
            Some(v) => v,
            None => {
                tracing::debug!(%path, "could not parse repo path");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        // Validate the repository exists in the store
        match self
            .repo_store
            .get_repository_by_name(&tenant_id, &namespace, &repo_name)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::debug!(%namespace, %repo_name, "repository not found");
                session.channel_failure(channel);
                return Ok(());
            }
            Err(e) => {
                tracing::error!(error = %e, "repo store error in SSH exec");
                session.channel_failure(channel);
                return Ok(());
            }
        }

        let repo_path = self.storage.repo_path(&tenant_id, &namespace, &repo_name);

        if git_cmd == "git-receive-pack" {
            let can_push = self
                .authenticated_key
                .as_ref()
                .map_or(false, |k| k.has_permission(GitPermission::Push));
            if !can_push {
                tracing::info!("SSH push rejected: key lacks Push permission");
                session.channel_failure(channel);
                return Ok(());
            }
        }

        spawn_git_process(
            git_cmd,
            repo_path,
            &tenant_id,
            channel,
            session,
            &mut self.processes,
        )
        .await
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(proc) = self.processes.get(&channel) {
            let _ = proc.stdin_tx.send(data.to_vec()).await;
        }
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.processes.remove(&channel);
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.processes.remove(&channel);
        Ok(())
    }
}

async fn spawn_git_process(
    git_cmd: String,
    repo_path: PathBuf,
    tenant_id: &str,
    channel: ChannelId,
    session: &mut Session,
    processes: &mut HashMap<ChannelId, ProcessHandle>,
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
            session.channel_failure(channel);
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
                    let cv = CryptoVec::from_slice(&buf[..n]);
                    if handle.data(channel, cv).await.is_err() {
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

    session.channel_success(channel);
    processes.insert(channel, ProcessHandle { stdin_tx });
    Ok(())
}
