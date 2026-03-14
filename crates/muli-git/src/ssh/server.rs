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

use muli_core::git::{GitPermission, HasPermissions};
use muli_core::traits::{OrgMemberStore, OrgStore, RepositoryStore, SshKeyStore};

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
    pub default_tenant_id: Option<String>,
    pub org_store: Arc<dyn OrgStore>,
    pub org_member_store: Arc<dyn OrgMemberStore>,
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
                        default_tenant_id: self.default_tenant_id.clone(),
                        org_store: self.org_store.clone(),
                        org_member_store: self.org_member_store.clone(),
                        authenticated_fingerprint: None,
                        authenticated_user_id: None,
                        authenticated_key_tenant_id: None,
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
    default_tenant_id: Option<String>,
    org_store: Arc<dyn OrgStore>,
    org_member_store: Arc<dyn OrgMemberStore>,
    authenticated_fingerprint: Option<String>,
    authenticated_user_id: Option<String>,
    /// The tenant_id from the SSH key record — tells us which tenant DB the key lives in.
    authenticated_key_tenant_id: Option<String>,
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

        // Find the key across all tenants. The key's tenant_id tells us which
        // DB it lives in, which works for every deployment model:
        //   - Flightdeck (single tenant): key is in "local"
        //   - Subdomain multi-tenant: key is in its respective tenant DB
        match self.ssh_key_store.find_by_fingerprint(&fingerprint).await {
            Ok(Some(key)) => {
                // Require user_id on the key
                match key.user_id {
                    Some(ref uid) => {
                        tracing::debug!(
                            %fingerprint,
                            user_id = %uid,
                            key_tenant = %key.tenant_id,
                            "SSH key accepted"
                        );
                        self.authenticated_fingerprint = Some(fingerprint);
                        self.authenticated_user_id = Some(uid.clone());
                        self.authenticated_key_tenant_id = Some(key.tenant_id.clone());
                        Ok(Auth::Accept)
                    }
                    None => {
                        tracing::info!(%fingerprint, "SSH key rejected: no user_id");
                        Ok(Auth::Reject {
                            proceed_with_methods: None,
                        })
                    }
                }
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

        // 1. Check authentication
        let fingerprint = match self.authenticated_fingerprint.as_deref() {
            Some(fp) => fp.to_string(),
            None => {
                tracing::warn!("exec on unauthenticated SSH session");
                session.channel_failure(channel);
                return Ok(());
            }
        };
        let user_id = match self.authenticated_user_id.as_deref() {
            Some(uid) => uid.to_string(),
            None => {
                tracing::warn!("exec on SSH session without authenticated user_id");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        // 2. Parse git command
        let (git_cmd, path) = match parse_git_ssh_command(&command) {
            Some(v) => v,
            None => {
                tracing::debug!(%command, "unrecognised SSH command");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        // 3. Parse repo path → (namespace, repo_name)
        let (namespace, repo_name) = match parse_repo_path(&path) {
            Some(v) => v,
            None => {
                tracing::debug!(%path, "could not parse repo path");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        // 4. Resolve tenant: try namespace as tenant first, then fall back to default
        let tenant_id = match self
            .repo_store
            .get_repository_by_name(&namespace, &namespace, &repo_name)
            .await
        {
            Ok(Some(_)) => namespace.clone(),
            _ => {
                // Try default tenant
                if let Some(ref default_tid) = self.default_tenant_id {
                    match self
                        .repo_store
                        .get_repository_by_name(default_tid, &namespace, &repo_name)
                        .await
                    {
                        Ok(Some(_)) => default_tid.clone(),
                        _ => {
                            tracing::debug!(%namespace, %repo_name, "repository not found in any tenant");
                            session.channel_failure(channel);
                            return Ok(());
                        }
                    }
                } else {
                    tracing::debug!(%namespace, %repo_name, "repository not found and no default tenant configured");
                    session.channel_failure(channel);
                    return Ok(());
                }
            }
        };
        tracing::debug!(%tenant_id, %namespace, %repo_name, "resolved tenant for SSH exec");

        // 5. Fetch SSH key from its tenant DB for permission check.
        //    The key's tenant_id was captured at auth time.
        let key_tenant_id = match self.authenticated_key_tenant_id.as_deref() {
            Some(tid) => tid.to_string(),
            None => {
                tracing::warn!("exec without key tenant_id on session");
                session.channel_failure(channel);
                return Ok(());
            }
        };
        let ssh_key = match self
            .ssh_key_store
            .find_by_fingerprint_in_tenant(&key_tenant_id, &fingerprint)
            .await
        {
            Ok(Some(key)) => key,
            Ok(None) => {
                tracing::info!(%fingerprint, %key_tenant_id, "SSH key not found – rejecting");
                session.channel_failure(channel);
                return Ok(());
            }
            Err(e) => {
                tracing::error!(error = %e, "SSH key store error during authorization");
                session.channel_failure(channel);
                return Ok(());
            }
        };

        // 6. Org membership check: if repo lives in a different tenant than
        //    the key's tenant, verify the user is a member of that org.
        if tenant_id != key_tenant_id {
            // Namespace is an org handle — look up the org and check membership
            match self
                .org_store
                .get_org_by_handle(&tenant_id, &namespace)
                .await
            {
                Ok(Some(org)) => {
                    match self
                        .org_member_store
                        .get_member(&org.id, &user_id)
                        .await
                    {
                        Ok(Some(_)) => {
                            tracing::debug!(%user_id, org_id = %org.id, "org membership verified for SSH");
                        }
                        Ok(None) => {
                            tracing::info!(%user_id, org = %namespace, "SSH rejected: not an org member");
                            session.channel_failure(channel);
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "org member store error");
                            session.channel_failure(channel);
                            return Ok(());
                        }
                    }
                }
                Ok(None) => {
                    tracing::debug!(org = %namespace, %tenant_id, "org not found – skipping membership check");
                }
                Err(e) => {
                    tracing::error!(error = %e, "org store error");
                    session.channel_failure(channel);
                    return Ok(());
                }
            }
        }

        // 7. Check permissions for push
        if git_cmd == "git-receive-pack" && !ssh_key.has_permission(GitPermission::Push) {
            tracing::info!("SSH push rejected: key lacks Push permission");
            session.channel_failure(channel);
            return Ok(());
        }

        let repo_path = self.storage.repo_path(&tenant_id, &namespace, &repo_name);

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
