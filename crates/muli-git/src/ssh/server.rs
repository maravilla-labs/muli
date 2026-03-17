// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH server startup and host-key management.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use russh_keys::key::KeyPair;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use muli_core::traits::{
    CollaboratorStore, GitTokenStore, OrgMemberStore, OrgStore, RepositoryStore, SshKeyStore,
};

use crate::hooks::PostPushHooks;
use crate::storage::FilesystemStorage;

use super::session::SshSessionHandler;

/// Maximum number of concurrent SSH sessions.
const MAX_SSH_SESSIONS: usize = 128;

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
    pub collaborator_store: Arc<dyn CollaboratorStore>,
    /// Token store for generating short-lived LFS auth tokens (None = LFS SSH disabled).
    pub token_store: Option<Arc<dyn GitTokenStore>>,
    /// Git domain for building LFS endpoint URLs in SSH authenticate responses.
    pub git_domain: Option<String>,
    /// Shared post-push hook infrastructure (pipelines, webhooks, cache invalidation).
    pub post_push_hooks: PostPushHooks,
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

        let semaphore = Arc::new(Semaphore::new(MAX_SSH_SESSIONS));

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    let (stream, addr) = match result {
                        Ok(pair) => pair,
                        Err(e) => {
                            tracing::error!(error = %e, "SSH accept error, retrying");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            tracing::warn!(%addr, "SSH connection rejected: max sessions ({MAX_SSH_SESSIONS}) reached");
                            drop(stream);
                            continue;
                        }
                    };

                    let handler = SshSessionHandler {
                        ssh_key_store: self.ssh_key_store.clone(),
                        repo_store: self.repo_store.clone(),
                        storage: self.storage.clone(),
                        default_tenant_id: self.default_tenant_id.clone(),
                        org_store: self.org_store.clone(),
                        org_member_store: self.org_member_store.clone(),
                        collaborator_store: self.collaborator_store.clone(),
                        token_store: self.token_store.clone(),
                        git_domain: self.git_domain.clone(),
                        post_push_hooks: self.post_push_hooks.clone(),
                        authenticated_fingerprint: None,
                        authenticated_user_id: None,
                        authenticated_key_tenant_id: None,
                        processes: HashMap::new(),
                    };

                    let config = config.clone();
                    tokio::spawn(async move {
                        let _permit = permit; // held until session ends
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
