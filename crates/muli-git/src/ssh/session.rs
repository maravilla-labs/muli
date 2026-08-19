// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH session handler — authenticates users and dispatches git commands.

use std::collections::HashMap;
use std::sync::Arc;

use russh::keys::{HashAlg, PublicKey};
use russh::server::{Auth, Handler, Msg, Session};
use russh::{Channel, ChannelId};
use tokio::sync::mpsc;

use muli_core::git::{
    GitPermission, HasPermissions, RepoAccessVerdict, check_repo_access_with_org_lookup,
};
use muli_core::traits::{
    CollaboratorStore, GitTokenStore, OrgMemberStore, OrgStore, RepositoryStore, SshKeyStore,
};

use crate::hooks::PostPushHooks;
use crate::ssh::auth::{parse_git_ssh_command, parse_repo_path};
use crate::storage::FilesystemStorage;

use super::process::spawn_git_process;
use super::ref_tracking::{SshPrePushSnapshot, read_refs};

/// Handle for piping data to a running git subprocess.
pub(super) struct ProcessHandle {
    pub stdin_tx: mpsc::Sender<Vec<u8>>,
}

/// Per-connection SSH session handler.
pub(super) struct SshSessionHandler {
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub storage: Arc<FilesystemStorage>,
    pub default_tenant_id: Option<String>,
    pub org_store: Arc<dyn OrgStore>,
    pub org_member_store: Arc<dyn OrgMemberStore>,
    pub collaborator_store: Arc<dyn CollaboratorStore>,
    #[allow(dead_code)] // reserved for LFS token generation
    pub token_store: Option<Arc<dyn GitTokenStore>>,
    pub git_domain: Option<String>,
    pub post_push_hooks: PostPushHooks,
    pub authenticated_fingerprint: Option<String>,
    pub authenticated_user_id: Option<String>,
    /// The tenant_id from the SSH key record — tells us which tenant DB the key lives in.
    pub authenticated_key_tenant_id: Option<String>,
    pub processes: HashMap<ChannelId, ProcessHandle>,
}

impl Handler for SshSessionHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        public_key: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        // ssh-key formats a SHA-256 fingerprint as "SHA256:<base64-no-pad>",
        // matching what `ssh-keygen -l -E sha256` and our stored keys use.
        let fingerprint = public_key.fingerprint(HashAlg::Sha256).to_string();

        // Find the key across all tenants. The key's tenant_id tells us which
        // DB it lives in, which works for every deployment model:
        //   - Single-tenant deployment: key is in the default tenant ("local")
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
                        Ok(Auth::reject())
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(%fingerprint, "SSH key not found – rejecting");
                Ok(Auth::reject())
            }
            Err(e) => {
                tracing::error!(error = %e, "SSH key store error during auth");
                Ok(Auth::reject())
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
                let _ = session.channel_failure(channel);
                return Ok(());
            }
        };
        let user_id = match self.authenticated_user_id.as_deref() {
            Some(uid) => uid.to_string(),
            None => {
                tracing::warn!("exec on SSH session without authenticated user_id");
                let _ = session.channel_failure(channel);
                return Ok(());
            }
        };

        // 2. Parse git command
        let (git_cmd, path) = match parse_git_ssh_command(&command) {
            Some(v) => v,
            None => {
                tracing::debug!(%command, "unrecognised SSH command");
                let _ = session.channel_failure(channel);
                return Ok(());
            }
        };

        // 2b. Handle git-lfs-authenticate (returns JSON, no subprocess)
        if git_cmd == "git-lfs-authenticate" {
            return crate::ssh::lfs_auth::handle_lfs_authenticate(
                channel,
                session,
                &path,
                &user_id,
                &self.repo_store,
                &self.org_store,
                self.default_tenant_id.as_deref(),
                self.git_domain.as_deref(),
            )
            .await;
        }

        // 3. Parse repo path → (namespace, repo_name)
        let (namespace, repo_name) = match parse_repo_path(&path) {
            Some(v) => v,
            None => {
                tracing::debug!(%path, "could not parse repo path");
                let _ = session.channel_failure(channel);
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
                            let _ = session.channel_failure(channel);
                            return Ok(());
                        }
                    }
                } else {
                    tracing::debug!(%namespace, %repo_name, "repository not found and no default tenant configured");
                    let _ = session.channel_failure(channel);
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
                let _ = session.channel_failure(channel);
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
                let _ = session.channel_failure(channel);
                return Ok(());
            }
            Err(e) => {
                tracing::error!(error = %e, "SSH key store error during authorization");
                let _ = session.channel_failure(channel);
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
                Ok(Some(org)) => match self.org_member_store.get_member(&org.id, &user_id).await {
                    Ok(Some(_)) => {
                        tracing::debug!(%user_id, org_id = %org.id, "org membership verified for SSH");
                    }
                    Ok(None) => {
                        tracing::info!(%user_id, org = %namespace, "SSH rejected: not an org member");
                        let _ = session.channel_failure(channel);
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "org member store error");
                        let _ = session.channel_failure(channel);
                        return Ok(());
                    }
                },
                Ok(None) => {
                    tracing::info!(org = %namespace, %tenant_id, "SSH rejected: org not found in cross-tenant check");
                    let _ = session.channel_failure(channel);
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(error = %e, "org store error");
                    let _ = session.channel_failure(channel);
                    return Ok(());
                }
            }
        }

        // 7. Check permissions for push
        if git_cmd == "git-receive-pack" && !ssh_key.has_permission(GitPermission::Push) {
            tracing::info!("SSH push rejected: key lacks Push permission");
            let _ = session.channel_failure(channel);
            return Ok(());
        }

        // 8. Per-repo collaborator/owner check.
        //    - Private repos: owner or collaborator required for any access
        //    - Public repos: anyone can read, but only owner or collaborator can push
        //    - Org-owned repos: org members get access based on their role
        let is_push = git_cmd == "git-receive-pack";
        let required = if is_push {
            GitPermission::Push
        } else {
            GitPermission::Pull
        };
        let repo = match self
            .repo_store
            .get_repository_by_name(&tenant_id, &namespace, &repo_name)
            .await
        {
            Ok(Some(repo)) => {
                let verdict = check_repo_access_with_org_lookup(
                    &repo,
                    Some(&user_id),
                    required,
                    self.collaborator_store.as_ref(),
                    Some(self.org_store.as_ref()),
                    Some(self.org_member_store.as_ref()),
                    &tenant_id,
                )
                .await;
                if let RepoAccessVerdict::Denied { .. } = verdict {
                    tracing::info!(
                        %user_id, %namespace, %repo_name, %is_push,
                        is_private = %repo.is_private,
                        "SSH rejected: not owner or collaborator"
                    );
                    let _ = session.channel_failure(channel);
                    return Ok(());
                }
                repo
            }
            Ok(None) => {
                tracing::debug!(%namespace, %repo_name, "repo not found during ACL check");
                let _ = session.channel_failure(channel);
                return Ok(());
            }
            Err(e) => {
                tracing::error!(error = %e, "repo store error during SSH ACL check");
                let _ = session.channel_failure(channel);
                return Ok(());
            }
        };

        let repo_path = self.storage.repo_path(&tenant_id, &namespace, &repo_name);

        // Pre-push quota gate: reject if tenant is already over quota.
        if is_push {
            if let Some(ref store) = self.post_push_hooks.quota_store {
                if let Ok(Some(quota)) = store.get_quota(&tenant_id).await {
                    if quota.max_storage_bytes > 0
                        && quota.current_usage_bytes >= quota.max_storage_bytes
                    {
                        tracing::info!(
                            %tenant_id, %repo_name,
                            "SSH push rejected: tenant storage quota exceeded"
                        );
                        let _ = session.channel_failure(channel);
                        return Ok(());
                    }
                }
            }
        }

        // Build pre-push snapshot for receive-pack so we can fire post-push
        // hooks (pipelines, webhooks, cache, quota) after the push completes.
        let post_push = if is_push {
            let old_refs = read_refs(&repo_path).await;
            let repo_size_before = crate::hooks::compute_dir_size(&repo_path).await.ok();
            Some(SshPrePushSnapshot {
                hooks: self.post_push_hooks.clone(),
                tenant_id: tenant_id.clone(),
                repo_id: repo.id.clone(),
                repo_name: repo_name.clone(),
                old_refs,
                repo_path: repo_path.clone(),
                repo_size_before,
            })
        } else {
            None
        };

        spawn_git_process(
            git_cmd,
            repo_path,
            &tenant_id,
            channel,
            session,
            &mut self.processes,
            post_push,
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
