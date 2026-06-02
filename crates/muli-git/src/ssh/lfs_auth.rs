// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH `git-lfs-authenticate` handler.
//!
//! Returns a JSON response with the LFS endpoint URL so the git-lfs client
//! can use HTTP for actual transfers.

use std::sync::Arc;

use russh::ChannelId;
use russh::server::Session;

use muli_core::traits::{OrgStore, RepositoryStore};

use super::auth::{parse_lfs_authenticate_args, parse_repo_path};

/// Handle `git-lfs-authenticate <repo> upload|download`.
///
/// Resolves the tenant and returns JSON with the LFS endpoint URL.
pub async fn handle_lfs_authenticate(
    channel: ChannelId,
    session: &mut Session,
    args: &str,
    _user_id: &str,
    repo_store: &Arc<dyn RepositoryStore>,
    _org_store: &Arc<dyn OrgStore>,
    default_tenant_id: Option<&str>,
    git_domain: Option<&str>,
) -> Result<(), anyhow::Error> {
    // Parse "<repo_path> upload|download"
    let (repo_path, _operation) = match parse_lfs_authenticate_args(args) {
        Some(v) => v,
        None => {
            tracing::debug!(%args, "invalid git-lfs-authenticate args");
            let _ = session.channel_failure(channel);
            return Ok(());
        }
    };

    let (namespace, repo_name) = match parse_repo_path(&repo_path) {
        Some(v) => v,
        None => {
            tracing::debug!(%repo_path, "could not parse LFS repo path");
            let _ = session.channel_failure(channel);
            return Ok(());
        }
    };

    // Resolve tenant (same logic as regular git commands)
    let tenant_id = match repo_store
        .get_repository_by_name(&namespace, &namespace, &repo_name)
        .await
    {
        Ok(Some(_)) => namespace.clone(),
        _ => match default_tenant_id {
            Some(default_tid) => match repo_store
                .get_repository_by_name(default_tid, &namespace, &repo_name)
                .await
            {
                Ok(Some(_)) => default_tid.to_string(),
                _ => {
                    tracing::debug!(%namespace, %repo_name, "LFS: repo not found");
                    let _ = session.channel_failure(channel);
                    return Ok(());
                }
            },
            None => {
                let _ = session.channel_failure(channel);
                return Ok(());
            }
        },
    };

    // Build the LFS endpoint URL
    let domain = git_domain.unwrap_or("localhost");
    let href = format!("https://{tenant_id}.{domain}/{namespace}/{repo_name}.git/info/lfs");

    let response = serde_json::json!({
        "href": href,
        "header": {},
        "expires_in": 3600
    });

    let response_bytes = serde_json::to_vec(&response)?;
    let _ = session.data(channel, response_bytes);
    let handle = session.handle();
    let _ = handle.exit_status_request(channel, 0).await;
    let _ = handle.eof(channel).await;
    let _ = handle.close(channel).await;

    tracing::debug!(%tenant_id, %namespace, %repo_name, "LFS authenticate response sent");
    Ok(())
}
