// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared secret resolution: loads org and repo secrets, decrypts, and merges with caller env_vars.
//!
//! Merge order (each level overrides previous):
//!   1. Org-level secrets (if org_id provided)
//!   2. Repo-level secrets
//!   3. Caller-provided env_vars

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Status;
use tracing::warn;

use muli_core::traits::{OrgSecretStore, PipelineSecretStore};

/// Resolve pipeline secrets and merge with caller env_vars.
///
/// - If `encryption_key` is `None`, secrets are skipped with a warning.
/// - If `org_id` is provided, org-level secrets are loaded first (repo overrides org on conflict).
/// - `caller_env_vars` override everything.
pub async fn resolve_env_vars(
    secret_store: &Arc<dyn PipelineSecretStore>,
    org_secret_store: &Arc<dyn OrgSecretStore>,
    tenant_id: &str,
    repo_id: &str,
    org_id: Option<&str>,
    encryption_key: Option<&[u8; 32]>,
    caller_env_vars: HashMap<String, String>,
) -> Result<HashMap<String, String>, Status> {
    let mut env = HashMap::new();

    let Some(key) = encryption_key else {
        if !caller_env_vars.is_empty() || has_secrets(secret_store, tenant_id, repo_id).await {
            warn!(
                repo_id = %repo_id,
                "pipeline secret encryption key not configured; skipping secret decryption"
            );
        }
        env.extend(caller_env_vars);
        return Ok(env);
    };

    // 1. Org-level secrets
    if let Some(oid) = org_id {
        let org_names = org_secret_store
            .list_org_names(tenant_id, oid)
            .await
            .map_err(|e| Status::internal(format!("failed to list org secrets: {e}")))?;

        for name in org_names {
            if let Some(secret) = org_secret_store
                .get_org_secret(tenant_id, oid, &name)
                .await
                .map_err(|e| Status::internal(format!("failed to get org secret: {e}")))?
            {
                match muli_core::crypto::decrypt(&secret.encrypted_value, key) {
                    Ok(plaintext) => {
                        env.insert(name, plaintext);
                    }
                    Err(e) => {
                        warn!(
                            secret_name = %secret.name,
                            org_id = %oid,
                            error = %e,
                            "failed to decrypt org secret; skipping"
                        );
                    }
                }
            }
        }
    }

    // 2. Repo-level secrets (override org on conflict)
    let repo_names = secret_store
        .list_names(tenant_id, repo_id)
        .await
        .map_err(|e| Status::internal(format!("failed to list repo secrets: {e}")))?;

    for name in repo_names {
        if let Some(secret) = secret_store
            .get_secret(tenant_id, repo_id, &name)
            .await
            .map_err(|e| Status::internal(format!("failed to get repo secret: {e}")))?
        {
            match muli_core::crypto::decrypt(&secret.encrypted_value, key) {
                Ok(plaintext) => {
                    env.insert(name, plaintext);
                }
                Err(e) => {
                    warn!(
                        secret_name = %secret.name,
                        repo_id = %repo_id,
                        error = %e,
                        "failed to decrypt repo secret; skipping"
                    );
                }
            }
        }
    }

    // 3. Caller env_vars override everything
    env.extend(caller_env_vars);

    Ok(env)
}

/// Quick check whether any secrets exist for this repo (used to gate warning messages).
async fn has_secrets(
    secret_store: &Arc<dyn PipelineSecretStore>,
    tenant_id: &str,
    repo_id: &str,
) -> bool {
    secret_store
        .list_names(tenant_id, repo_id)
        .await
        .map(|names| !names.is_empty())
        .unwrap_or(false)
}
