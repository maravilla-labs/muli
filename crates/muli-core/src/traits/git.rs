// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git repository, token, webhook, and SSH key storage traits.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::git::{GitToken, Repository, RepositoryCollaborator, SshKey, Webhook};

/// Persistent storage for git repositories.
#[async_trait]
pub trait RepositoryStore: Send + Sync {
    /// Create a new repository record.
    async fn create_repository(&self, repo: &Repository) -> Result<String>;

    /// Get a repository by its ID.
    async fn get_repository(&self, repo_id: &str) -> Result<Option<Repository>>;

    /// Get a repository by tenant + namespace + name.
    async fn get_repository_by_name(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Repository>>;

    /// List all repositories for a tenant.
    async fn list_repositories(&self, tenant_id: &str) -> Result<Vec<Repository>>;

    /// Delete a repository by ID.
    async fn delete_repository(&self, repo_id: &str) -> Result<()>;

    /// Update a repository record.
    async fn update_repository(&self, repo: &Repository) -> Result<()>;

    /// List all forks of a given parent repository.
    async fn list_forks(&self, parent_repo_id: &str) -> Result<Vec<Repository>>;

    /// Transfer a repository to a new namespace (rename the owner).
    async fn transfer_repository(&self, repo_id: &str, new_namespace: &str) -> Result<()>;
}

/// Persistent storage for git access tokens.
#[async_trait]
pub trait GitTokenStore: Send + Sync {
    /// Store a new token record.
    async fn create_token(&self, token: &GitToken) -> Result<String>;

    /// Look up a non-revoked, non-expired token by its prefix (used during authentication).
    /// The caller must verify the full token against the Argon2id hash.
    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<GitToken>>;

    /// Look up a token by its ID.
    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<GitToken>>;

    /// List all tokens for a tenant.
    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<GitToken>>;

    /// List all tokens belonging to a specific user within a tenant.
    async fn list_tokens_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<GitToken>>;

    /// Mark a token as revoked.
    async fn revoke_token(&self, token_id: &str) -> Result<()>;

    /// Remove expired tokens.
    async fn delete_expired_tokens(&self) -> Result<u64>;

    /// Update the expiration time for a token (rotation).
    async fn set_token_expiry(&self, token_id: &str, expires_at: DateTime<Utc>) -> Result<()>;
}

/// Persistent storage for SSH public keys.
#[async_trait]
pub trait SshKeyStore: Send + Sync {
    /// Store a new SSH key.
    async fn add_key(&self, key: &SshKey) -> Result<String>;

    /// Remove an SSH key by ID.
    async fn remove_key(&self, key_id: &str) -> Result<()>;

    /// Find a key by its SHA256 fingerprint.
    async fn find_by_fingerprint(&self, fingerprint: &str) -> Result<Option<SshKey>>;

    /// List all keys for a tenant.
    async fn list_keys(&self, tenant_id: &str) -> Result<Vec<SshKey>>;

    /// List all keys belonging to a specific user within a tenant.
    async fn list_keys_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<SshKey>>;
}

/// Persistent storage for repository webhooks.
#[async_trait]
pub trait WebhookStore: Send + Sync {
    /// Create a new webhook.
    async fn create_webhook(&self, webhook: &Webhook) -> Result<String>;

    /// Get a webhook by ID.
    async fn get_webhook(&self, webhook_id: &str) -> Result<Option<Webhook>>;

    /// List all webhooks for a repository.
    async fn list_webhooks(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<Webhook>>;

    /// Delete a webhook by ID.
    async fn delete_webhook(&self, webhook_id: &str) -> Result<()>;

    /// Update a webhook record.
    async fn update_webhook(&self, webhook: &Webhook) -> Result<()>;
}

/// Persistent storage for repository collaborators (per-repo access grants).
#[async_trait]
pub trait CollaboratorStore: Send + Sync {
    /// Add or update a collaborator for a repository.
    async fn upsert_collaborator(&self, collaborator: &RepositoryCollaborator) -> Result<String>;

    /// Remove a collaborator from a repository.
    async fn remove_collaborator(&self, repo_id: &str, user_id: &str) -> Result<()>;

    /// List all collaborators for a repository.
    async fn list_collaborators(&self, repo_id: &str) -> Result<Vec<RepositoryCollaborator>>;

    /// Get a specific collaborator record.
    async fn get_collaborator(
        &self,
        repo_id: &str,
        user_id: &str,
    ) -> Result<Option<RepositoryCollaborator>>;
}

/// Read-through cache for the last-commit-per-entry data returned by
/// `GET /tree-commits`. Keys on (tenant_id, repo_id, commit_sha, dir_path).
/// Entries are stored and returned as raw JSON strings to keep the
/// core crate free of git-service types.
#[async_trait]
pub trait TreeCommitCacheStore: Send + Sync {
    /// Return a cached JSON string for the given key, or None on a cache miss.
    async fn get_cached(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        dir_path: &str,
    ) -> Result<Option<String>>;

    /// Store a JSON string for the given key.
    async fn set_cached(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        dir_path: &str,
        entries_json: &str,
    ) -> Result<()>;

    /// Wipe all cache rows for a repo (call after a successful push).
    async fn invalidate_repo(&self, tenant_id: &str, repo_id: &str) -> Result<()>;
}
