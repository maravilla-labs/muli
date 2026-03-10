// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository, token, SSH key, and collaborator models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::MuliError;
use crate::token;

/// Identifies whether a repository is owned by a user or an organization.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnerType {
    #[default]
    User,
    Organization,
}

/// Permission level for a git token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GitPermission {
    Pull,
    Push,
    Admin,
}

/// Trait for types that carry a `Vec<GitPermission>` and can check access.
pub trait HasPermissions {
    /// Return a reference to the permissions list.
    fn permissions(&self) -> &[GitPermission];

    /// Check if a specific permission is granted.
    fn has_permission(&self, permission: GitPermission) -> bool {
        token::has_permission(self.permissions(), &permission)
    }
}

/// A git repository record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repository {
    pub id: String,
    pub tenant_id: String,
    /// Namespace (user or organization slug).
    pub namespace: String,
    /// Repository name (e.g., "my-project").
    pub name: String,
    pub description: String,
    pub is_private: bool,
    pub default_branch: String,
    /// If set, this repo was forked from the given repo ID.
    pub fork_of: Option<String>,
    /// Staticlab ObjectId hex of the owner (user or org). Empty string if not set.
    pub owner_id: String,
    pub owner_type: OwnerType,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Validate that a string is a valid repository namespace or name component.
fn validate_repo_component(value: &str, label: &str) -> Result<(), MuliError> {
    if value.is_empty() {
        return Err(MuliError::Validation(format!("{label} must not be empty")));
    }
    if value.contains("..") {
        return Err(MuliError::Validation(format!(
            "{label} must not contain path traversal (..)"
        )));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(MuliError::Validation(format!(
            "{label} must not contain path separators"
        )));
    }
    if value.contains('\0') {
        return Err(MuliError::Validation(format!(
            "{label} must not contain null bytes"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(MuliError::Validation(format!(
            "{label} contains invalid characters (allowed: alphanumeric, hyphens, underscores, dots)"
        )));
    }
    Ok(())
}

impl Repository {
    pub fn new(
        tenant_id: String,
        namespace: String,
        name: String,
        description: String,
        is_private: bool,
    ) -> Result<Self, MuliError> {
        validate_repo_component(&namespace, "namespace")?;
        validate_repo_component(&name, "name")?;

        let now = Utc::now();
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            namespace,
            name,
            description,
            is_private,
            default_branch: "main".to_string(),
            fork_of: None,
            owner_id: String::new(),
            owner_type: OwnerType::User,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn owner_type_str(&self) -> &str {
        match self.owner_type {
            OwnerType::User => "user",
            OwnerType::Organization => "organization",
        }
    }
}

/// A scoped authentication token for git access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitToken {
    pub id: String,
    pub tenant_id: String,
    /// The user this token belongs to, if scoped to a specific user.
    pub user_id: Option<String>,
    /// Argon2id PHC hash of the plaintext token.
    pub token_hash: String,
    /// Short prefix of the plaintext token for O(1) DB lookup.
    #[serde(default)]
    pub token_prefix: String,
    pub permissions: Vec<GitPermission>,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked: bool,
}

impl GitToken {
    pub fn new(
        tenant_id: String,
        token_hash: String,
        token_prefix: String,
        permissions: Vec<GitPermission>,
        description: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            user_id: None,
            token_hash,
            token_prefix,
            permissions,
            description,
            created_at: Utc::now(),
            expires_at,
            revoked: false,
        }
    }

    /// Check if this token is currently valid (not expired, not revoked).
    pub fn is_valid(&self) -> bool {
        token::is_token_valid(self.revoked, self.expires_at)
    }
}

impl HasPermissions for GitToken {
    fn permissions(&self) -> &[GitPermission] {
        &self.permissions
    }
}

/// An SSH public key for authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshKey {
    pub id: String,
    pub tenant_id: String,
    /// The user this SSH key belongs to, if scoped to a specific user.
    pub user_id: Option<String>,
    /// SHA256 fingerprint of the public key.
    pub fingerprint: String,
    /// OpenSSH format public key.
    pub public_key: String,
    pub title: String,
    pub permissions: Vec<GitPermission>,
    pub created_at: DateTime<Utc>,
}

impl SshKey {
    pub fn new(
        tenant_id: String,
        fingerprint: String,
        public_key: String,
        title: String,
        permissions: Vec<GitPermission>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            user_id: None,
            fingerprint,
            public_key,
            title,
            permissions,
            created_at: Utc::now(),
        }
    }
}

impl HasPermissions for SshKey {
    fn permissions(&self) -> &[GitPermission] {
        &self.permissions
    }
}

/// A collaborator record granting a specific user access to a private repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryCollaborator {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    /// Staticlab user ObjectId hex string (external_id in muli UserService).
    pub user_id: String,
    pub permissions: Vec<GitPermission>,
    pub created_at: DateTime<Utc>,
}

impl RepositoryCollaborator {
    pub fn new(
        tenant_id: String,
        repo_id: String,
        user_id: String,
        permissions: Vec<GitPermission>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            repo_id,
            user_id,
            permissions,
            created_at: Utc::now(),
        }
    }
}

impl HasPermissions for RepositoryCollaborator {
    fn permissions(&self) -> &[GitPermission] {
        &self.permissions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_new_sets_defaults() {
        let repo = Repository::new(
            "tenant-1".into(),
            "acme".into(),
            "my-repo".into(),
            "A test repo".into(),
            false,
        )
        .unwrap();
        assert!(!repo.id.is_empty());
        assert_eq!(repo.default_branch, "main");
        assert!(repo.fork_of.is_none());
        assert!(repo.owner_id.is_empty());
        assert_eq!(repo.owner_type, OwnerType::User);
    }

    #[test]
    fn repository_new_rejects_empty_namespace() {
        let err = Repository::new("t".into(), "".into(), "repo".into(), "".into(), false);
        assert!(err.is_err());
    }

    #[test]
    fn repository_new_rejects_empty_name() {
        let err = Repository::new("t".into(), "acme".into(), "".into(), "".into(), false);
        assert!(err.is_err());
    }

    #[test]
    fn repository_new_rejects_path_traversal() {
        let err = Repository::new("t".into(), "..".into(), "repo".into(), "".into(), false);
        assert!(err.is_err());
    }

    #[test]
    fn repository_new_rejects_slashes() {
        let err = Repository::new("t".into(), "a/b".into(), "repo".into(), "".into(), false);
        assert!(err.is_err());
    }

    #[test]
    fn repository_new_rejects_invalid_chars() {
        let err = Repository::new("t".into(), "acme".into(), "re po".into(), "".into(), false);
        assert!(err.is_err());
    }

    #[test]
    fn git_token_is_valid() {
        let token = GitToken::new(
            "tenant-1".into(),
            "hash".into(),
            "prefix".into(),
            vec![GitPermission::Pull, GitPermission::Push],
            "CI token".into(),
            None,
        );
        assert!(token.is_valid());
        assert!(token.has_permission(GitPermission::Pull));
        assert!(token.has_permission(GitPermission::Push));
        assert!(!token.has_permission(GitPermission::Admin));
    }

    #[test]
    fn git_token_revoked_is_invalid() {
        let mut token = GitToken::new(
            "tenant-1".into(),
            "hash".into(),
            "prefix".into(),
            vec![GitPermission::Pull],
            "test".into(),
            None,
        );
        token.revoked = true;
        assert!(!token.is_valid());
    }

    #[test]
    fn git_token_expired_is_invalid() {
        use chrono::Duration;
        let token = GitToken::new(
            "tenant-1".into(),
            "hash".into(),
            "prefix".into(),
            vec![GitPermission::Pull],
            "test".into(),
            Some(Utc::now() - Duration::hours(1)),
        );
        assert!(!token.is_valid());
    }

    #[test]
    fn collaborator_has_permission() {
        let collab = RepositoryCollaborator::new(
            "tenant-1".into(),
            "repo-1".into(),
            "user-1".into(),
            vec![GitPermission::Pull, GitPermission::Push],
        );
        assert!(collab.has_permission(GitPermission::Pull));
        assert!(collab.has_permission(GitPermission::Push));
        assert!(!collab.has_permission(GitPermission::Admin));
    }
}
