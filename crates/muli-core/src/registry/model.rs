// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Registry token, tenant quota, and permission models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::token;

/// Permission level for a registry token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RegistryPermission {
    Pull,
    Push,
    Admin,
}

/// Read-access visibility for a tenant's registry. Governs who may PULL; PUSH
/// always requires a tenant token with the Push permission regardless of this.
///
/// muli only acts on `Public` (it allows anonymous reads). `Authenticated` and
/// `Private` are both "a valid tenant token is required" from muli's side — the
/// breadth of who is *issued* such a token is decided upstream at token issuance
/// (the control plane hands an authenticated user a Pull-only token when the level
/// is `Authenticated`). Storing the exact level here keeps muli the source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryVisibilityLevel {
    /// Members only (a valid tenant token). The default and today's behaviour.
    #[default]
    Private,
    /// Any authenticated user — enforced at token issuance, not by muli.
    Authenticated,
    /// Anyone, no token: muli serves anonymous reads for this tenant.
    Public,
}

impl RegistryVisibilityLevel {
    /// Stable string form for storage/wire.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Authenticated => "authenticated",
            Self::Public => "public",
        }
    }

    /// Parse from the stored/wire string. Unknown values FAIL CLOSED to `Private`
    /// so a corrupt or future value can never accidentally open a registry.
    pub fn parse_lenient(s: &str) -> Self {
        match s {
            "authenticated" => Self::Authenticated,
            "public" => Self::Public,
            _ => Self::Private,
        }
    }
}

/// Per-tenant registry read visibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryVisibility {
    pub tenant_id: String,
    pub visibility: RegistryVisibilityLevel,
}

/// A scoped authentication token for registry access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryToken {
    pub id: String,
    pub tenant_id: String,
    /// Argon2id PHC hash of the plaintext token.
    pub token_hash: String,
    /// Short prefix of the plaintext token for O(1) DB lookup.
    #[serde(default)]
    pub token_prefix: String,
    pub permissions: Vec<RegistryPermission>,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked: bool,
}

/// Storage quota and current usage for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuota {
    pub tenant_id: String,
    pub max_storage_bytes: u64,
    pub current_usage_bytes: u64,
}

impl RegistryToken {
    pub fn new(
        tenant_id: String,
        token_hash: String,
        token_prefix: String,
        permissions: Vec<RegistryPermission>,
        description: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
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

    /// Check if this token has a specific permission.
    pub fn has_permission(&self, permission: RegistryPermission) -> bool {
        token::has_permission(&self.permissions, &permission)
    }
}

impl TenantQuota {
    pub fn new(tenant_id: String, max_storage_bytes: u64) -> Self {
        Self {
            tenant_id,
            max_storage_bytes,
            current_usage_bytes: 0,
        }
    }

    /// Check if adding `bytes` would exceed the quota.
    pub fn would_exceed(&self, bytes: u64) -> bool {
        self.current_usage_bytes.saturating_add(bytes) > self.max_storage_bytes
    }

    /// Remaining bytes before hitting the quota limit.
    pub fn remaining_bytes(&self) -> u64 {
        self.max_storage_bytes
            .saturating_sub(self.current_usage_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_registry_token_new() {
        let token = RegistryToken::new(
            "tenant-1".into(),
            "hash123".into(),
            "prefix".into(),
            vec![RegistryPermission::Pull, RegistryPermission::Push],
            "CI token".into(),
            None,
        );
        assert!(!token.id.is_empty());
        assert_eq!(token.tenant_id, "tenant-1");
        assert!(!token.revoked);
        assert!(token.is_valid());
    }

    #[test]
    fn test_registry_token_expired() {
        let token = RegistryToken::new(
            "tenant-1".into(),
            "hash123".into(),
            "prefix".into(),
            vec![RegistryPermission::Pull],
            "expired token".into(),
            Some(Utc::now() - Duration::hours(1)),
        );
        assert!(!token.is_valid());
    }

    #[test]
    fn test_registry_token_revoked() {
        let mut token = RegistryToken::new(
            "tenant-1".into(),
            "hash123".into(),
            "prefix".into(),
            vec![RegistryPermission::Pull],
            "revoked token".into(),
            None,
        );
        token.revoked = true;
        assert!(!token.is_valid());
    }

    #[test]
    fn test_has_permission() {
        let token = RegistryToken::new(
            "tenant-1".into(),
            "hash123".into(),
            "prefix".into(),
            vec![RegistryPermission::Pull, RegistryPermission::Push],
            "test".into(),
            None,
        );
        assert!(token.has_permission(RegistryPermission::Pull));
        assert!(token.has_permission(RegistryPermission::Push));
        assert!(!token.has_permission(RegistryPermission::Admin));
    }

    #[test]
    fn test_tenant_quota_new() {
        let quota = TenantQuota::new("tenant-1".into(), 1_000_000);
        assert_eq!(quota.current_usage_bytes, 0);
        assert_eq!(quota.remaining_bytes(), 1_000_000);
    }

    #[test]
    fn test_tenant_quota_would_exceed() {
        let mut quota = TenantQuota::new("tenant-1".into(), 1000);
        quota.current_usage_bytes = 900;
        assert!(!quota.would_exceed(100));
        assert!(quota.would_exceed(101));
    }

    #[test]
    fn test_tenant_quota_remaining() {
        let mut quota = TenantQuota::new("tenant-1".into(), 1000);
        quota.current_usage_bytes = 750;
        assert_eq!(quota.remaining_bytes(), 250);
    }
}
