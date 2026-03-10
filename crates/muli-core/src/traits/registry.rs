// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Registry token, tenant, and quota storage traits.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::registry::model::{RegistryToken, TenantQuota};
use crate::tenant::Tenant;

/// Persistent storage for registry authentication tokens.
#[async_trait]
pub trait RegistryTokenStore: Send + Sync {
    /// Store a new token record.
    async fn create_token(&self, token: &RegistryToken) -> Result<String>;

    /// Look up a non-revoked, non-expired token by its prefix (used during authentication).
    /// The caller must verify the full token against the Argon2id hash.
    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<RegistryToken>>;

    /// Look up a token by its ID.
    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<RegistryToken>>;

    /// List all tokens belonging to a tenant.
    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<RegistryToken>>;

    /// Mark a token as revoked.
    async fn revoke_token(&self, token_id: &str) -> Result<()>;

    /// Remove tokens that have passed their expiration date.
    async fn delete_expired_tokens(&self) -> Result<u64>;

    /// Update the expiration time for a token (used during token rotation).
    async fn set_token_expiry(&self, token_id: &str, expires_at: DateTime<Utc>) -> Result<()>;
}

/// Persistent storage for tenant quota configuration and usage tracking.
#[async_trait]
pub trait TenantQuotaStore: Send + Sync {
    /// Get the quota for a tenant.
    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>>;

    /// Set (create or update) the maximum storage for a tenant.
    async fn set_quota(&self, tenant_id: &str, max_storage_bytes: u64) -> Result<()>;

    /// Update the current usage counter for a tenant.
    async fn update_usage(&self, tenant_id: &str, current_usage_bytes: u64) -> Result<()>;
}

/// Persistent storage for tenants (global, not scoped to a tenant DB).
#[async_trait]
pub trait TenantStore: Send + Sync {
    /// Create a new tenant record.
    async fn create_tenant(&self, tenant: &Tenant) -> Result<()>;

    /// Get a tenant by its ID (slug).
    async fn get_tenant(&self, id: &str) -> Result<Option<Tenant>>;

    /// List all tenants.
    async fn list_tenants(&self) -> Result<Vec<Tenant>>;

    /// Delete a tenant by its ID.
    async fn delete_tenant(&self, id: &str) -> Result<()>;
}
