// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! User storage trait.

use async_trait::async_trait;

use crate::error::Result;
use crate::user::TenantUser;

/// Persistent storage for tenant users.
#[async_trait]
pub trait UserStore: Send + Sync {
    /// Create a new user record.
    async fn create_user(&self, user: &TenantUser) -> Result<String>;

    /// Get a user by ID.
    async fn get_user(&self, user_id: &str) -> Result<Option<TenantUser>>;

    /// Get a user by tenant + handle (unique within tenant).
    async fn get_user_by_handle(&self, tenant_id: &str, handle: &str)
    -> Result<Option<TenantUser>>;

    /// Get a user by tenant + external ID.
    async fn get_user_by_external_id(
        &self,
        tenant_id: &str,
        external_id: &str,
    ) -> Result<Option<TenantUser>>;

    /// Delete a user by ID.
    async fn delete_user(&self, user_id: &str) -> Result<()>;

    /// List all users for a tenant.
    async fn list_users(&self, tenant_id: &str) -> Result<Vec<TenantUser>>;
}
