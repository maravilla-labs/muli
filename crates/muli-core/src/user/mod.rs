// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! User accounts: profile, SSH keys, and authentication.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A user identity within a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantUser {
    pub id: String,
    pub tenant_id: String,
    /// Unique handle within the tenant (maps to repo namespace).
    pub handle: String,
    /// External ID from the calling platform (e.g., staticlab user ObjectId).
    pub external_id: String,
    pub email: String,
    pub created_at: DateTime<Utc>,
}

impl TenantUser {
    pub fn new(tenant_id: String, handle: String, external_id: String, email: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            handle,
            external_id,
            email,
            created_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_user_new_sets_defaults() {
        let user = TenantUser::new(
            "tenant-1".into(),
            "senol".into(),
            "ext-abc123".into(),
            "senol@example.com".into(),
        );
        assert!(!user.id.is_empty());
        assert_eq!(user.tenant_id, "tenant-1");
        assert_eq!(user.handle, "senol");
        assert_eq!(user.external_id, "ext-abc123");
        assert_eq!(user.email, "senol@example.com");
    }
}
