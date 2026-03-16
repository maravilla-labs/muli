// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Organization models: teams, membership, and roles.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Role of a member within an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrgRole {
    Owner,
    Admin,
    Member,
    Viewer,
}

/// An organization within a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub id: String,
    pub tenant_id: String,
    /// Unique handle within the tenant (maps to repo namespace for org-owned repos).
    pub handle: String,
    pub display_name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
}

impl Organization {
    pub fn new(
        tenant_id: String,
        handle: String,
        display_name: String,
        description: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            handle,
            display_name,
            description,
            created_at: Utc::now(),
        }
    }
}

/// A member of an organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgMember {
    pub id: String,
    pub org_id: String,
    pub user_id: String,
    pub role: OrgRole,
    pub created_at: DateTime<Utc>,
}

impl OrgMember {
    pub fn new(org_id: String, user_id: String, role: OrgRole) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            org_id,
            user_id,
            role,
            created_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn organization_new_sets_defaults() {
        let org = Organization::new(
            "tenant-1".into(),
            "acme".into(),
            "Acme Corp".into(),
            "A test org".into(),
        );
        assert!(!org.id.is_empty());
        assert_eq!(org.tenant_id, "tenant-1");
        assert_eq!(org.handle, "acme");
        assert_eq!(org.display_name, "Acme Corp");
    }

    #[test]
    fn org_member_new_sets_defaults() {
        let member = OrgMember::new("org-1".into(), "user-1".into(), OrgRole::Member);
        assert!(!member.id.is_empty());
        assert_eq!(member.org_id, "org-1");
        assert_eq!(member.user_id, "user-1");
        assert_eq!(member.role, OrgRole::Member);
    }
}
