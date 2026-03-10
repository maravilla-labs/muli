// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory organization and membership store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::org::{OrgMember, OrgRole, Organization};
use muli_core::traits::{OrgMemberStore, OrgStore};

#[derive(Debug, Clone)]
pub struct MemoryOrgStore {
    orgs: Arc<DashMap<String, Organization>>,
}

impl MemoryOrgStore {
    pub fn new() -> Self {
        Self {
            orgs: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryOrgStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OrgStore for MemoryOrgStore {
    async fn create_org(&self, org: &Organization) -> Result<String> {
        // Enforce unique handle within tenant
        let handle_taken = self.orgs.iter().any(|e| {
            let o = e.value();
            o.tenant_id == org.tenant_id && o.handle == org.handle
        });
        if handle_taken {
            return Err(MuliError::Storage(format!(
                "handle '{}' already exists in tenant '{}'",
                org.handle, org.tenant_id
            )));
        }
        let id = org.id.clone();
        self.orgs.insert(id.clone(), org.clone());
        Ok(id)
    }

    async fn get_org(&self, org_id: &str) -> Result<Option<Organization>> {
        Ok(self.orgs.get(org_id).map(|e| e.value().clone()))
    }

    async fn get_org_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<Organization>> {
        Ok(self
            .orgs
            .iter()
            .find(|e| {
                let o = e.value();
                o.tenant_id == tenant_id && o.handle == handle
            })
            .map(|e| e.value().clone()))
    }

    async fn delete_org(&self, org_id: &str) -> Result<()> {
        self.orgs
            .remove(org_id)
            .ok_or_else(|| MuliError::Storage(format!("org {org_id} not found")))?;
        Ok(())
    }

    async fn list_orgs(&self, tenant_id: &str) -> Result<Vec<Organization>> {
        Ok(self
            .orgs
            .iter()
            .filter(|e| e.value().tenant_id == tenant_id)
            .map(|e| e.value().clone())
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct MemoryOrgMemberStore {
    members: Arc<DashMap<String, OrgMember>>,
}

impl MemoryOrgMemberStore {
    pub fn new() -> Self {
        Self {
            members: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryOrgMemberStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OrgMemberStore for MemoryOrgMemberStore {
    async fn add_member(&self, member: &OrgMember) -> Result<String> {
        // Enforce unique (org_id, user_id)
        let already_member = self.members.iter().any(|e| {
            let m = e.value();
            m.org_id == member.org_id && m.user_id == member.user_id
        });
        if already_member {
            return Err(MuliError::Storage(format!(
                "user '{}' is already a member of org '{}'",
                member.user_id, member.org_id
            )));
        }
        let id = member.id.clone();
        self.members.insert(id.clone(), member.clone());
        Ok(id)
    }

    async fn remove_member(&self, org_id: &str, user_id: &str) -> Result<()> {
        let key = self
            .members
            .iter()
            .find(|e| {
                let m = e.value();
                m.org_id == org_id && m.user_id == user_id
            })
            .map(|e| e.key().clone());

        match key {
            Some(k) => {
                self.members.remove(&k);
                Ok(())
            }
            None => Err(MuliError::Storage(format!(
                "member user_id={user_id} not found in org {org_id}"
            ))),
        }
    }

    async fn list_members(&self, org_id: &str) -> Result<Vec<OrgMember>> {
        Ok(self
            .members
            .iter()
            .filter(|e| e.value().org_id == org_id)
            .map(|e| e.value().clone())
            .collect())
    }

    async fn get_member(&self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>> {
        Ok(self
            .members
            .iter()
            .find(|e| {
                let m = e.value();
                m.org_id == org_id && m.user_id == user_id
            })
            .map(|e| e.value().clone()))
    }

    async fn update_member_role(&self, org_id: &str, user_id: &str, role: OrgRole) -> Result<()> {
        let key = self
            .members
            .iter()
            .find(|e| {
                let m = e.value();
                m.org_id == org_id && m.user_id == user_id
            })
            .map(|e| e.key().clone());

        match key {
            Some(k) => {
                if let Some(mut entry) = self.members.get_mut(&k) {
                    entry.role = role;
                }
                Ok(())
            }
            None => Err(MuliError::Storage(format!(
                "member user_id={user_id} not found in org {org_id}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_org_store_crud() {
        let store = MemoryOrgStore::new();
        let org = Organization::new(
            "tenant-1".into(),
            "acme".into(),
            "Acme Corp".into(),
            "Test org".into(),
        );

        let id = store.create_org(&org).await.unwrap();
        assert_eq!(id, org.id);

        let fetched = store.get_org(&id).await.unwrap().unwrap();
        assert_eq!(fetched.handle, "acme");

        let by_handle = store
            .get_org_by_handle("tenant-1", "acme")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_handle.id, id);

        let list = store.list_orgs("tenant-1").await.unwrap();
        assert_eq!(list.len(), 1);

        store.delete_org(&id).await.unwrap();
        assert!(store.get_org(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_org_handle_rejected() {
        let store = MemoryOrgStore::new();
        let org1 = Organization::new("t1".into(), "acme".into(), "Acme".into(), "".into());
        let org2 = Organization::new("t1".into(), "acme".into(), "Acme2".into(), "".into());
        store.create_org(&org1).await.unwrap();
        assert!(store.create_org(&org2).await.is_err());
    }

    #[tokio::test]
    async fn test_org_member_store_crud() {
        let store = MemoryOrgMemberStore::new();
        let member = OrgMember::new("org-1".into(), "user-1".into(), OrgRole::Member);

        store.add_member(&member).await.unwrap();

        let list = store.list_members("org-1").await.unwrap();
        assert_eq!(list.len(), 1);

        let fetched = store.get_member("org-1", "user-1").await.unwrap().unwrap();
        assert_eq!(fetched.role, OrgRole::Member);

        store
            .update_member_role("org-1", "user-1", OrgRole::Admin)
            .await
            .unwrap();
        let updated = store.get_member("org-1", "user-1").await.unwrap().unwrap();
        assert_eq!(updated.role, OrgRole::Admin);

        store.remove_member("org-1", "user-1").await.unwrap();
        assert!(store.get_member("org-1", "user-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_member_rejected() {
        let store = MemoryOrgMemberStore::new();
        let m1 = OrgMember::new("org-1".into(), "user-1".into(), OrgRole::Member);
        let m2 = OrgMember::new("org-1".into(), "user-1".into(), OrgRole::Admin);
        store.add_member(&m1).await.unwrap();
        assert!(store.add_member(&m2).await.is_err());
    }
}
