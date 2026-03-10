// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory user account store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::traits::UserStore;
use muli_core::user::TenantUser;

#[derive(Debug, Clone)]
pub struct MemoryUserStore {
    users: Arc<DashMap<String, TenantUser>>,
}

impl MemoryUserStore {
    pub fn new() -> Self {
        Self {
            users: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryUserStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl UserStore for MemoryUserStore {
    async fn create_user(&self, user: &TenantUser) -> Result<String> {
        // Enforce unique handle within tenant
        let handle_taken = self.users.iter().any(|e| {
            let u = e.value();
            u.tenant_id == user.tenant_id && u.handle == user.handle
        });
        if handle_taken {
            return Err(MuliError::Storage(format!(
                "handle '{}' already exists in tenant '{}'",
                user.handle, user.tenant_id
            )));
        }
        let id = user.id.clone();
        self.users.insert(id.clone(), user.clone());
        Ok(id)
    }

    async fn get_user(&self, user_id: &str) -> Result<Option<TenantUser>> {
        Ok(self.users.get(user_id).map(|e| e.value().clone()))
    }

    async fn get_user_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<TenantUser>> {
        Ok(self
            .users
            .iter()
            .find(|e| {
                let u = e.value();
                u.tenant_id == tenant_id && u.handle == handle
            })
            .map(|e| e.value().clone()))
    }

    async fn get_user_by_external_id(
        &self,
        tenant_id: &str,
        external_id: &str,
    ) -> Result<Option<TenantUser>> {
        Ok(self
            .users
            .iter()
            .find(|e| {
                let u = e.value();
                u.tenant_id == tenant_id && u.external_id == external_id
            })
            .map(|e| e.value().clone()))
    }

    async fn delete_user(&self, user_id: &str) -> Result<()> {
        self.users
            .remove(user_id)
            .ok_or_else(|| MuliError::Storage(format!("user {user_id} not found")))?;
        Ok(())
    }

    async fn list_users(&self, tenant_id: &str) -> Result<Vec<TenantUser>> {
        Ok(self
            .users
            .iter()
            .filter(|e| e.value().tenant_id == tenant_id)
            .map(|e| e.value().clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_user_store_crud() {
        let store = MemoryUserStore::new();
        let user = TenantUser::new(
            "tenant-1".into(),
            "alice".into(),
            "ext-001".into(),
            "alice@example.com".into(),
        );

        let id = store.create_user(&user).await.unwrap();
        assert_eq!(id, user.id);

        let fetched = store.get_user(&id).await.unwrap().unwrap();
        assert_eq!(fetched.handle, "alice");

        let by_handle = store
            .get_user_by_handle("tenant-1", "alice")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_handle.id, id);

        let by_ext = store
            .get_user_by_external_id("tenant-1", "ext-001")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_ext.id, id);

        let list = store.list_users("tenant-1").await.unwrap();
        assert_eq!(list.len(), 1);

        store.delete_user(&id).await.unwrap();
        assert!(store.get_user(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_handle_rejected() {
        let store = MemoryUserStore::new();
        let user1 = TenantUser::new(
            "tenant-1".into(),
            "bob".into(),
            "ext-bob".into(),
            "bob@example.com".into(),
        );
        let user2 = TenantUser::new(
            "tenant-1".into(),
            "bob".into(),
            "ext-bob2".into(),
            "bob2@example.com".into(),
        );
        store.create_user(&user1).await.unwrap();
        assert!(store.create_user(&user2).await.is_err());
    }
}
