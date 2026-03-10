// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed user account store.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::traits::UserStore;
use muli_core::user::TenantUser;

const USERS_COLLECTION: &str = "tenant_users";

#[derive(Clone)]
pub struct MongoUserStore {
    collection: Collection<TenantUser>,
}

impl MongoUserStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection(USERS_COLLECTION),
        }
    }
}

#[async_trait]
impl UserStore for MongoUserStore {
    async fn create_user(&self, user: &TenantUser) -> Result<String> {
        self.collection
            .insert_one(user)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create user: {e}")))?;
        Ok(user.id.clone())
    }

    async fn get_user(&self, user_id: &str) -> Result<Option<TenantUser>> {
        self.collection
            .find_one(doc! { "id": user_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get user: {e}")))
    }

    async fn get_user_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<TenantUser>> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id, "handle": handle })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get user by handle: {e}")))
    }

    async fn get_user_by_external_id(
        &self,
        tenant_id: &str,
        external_id: &str,
    ) -> Result<Option<TenantUser>> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id, "external_id": external_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get user by external_id: {e}")))
    }

    async fn delete_user(&self, user_id: &str) -> Result<()> {
        let result = self
            .collection
            .delete_one(doc! { "id": user_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete user: {e}")))?;
        if result.deleted_count == 0 {
            return Err(MuliError::Storage(format!("user {user_id} not found")));
        }
        Ok(())
    }

    async fn list_users(&self, tenant_id: &str) -> Result<Vec<TenantUser>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list users: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect users: {e}")))
    }
}
