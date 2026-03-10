// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed git SSH key store.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::git::SshKey;
use muli_core::traits::SshKeyStore;

#[derive(Clone)]
pub struct MongoSshKeyStore {
    collection: Collection<SshKey>,
}

impl MongoSshKeyStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection("git_ssh_keys"),
        }
    }
}

#[async_trait]
impl SshKeyStore for MongoSshKeyStore {
    async fn add_key(&self, key: &SshKey) -> Result<String> {
        self.collection
            .insert_one(key)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to add SSH key: {e}")))?;
        Ok(key.id.clone())
    }

    async fn remove_key(&self, key_id: &str) -> Result<()> {
        let result = self
            .collection
            .delete_one(doc! { "id": key_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to remove SSH key: {e}")))?;
        if result.deleted_count == 0 {
            return Err(MuliError::Storage(format!("SSH key {key_id} not found")));
        }
        Ok(())
    }

    async fn find_by_fingerprint(&self, fingerprint: &str) -> Result<Option<SshKey>> {
        self.collection
            .find_one(doc! { "fingerprint": fingerprint })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to find SSH key: {e}")))
    }

    async fn list_keys(&self, tenant_id: &str) -> Result<Vec<SshKey>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list SSH keys: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect SSH keys: {e}")))
    }

    async fn list_keys_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<SshKey>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id, "user_id": user_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list SSH keys by user: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect SSH keys by user: {e}")))
    }
}
