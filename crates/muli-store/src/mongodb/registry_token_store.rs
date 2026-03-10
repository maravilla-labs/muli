// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed registry token store.

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::registry::model::RegistryToken;
use muli_core::traits::RegistryTokenStore;

use super::util::chrono_to_bson;

const COLLECTION_NAME: &str = "registry_tokens";

/// MongoDB implementation of RegistryTokenStore.
#[derive(Debug, Clone)]
pub struct MongoRegistryTokenStore {
    collection: Collection<RegistryToken>,
}

impl MongoRegistryTokenStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection::<RegistryToken>(COLLECTION_NAME),
        }
    }
}

#[async_trait]
impl RegistryTokenStore for MongoRegistryTokenStore {
    async fn create_token(&self, token: &RegistryToken) -> Result<String> {
        self.collection
            .insert_one(token)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create token: {e}")))?;
        Ok(token.id.clone())
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<RegistryToken>> {
        let now = chrono_to_bson(Utc::now());
        self.collection
            .find_one(doc! {
                "token_prefix": token_prefix,
                "revoked": false,
                "$or": [
                    { "expires_at": null },
                    { "expires_at": { "$gt": now } }
                ]
            })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get token by prefix: {e}")))
    }

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<RegistryToken>> {
        self.collection
            .find_one(doc! { "id": token_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get token by id: {e}")))
    }

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<RegistryToken>> {
        let opts = FindOptions::builder()
            .projection(doc! { "token_hash": 0, "token_prefix": 0 })
            .sort(doc! { "created_at": -1 })
            .build();

        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .with_options(opts)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list tokens: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect tokens: {e}")))
    }

    async fn revoke_token(&self, token_id: &str) -> Result<()> {
        let result = self
            .collection
            .update_one(
                doc! { "id": token_id },
                doc! { "$set": { "revoked": true } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to revoke token: {e}")))?;

        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!("Token not found: {token_id}")));
        }
        Ok(())
    }

    async fn delete_expired_tokens(&self) -> Result<u64> {
        let now = chrono_to_bson(Utc::now());
        let result = self
            .collection
            .delete_many(doc! {
                "expires_at": { "$ne": null, "$lt": now }
            })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete expired tokens: {e}")))?;

        Ok(result.deleted_count)
    }

    async fn set_token_expiry(
        &self,
        token_id: &str,
        expires_at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        let bson_expiry = chrono_to_bson(expires_at);
        let result = self
            .collection
            .update_one(
                doc! { "id": token_id },
                doc! { "$set": { "expires_at": bson_expiry } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to set token expiry: {e}")))?;

        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!("Token not found: {token_id}")));
        }
        Ok(())
    }
}
