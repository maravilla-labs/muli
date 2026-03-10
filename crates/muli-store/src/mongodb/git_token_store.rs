// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed git authentication token store.

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::git::GitToken;
use muli_core::traits::GitTokenStore;

use super::util::chrono_to_bson;

#[derive(Clone)]
pub struct MongoGitTokenStore {
    collection: Collection<GitToken>,
}

impl MongoGitTokenStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection("git_tokens"),
        }
    }
}

#[async_trait]
impl GitTokenStore for MongoGitTokenStore {
    async fn create_token(&self, token: &GitToken) -> Result<String> {
        self.collection
            .insert_one(token)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create token: {e}")))?;
        Ok(token.id.clone())
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<GitToken>> {
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
            .map_err(|e| MuliError::Storage(format!("Failed to get token: {e}")))
    }

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<GitToken>> {
        self.collection
            .find_one(doc! { "id": token_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get token by id: {e}")))
    }

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<GitToken>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list tokens: {e}")))?;
        let mut tokens: Vec<GitToken> = cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect tokens: {e}")))?;
        for t in &mut tokens {
            t.token_hash = String::new();
        }
        Ok(tokens)
    }

    async fn list_tokens_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<GitToken>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id, "user_id": user_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list tokens by user: {e}")))?;
        let mut tokens: Vec<GitToken> = cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect tokens by user: {e}")))?;
        for t in &mut tokens {
            t.token_hash = String::new();
        }
        Ok(tokens)
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
            return Err(MuliError::Storage(format!("Token {token_id} not found")));
        }
        Ok(())
    }

    async fn delete_expired_tokens(&self) -> Result<u64> {
        let now = chrono_to_bson(Utc::now());
        let result = self
            .collection
            .delete_many(doc! { "expires_at": { "$lt": now } })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete expired tokens: {e}")))?;
        Ok(result.deleted_count)
    }

    async fn set_token_expiry(
        &self,
        token_id: &str,
        expires_at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        let bson_expires = chrono_to_bson(expires_at);
        let result = self
            .collection
            .update_one(
                doc! { "id": token_id },
                doc! { "$set": { "expires_at": bson_expires } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to set token expiry: {e}")))?;
        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!("Token {token_id} not found")));
        }
        Ok(())
    }
}
