// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed git webhook configuration store.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::git::Webhook;
use muli_core::traits::WebhookStore;

#[derive(Clone)]
pub struct MongoWebhookStore {
    collection: Collection<Webhook>,
}

impl MongoWebhookStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection("git_webhooks"),
        }
    }
}

#[async_trait]
impl WebhookStore for MongoWebhookStore {
    async fn create_webhook(&self, webhook: &Webhook) -> Result<String> {
        self.collection
            .insert_one(webhook)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create webhook: {e}")))?;
        Ok(webhook.id.clone())
    }

    async fn get_webhook(&self, webhook_id: &str) -> Result<Option<Webhook>> {
        self.collection
            .find_one(doc! { "id": webhook_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get webhook: {e}")))
    }

    async fn list_webhooks(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<Webhook>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id, "repo_id": repo_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list webhooks: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect webhooks: {e}")))
    }

    async fn delete_webhook(&self, webhook_id: &str) -> Result<()> {
        let result = self
            .collection
            .delete_one(doc! { "id": webhook_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete webhook: {e}")))?;
        if result.deleted_count == 0 {
            return Err(MuliError::Storage(format!(
                "Webhook {webhook_id} not found"
            )));
        }
        Ok(())
    }

    async fn update_webhook(&self, webhook: &Webhook) -> Result<()> {
        let update_doc = mongodb::bson::to_document(webhook)
            .map_err(|e| MuliError::Storage(format!("Serialization error: {e}")))?;
        let result = self
            .collection
            .update_one(doc! { "id": &webhook.id }, doc! { "$set": update_doc })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update webhook: {e}")))?;
        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!(
                "Webhook {} not found",
                webhook.id
            )));
        }
        Ok(())
    }
}
