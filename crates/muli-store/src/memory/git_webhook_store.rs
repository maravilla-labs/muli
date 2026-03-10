// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory git webhook configuration store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::git::Webhook;
use muli_core::traits::WebhookStore;

#[derive(Debug, Clone)]
pub struct MemoryWebhookStore {
    webhooks: Arc<DashMap<String, Webhook>>,
}

impl MemoryWebhookStore {
    pub fn new() -> Self {
        Self {
            webhooks: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryWebhookStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WebhookStore for MemoryWebhookStore {
    async fn create_webhook(&self, webhook: &Webhook) -> Result<String> {
        let id = webhook.id.clone();
        self.webhooks.insert(id.clone(), webhook.clone());
        Ok(id)
    }

    async fn get_webhook(&self, webhook_id: &str) -> Result<Option<Webhook>> {
        Ok(self.webhooks.get(webhook_id).map(|e| e.value().clone()))
    }

    async fn list_webhooks(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<Webhook>> {
        Ok(self
            .webhooks
            .iter()
            .filter(|e| {
                let w = e.value();
                w.tenant_id == tenant_id && w.repo_id == repo_id
            })
            .map(|e| e.value().clone())
            .collect())
    }

    async fn delete_webhook(&self, webhook_id: &str) -> Result<()> {
        self.webhooks
            .remove(webhook_id)
            .ok_or_else(|| MuliError::Storage(format!("Webhook {webhook_id} not found")))?;
        Ok(())
    }

    async fn update_webhook(&self, webhook: &Webhook) -> Result<()> {
        let mut entry = self
            .webhooks
            .get_mut(&webhook.id)
            .ok_or_else(|| MuliError::Storage(format!("Webhook {} not found", webhook.id)))?;
        *entry.value_mut() = webhook.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::git::WebhookEvent;

    #[tokio::test]
    async fn test_webhook_store_crud() {
        let store = MemoryWebhookStore::new();
        let hook = Webhook::new(
            "tenant-1".into(),
            "repo-123".into(),
            "https://example.com/hook".into(),
            "secret".into(),
            vec![WebhookEvent::Push],
        );
        let id = store.create_webhook(&hook).await.unwrap();
        let list = store.list_webhooks("tenant-1", "repo-123").await.unwrap();
        assert_eq!(list.len(), 1);
        store.delete_webhook(&id).await.unwrap();
        assert!(store.get_webhook(&id).await.unwrap().is_none());
    }
}
