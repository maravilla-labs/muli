// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git webhook configuration store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::git::Webhook;
use muli_core::traits::WebhookStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteWebhookStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteWebhookStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl WebhookStore for SqliteWebhookStore {
    async fn create_webhook(&self, webhook: &Webhook) -> Result<String> {
        let conn = self.factory.tenant_conn(&webhook.tenant_id).await?;
        let webhook = webhook.clone();
        let id = webhook.id.clone();
        conn.call(move |c| {
            let json = to_json(&webhook)?;
            c.execute(
                "INSERT INTO webhooks (id, tenant_id, repo_id, full_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![webhook.id, webhook.tenant_id, webhook.repo_id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_webhook(&self, webhook_id: &str) -> Result<Option<Webhook>> {
        let wid = webhook_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let w = wid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare("SELECT full_json FROM webhooks WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![w])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Webhook>(&json)?))
                    } else {
                        Ok(None)
                    }
                })
                .await
                .map_err(store_err)?;
            if result.is_some() {
                return Ok(result);
            }
        }
        Ok(None)
    }

    async fn list_webhooks(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<Webhook>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let repo_id = repo_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM webhooks WHERE repo_id = ?1")?;
            let rows = stmt.query_map(rusqlite::params![repo_id], |row| {
                let json: String = row.get(0)?;
                from_json::<Webhook>(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }

    async fn delete_webhook(&self, webhook_id: &str) -> Result<()> {
        let wid = webhook_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let w = wid.clone();
            let rows = conn
                .call(move |c| {
                    let rows =
                        c.execute("DELETE FROM webhooks WHERE id = ?1", rusqlite::params![w])?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!(
            "Webhook {webhook_id} not found"
        )))
    }

    async fn update_webhook(&self, webhook: &Webhook) -> Result<()> {
        let conn = self.factory.tenant_conn(&webhook.tenant_id).await?;
        let webhook = webhook.clone();
        let webhook_id_str = webhook.id.clone();
        let rows = conn
            .call(move |c| {
                let json = to_json(&webhook)?;
                let rows = c.execute(
                    "UPDATE webhooks SET repo_id = ?1, full_json = ?2 WHERE id = ?3",
                    rusqlite::params![webhook.repo_id, json, webhook.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "Webhook {webhook_id_str} not found"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::git::WebhookEvent;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_webhook_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteWebhookStore::new(factory);
        let hook = Webhook::new(
            "t1".into(),
            "repo-1".into(),
            "https://example.com".into(),
            "s".into(),
            vec![WebhookEvent::Push],
        );
        let id = store.create_webhook(&hook).await.unwrap();
        let fetched = store.get_webhook(&id).await.unwrap().unwrap();
        assert_eq!(fetched.repo_id, "repo-1");
        let list = store.list_webhooks("t1", "repo-1").await.unwrap();
        assert_eq!(list.len(), 1);
        store.delete_webhook(&id).await.unwrap();
        assert!(store.get_webhook(&id).await.unwrap().is_none());
    }
}
