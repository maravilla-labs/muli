// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite registry token store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use muli_core::error::{MuliError, Result};
use muli_core::registry::model::RegistryToken;
use muli_core::traits::RegistryTokenStore;

use super::factory::SqliteStoreFactory;
use super::util::{dt_to_ms, from_json as token_from_json, store_err};

pub struct SqliteRegistryTokenStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteRegistryTokenStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl RegistryTokenStore for SqliteRegistryTokenStore {
    async fn create_token(&self, token: &RegistryToken) -> Result<String> {
        let conn = self.factory.global_conn();
        let token = token.clone();
        let id = token.id.clone();
        conn.call(move |c| {
            let json = serde_json::to_string(&token)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            let expires_ms: Option<i64> = token.expires_at.map(dt_to_ms);
            c.execute(
                "INSERT INTO registry_tokens (id, tenant_id, token_hash, token_prefix, expires_at, revoked, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    token.id,
                    token.tenant_id,
                    token.token_hash,
                    token.token_prefix,
                    expires_ms,
                    if token.revoked { 1i64 } else { 0i64 },
                    json,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<RegistryToken>> {
        let conn = self.factory.global_conn();
        let token_prefix = token_prefix.to_string();
        let now_ms = dt_to_ms(Utc::now());
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM registry_tokens
                 WHERE token_prefix = ?1 AND revoked = 0
                   AND (expires_at IS NULL OR expires_at > ?2)",
            )?;
            let mut rows = stmt.query(rusqlite::params![token_prefix, now_ms])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(token_from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<RegistryToken>> {
        let conn = self.factory.global_conn();
        let token_id = token_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM registry_tokens WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![token_id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(token_from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<RegistryToken>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM registry_tokens WHERE tenant_id = ?1")?;
            let rows = stmt.query_map(rusqlite::params![tenant_id], |row| {
                let json: String = row.get(0)?;
                token_from_json(&json)
            })?;
            let mut tokens = rows.collect::<rusqlite::Result<Vec<RegistryToken>>>()?;
            // Strip token_hash for security
            for t in &mut tokens {
                t.token_hash = String::new();
            }
            Ok(tokens)
        })
        .await
        .map_err(store_err)
    }

    async fn revoke_token(&self, token_id: &str) -> Result<()> {
        let conn = self.factory.global_conn();
        let token_id = token_id.to_string();
        let token_id_owned = token_id.clone();
        let rows = conn
            .call(move |c| {
                // Read and update full_json too
                let existing: Option<String> = {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM registry_tokens WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![token_id])?;
                    rows.next()?
                        .map(|row| row.get::<_, String>(0))
                        .transpose()?
                };
                let Some(json) = existing else {
                    return Ok(0usize);
                };
                let mut token: RegistryToken = serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                token.revoked = true;
                let new_json = serde_json::to_string(&token)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let rows = c.execute(
                    "UPDATE registry_tokens SET revoked = 1, full_json = ?1 WHERE id = ?2",
                    rusqlite::params![new_json, token.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "Token {token_id_owned} not found"
            )));
        }
        Ok(())
    }

    async fn delete_expired_tokens(&self) -> Result<u64> {
        let conn = self.factory.global_conn();
        let now_ms = dt_to_ms(Utc::now());
        conn.call(move |c| {
            let rows = c.execute(
                "DELETE FROM registry_tokens WHERE expires_at IS NOT NULL AND expires_at < ?1",
                rusqlite::params![now_ms],
            )?;
            Ok(rows as u64)
        })
        .await
        .map_err(store_err)
    }

    async fn set_token_expiry(&self, token_id: &str, expires_at: DateTime<Utc>) -> Result<()> {
        let conn = self.factory.global_conn();
        let token_id = token_id.to_string();
        let token_id_owned = token_id.clone();
        let expires_ms = dt_to_ms(expires_at);
        let rows = conn
            .call(move |c| {
                let existing: Option<String> = {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM registry_tokens WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![token_id])?;
                    rows.next()?
                        .map(|row| row.get::<_, String>(0))
                        .transpose()?
                };
                let Some(json) = existing else {
                    return Ok(0usize);
                };
                let mut token: RegistryToken = serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                token.expires_at = Some(expires_at);
                let new_json = serde_json::to_string(&token)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let rows = c.execute(
                    "UPDATE registry_tokens SET expires_at = ?1, full_json = ?2 WHERE id = ?3",
                    rusqlite::params![expires_ms, new_json, token.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "Token {token_id_owned} not found"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::registry::model::RegistryPermission;

    async fn make_store() -> (SqliteRegistryTokenStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteRegistryTokenStore::new(factory), dir)
    }

    fn make_token(tenant_id: &str, hash: &str) -> RegistryToken {
        RegistryToken::new(
            tenant_id.into(),
            hash.into(),
            hash.into(),
            vec![RegistryPermission::Pull],
            "test".into(),
            None,
        )
    }

    #[tokio::test]
    async fn test_create_and_get_by_hash() {
        let (store, _dir) = make_store().await;
        let token = make_token("t1", "hash1");
        store.create_token(&token).await.unwrap();
        let fetched = store.get_token_by_prefix("hash1").await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id, "t1");
    }

    #[tokio::test]
    async fn test_revoke() {
        let (store, _dir) = make_store().await;
        let token = make_token("t1", "hash1");
        let id = token.id.clone();
        store.create_token(&token).await.unwrap();
        store.revoke_token(&id).await.unwrap();
        assert!(store.get_token_by_prefix("hash1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_excludes_hash() {
        let (store, _dir) = make_store().await;
        store.create_token(&make_token("t1", "h1")).await.unwrap();
        store.create_token(&make_token("t1", "h2")).await.unwrap();
        let tokens = store.list_tokens("t1").await.unwrap();
        assert_eq!(tokens.len(), 2);
        for t in &tokens {
            assert!(t.token_hash.is_empty());
        }
    }

    #[tokio::test]
    async fn test_expired_not_returned() {
        let (store, _dir) = make_store().await;
        let mut token = make_token("t1", "h1");
        token.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        store.create_token(&token).await.unwrap();
        assert!(store.get_token_by_prefix("h1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_expired() {
        let (store, _dir) = make_store().await;
        let mut expired = make_token("t1", "h1");
        expired.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        store.create_token(&expired).await.unwrap();
        store.create_token(&make_token("t1", "h2")).await.unwrap();
        let deleted = store.delete_expired_tokens().await.unwrap();
        assert_eq!(deleted, 1);
    }
}
