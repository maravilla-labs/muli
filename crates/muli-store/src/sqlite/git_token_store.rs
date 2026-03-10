// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git authentication token store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use muli_core::error::{MuliError, Result};
use muli_core::git::GitToken;
use muli_core::traits::GitTokenStore;

use super::factory::SqliteStoreFactory;
use super::util::{dt_to_ms, from_json, store_err, to_json};

pub struct SqliteGitTokenStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteGitTokenStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl GitTokenStore for SqliteGitTokenStore {
    async fn create_token(&self, token: &GitToken) -> Result<String> {
        let conn = self.factory.tenant_conn(&token.tenant_id).await?;
        let token = token.clone();
        let id = token.id.clone();
        conn.call(move |c| {
            let json = to_json(&token)?;
            let expires_ms: Option<i64> = token.expires_at.map(dt_to_ms);
            c.execute(
                "INSERT INTO git_tokens (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    token.id, token.tenant_id, token.user_id, token.token_hash,
                    token.token_prefix, expires_ms, if token.revoked { 1i64 } else { 0i64 }, json,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<GitToken>> {
        let prefix = token_prefix.to_string();
        let now_ms = dt_to_ms(Utc::now());
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let p = prefix.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare(
                        "SELECT full_json FROM git_tokens
                     WHERE token_prefix = ?1 AND revoked = 0
                       AND (expires_at IS NULL OR expires_at > ?2)",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![p, now_ms])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<GitToken>(&json)?))
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

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<GitToken>> {
        let tid = token_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let id = tid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare("SELECT full_json FROM git_tokens WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![id])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<GitToken>(&json)?))
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

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<GitToken>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM git_tokens")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                from_json::<GitToken>(&json)
            })?;
            let mut tokens = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            for t in &mut tokens {
                t.token_hash = String::new();
            }
            Ok(tokens)
        })
        .await
        .map_err(store_err)
    }

    async fn list_tokens_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<GitToken>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let user_id = user_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM git_tokens WHERE user_id = ?1")?;
            let rows = stmt.query_map(rusqlite::params![user_id], |row| {
                let json: String = row.get(0)?;
                from_json::<GitToken>(&json)
            })?;
            let mut tokens = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            for t in &mut tokens {
                t.token_hash = String::new();
            }
            Ok(tokens)
        })
        .await
        .map_err(store_err)
    }

    async fn revoke_token(&self, token_id: &str) -> Result<()> {
        let token_id = token_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let tid = token_id.clone();
            let rows = conn
                .call(move |c| {
                    let existing: Option<String> = {
                        let mut stmt =
                            c.prepare("SELECT full_json FROM git_tokens WHERE id = ?1")?;
                        let mut rows = stmt.query(rusqlite::params![tid])?;
                        rows.next()?
                            .map(|row| row.get::<_, String>(0))
                            .transpose()?
                    };
                    let Some(json) = existing else {
                        return Ok(0usize);
                    };
                    let mut token: GitToken = from_json(&json)?;
                    token.revoked = true;
                    let new_json = to_json(&token)?;
                    let rows = c.execute(
                        "UPDATE git_tokens SET revoked = 1, full_json = ?1 WHERE id = ?2",
                        rusqlite::params![new_json, token.id],
                    )?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!("Token {token_id} not found")))
    }

    async fn delete_expired_tokens(&self) -> Result<u64> {
        let now_ms = dt_to_ms(Utc::now());
        let mut total = 0u64;
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let count = conn
                .call(move |c| {
                    let rows = c.execute(
                        "DELETE FROM git_tokens WHERE expires_at IS NOT NULL AND expires_at < ?1",
                        rusqlite::params![now_ms],
                    )?;
                    Ok(rows as u64)
                })
                .await
                .map_err(store_err)?;
            total += count;
        }
        Ok(total)
    }

    async fn set_token_expiry(&self, token_id: &str, expires_at: DateTime<Utc>) -> Result<()> {
        let token_id = token_id.to_string();
        let expires_ms = dt_to_ms(expires_at);
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let tid = token_id.clone();
            let rows = conn
                .call(move |c| {
                    let existing: Option<String> = {
                        let mut stmt =
                            c.prepare("SELECT full_json FROM git_tokens WHERE id = ?1")?;
                        let mut rows = stmt.query(rusqlite::params![tid])?;
                        rows.next()?
                            .map(|row| row.get::<_, String>(0))
                            .transpose()?
                    };
                    let Some(json) = existing else {
                        return Ok(0usize);
                    };
                    let mut token: GitToken = from_json(&json)?;
                    token.expires_at = Some(expires_at);
                    let new_json = to_json(&token)?;
                    let rows = c.execute(
                        "UPDATE git_tokens SET expires_at = ?1, full_json = ?2 WHERE id = ?3",
                        rusqlite::params![expires_ms, new_json, token.id],
                    )?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!("Token {token_id} not found")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::git::GitPermission;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_git_token_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteGitTokenStore::new(factory);
        let token = GitToken::new(
            "t1".into(),
            "hash123".into(),
            "hash123".into(),
            vec![GitPermission::Pull],
            "test".into(),
            None,
        );
        let id = store.create_token(&token).await.unwrap();
        let fetched = store.get_token_by_prefix("hash123").await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id, "t1");
        store.revoke_token(&id).await.unwrap();
        assert!(
            store
                .get_token_by_prefix("hash123")
                .await
                .unwrap()
                .is_none()
        );
    }
}
