// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git authentication token store.
//!
//! All queries use the `global_git_tokens` table in `_global.db` for O(1) lookups.
//! Writes are dual-written to the per-tenant `git_tokens` table for backward compatibility.

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
        let id = token.id.clone();
        let json = serde_json::to_string(token)
            .map_err(|e| MuliError::Storage(format!("serialize git token: {e}")))?;

        // Write to global table (primary).
        let global = self.factory.global_conn();
        let token_g = token.clone();
        let json_g = json.clone();
        global
            .call(move |c| {
                let expires_ms: Option<i64> = token_g.expires_at.map(dt_to_ms);
                c.execute(
                    "INSERT INTO global_git_tokens (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        token_g.id, token_g.tenant_id, token_g.user_id, token_g.token_hash,
                        token_g.token_prefix, expires_ms, if token_g.revoked { 1i64 } else { 0i64 }, json_g,
                    ],
                )?;
                Ok(())
            })
            .await
            .map_err(store_err)?;

        // Dual-write to per-tenant table for backward compatibility.
        let conn = self.factory.tenant_conn(&token.tenant_id).await?;
        let token_t = token.clone();
        let json_t = json;
        conn.call(move |c| {
            let expires_ms: Option<i64> = token_t.expires_at.map(dt_to_ms);
            c.execute(
                "INSERT OR IGNORE INTO git_tokens (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    token_t.id, token_t.tenant_id, token_t.user_id, token_t.token_hash,
                    token_t.token_prefix, expires_ms, if token_t.revoked { 1i64 } else { 0i64 }, json_t,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;

        Ok(id)
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<GitToken>> {
        let conn = self.factory.global_conn();
        let prefix = token_prefix.to_string();
        let now_ms = dt_to_ms(Utc::now());
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM global_git_tokens
                 WHERE token_prefix = ?1 AND revoked = 0
                   AND (expires_at IS NULL OR expires_at > ?2)",
            )?;
            let mut rows = stmt.query(rusqlite::params![prefix, now_ms])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<GitToken>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<GitToken>> {
        let conn = self.factory.global_conn();
        let id = token_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM global_git_tokens WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<GitToken>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<GitToken>> {
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM global_git_tokens WHERE tenant_id = ?1")?;
            let rows = stmt.query_map(rusqlite::params![tenant_id], |row| {
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
        let conn = self.factory.global_conn();
        let tenant_id = tenant_id.to_string();
        let user_id = user_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT full_json FROM global_git_tokens WHERE tenant_id = ?1 AND user_id = ?2",
            )?;
            let rows = stmt.query_map(rusqlite::params![tenant_id, user_id], |row| {
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
        let conn = self.factory.global_conn();
        let token_id = token_id.to_string();
        let token_id_owned = token_id.clone();
        let rows = conn
            .call(move |c| {
                let existing: Option<String> = {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM global_git_tokens WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![token_id])?;
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
                    "UPDATE global_git_tokens SET revoked = 1, full_json = ?1 WHERE id = ?2",
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
                "DELETE FROM global_git_tokens WHERE expires_at IS NOT NULL AND expires_at < ?1",
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
                        c.prepare("SELECT full_json FROM global_git_tokens WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![token_id])?;
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
                    "UPDATE global_git_tokens SET expires_at = ?1, full_json = ?2 WHERE id = ?3",
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

    #[tokio::test]
    async fn test_git_token_list_by_tenant() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteGitTokenStore::new(factory);
        let t1 = GitToken::new(
            "t1".into(),
            "h1".into(),
            "h1".into(),
            vec![GitPermission::Pull],
            "test".into(),
            None,
        );
        let t2 = GitToken::new(
            "t1".into(),
            "h2".into(),
            "h2".into(),
            vec![GitPermission::Pull],
            "test".into(),
            None,
        );
        let t3 = GitToken::new(
            "t2".into(),
            "h3".into(),
            "h3".into(),
            vec![GitPermission::Pull],
            "test".into(),
            None,
        );
        store.create_token(&t1).await.unwrap();
        store.create_token(&t2).await.unwrap();
        store.create_token(&t3).await.unwrap();
        let tokens = store.list_tokens("t1").await.unwrap();
        assert_eq!(tokens.len(), 2);
        for t in &tokens {
            assert!(t.token_hash.is_empty());
        }
    }

    #[tokio::test]
    async fn test_git_token_backfill_on_restart() {
        let dir = tempfile::tempdir().unwrap();

        // Create a factory and add a token via per-tenant path.
        {
            let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
            let store = SqliteGitTokenStore::new(factory);
            let token = GitToken::new(
                "tenant-bf".into(),
                "bf-hash".into(),
                "bf-hash".into(),
                vec![GitPermission::Pull],
                "backfill test".into(),
                None,
            );
            store.create_token(&token).await.unwrap();
        }

        // Create a new factory (simulates restart). Backfill should copy to global.
        {
            let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
            let store = SqliteGitTokenStore::new(factory);
            let fetched = store.get_token_by_prefix("bf-hash").await.unwrap();
            assert!(fetched.is_some());
            assert_eq!(fetched.unwrap().tenant_id, "tenant-bf");
        }
    }
}
