// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory git authentication token store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::git::GitToken;
use muli_core::traits::GitTokenStore;

#[derive(Debug, Clone)]
pub struct MemoryGitTokenStore {
    tokens: Arc<DashMap<String, GitToken>>,
}

impl MemoryGitTokenStore {
    pub fn new() -> Self {
        Self {
            tokens: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryGitTokenStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GitTokenStore for MemoryGitTokenStore {
    async fn create_token(&self, token: &GitToken) -> Result<String> {
        let hash_exists = self
            .tokens
            .iter()
            .any(|e| e.value().token_prefix == token.token_prefix);
        if hash_exists {
            return Err(MuliError::Storage(
                "Token with duplicate hash already exists".to_string(),
            ));
        }
        let id = token.id.clone();
        self.tokens.insert(id.clone(), token.clone());
        Ok(id)
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<GitToken>> {
        Ok(self
            .tokens
            .iter()
            .find(|e| {
                let t = e.value();
                t.token_prefix == token_prefix && t.is_valid()
            })
            .map(|e| e.value().clone()))
    }

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<GitToken>> {
        Ok(self.tokens.get(token_id).map(|e| e.value().clone()))
    }

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<GitToken>> {
        Ok(self
            .tokens
            .iter()
            .filter(|e| e.value().tenant_id == tenant_id)
            .map(|e| {
                let mut t = e.value().clone();
                t.token_hash = String::new();
                t
            })
            .collect())
    }

    async fn list_tokens_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<GitToken>> {
        Ok(self
            .tokens
            .iter()
            .filter(|e| {
                let t = e.value();
                t.tenant_id == tenant_id && t.user_id.as_deref() == Some(user_id)
            })
            .map(|e| {
                let mut t = e.value().clone();
                t.token_hash = String::new();
                t
            })
            .collect())
    }

    async fn revoke_token(&self, token_id: &str) -> Result<()> {
        let mut entry = self
            .tokens
            .get_mut(token_id)
            .ok_or_else(|| MuliError::Storage(format!("Token {token_id} not found")))?;
        entry.value_mut().revoked = true;
        Ok(())
    }

    async fn delete_expired_tokens(&self) -> Result<u64> {
        let now = Utc::now();
        let to_remove: Vec<String> = self
            .tokens
            .iter()
            .filter(|e| e.value().expires_at.map(|exp| exp < now).unwrap_or(false))
            .map(|e| e.key().clone())
            .collect();
        let count = to_remove.len() as u64;
        for id in to_remove {
            self.tokens.remove(&id);
        }
        Ok(count)
    }

    async fn set_token_expiry(
        &self,
        token_id: &str,
        expires_at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        let mut entry = self
            .tokens
            .get_mut(token_id)
            .ok_or_else(|| MuliError::Storage(format!("Token {token_id} not found")))?;
        entry.value_mut().expires_at = Some(expires_at);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::git::GitPermission;

    #[tokio::test]
    async fn test_git_token_store_crud() {
        let store = MemoryGitTokenStore::new();
        let token = GitToken::new(
            "tenant-1".into(),
            "hash123".into(),
            "hash123".into(),
            vec![GitPermission::Pull],
            "test".into(),
            None,
        );
        let id = store.create_token(&token).await.unwrap();
        assert_eq!(id, token.id);
        let fetched = store.get_token_by_prefix("hash123").await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id, "tenant-1");
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
