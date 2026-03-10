// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory registry authentication token store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::registry::model::RegistryToken;
use muli_core::traits::RegistryTokenStore;

/// In-memory implementation of RegistryTokenStore for testing.
#[derive(Debug, Clone)]
pub struct MemoryRegistryTokenStore {
    /// Keyed by token id.
    tokens: Arc<DashMap<String, RegistryToken>>,
}

impl MemoryRegistryTokenStore {
    pub fn new() -> Self {
        Self {
            tokens: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryRegistryTokenStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RegistryTokenStore for MemoryRegistryTokenStore {
    async fn create_token(&self, token: &RegistryToken) -> Result<String> {
        // Check for duplicate token_prefix
        let hash_exists = self
            .tokens
            .iter()
            .any(|entry| entry.value().token_prefix == token.token_prefix);
        if hash_exists {
            return Err(MuliError::Storage(
                "Token with duplicate hash already exists".to_string(),
            ));
        }

        let id = token.id.clone();
        self.tokens.insert(id.clone(), token.clone());
        Ok(id)
    }

    async fn get_token_by_prefix(&self, token_prefix: &str) -> Result<Option<RegistryToken>> {
        Ok(self
            .tokens
            .iter()
            .find(|entry| {
                let t = entry.value();
                t.token_prefix == token_prefix && !t.revoked && t.is_valid()
            })
            .map(|entry| entry.value().clone()))
    }

    async fn get_token_by_id(&self, token_id: &str) -> Result<Option<RegistryToken>> {
        Ok(self.tokens.get(token_id).map(|e| e.value().clone()))
    }

    async fn list_tokens(&self, tenant_id: &str) -> Result<Vec<RegistryToken>> {
        Ok(self
            .tokens
            .iter()
            .filter(|entry| entry.value().tenant_id == tenant_id)
            .map(|entry| {
                let mut t = entry.value().clone();
                // Exclude the hash from listing results for security
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
            .filter(|entry| {
                entry
                    .value()
                    .expires_at
                    .map(|exp| exp < now)
                    .unwrap_or(false)
            })
            .map(|entry| entry.key().clone())
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
    use chrono::Duration;
    use muli_core::registry::model::RegistryPermission;

    fn make_token(tenant_id: &str, hash: &str) -> RegistryToken {
        RegistryToken::new(
            tenant_id.to_string(),
            hash.to_string(),
            hash.to_string(),
            vec![RegistryPermission::Pull, RegistryPermission::Push],
            "test token".to_string(),
            None,
        )
    }

    #[tokio::test]
    async fn test_create_and_get_by_hash() {
        let store = MemoryRegistryTokenStore::new();
        let token = make_token("tenant-1", "hash123");
        let id = store.create_token(&token).await.unwrap();
        assert_eq!(id, token.id);

        let fetched = store.get_token_by_prefix("hash123").await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id, "tenant-1");
    }

    #[tokio::test]
    async fn test_duplicate_hash_fails() {
        let store = MemoryRegistryTokenStore::new();
        let t1 = make_token("tenant-1", "same-hash");
        let t2 = make_token("tenant-2", "same-hash");
        store.create_token(&t1).await.unwrap();
        assert!(store.create_token(&t2).await.is_err());
    }

    #[tokio::test]
    async fn test_get_revoked_returns_none() {
        let store = MemoryRegistryTokenStore::new();
        let token = make_token("tenant-1", "hash123");
        let id = token.id.clone();
        store.create_token(&token).await.unwrap();
        store.revoke_token(&id).await.unwrap();

        let fetched = store.get_token_by_prefix("hash123").await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_get_expired_returns_none() {
        let store = MemoryRegistryTokenStore::new();
        let mut token = make_token("tenant-1", "hash123");
        token.expires_at = Some(Utc::now() - Duration::hours(1));
        store.create_token(&token).await.unwrap();

        let fetched = store.get_token_by_prefix("hash123").await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_list_tokens_excludes_hash() {
        let store = MemoryRegistryTokenStore::new();
        store
            .create_token(&make_token("tenant-1", "hash1"))
            .await
            .unwrap();
        store
            .create_token(&make_token("tenant-1", "hash2"))
            .await
            .unwrap();
        store
            .create_token(&make_token("tenant-2", "hash3"))
            .await
            .unwrap();

        let tokens = store.list_tokens("tenant-1").await.unwrap();
        assert_eq!(tokens.len(), 2);
        for t in &tokens {
            assert!(t.token_hash.is_empty());
        }
    }

    #[tokio::test]
    async fn test_revoke_nonexistent_fails() {
        let store = MemoryRegistryTokenStore::new();
        assert!(store.revoke_token("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_expired_tokens() {
        let store = MemoryRegistryTokenStore::new();

        let mut expired = make_token("tenant-1", "hash1");
        expired.expires_at = Some(Utc::now() - Duration::hours(1));
        store.create_token(&expired).await.unwrap();

        let valid = make_token("tenant-1", "hash2");
        store.create_token(&valid).await.unwrap();

        let deleted = store.delete_expired_tokens().await.unwrap();
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_get_nonexistent_hash() {
        let store = MemoryRegistryTokenStore::new();
        let result = store.get_token_by_prefix("nonexistent").await.unwrap();
        assert!(result.is_none());
    }
}
