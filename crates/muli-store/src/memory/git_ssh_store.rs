// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory git SSH key store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::git::SshKey;
use muli_core::traits::SshKeyStore;

#[derive(Debug, Clone)]
pub struct MemorySshKeyStore {
    keys: Arc<DashMap<String, SshKey>>,
}

impl MemorySshKeyStore {
    pub fn new() -> Self {
        Self {
            keys: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemorySshKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SshKeyStore for MemorySshKeyStore {
    async fn add_key(&self, key: &SshKey) -> Result<String> {
        let fingerprint_exists = self
            .keys
            .iter()
            .any(|e| e.value().fingerprint == key.fingerprint);
        if fingerprint_exists {
            return Err(MuliError::Storage(format!(
                "SSH key with fingerprint {} already exists",
                key.fingerprint
            )));
        }
        let id = key.id.clone();
        self.keys.insert(id.clone(), key.clone());
        Ok(id)
    }

    async fn remove_key(&self, key_id: &str) -> Result<()> {
        self.keys
            .remove(key_id)
            .ok_or_else(|| MuliError::Storage(format!("SSH key {key_id} not found")))?;
        Ok(())
    }

    async fn find_by_fingerprint(&self, fingerprint: &str) -> Result<Option<SshKey>> {
        Ok(self
            .keys
            .iter()
            .find(|e| e.value().fingerprint == fingerprint)
            .map(|e| e.value().clone()))
    }

    async fn list_keys(&self, tenant_id: &str) -> Result<Vec<SshKey>> {
        Ok(self
            .keys
            .iter()
            .filter(|e| e.value().tenant_id == tenant_id)
            .map(|e| e.value().clone())
            .collect())
    }

    async fn list_keys_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<SshKey>> {
        Ok(self
            .keys
            .iter()
            .filter(|e| {
                let k = e.value();
                k.tenant_id == tenant_id && k.user_id.as_deref() == Some(user_id)
            })
            .map(|e| e.value().clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ssh_key_store_crud() {
        let store = MemorySshKeyStore::new();
        let key = SshKey::new(
            "tenant-1".into(),
            "SHA256:abc123".into(),
            "ssh-ed25519 AAAA...".into(),
            "my key".into(),
            vec![],
        );
        let id = store.add_key(&key).await.unwrap();
        let fetched = store
            .find_by_fingerprint("SHA256:abc123")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.id, id);
        store.remove_key(&id).await.unwrap();
        assert!(
            store
                .find_by_fingerprint("SHA256:abc123")
                .await
                .unwrap()
                .is_none()
        );
    }
}
