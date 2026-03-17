// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory git repository metadata store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::git::Repository;
use muli_core::traits::RepositoryStore;

#[derive(Debug, Clone)]
pub struct MemoryRepositoryStore {
    repos: Arc<DashMap<String, Repository>>,
}

impl MemoryRepositoryStore {
    pub fn new() -> Self {
        Self {
            repos: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryRepositoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RepositoryStore for MemoryRepositoryStore {
    async fn create_repository(&self, repo: &Repository) -> Result<String> {
        if self.repos.contains_key(&repo.id) {
            return Err(MuliError::Storage(format!(
                "Repository {} already exists",
                repo.id
            )));
        }
        let id = repo.id.clone();
        self.repos.insert(id.clone(), repo.clone());
        Ok(id)
    }

    async fn get_repository(&self, repo_id: &str) -> Result<Option<Repository>> {
        Ok(self.repos.get(repo_id).map(|e| e.value().clone()))
    }

    async fn get_repository_by_name(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Repository>> {
        Ok(self
            .repos
            .iter()
            .find(|e| {
                let r = e.value();
                r.tenant_id == tenant_id && r.namespace == namespace && r.name == name
            })
            .map(|e| e.value().clone()))
    }

    async fn list_repositories(&self, tenant_id: &str) -> Result<Vec<Repository>> {
        Ok(self
            .repos
            .iter()
            .filter(|e| e.value().tenant_id == tenant_id)
            .map(|e| e.value().clone())
            .collect())
    }

    async fn delete_repository(&self, repo_id: &str) -> Result<()> {
        self.repos
            .remove(repo_id)
            .ok_or_else(|| MuliError::Storage(format!("Repository {repo_id} not found")))?;
        Ok(())
    }

    async fn update_repository(&self, repo: &Repository) -> Result<()> {
        let mut entry = self
            .repos
            .get_mut(&repo.id)
            .ok_or_else(|| MuliError::Storage(format!("Repository {} not found", repo.id)))?;
        *entry.value_mut() = repo.clone();
        Ok(())
    }

    async fn list_forks(&self, parent_repo_id: &str) -> Result<Vec<Repository>> {
        Ok(self
            .repos
            .iter()
            .filter(|e| e.value().fork_of.as_deref() == Some(parent_repo_id))
            .map(|e| e.value().clone())
            .collect())
    }

    async fn transfer_repository(&self, repo_id: &str, new_namespace: &str) -> Result<()> {
        match self.repos.get_mut(repo_id) {
            Some(mut entry) => {
                entry.namespace = new_namespace.to_string();
                entry.updated_at = Utc::now();
                Ok(())
            }
            None => Err(MuliError::Storage(format!(
                "Repository {repo_id} not found"
            ))),
        }
    }

    async fn count_by_tenant(&self, tenant_id: &str) -> Result<u64> {
        Ok(self
            .repos
            .iter()
            .filter(|e| e.value().tenant_id == tenant_id)
            .count() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_repository_store_crud() {
        let store = MemoryRepositoryStore::new();
        let repo = Repository::new(
            "tenant-1".into(),
            "acme".into(),
            "my-repo".into(),
            "test".into(),
            false,
        )
        .unwrap();
        let id = store.create_repository(&repo).await.unwrap();
        assert_eq!(id, repo.id);
        let fetched = store.get_repository(&id).await.unwrap().unwrap();
        assert_eq!(fetched.name, "my-repo");
        let by_name = store
            .get_repository_by_name("tenant-1", "acme", "my-repo")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_name.id, id);
        let list = store.list_repositories("tenant-1").await.unwrap();
        assert_eq!(list.len(), 1);
        store.delete_repository(&id).await.unwrap();
        assert!(store.get_repository(&id).await.unwrap().is_none());
    }
}
