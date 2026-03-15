// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory collaborator store for testing.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::git::RepositoryCollaborator;
use muli_core::traits::CollaboratorStore;

#[derive(Debug, Clone)]
pub struct MemoryCollaboratorStore {
    /// Key: "{repo_id}:{user_id}"
    collabs: Arc<DashMap<String, RepositoryCollaborator>>,
}

impl MemoryCollaboratorStore {
    pub fn new() -> Self {
        Self {
            collabs: Arc::new(DashMap::new()),
        }
    }

    fn key(repo_id: &str, user_id: &str) -> String {
        format!("{repo_id}:{user_id}")
    }
}

impl Default for MemoryCollaboratorStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CollaboratorStore for MemoryCollaboratorStore {
    async fn upsert_collaborator(&self, collaborator: &RepositoryCollaborator) -> Result<String> {
        let key = Self::key(&collaborator.repo_id, &collaborator.user_id);
        let id = collaborator.id.clone();
        self.collabs.insert(key, collaborator.clone());
        Ok(id)
    }

    async fn remove_collaborator(&self, repo_id: &str, user_id: &str) -> Result<()> {
        let key = Self::key(repo_id, user_id);
        self.collabs.remove(&key);
        Ok(())
    }

    async fn list_collaborators(&self, repo_id: &str) -> Result<Vec<RepositoryCollaborator>> {
        Ok(self
            .collabs
            .iter()
            .filter(|e| e.value().repo_id == repo_id)
            .map(|e| e.value().clone())
            .collect())
    }

    async fn get_collaborator(
        &self,
        repo_id: &str,
        user_id: &str,
    ) -> Result<Option<RepositoryCollaborator>> {
        let key = Self::key(repo_id, user_id);
        Ok(self.collabs.get(&key).map(|e| e.value().clone()))
    }
}
