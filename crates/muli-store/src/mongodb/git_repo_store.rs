// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed git repository metadata store.

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::git::Repository;
use muli_core::traits::RepositoryStore;

use super::util::chrono_to_bson;

#[derive(Clone)]
pub struct MongoRepositoryStore {
    collection: Collection<Repository>,
    db: Database,
}

impl MongoRepositoryStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection("git_repositories"),
            db: db.clone(),
        }
    }
}

#[async_trait]
impl RepositoryStore for MongoRepositoryStore {
    async fn create_repository(&self, repo: &Repository) -> Result<String> {
        self.collection
            .insert_one(repo)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create repository: {e}")))?;
        Ok(repo.id.clone())
    }

    async fn get_repository(&self, repo_id: &str) -> Result<Option<Repository>> {
        self.collection
            .find_one(doc! { "id": repo_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get repository: {e}")))
    }

    async fn get_repository_by_name(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Repository>> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id, "namespace": namespace, "name": name })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get repository: {e}")))
    }

    async fn list_repositories(&self, tenant_id: &str) -> Result<Vec<Repository>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list repositories: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect repositories: {e}")))
    }

    async fn delete_repository(&self, repo_id: &str) -> Result<()> {
        // Cascade: remove all dependent records before deleting the repository
        // 1. Delete PR comments for all PRs in this repo
        let pr_ids: Vec<String> = {
            let cursor = self
                .db
                .collection::<mongodb::bson::Document>("pull_requests")
                .find(doc! { "repo_id": repo_id })
                .projection(doc! { "id": 1 })
                .await
                .map_err(|e| MuliError::Storage(format!("Failed to find PRs for cascade: {e}")))?;
            cursor
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| MuliError::Storage(format!("Failed to collect PR ids: {e}")))?
                .into_iter()
                .filter_map(|d| d.get_str("id").ok().map(String::from))
                .collect()
        };
        if !pr_ids.is_empty() {
            self.db
                .collection::<mongodb::bson::Document>("pr_comments")
                .delete_many(doc! { "pr_id": { "$in": &pr_ids } })
                .await
                .map_err(|e| MuliError::Storage(format!("Failed to delete PR comments: {e}")))?;
        }
        // 2. Delete pull requests
        self.db
            .collection::<mongodb::bson::Document>("pull_requests")
            .delete_many(doc! { "repo_id": repo_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete PRs: {e}")))?;
        // 3. Delete webhooks
        self.db
            .collection::<mongodb::bson::Document>("git_webhooks")
            .delete_many(doc! { "repo_id": repo_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete webhooks: {e}")))?;
        // 4. Delete the repository itself
        let result = self
            .collection
            .delete_one(doc! { "id": repo_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete repository: {e}")))?;
        if result.deleted_count == 0 {
            return Err(MuliError::Storage(format!(
                "Repository {repo_id} not found"
            )));
        }
        Ok(())
    }

    async fn update_repository(&self, repo: &Repository) -> Result<()> {
        let update_doc = mongodb::bson::to_document(repo)
            .map_err(|e| MuliError::Storage(format!("Serialization error: {e}")))?;
        let result = self
            .collection
            .update_one(doc! { "id": &repo.id }, doc! { "$set": update_doc })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update repository: {e}")))?;
        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!(
                "Repository {} not found",
                repo.id
            )));
        }
        Ok(())
    }

    async fn list_forks(&self, parent_repo_id: &str) -> Result<Vec<Repository>> {
        let cursor = self
            .collection
            .find(doc! { "fork_of": parent_repo_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list forks: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect forks: {e}")))
    }

    async fn transfer_repository(&self, repo_id: &str, new_namespace: &str) -> Result<()> {
        let result = self
            .collection
            .update_one(
                doc! { "id": repo_id },
                doc! { "$set": { "namespace": new_namespace, "updated_at": chrono_to_bson(Utc::now()) } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to transfer repository: {e}")))?;
        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!(
                "Repository {repo_id} not found"
            )));
        }
        Ok(())
    }
}
