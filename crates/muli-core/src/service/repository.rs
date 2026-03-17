// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository domain service: create, delete, fork, transfer.

use std::sync::Arc;

use tracing::{info, warn};

use crate::error::{MuliError, Result};
use crate::git::Repository;
use crate::traits::{GitStorage, RepositoryStore};

pub struct RepositoryService {
    repo_store: Arc<dyn RepositoryStore>,
    git_storage: Arc<dyn GitStorage>,
}

impl RepositoryService {
    pub fn new(repo_store: Arc<dyn RepositoryStore>, git_storage: Arc<dyn GitStorage>) -> Self {
        Self {
            repo_store,
            git_storage,
        }
    }

    /// Create a new repository with duplicate check and rollback.
    pub async fn create(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        description: &str,
        is_private: bool,
    ) -> Result<Repository> {
        // Check duplicate
        if self
            .repo_store
            .get_repository_by_name(tenant_id, namespace, name)
            .await?
            .is_some()
        {
            return Err(MuliError::Conflict("repository already exists".into()));
        }

        // Create model
        let repo = Repository::new(
            tenant_id.to_string(),
            namespace.to_string(),
            name.to_string(),
            description.to_string(),
            is_private,
        )?;

        // Persist DB record first
        self.repo_store.create_repository(&repo).await?;

        // Init git storage — rollback DB on failure
        if let Err(e) = self.git_storage.init_repo(tenant_id, namespace, name).await {
            let _ = self.repo_store.delete_repository(&repo.id).await;
            return Err(e);
        }

        info!(
            tenant_id = %tenant_id,
            namespace = %namespace,
            name = %name,
            "repository created"
        );

        Ok(repo)
    }

    /// Delete a repository (DB + filesystem best-effort).
    pub async fn delete(&self, tenant_id: &str, namespace: &str, name: &str) -> Result<()> {
        let repo = self
            .repo_store
            .get_repository_by_name(tenant_id, namespace, name)
            .await?
            .ok_or_else(|| MuliError::NotFound("repository not found".into()))?;

        self.repo_store.delete_repository(&repo.id).await?;

        // Best-effort filesystem cleanup
        if let Err(e) = self
            .git_storage
            .delete_repo(tenant_id, namespace, name)
            .await
        {
            warn!(error = %e, "failed to delete repository from filesystem");
        }

        info!(
            tenant_id = %tenant_id,
            namespace = %namespace,
            name = %name,
            "repository deleted"
        );

        Ok(())
    }

    /// Fork a repository.
    pub async fn fork(
        &self,
        tenant_id: &str,
        src_namespace: &str,
        src_name: &str,
        dst_namespace: &str,
        dst_name: &str,
    ) -> Result<Repository> {
        // Look up source
        let source = self
            .repo_store
            .get_repository_by_name(tenant_id, src_namespace, src_name)
            .await?
            .ok_or_else(|| MuliError::NotFound("source repository not found".into()))?;

        // Fork git storage
        self.git_storage
            .fork_repo(
                tenant_id,
                src_namespace,
                src_name,
                tenant_id,
                dst_namespace,
                dst_name,
            )
            .await?;

        // Create forked repo model
        let mut forked = Repository::new(
            tenant_id.to_string(),
            dst_namespace.to_string(),
            dst_name.to_string(),
            source.description.clone(),
            source.is_private,
        )?;
        forked.fork_of = Some(source.id.clone());

        // Persist — rollback git fork on failure
        if let Err(e) = self.repo_store.create_repository(&forked).await {
            let _ = self
                .git_storage
                .delete_repo(tenant_id, dst_namespace, dst_name)
                .await;
            return Err(e);
        }

        info!(
            tenant_id = %tenant_id,
            source = %format!("{}/{}", src_namespace, src_name),
            dest = %format!("{}/{}", dst_namespace, dst_name),
            "repository forked"
        );

        Ok(forked)
    }

    /// Transfer a repository to a new namespace.
    pub async fn transfer(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        new_namespace: &str,
    ) -> Result<Repository> {
        // Look up repo
        let repo = self
            .repo_store
            .get_repository_by_name(tenant_id, namespace, name)
            .await?
            .ok_or_else(|| MuliError::NotFound("repository not found".into()))?;

        if repo.namespace == new_namespace {
            return Err(MuliError::Validation(
                "new_namespace is same as current namespace".into(),
            ));
        }

        // Move filesystem first
        self.git_storage
            .transfer_repo(tenant_id, namespace, name, new_namespace)
            .await?;

        // Update DB — rollback filesystem on failure
        if let Err(e) = self
            .repo_store
            .transfer_repository(&repo.id, new_namespace)
            .await
        {
            let _ = self
                .git_storage
                .transfer_repo(tenant_id, new_namespace, name, namespace)
                .await;
            return Err(e);
        }

        let mut updated = repo;
        updated.namespace = new_namespace.to_string();

        info!(
            tenant_id = %tenant_id,
            from = %format!("{}/{}", namespace, name),
            to = %format!("{}/{}", new_namespace, name),
            "repository transferred"
        );

        Ok(updated)
    }
}
