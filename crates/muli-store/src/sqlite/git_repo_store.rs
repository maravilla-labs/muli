// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git repository metadata store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::git::Repository;
use muli_core::traits::RepositoryStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteRepositoryStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteRepositoryStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl RepositoryStore for SqliteRepositoryStore {
    async fn create_repository(&self, repo: &Repository) -> Result<String> {
        let conn = self.factory.tenant_conn(&repo.tenant_id).await?;
        let repo = repo.clone();
        let id = repo.id.clone();
        conn.call(move |c| {
            let json = to_json(&repo)?;
            c.execute(
                "INSERT INTO repositories (id, tenant_id, namespace, name, fork_of, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    repo.id,
                    repo.tenant_id,
                    repo.namespace,
                    repo.name,
                    repo.fork_of,
                    json
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_repository(&self, repo_id: &str) -> Result<Option<Repository>> {
        let repo_id = repo_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rid = repo_id.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare("SELECT full_json FROM repositories WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![rid])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Repository>(&json)?))
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

    async fn get_repository_by_name(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Repository>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let namespace = namespace.to_string();
        let name = name.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM repositories WHERE namespace = ?1 AND name = ?2")?;
            let mut rows = stmt.query(rusqlite::params![namespace, name])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<Repository>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn list_repositories(&self, tenant_id: &str) -> Result<Vec<Repository>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM repositories")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                from_json::<Repository>(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }

    async fn delete_repository(&self, repo_id: &str) -> Result<()> {
        let repo_id = repo_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rid = repo_id.clone();
            let rows = conn.call(move |c| {
                // Cascade: remove all dependent records before deleting the repository
                // 1. Delete PR comments for all PRs in this repo
                c.execute(
                    "DELETE FROM pr_comments WHERE pr_id IN (SELECT id FROM pull_requests WHERE repo_id = ?1)",
                    rusqlite::params![rid],
                )?;
                // 2. Delete pull requests
                c.execute("DELETE FROM pull_requests WHERE repo_id = ?1", rusqlite::params![rid])?;
                // 3. Delete webhooks
                c.execute("DELETE FROM webhooks WHERE repo_id = ?1", rusqlite::params![rid])?;
                // 4. Delete collaborators
                c.execute("DELETE FROM repo_collaborators WHERE repo_id = ?1", rusqlite::params![rid])?;
                // 5. Delete tree commit cache
                c.execute("DELETE FROM tree_commit_cache WHERE repo_id = ?1", rusqlite::params![rid])?;
                // 6. Delete the repository itself
                let rows = c.execute(
                    "DELETE FROM repositories WHERE id = ?1",
                    rusqlite::params![rid],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!(
            "Repository {repo_id} not found"
        )))
    }

    async fn update_repository(&self, repo: &Repository) -> Result<()> {
        let conn = self.factory.tenant_conn(&repo.tenant_id).await?;
        let repo = repo.clone();
        let repo_id_str = repo.id.clone();
        let rows = conn
            .call(move |c| {
                let json = to_json(&repo)?;
                let rows = c.execute(
                    "UPDATE repositories SET namespace = ?1, name = ?2, fork_of = ?3, full_json = ?4 WHERE id = ?5",
                    rusqlite::params![repo.namespace, repo.name, repo.fork_of, json, repo.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "Repository {repo_id_str} not found"
            )));
        }
        Ok(())
    }

    async fn list_forks(&self, parent_repo_id: &str) -> Result<Vec<Repository>> {
        let parent_id = parent_repo_id.to_string();
        let mut all = Vec::new();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let pid = parent_id.clone();
            let mut results = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM repositories WHERE fork_of = ?1")?;
                    let rows = stmt.query_map(rusqlite::params![pid], |row| {
                        let json: String = row.get(0)?;
                        from_json::<Repository>(&json)
                    })?;
                    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
                })
                .await
                .map_err(store_err)?;
            all.append(&mut results);
        }
        Ok(all)
    }

    async fn transfer_repository(&self, repo_id: &str, new_namespace: &str) -> Result<()> {
        let repo_id = repo_id.to_string();
        let new_namespace = new_namespace.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rid = repo_id.clone();
            let ns = new_namespace.clone();
            let rows = conn
                .call(move |c| {
                    let existing: Option<String> = {
                        let mut stmt =
                            c.prepare("SELECT full_json FROM repositories WHERE id = ?1")?;
                        let mut rows = stmt.query(rusqlite::params![rid])?;
                        rows.next()?
                            .map(|row| row.get::<_, String>(0))
                            .transpose()?
                    };
                    let Some(json) = existing else {
                        return Ok(0usize);
                    };
                    let mut repo: Repository = from_json(&json)?;
                    repo.namespace = ns;
                    repo.updated_at = chrono::Utc::now();
                    let new_json = to_json(&repo)?;
                    let rows = c.execute(
                        "UPDATE repositories SET namespace = ?1, full_json = ?2 WHERE id = ?3",
                        rusqlite::params![repo.namespace, new_json, repo.id],
                    )?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!(
            "Repository {repo_id} not found"
        )))
    }

    async fn count_by_tenant(&self, tenant_id: &str) -> Result<u64> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        conn.call(move |c| {
            let count: i64 =
                c.query_row("SELECT COUNT(*) FROM repositories", [], |row| row.get(0))?;
            Ok(count as u64)
        })
        .await
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_repository_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteRepositoryStore::new(factory);
        let repo = Repository::new(
            "t1".into(),
            "acme".into(),
            "my-repo".into(),
            "test".into(),
            false,
        )
        .unwrap();
        let id = store.create_repository(&repo).await.unwrap();
        let by_name = store
            .get_repository_by_name("t1", "acme", "my-repo")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_name.id, id);
        let by_id = store.get_repository(&id).await.unwrap().unwrap();
        assert_eq!(by_id.namespace, "acme");
        let list = store.list_repositories("t1").await.unwrap();
        assert_eq!(list.len(), 1);
        store.delete_repository(&id).await.unwrap();
        assert!(store.get_repository(&id).await.unwrap().is_none());
    }
}
