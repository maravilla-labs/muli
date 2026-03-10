// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git repository collaborator store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::git::RepositoryCollaborator;
use muli_core::traits::CollaboratorStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteCollaboratorStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteCollaboratorStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl CollaboratorStore for SqliteCollaboratorStore {
    async fn upsert_collaborator(&self, collaborator: &RepositoryCollaborator) -> Result<String> {
        let conn = self.factory.tenant_conn(&collaborator.tenant_id).await?;
        let collaborator = collaborator.clone();
        let id = collaborator.id.clone();
        conn.call(move |c| {
            let json = to_json(&collaborator)?;
            c.execute(
                "INSERT INTO repo_collaborators (id, tenant_id, repo_id, user_id, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(repo_id, user_id) DO UPDATE SET
                   id = excluded.id,
                   full_json = excluded.full_json",
                rusqlite::params![
                    collaborator.id,
                    collaborator.tenant_id,
                    collaborator.repo_id,
                    collaborator.user_id,
                    json,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn remove_collaborator(&self, repo_id: &str, user_id: &str) -> Result<()> {
        let repo_id = repo_id.to_string();
        let user_id = user_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rid = repo_id.clone();
            let uid = user_id.clone();
            let rows = conn
                .call(move |c| {
                    let rows = c.execute(
                        "DELETE FROM repo_collaborators WHERE repo_id = ?1 AND user_id = ?2",
                        rusqlite::params![rid, uid],
                    )?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        // Not found is not an error for remove
        Ok(())
    }

    async fn list_collaborators(&self, repo_id: &str) -> Result<Vec<RepositoryCollaborator>> {
        let repo_id = repo_id.to_string();
        let mut all = Vec::new();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rid = repo_id.clone();
            let mut results = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM repo_collaborators WHERE repo_id = ?1")?;
                    let rows = stmt.query_map(rusqlite::params![rid], |row| {
                        let json: String = row.get(0)?;
                        from_json::<RepositoryCollaborator>(&json)
                    })?;
                    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
                })
                .await
                .map_err(store_err)?;
            all.append(&mut results);
        }
        Ok(all)
    }

    async fn get_collaborator(
        &self,
        repo_id: &str,
        user_id: &str,
    ) -> Result<Option<RepositoryCollaborator>> {
        let repo_id = repo_id.to_string();
        let user_id = user_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let rid = repo_id.clone();
            let uid = user_id.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare(
                    "SELECT full_json FROM repo_collaborators WHERE repo_id = ?1 AND user_id = ?2",
                )?;
                    let mut rows = stmt.query(rusqlite::params![rid, uid])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<RepositoryCollaborator>(&json)?))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::git::{GitPermission, HasPermissions};

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_collaborator_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteCollaboratorStore::new(factory);
        let collab = RepositoryCollaborator::new(
            "t1".into(),
            "repo-1".into(),
            "user-1".into(),
            vec![GitPermission::Pull, GitPermission::Push],
        );
        let id = store.upsert_collaborator(&collab).await.unwrap();
        assert!(!id.is_empty());

        let fetched = store
            .get_collaborator("repo-1", "user-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.user_id, "user-1");
        assert!(fetched.has_permission(GitPermission::Pull));

        let list = store.list_collaborators("repo-1").await.unwrap();
        assert_eq!(list.len(), 1);

        store.remove_collaborator("repo-1", "user-1").await.unwrap();
        assert!(
            store
                .get_collaborator("repo-1", "user-1")
                .await
                .unwrap()
                .is_none()
        );
    }
}
