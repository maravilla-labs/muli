// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite git cache store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::traits::TreeCommitCacheStore;

use super::factory::SqliteStoreFactory;
use super::util::store_err;

pub struct SqliteTreeCommitCacheStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteTreeCommitCacheStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl TreeCommitCacheStore for SqliteTreeCommitCacheStore {
    async fn get_cached(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        dir_path: &str,
    ) -> muli_core::error::Result<Option<String>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let (repo_id, commit_sha, dir_path) = (
            repo_id.to_owned(),
            commit_sha.to_owned(),
            dir_path.to_owned(),
        );
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT entries_json FROM tree_commit_cache
                 WHERE repo_id=?1 AND commit_sha=?2 AND dir_path=?3",
            )?;
            let mut rows = stmt.query(rusqlite::params![repo_id, commit_sha, dir_path])?;
            Ok(rows
                .next()?
                .map(|row| row.get::<_, String>(0))
                .transpose()?)
        })
        .await
        .map_err(store_err)
    }

    async fn set_cached(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
        dir_path: &str,
        entries_json: &str,
    ) -> muli_core::error::Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let (repo_id, commit_sha, dir_path, json) = (
            repo_id.to_owned(),
            commit_sha.to_owned(),
            dir_path.to_owned(),
            entries_json.to_owned(),
        );
        conn.call(move |c| {
            c.execute(
                "INSERT OR REPLACE INTO tree_commit_cache
                 (repo_id, commit_sha, dir_path, entries_json)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![repo_id, commit_sha, dir_path, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn invalidate_repo(
        &self,
        tenant_id: &str,
        repo_id: &str,
    ) -> muli_core::error::Result<()> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let repo_id = repo_id.to_owned();
        conn.call(move |c| {
            c.execute(
                "DELETE FROM tree_commit_cache WHERE repo_id=?1",
                rusqlite::params![repo_id],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)
    }
}
