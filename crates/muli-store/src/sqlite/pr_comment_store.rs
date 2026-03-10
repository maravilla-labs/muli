// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pull request comment store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::pr::PrComment;
use muli_core::traits::PrCommentStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqlitePrCommentStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqlitePrCommentStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }

    /// Find which tenant DB contains the given pr_id.
    async fn find_conn_for_pr(
        &self,
        pr_id: &str,
    ) -> Result<Option<Arc<tokio_rusqlite::Connection>>> {
        let pid = pr_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let p = pid.clone();
            let exists: bool = conn
                .call(move |c| {
                    let count: i64 = c
                        .query_row(
                            "SELECT COUNT(*) FROM pull_requests WHERE id = ?1",
                            rusqlite::params![p],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    Ok(count > 0)
                })
                .await
                .map_err(store_err)?;
            if exists {
                return Ok(Some(conn));
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl PrCommentStore for SqlitePrCommentStore {
    async fn add_comment(&self, comment: &PrComment) -> Result<String> {
        let conn = match self.find_conn_for_pr(&comment.pr_id).await? {
            Some(c) => c,
            None => {
                return Err(MuliError::Storage(format!(
                    "PR {} not found",
                    comment.pr_id
                )));
            }
        };
        let comment = comment.clone();
        let id = comment.id.clone();
        conn.call(move |c| {
            let json = to_json(&comment)?;
            c.execute(
                "INSERT INTO pr_comments (id, pr_id, full_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![comment.id, comment.pr_id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn list_comments(&self, pr_id: &str) -> Result<Vec<PrComment>> {
        let pid = pr_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let p = pid.clone();
            let comments: Vec<PrComment> = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM pr_comments WHERE pr_id = ?1")?;
                    let rows = stmt.query_map(rusqlite::params![p], |row| {
                        let json: String = row.get(0)?;
                        from_json::<PrComment>(&json)
                    })?;
                    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
                })
                .await
                .map_err(store_err)?;
            if !comments.is_empty() {
                return Ok(comments);
            }
        }
        Ok(vec![])
    }
}
