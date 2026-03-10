// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite pull request store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::pr::{PrState, PullRequest};
use muli_core::traits::PullRequestStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

fn pr_state_str(s: PrState) -> &'static str {
    match s {
        PrState::Open => "Open",
        PrState::Merged => "Merged",
        PrState::Closed => "Closed",
    }
}

// ─── PullRequestStore ─────────────────────────────────────────────────────

pub struct SqlitePullRequestStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqlitePullRequestStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl PullRequestStore for SqlitePullRequestStore {
    async fn create_pr(&self, pr: &PullRequest) -> Result<String> {
        let conn = self.factory.tenant_conn(&pr.tenant_id).await?;
        let pr = pr.clone();
        let id = pr.id.clone();
        conn.call(move |c| {
            let json = to_json(&pr)?;
            c.execute(
                "INSERT INTO pull_requests (id, number, repo_id, state, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    pr.id,
                    pr.number as i64,
                    pr.repo_id,
                    pr_state_str(pr.state),
                    json,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_pr(&self, pr_id: &str) -> Result<Option<PullRequest>> {
        let pid = pr_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let p = pid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM pull_requests WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![p])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<PullRequest>(&json)?))
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

    async fn get_pr_by_number(&self, repo_id: &str, number: u64) -> Result<Option<PullRequest>> {
        let rid = repo_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let n = number as i64;
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare(
                        "SELECT full_json FROM pull_requests WHERE repo_id = ?1 AND number = ?2",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![r, n])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<PullRequest>(&json)?))
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

    async fn list_prs(&self, repo_id: &str, state: Option<PrState>) -> Result<Vec<PullRequest>> {
        let rid = repo_id.to_string();
        let state_str = state.map(pr_state_str);
        let mut all = Vec::new();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let s = state_str;
            let mut results: Vec<PullRequest> = conn
                .call(move |c| {
                    let prs: Vec<PullRequest> = match s {
                        Some(state_s) => {
                            let mut stmt = c.prepare(
                            "SELECT full_json FROM pull_requests WHERE repo_id = ?1 AND state = ?2",
                        )?;
                            let mut rows = stmt.query(rusqlite::params![r, state_s])?;
                            let mut result = Vec::new();
                            while let Some(row) = rows.next()? {
                                let json: String = row.get(0)?;
                                result.push(from_json::<PullRequest>(&json)?);
                            }
                            result
                        }
                        None => {
                            let mut stmt = c.prepare(
                                "SELECT full_json FROM pull_requests WHERE repo_id = ?1",
                            )?;
                            let mut rows = stmt.query(rusqlite::params![r])?;
                            let mut result = Vec::new();
                            while let Some(row) = rows.next()? {
                                let json: String = row.get(0)?;
                                result.push(from_json::<PullRequest>(&json)?);
                            }
                            result
                        }
                    };
                    Ok(prs)
                })
                .await
                .map_err(store_err)?;
            all.append(&mut results);
        }
        Ok(all)
    }

    async fn update_pr(&self, pr: &PullRequest) -> Result<()> {
        let conn = self.factory.tenant_conn(&pr.tenant_id).await?;
        let pr = pr.clone();
        let pr_id_for_err = pr.id.clone();
        let rows = conn
            .call(move |c| {
                let json = to_json(&pr)?;
                let rows = c.execute(
                    "UPDATE pull_requests SET state = ?1, full_json = ?2 WHERE id = ?3",
                    rusqlite::params![pr_state_str(pr.state), json, pr.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            return Err(MuliError::Storage(format!("PR {pr_id_for_err} not found")));
        }
        Ok(())
    }

    async fn next_pr_number(&self, repo_id: &str) -> Result<u64> {
        let rid = repo_id.to_string();
        // Search all open tenant connections for this repo
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let result: Option<i64> = conn
                .call(move |c| {
                    // Check if this tenant has any PRs for this repo
                    let max: Option<i64> = c.query_row(
                        "SELECT MAX(number) FROM pull_requests WHERE repo_id = ?1",
                        rusqlite::params![r],
                        |row| row.get(0),
                    )?;
                    Ok(max)
                })
                .await
                .map_err(store_err)?;
            // If we found PRs in this tenant, return max+1
            if let Some(max) = result {
                return Ok(max as u64 + 1);
            }
            // Also check if the repo exists in this tenant (for first PR)
            let r = rid.clone();
            let has_repo: bool = conn
                .call(move |c| {
                    let count: i64 = c
                        .query_row(
                            "SELECT COUNT(*) FROM repositories WHERE id = ?1",
                            rusqlite::params![r],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    Ok(count > 0)
                })
                .await
                .map_err(store_err)?;
            if has_repo {
                return Ok(1);
            }
        }
        // Default: return 1 if we don't know the repo
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::super::pr_comment_store::SqlitePrCommentStore;
    use super::*;
    use muli_core::pr::{NewPullRequest, PrComment};
    use muli_core::traits::PrCommentStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_pr_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePullRequestStore::new(factory);
        let pr = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "t1".into(),
            repo_id: "repo-1".into(),
            author_user_id: "user-1".into(),
            source_branch: "feature".into(),
            target_branch: "main".into(),
            title: "Add feature".into(),
            description: "".into(),
        })
        .unwrap();
        let id = store.create_pr(&pr).await.unwrap();
        let fetched = store.get_pr(&id).await.unwrap().unwrap();
        assert_eq!(fetched.number, 1);
        let by_num = store.get_pr_by_number("repo-1", 1).await.unwrap().unwrap();
        assert_eq!(by_num.id, id);
        let list = store.list_prs("repo-1", None).await.unwrap();
        assert_eq!(list.len(), 1);
        let open = store.list_prs("repo-1", Some(PrState::Open)).await.unwrap();
        assert_eq!(open.len(), 1);
        let merged = store
            .list_prs("repo-1", Some(PrState::Merged))
            .await
            .unwrap();
        assert_eq!(merged.len(), 0);
    }

    #[tokio::test]
    async fn test_next_pr_number() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePullRequestStore::new(factory);
        // First PR
        let pr1 = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "t1".into(),
            repo_id: "repo-1".into(),
            author_user_id: "u1".into(),
            source_branch: "f".into(),
            target_branch: "main".into(),
            title: "PR1".into(),
            description: "".into(),
        })
        .unwrap();
        store.create_pr(&pr1).await.unwrap();
        let next = store.next_pr_number("repo-1").await.unwrap();
        assert_eq!(next, 2);
    }

    #[tokio::test]
    async fn test_update_pr() {
        let (factory, _dir) = make_factory().await;
        let store = SqlitePullRequestStore::new(factory);
        let mut pr = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "t1".into(),
            repo_id: "repo-1".into(),
            author_user_id: "u1".into(),
            source_branch: "f".into(),
            target_branch: "main".into(),
            title: "PR".into(),
            description: "".into(),
        })
        .unwrap();
        store.create_pr(&pr).await.unwrap();
        pr.state = PrState::Merged;
        store.update_pr(&pr).await.unwrap();
        let fetched = store.get_pr(&pr.id).await.unwrap().unwrap();
        assert_eq!(fetched.state, PrState::Merged);
    }

    #[tokio::test]
    async fn test_pr_comment() {
        let (factory, _dir) = make_factory().await;
        let pr_store = SqlitePullRequestStore::new(factory.clone());
        let comment_store = SqlitePrCommentStore::new(factory);
        let pr = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "t1".into(),
            repo_id: "repo-1".into(),
            author_user_id: "u1".into(),
            source_branch: "f".into(),
            target_branch: "main".into(),
            title: "PR".into(),
            description: "".into(),
        })
        .unwrap();
        let pr_id = pr_store.create_pr(&pr).await.unwrap();
        let comment = PrComment::new(pr_id.clone(), "u1".into(), "LGTM".into());
        comment_store.add_comment(&comment).await.unwrap();
        let list = comment_store.list_comments(&pr_id).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].body, "LGTM");
    }
}
