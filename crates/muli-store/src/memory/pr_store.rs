// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory pull request and comment store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::{MuliError, Result};
use muli_core::pr::{PrComment, PrState, PullRequest};
use muli_core::traits::{PrCommentStore, PullRequestStore};

#[derive(Debug, Clone)]
pub struct MemoryPullRequestStore {
    prs: Arc<DashMap<String, PullRequest>>,
}

impl MemoryPullRequestStore {
    pub fn new() -> Self {
        Self {
            prs: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryPullRequestStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PullRequestStore for MemoryPullRequestStore {
    async fn create_pr(&self, pr: &PullRequest) -> Result<String> {
        let id = pr.id.clone();
        self.prs.insert(id.clone(), pr.clone());
        Ok(id)
    }

    async fn get_pr(&self, pr_id: &str) -> Result<Option<PullRequest>> {
        Ok(self.prs.get(pr_id).map(|e| e.value().clone()))
    }

    async fn get_pr_by_number(&self, repo_id: &str, number: u64) -> Result<Option<PullRequest>> {
        Ok(self
            .prs
            .iter()
            .find(|e| {
                let pr = e.value();
                pr.repo_id == repo_id && pr.number == number
            })
            .map(|e| e.value().clone()))
    }

    async fn list_prs(&self, repo_id: &str, state: Option<PrState>) -> Result<Vec<PullRequest>> {
        Ok(self
            .prs
            .iter()
            .filter(|e| {
                let pr = e.value();
                pr.repo_id == repo_id && state.is_none_or(|s| pr.state == s)
            })
            .map(|e| e.value().clone())
            .collect())
    }

    async fn update_pr(&self, pr: &PullRequest) -> Result<()> {
        if self.prs.contains_key(&pr.id) {
            self.prs.insert(pr.id.clone(), pr.clone());
            Ok(())
        } else {
            Err(MuliError::Storage(format!("PR {} not found", pr.id)))
        }
    }

    async fn next_pr_number(&self, repo_id: &str) -> Result<u64> {
        let max = self
            .prs
            .iter()
            .filter(|e| e.value().repo_id == repo_id)
            .map(|e| e.value().number)
            .max()
            .unwrap_or(0);
        Ok(max + 1)
    }
}

#[derive(Debug, Clone)]
pub struct MemoryPrCommentStore {
    comments: Arc<DashMap<String, PrComment>>,
}

impl MemoryPrCommentStore {
    pub fn new() -> Self {
        Self {
            comments: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryPrCommentStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PrCommentStore for MemoryPrCommentStore {
    async fn add_comment(&self, comment: &PrComment) -> Result<String> {
        let id = comment.id.clone();
        self.comments.insert(id.clone(), comment.clone());
        Ok(id)
    }

    async fn list_comments(&self, pr_id: &str) -> Result<Vec<PrComment>> {
        Ok(self
            .comments
            .iter()
            .filter(|e| e.value().pr_id == pr_id)
            .map(|e| e.value().clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::pr::NewPullRequest;

    #[tokio::test]
    async fn test_pr_store_crud() {
        let store = MemoryPullRequestStore::new();
        let pr = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "tenant-1".into(),
            repo_id: "repo-1".into(),
            author_user_id: "user-1".into(),
            source_branch: "feature".into(),
            target_branch: "main".into(),
            title: "Add feature".into(),
            description: "".into(),
        })
        .unwrap();

        let id = store.create_pr(&pr).await.unwrap();
        assert_eq!(id, pr.id);

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

        let next = store.next_pr_number("repo-1").await.unwrap();
        assert_eq!(next, 2);
    }

    #[tokio::test]
    async fn test_pr_comment_store() {
        let store = MemoryPrCommentStore::new();
        let comment = PrComment::new("pr-1".into(), "user-1".into(), "LGTM".into());

        store.add_comment(&comment).await.unwrap();
        let list = store.list_comments("pr-1").await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].body, "LGTM");
    }
}
