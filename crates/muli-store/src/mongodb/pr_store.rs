// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed pull request store.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::{Document, doc, to_bson};
use mongodb::options::ReturnDocument;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::pr::{PrComment, PrState, PullRequest};
use muli_core::traits::{PrCommentStore, PullRequestStore};

const PRS_COLLECTION: &str = "pull_requests";
const PR_COMMENTS_COLLECTION: &str = "pr_comments";
const PR_COUNTERS_COLLECTION: &str = "pr_counters";

#[derive(Clone)]
pub struct MongoPullRequestStore {
    collection: Collection<PullRequest>,
    counters: Collection<Document>,
}

impl MongoPullRequestStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection(PRS_COLLECTION),
            counters: db.collection(PR_COUNTERS_COLLECTION),
        }
    }
}

#[async_trait]
impl PullRequestStore for MongoPullRequestStore {
    async fn create_pr(&self, pr: &PullRequest) -> Result<String> {
        self.collection
            .insert_one(pr)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create PR: {e}")))?;
        Ok(pr.id.clone())
    }

    async fn get_pr(&self, pr_id: &str) -> Result<Option<PullRequest>> {
        self.collection
            .find_one(doc! { "id": pr_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get PR: {e}")))
    }

    async fn get_pr_by_number(&self, repo_id: &str, number: u64) -> Result<Option<PullRequest>> {
        self.collection
            .find_one(doc! { "repo_id": repo_id, "number": number as i64 })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get PR by number: {e}")))
    }

    async fn list_prs(&self, repo_id: &str, state: Option<PrState>) -> Result<Vec<PullRequest>> {
        let filter = match state {
            Some(s) => {
                let state_bson = to_bson(&s)
                    .map_err(|e| MuliError::Storage(format!("Failed to serialize state: {e}")))?;
                doc! { "repo_id": repo_id, "state": state_bson }
            }
            None => doc! { "repo_id": repo_id },
        };
        let cursor = self
            .collection
            .find(filter)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list PRs: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect PRs: {e}")))
    }

    async fn update_pr(&self, pr: &PullRequest) -> Result<()> {
        let result = self
            .collection
            .replace_one(doc! { "id": &pr.id }, pr)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update PR: {e}")))?;
        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!("PR {} not found", pr.id)));
        }
        Ok(())
    }

    async fn next_pr_number(&self, repo_id: &str) -> Result<u64> {
        // Atomically increment a per-repo counter using findOneAndUpdate + upsert.
        // This prevents race conditions when concurrent requests create PRs.
        let result = self
            .counters
            .find_one_and_update(
                doc! { "repo_id": repo_id },
                doc! { "$inc": { "next_number": 1_i64 } },
            )
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get next PR number: {e}")))?;

        match result {
            Some(counter_doc) => {
                let number = counter_doc
                    .get_i64("next_number")
                    .map_err(|e| MuliError::Storage(format!("Invalid PR counter: {e}")))?;
                Ok(number as u64)
            }
            None => Err(MuliError::Storage(
                "Failed to upsert PR counter".to_string(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct MongoPrCommentStore {
    collection: Collection<PrComment>,
}

impl MongoPrCommentStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection(PR_COMMENTS_COLLECTION),
        }
    }
}

#[async_trait]
impl PrCommentStore for MongoPrCommentStore {
    async fn add_comment(&self, comment: &PrComment) -> Result<String> {
        self.collection
            .insert_one(comment)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to add PR comment: {e}")))?;
        Ok(comment.id.clone())
    }

    async fn list_comments(&self, pr_id: &str) -> Result<Vec<PrComment>> {
        let cursor = self
            .collection
            .find(doc! { "pr_id": pr_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list PR comments: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect PR comments: {e}")))
    }
}
