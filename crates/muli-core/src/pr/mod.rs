// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pull request models: state, comments, and review workflow.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::MuliError;

/// State of a pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrState {
    Open,
    Merged,
    Closed,
}

/// A pull request in a repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequest {
    pub id: String,
    /// Sequential number within the repository (PR #1, #2, ...).
    pub number: u64,
    pub tenant_id: String,
    pub repo_id: String,
    pub author_user_id: String,
    pub source_branch: String,
    pub target_branch: String,
    pub title: String,
    pub description: String,
    pub state: PrState,
    pub merge_commit_sha: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Parameters for creating a new pull request.
pub struct NewPullRequest {
    pub number: u64,
    pub tenant_id: String,
    pub repo_id: String,
    pub author_user_id: String,
    pub source_branch: String,
    pub target_branch: String,
    pub title: String,
    pub description: String,
}

impl PullRequest {
    pub fn new(params: NewPullRequest) -> Result<Self, MuliError> {
        if params.title.is_empty() {
            return Err(MuliError::Validation("title must not be empty".into()));
        }
        if params.source_branch.is_empty() {
            return Err(MuliError::Validation(
                "source_branch must not be empty".into(),
            ));
        }
        if params.target_branch.is_empty() {
            return Err(MuliError::Validation(
                "target_branch must not be empty".into(),
            ));
        }
        if params.source_branch == params.target_branch {
            return Err(MuliError::Validation(
                "source_branch and target_branch must differ".into(),
            ));
        }

        let now = Utc::now();
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            number: params.number,
            tenant_id: params.tenant_id,
            repo_id: params.repo_id,
            author_user_id: params.author_user_id,
            source_branch: params.source_branch,
            target_branch: params.target_branch,
            title: params.title,
            description: params.description,
            state: PrState::Open,
            merge_commit_sha: None,
            created_at: now,
            updated_at: now,
        })
    }
}

/// A comment on a pull request.
///
/// Optional fields enable two extended modes:
/// - **Inline diff comment**: `file` + `line` (+ optionally `start_line`) anchor the comment to a
///   specific line range in the diff.
/// - **Threaded reply**: `reply_to` contains the `id` of the parent comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrComment {
    pub id: String,
    pub pr_id: String,
    pub user_id: String,
    pub body: String,
    pub created_at: DateTime<Utc>,
    /// Path of the file this comment is anchored to (inline comment).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// New-file line number the comment points to (required when `file` is set).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    /// First line of a multi-line selection (≤ `line`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u32>,
    /// ID of the parent comment this is a reply to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
}

impl PrComment {
    pub fn new(pr_id: String, user_id: String, body: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            pr_id,
            user_id,
            body,
            created_at: Utc::now(),
            file: None,
            line: None,
            start_line: None,
            reply_to: None,
        }
    }

    /// Validate line range fields after they have been set.
    /// Returns an error if `start_line` > `line` when both are present.
    pub fn validate(&self) -> Result<(), MuliError> {
        if let (Some(start), Some(end)) = (self.start_line, self.line) {
            if start > end {
                return Err(MuliError::Validation(format!(
                    "start_line ({start}) must be <= line ({end})"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_request_new_sets_defaults() {
        let pr = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "tenant-1".into(),
            repo_id: "repo-1".into(),
            author_user_id: "user-1".into(),
            source_branch: "feature".into(),
            target_branch: "main".into(),
            title: "Add feature".into(),
            description: "Description".into(),
        })
        .unwrap();
        assert!(!pr.id.is_empty());
        assert_eq!(pr.number, 1);
        assert_eq!(pr.state, PrState::Open);
        assert!(pr.merge_commit_sha.is_none());
    }

    #[test]
    fn pull_request_rejects_empty_title() {
        let err = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "t".into(),
            repo_id: "r".into(),
            author_user_id: "u".into(),
            source_branch: "feat".into(),
            target_branch: "main".into(),
            title: "".into(),
            description: "".into(),
        });
        assert!(err.is_err());
    }

    #[test]
    fn pull_request_rejects_same_branches() {
        let err = PullRequest::new(NewPullRequest {
            number: 1,
            tenant_id: "t".into(),
            repo_id: "r".into(),
            author_user_id: "u".into(),
            source_branch: "main".into(),
            target_branch: "main".into(),
            title: "PR".into(),
            description: "".into(),
        });
        assert!(err.is_err());
    }

    #[test]
    fn pr_comment_new_sets_defaults() {
        let comment = PrComment::new("pr-1".into(), "user-1".into(), "Looks good!".into());
        assert!(!comment.id.is_empty());
        assert_eq!(comment.body, "Looks good!");
        assert!(comment.validate().is_ok());
    }

    #[test]
    fn pr_comment_validate_rejects_invalid_line_range() {
        let mut comment = PrComment::new("pr-1".into(), "user-1".into(), "bad".into());
        comment.start_line = Some(10);
        comment.line = Some(5);
        assert!(comment.validate().is_err());
    }

    #[test]
    fn pr_comment_validate_accepts_valid_line_range() {
        let mut comment = PrComment::new("pr-1".into(), "user-1".into(), "ok".into());
        comment.start_line = Some(5);
        comment.line = Some(10);
        assert!(comment.validate().is_ok());
    }
}
