// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pull request API request/response types.

use muli_core::pr::{PrState, PullRequest};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CreatePrRequest {
    pub source_branch: String,
    pub target_branch: String,
    pub title: String,
    pub description: Option<String>,
    /// Deprecated: ignored if present. Author is determined from auth context.
    pub author_user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListPrsQuery {
    pub state: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PatchPrRequest {
    pub action: String,
}

#[derive(Debug, Serialize)]
pub struct PrResponse {
    pub id: String,
    pub number: u64,
    pub tenant_id: String,
    pub repo_id: String,
    pub author_user_id: String,
    pub source_branch: String,
    pub target_branch: String,
    pub title: String,
    pub description: String,
    pub state: String,
    pub merge_commit_sha: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<PullRequest> for PrResponse {
    fn from(pr: PullRequest) -> Self {
        let state = match pr.state {
            PrState::Open => "open",
            PrState::Merged => "merged",
            PrState::Closed => "closed",
        };
        PrResponse {
            id: pr.id,
            number: pr.number,
            tenant_id: pr.tenant_id,
            repo_id: pr.repo_id,
            author_user_id: pr.author_user_id,
            source_branch: pr.source_branch,
            target_branch: pr.target_branch,
            title: pr.title,
            description: pr.description,
            state: state.to_string(),
            merge_commit_sha: pr.merge_commit_sha,
            created_at: pr.created_at.to_rfc3339(),
            updated_at: pr.updated_at.to_rfc3339(),
        }
    }
}

pub fn parse_pr_state(s: &str) -> Option<PrState> {
    match s {
        "open" => Some(PrState::Open),
        "merged" => Some(PrState::Merged),
        "closed" => Some(PrState::Closed),
        _ => None,
    }
}
