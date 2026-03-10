// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Organization and pull request storage traits.

use async_trait::async_trait;

use crate::error::Result;
use crate::org::{OrgMember, OrgRole, Organization};
use crate::pr::{PrComment, PrState, PullRequest};

/// Persistent storage for organizations.
#[async_trait]
pub trait OrgStore: Send + Sync {
    /// Create a new organization record.
    async fn create_org(&self, org: &Organization) -> Result<String>;

    /// Get an organization by ID.
    async fn get_org(&self, org_id: &str) -> Result<Option<Organization>>;

    /// Get an organization by tenant + handle (unique within tenant).
    async fn get_org_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<Organization>>;

    /// Delete an organization by ID.
    async fn delete_org(&self, org_id: &str) -> Result<()>;

    /// List all organizations for a tenant.
    async fn list_orgs(&self, tenant_id: &str) -> Result<Vec<Organization>>;
}

/// Persistent storage for organization memberships.
#[async_trait]
pub trait OrgMemberStore: Send + Sync {
    /// Add a user to an organization.
    async fn add_member(&self, member: &OrgMember) -> Result<String>;

    /// Remove a user from an organization.
    async fn remove_member(&self, org_id: &str, user_id: &str) -> Result<()>;

    /// List all members of an organization.
    async fn list_members(&self, org_id: &str) -> Result<Vec<OrgMember>>;

    /// Get a specific membership record.
    async fn get_member(&self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>>;

    /// Update the role of an existing member.
    async fn update_member_role(&self, org_id: &str, user_id: &str, role: OrgRole) -> Result<()>;
}

/// Persistent storage for pull requests.
#[async_trait]
pub trait PullRequestStore: Send + Sync {
    /// Create a new pull request record.
    async fn create_pr(&self, pr: &PullRequest) -> Result<String>;

    /// Get a pull request by ID.
    async fn get_pr(&self, pr_id: &str) -> Result<Option<PullRequest>>;

    /// Get a pull request by repository + sequential number.
    async fn get_pr_by_number(&self, repo_id: &str, number: u64) -> Result<Option<PullRequest>>;

    /// List pull requests for a repository, optionally filtered by state.
    async fn list_prs(&self, repo_id: &str, state: Option<PrState>) -> Result<Vec<PullRequest>>;

    /// Update an existing pull request record.
    async fn update_pr(&self, pr: &PullRequest) -> Result<()>;

    /// Return the next available sequential PR number for the given repository.
    async fn next_pr_number(&self, repo_id: &str) -> Result<u64>;
}

/// Persistent storage for pull request comments.
#[async_trait]
pub trait PrCommentStore: Send + Sync {
    /// Add a comment to a pull request.
    async fn add_comment(&self, comment: &PrComment) -> Result<String>;

    /// List all comments for a pull request.
    async fn list_comments(&self, pr_id: &str) -> Result<Vec<PrComment>>;
}
