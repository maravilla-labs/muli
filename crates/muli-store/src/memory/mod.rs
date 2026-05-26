// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory storage backend for testing and development.

pub mod agent_store;
pub mod git_collaborator_store;
pub mod git_repo_store;
pub mod git_ssh_store;
pub mod git_token_store;
pub mod git_webhook_store;
pub mod job_log_store;
mod job_query;
pub mod job_store;
pub mod org_store;
pub mod pr_store;
pub mod registry_token_store;
pub mod release_store;
pub mod tenant_limits_store;
pub mod tenant_quota_store;
pub mod user_store;

pub use agent_store::MemoryAgentStore;
pub use git_collaborator_store::MemoryCollaboratorStore;
pub use git_repo_store::MemoryRepositoryStore;
pub use git_ssh_store::MemorySshKeyStore;
pub use git_token_store::MemoryGitTokenStore;
pub use git_webhook_store::MemoryWebhookStore;
pub use job_log_store::MemoryJobLogStore;
pub use job_store::MemoryJobStore;
pub use org_store::{MemoryOrgMemberStore, MemoryOrgStore};
pub use pr_store::{MemoryPrCommentStore, MemoryPullRequestStore};
pub use registry_token_store::MemoryRegistryTokenStore;
pub use release_store::MemoryReleaseStore;
pub use tenant_limits_store::MemoryTenantLimitsStore;
pub use tenant_quota_store::MemoryTenantQuotaStore;
pub use user_store::MemoryUserStore;
