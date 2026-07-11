// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB storage backend for production deployments.

pub mod agent_store;
pub mod git_repo_store;
pub mod git_ssh_store;
pub mod git_token_store;
pub mod git_webhook_store;
pub mod indexes;
pub mod job_log_store;
pub mod job_store;
pub mod org_store;
pub mod pr_store;
pub mod registry_token_store;
mod schema;
pub mod registry_visibility_store;
pub mod tenant_quota_store;
pub mod user_store;
mod util;

pub use agent_store::MongoAgentStore;
pub use git_repo_store::MongoRepositoryStore;
pub use git_ssh_store::MongoSshKeyStore;
pub use git_token_store::MongoGitTokenStore;
pub use git_webhook_store::MongoWebhookStore;
pub use indexes::setup_indexes;
pub use job_log_store::MongoJobLogStore;
pub use job_store::MongoJobStore;
pub use org_store::{MongoOrgMemberStore, MongoOrgStore};
pub use pr_store::{MongoPrCommentStore, MongoPullRequestStore};
pub use registry_token_store::MongoRegistryTokenStore;
pub use registry_visibility_store::MongoRegistryVisibilityStore;
pub use tenant_quota_store::MongoTenantQuotaStore;
pub use user_store::MongoUserStore;
