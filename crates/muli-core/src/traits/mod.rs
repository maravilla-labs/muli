// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait abstractions (repository pattern) for all domain entities.

mod git;
mod job;
mod org;
mod registry;
mod user;

pub use git::{
    CollaboratorStore, GitTokenStore, RepositoryStore, SshKeyStore, TreeCommitCacheStore,
    WebhookStore,
};
pub use job::{AgentRegistry, JobLogStore, JobStore};
pub use org::{OrgMemberStore, OrgStore, PrCommentStore, PullRequestStore};
pub use registry::{RegistryTokenStore, TenantQuotaStore, TenantStore};
pub use user::UserStore;
