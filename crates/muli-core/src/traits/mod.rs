// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait abstractions (repository pattern) for all domain entities.

mod git;
mod job;
mod org;
mod pipeline;
mod registry;
mod release;
mod tenant_limits;
mod user;

pub use git::{
    CollaboratorStore, GitStorage, GitTokenStore, RepositoryStore, SshKeyStore,
    TreeCommitCacheStore, WebhookStore,
};
pub use job::{AgentRegistry, JobLogStore, JobStore};
pub use org::{OrgMemberStore, OrgStore, PrCommentStore, PullRequestStore};
pub use pipeline::{
    ArtifactStore, CacheStore, OrgSecretStore, PipelineRunStore, PipelineSecretStore,
    PipelineStore, StepRunStore,
};
pub use registry::{
    RegistryTokenStore, RegistryVisibilityStore, TenantQuotaStore, TenantStore,
};
pub use release::ReleaseStore;
pub use tenant_limits::TenantLimitsStore;
pub use user::UserStore;
