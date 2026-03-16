// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage backend initialization (SQLite/MongoDB/memory).

use std::sync::Arc;

use anyhow::Context;
use tracing::{info, warn};

use muli_core::tenant::Tenant;
use muli_core::traits::{
    ArtifactStore, CacheStore, CollaboratorStore, GitTokenStore, JobLogStore, JobStore,
    OrgMemberStore, OrgStore, PipelineRunStore, PipelineSecretStore, PipelineStore,
    PrCommentStore, PullRequestStore, RegistryTokenStore, RepositoryStore, SshKeyStore,
    StepRunStore, TenantQuotaStore, TenantStore, TreeCommitCacheStore, UserStore, WebhookStore,
};
use muli_store::sqlite::{
    SqliteArtifactStore, SqliteCacheStore, SqliteCollaboratorStore, SqliteGitTokenStore,
    SqliteJobLogStore, SqliteJobStore, SqliteOrgMemberStore, SqliteOrgStore,
    SqlitePipelineRunStore, SqlitePipelineSecretStore, SqlitePipelineStore, SqlitePrCommentStore,
    SqlitePullRequestStore, SqliteRegistryTokenStore, SqliteRepositoryStore, SqliteSshKeyStore,
    SqliteStepRunStore, SqliteStoreFactory, SqliteTenantQuotaStore, SqliteTenantStore,
    SqliteTreeCommitCacheStore, SqliteUserStore, SqliteWebhookStore,
};

use crate::config::ServerConfig;

/// All stores needed by the server, created from a single SQLite factory.
pub(crate) struct Stores {
    pub tenant_store: Arc<dyn TenantStore>,
    pub job_store: Arc<dyn JobStore>,
    pub job_log_store: Arc<dyn JobLogStore>,
    pub agent_registry: Arc<muli_store::sqlite::SqliteAgentStore>,
    pub registry_token_store: Arc<dyn RegistryTokenStore>,
    pub tenant_quota_store: Arc<dyn TenantQuotaStore>,
    pub git_token_store: Arc<dyn GitTokenStore>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    pub webhook_store: Arc<dyn WebhookStore>,
    pub collaborator_store: Arc<dyn CollaboratorStore>,
    pub user_store: Arc<dyn UserStore>,
    pub org_store: Arc<dyn OrgStore>,
    pub org_member_store: Arc<dyn OrgMemberStore>,
    pub pr_store: Arc<dyn PullRequestStore>,
    pub pr_comment_store: Arc<dyn PrCommentStore>,
    pub tree_commit_cache: Arc<dyn TreeCommitCacheStore>,
    // Pipeline stores
    pub pipeline_store: Arc<dyn PipelineStore>,
    pub pipeline_run_store: Arc<dyn PipelineRunStore>,
    pub step_run_store: Arc<dyn StepRunStore>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub pipeline_cache_store: Arc<dyn CacheStore>,
    pub pipeline_secret_store: Arc<dyn PipelineSecretStore>,
}

/// Initialize all SQLite-backed stores from config.
pub(crate) async fn init_stores(config: &ServerConfig) -> anyhow::Result<Stores> {
    let factory = SqliteStoreFactory::new(&config.data_dir)
        .await
        .context("Failed to initialize SQLite store")?;
    info!(data_dir = %config.data_dir, "SQLite store initialized");

    let tenant_store: Arc<dyn TenantStore> = Arc::new(SqliteTenantStore::new(factory.clone()));
    if let Some(ref dt) = config.default_tenant_id {
        if tenant_store.get_tenant(dt).await?.is_none() {
            tenant_store
                .create_tenant(&Tenant::new(dt, dt, "auto-provisioned"))
                .await?;
        }
        info!(default_tenant = %dt, "Default tenant configured");
    }

    // Set up MongoDB indexes if MULI_MONGODB_URL is explicitly configured
    if let Ok(mongo_url) = std::env::var("MULI_MONGODB_URL") {
        let mongodb_client = mongodb::Client::with_uri_str(&mongo_url)
            .await
            .context("Failed to connect to MongoDB")?;
        let db = mongodb_client.database(&config.mongodb_database);
        match muli_store::mongodb::setup_indexes(&db).await {
            Ok(()) => info!("MongoDB indexes created successfully"),
            Err(e) => warn!(error = %e, "Failed to create MongoDB indexes (non-fatal)"),
        }
    }

    Ok(Stores {
        tenant_store,
        job_store: Arc::new(SqliteJobStore::new(factory.clone())),
        job_log_store: Arc::new(SqliteJobLogStore::new(factory.clone())),
        agent_registry: Arc::new(muli_store::sqlite::SqliteAgentStore::new(factory.clone())),
        registry_token_store: Arc::new(SqliteRegistryTokenStore::new(factory.clone())),
        tenant_quota_store: Arc::new(SqliteTenantQuotaStore::new(factory.clone())),
        git_token_store: Arc::new(SqliteGitTokenStore::new(factory.clone())),
        repo_store: Arc::new(SqliteRepositoryStore::new(factory.clone())),
        ssh_key_store: Arc::new(SqliteSshKeyStore::new(factory.clone())),
        webhook_store: Arc::new(SqliteWebhookStore::new(factory.clone())),
        collaborator_store: Arc::new(SqliteCollaboratorStore::new(factory.clone())),
        user_store: Arc::new(SqliteUserStore::new(factory.clone())),
        org_store: Arc::new(SqliteOrgStore::new(factory.clone())),
        org_member_store: Arc::new(SqliteOrgMemberStore::new(factory.clone())),
        pr_store: Arc::new(SqlitePullRequestStore::new(factory.clone())),
        pr_comment_store: Arc::new(SqlitePrCommentStore::new(factory.clone())),
        tree_commit_cache: Arc::new(SqliteTreeCommitCacheStore::new(factory.clone())),
        // Pipeline stores
        pipeline_store: Arc::new(SqlitePipelineStore::new(factory.clone())),
        pipeline_run_store: Arc::new(SqlitePipelineRunStore::new(factory.clone())),
        step_run_store: Arc::new(SqliteStepRunStore::new(factory.clone())),
        artifact_store: Arc::new(SqliteArtifactStore::new(factory.clone())),
        pipeline_cache_store: Arc::new(SqliteCacheStore::new(factory.clone())),
        pipeline_secret_store: Arc::new(SqlitePipelineSecretStore::new(factory)),
    })
}
