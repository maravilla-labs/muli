// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB collection schemas and BSON document types.

use mongodb::bson::doc;
use mongodb::options::IndexOptions;
use mongodb::{Database, IndexModel};

use muli_core::error::{MuliError, Result};

pub async fn setup_job_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("jobs");
    let indexes = vec![
        IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        IndexModel::builder()
            .keys(doc! { "state": 1, "priority_score": -1 })
            .build(),
        IndexModel::builder()
            .keys(doc! { "spec.tenant_id": 1 })
            .build(),
        IndexModel::builder().keys(doc! { "name": 1 }).build(),
        IndexModel::builder()
            .keys(doc! { "spec.tenant_id": 1, "spec.idempotency_key": 1 })
            .options(IndexOptions::builder().unique(true).sparse(true).build())
            .build(),
    ];
    collection
        .create_indexes(indexes)
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create job indexes: {e}")))?;
    Ok(())
}

pub async fn setup_job_log_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("job_logs");
    let indexes = vec![
        IndexModel::builder()
            .keys(doc! { "job_id": 1, "seq": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
    ];
    collection
        .create_indexes(indexes)
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create job log indexes: {e}")))?;
    Ok(())
}

pub async fn setup_agent_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("agents");
    let indexes = vec![
        IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        IndexModel::builder().keys(doc! { "status": 1 }).build(),
    ];
    collection
        .create_indexes(indexes)
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create agent indexes: {e}")))?;
    Ok(())
}

pub async fn setup_registry_token_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("registry_tokens");
    let indexes = vec![
        IndexModel::builder()
            .keys(doc! { "token_hash": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        IndexModel::builder().keys(doc! { "tenant_id": 1 }).build(),
        IndexModel::builder().keys(doc! { "expires_at": 1 }).build(),
    ];
    collection
        .create_indexes(indexes)
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create registry token indexes: {e}")))?;
    Ok(())
}

pub async fn setup_tenant_quota_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("tenant_quotas");
    let indexes = vec![
        IndexModel::builder()
            .keys(doc! { "tenant_id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
    ];
    collection
        .create_indexes(indexes)
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create tenant quota indexes: {e}")))?;
    Ok(())
}

pub async fn setup_git_indexes(db: &Database) -> Result<()> {
    // Repositories
    let repos = db.collection::<mongodb::bson::Document>("git_repositories");
    repos
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "id": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder()
                .keys(doc! { "tenant_id": 1, "namespace": 1, "name": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder().keys(doc! { "tenant_id": 1 }).build(),
            IndexModel::builder().keys(doc! { "fork_of": 1 }).build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create git repository indexes: {e}")))?;

    // Tokens
    let tokens = db.collection::<mongodb::bson::Document>("git_tokens");
    tokens
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "token_hash": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder().keys(doc! { "tenant_id": 1 }).build(),
            IndexModel::builder()
                .keys(doc! { "tenant_id": 1, "user_id": 1 })
                .build(),
            IndexModel::builder()
                .keys(doc! { "expires_at": 1 })
                .options(IndexOptions::builder().sparse(true).build())
                .build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create git token indexes: {e}")))?;

    // SSH keys
    let ssh_keys = db.collection::<mongodb::bson::Document>("git_ssh_keys");
    ssh_keys
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "fingerprint": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder().keys(doc! { "tenant_id": 1 }).build(),
            IndexModel::builder()
                .keys(doc! { "tenant_id": 1, "user_id": 1 })
                .build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create git SSH key indexes: {e}")))?;

    // Webhooks
    let webhooks = db.collection::<mongodb::bson::Document>("git_webhooks");
    webhooks
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "id": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder()
                .keys(doc! { "tenant_id": 1, "repo_id": 1 })
                .build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create git webhook indexes: {e}")))?;

    Ok(())
}

pub async fn setup_tenant_user_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("tenant_users");
    let indexes = vec![
        IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        IndexModel::builder()
            .keys(doc! { "tenant_id": 1, "handle": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        IndexModel::builder()
            .keys(doc! { "tenant_id": 1, "external_id": 1 })
            .options(IndexOptions::builder().unique(true).sparse(true).build())
            .build(),
        IndexModel::builder().keys(doc! { "tenant_id": 1 }).build(),
    ];
    collection
        .create_indexes(indexes)
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create tenant user indexes: {e}")))?;
    Ok(())
}

pub async fn setup_org_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("orgs");
    collection
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "id": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder()
                .keys(doc! { "tenant_id": 1, "handle": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder().keys(doc! { "tenant_id": 1 }).build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create org indexes: {e}")))?;

    let members = db.collection::<mongodb::bson::Document>("org_members");
    members
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "org_id": 1, "user_id": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder().keys(doc! { "org_id": 1 }).build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create org member indexes: {e}")))?;

    Ok(())
}

pub async fn setup_pull_request_indexes(db: &Database) -> Result<()> {
    let collection = db.collection::<mongodb::bson::Document>("pull_requests");
    collection
        .create_indexes(vec![
            IndexModel::builder()
                .keys(doc! { "id": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder()
                .keys(doc! { "repo_id": 1, "number": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            IndexModel::builder()
                .keys(doc! { "repo_id": 1, "state": 1 })
                .build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create pull request indexes: {e}")))?;

    let comments = db.collection::<mongodb::bson::Document>("pr_comments");
    comments
        .create_indexes(vec![
            IndexModel::builder().keys(doc! { "pr_id": 1 }).build(),
        ])
        .await
        .map_err(|e| MuliError::Storage(format!("Failed to create PR comment indexes: {e}")))?;

    Ok(())
}
