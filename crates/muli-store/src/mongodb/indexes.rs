// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB index definitions and initialization.

use mongodb::Database;

use muli_core::error::Result;

use super::schema;

/// Create all required indexes for all collections.
pub async fn setup_indexes(db: &Database) -> Result<()> {
    schema::setup_job_indexes(db).await?;
    schema::setup_job_log_indexes(db).await?;
    schema::setup_agent_indexes(db).await?;
    schema::setup_registry_token_indexes(db).await?;
    schema::setup_tenant_quota_indexes(db).await?;
    schema::setup_git_indexes(db).await?;
    schema::setup_tenant_user_indexes(db).await?;
    schema::setup_org_indexes(db).await?;
    schema::setup_pull_request_indexes(db).await?;
    Ok(())
}
