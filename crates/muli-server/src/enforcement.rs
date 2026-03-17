// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared enforcement helpers for tenant limits.
//!
//! All functions take `Option<&dyn TenantLimitsStore>`. When `None` or when
//! no limits are configured for the tenant, checks pass — zero impact on
//! standalone deployments.

use muli_core::traits::{JobStore, PipelineRunStore, RepositoryStore, TenantLimitsStore};
use tonic::Status;

/// Check that the tenant is not suspended.
pub async fn check_not_suspended(
    store: Option<&dyn TenantLimitsStore>,
    tenant_id: &str,
) -> Result<(), Status> {
    let Some(store) = store else { return Ok(()) };
    match store.is_suspended(tenant_id).await {
        Ok(true) => Err(Status::permission_denied(format!(
            "tenant {tenant_id} is suspended"
        ))),
        Ok(false) => Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "enforcement: failed to check suspension");
            Ok(()) // fail open
        }
    }
}

/// Check that the tenant has not exceeded its concurrent job limit.
pub async fn check_job_limit(
    store: Option<&dyn TenantLimitsStore>,
    job_store: &dyn JobStore,
    tenant_id: &str,
    server_default: usize,
) -> Result<(), Status> {
    let Some(limits_store) = store else {
        return Ok(());
    };
    let limits = match limits_store.get_limits(tenant_id).await {
        Ok(Some(l)) => l,
        Ok(None) => return Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "enforcement: failed to get tenant limits");
            return Ok(()); // fail open
        }
    };
    let max = if limits.max_concurrent_jobs > 0 {
        limits.max_concurrent_jobs as u64
    } else {
        server_default as u64
    };
    match job_store.count_active_by_tenant(tenant_id).await {
        Ok(active) if active >= max => Err(Status::resource_exhausted(format!(
            "concurrent job limit reached ({active}/{max})"
        ))),
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "enforcement: failed to count active jobs");
            Ok(()) // fail open
        }
    }
}

/// Check pipeline concurrency and daily run limits.
pub async fn check_pipeline_limits(
    store: Option<&dyn TenantLimitsStore>,
    run_store: &dyn PipelineRunStore,
    tenant_id: &str,
) -> Result<(), Status> {
    let Some(limits_store) = store else {
        return Ok(());
    };
    let limits = match limits_store.get_limits(tenant_id).await {
        Ok(Some(l)) => l,
        Ok(None) => return Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "enforcement: failed to get tenant limits");
            return Ok(());
        }
    };

    // Check concurrent pipeline limit
    if limits.max_concurrent_pipelines > 0 {
        match run_store.count_active(tenant_id).await {
            Ok(active) if active >= limits.max_concurrent_pipelines as u64 => {
                return Err(Status::resource_exhausted(format!(
                    "concurrent pipeline limit reached ({active}/{})",
                    limits.max_concurrent_pipelines
                )));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "enforcement: failed to count active pipelines");
            }
        }
    }

    // Check daily run limit
    if limits.max_pipeline_runs_per_day > 0 {
        match limits_store.get_daily_run_count(tenant_id).await {
            Ok(count) if count >= limits.max_pipeline_runs_per_day as u64 => {
                return Err(Status::resource_exhausted(format!(
                    "daily pipeline run limit reached ({count}/{})",
                    limits.max_pipeline_runs_per_day
                )));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "enforcement: failed to get daily run count");
            }
        }
    }

    Ok(())
}

/// Resolve the effective priority tier for a job submission.
/// If tenant has limits configured, always use the tenant's tier.
/// Otherwise fall back to the client-provided hint.
pub async fn resolve_effective_tier(
    store: Option<&dyn TenantLimitsStore>,
    tenant_id: &str,
    client_tier: i32,
) -> muli_core::job::model::PriorityTier {
    if let Some(store) = store {
        if let Ok(Some(limits)) = store.get_limits(tenant_id).await {
            return limits.priority_tier;
        }
    }
    // No limits configured — use client hint (standalone mode)
    crate::grpc::conversions::proto_tier_to_core(client_tier)
}

/// Check that the tenant has not exceeded its repository count limit.
pub async fn check_repo_limit(
    store: Option<&dyn TenantLimitsStore>,
    repo_store: &dyn RepositoryStore,
    tenant_id: &str,
) -> Result<(), Status> {
    let Some(limits_store) = store else {
        return Ok(());
    };
    let limits = match limits_store.get_limits(tenant_id).await {
        Ok(Some(l)) => l,
        Ok(None) => return Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "enforcement: failed to get tenant limits");
            return Ok(());
        }
    };
    if limits.max_repos == 0 {
        return Ok(());
    }
    match repo_store.count_by_tenant(tenant_id).await {
        Ok(count) if count >= limits.max_repos as u64 => Err(Status::resource_exhausted(format!(
            "repository limit reached ({count}/{})",
            limits.max_repos
        ))),
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "enforcement: failed to count repos");
            Ok(()) // fail open
        }
    }
}
