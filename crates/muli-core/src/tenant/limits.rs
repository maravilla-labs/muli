// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-tenant execution limits and configuration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::job::model::PriorityTier;

/// Per-tenant execution limits and configuration.
///
/// When all fields are at their zero/default values, the tenant has no
/// additional restrictions beyond server defaults (standalone mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantLimits {
    pub tenant_id: String,
    /// Maximum concurrent jobs. 0 = use server default.
    pub max_concurrent_jobs: u32,
    /// Maximum concurrent pipeline runs. 0 = unlimited.
    pub max_concurrent_pipelines: u32,
    /// Maximum pipeline runs per calendar day. 0 = unlimited.
    pub max_pipeline_runs_per_day: u32,
    /// Maximum number of repositories. 0 = unlimited.
    pub max_repos: u32,
    /// Maximum storage in bytes. 0 = unlimited (mirrors TenantQuota).
    pub max_storage_bytes: u64,
    /// Tenant-wide default priority tier.
    pub priority_tier: PriorityTier,
    /// Whether the tenant is suspended.
    pub suspended: bool,
    /// Reason for suspension (if any).
    pub suspended_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl TenantLimits {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            max_concurrent_jobs: 0,
            max_concurrent_pipelines: 0,
            max_pipeline_runs_per_day: 0,
            max_repos: 0,
            max_storage_bytes: 0,
            priority_tier: PriorityTier::Standard,
            suspended: false,
            suspended_reason: None,
            updated_at: Utc::now(),
        }
    }
}
