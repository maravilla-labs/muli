// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage trait for per-tenant execution limits.

use async_trait::async_trait;

use crate::error::Result;
use crate::tenant::limits::TenantLimits;

/// Storage for per-tenant execution limits and daily run counters.
#[async_trait]
pub trait TenantLimitsStore: Send + Sync {
    /// Get limits for a tenant. Returns None if no limits are configured.
    async fn get_limits(&self, tenant_id: &str) -> Result<Option<TenantLimits>>;
    /// Set (create or update) limits for a tenant.
    async fn set_limits(&self, limits: &TenantLimits) -> Result<()>;
    /// Check if a tenant is suspended.
    async fn is_suspended(&self, tenant_id: &str) -> Result<bool>;
    /// Atomically increment and return the daily pipeline run count.
    async fn increment_daily_run_count(&self, tenant_id: &str) -> Result<u64>;
    /// Get the current daily pipeline run count.
    async fn get_daily_run_count(&self, tenant_id: &str) -> Result<u64>;
}
