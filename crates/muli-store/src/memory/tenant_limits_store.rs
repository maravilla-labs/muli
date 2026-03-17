// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory tenant limits store for testing.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::tenant::limits::TenantLimits;
use muli_core::traits::TenantLimitsStore;

#[derive(Debug, Clone)]
pub struct MemoryTenantLimitsStore {
    limits: Arc<DashMap<String, TenantLimits>>,
    /// Keyed by "tenant_id:YYYY-MM-DD".
    daily_counts: Arc<DashMap<String, u64>>,
}

impl MemoryTenantLimitsStore {
    pub fn new() -> Self {
        Self {
            limits: Arc::new(DashMap::new()),
            daily_counts: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryTenantLimitsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TenantLimitsStore for MemoryTenantLimitsStore {
    async fn get_limits(&self, tenant_id: &str) -> Result<Option<TenantLimits>> {
        Ok(self.limits.get(tenant_id).map(|e| e.value().clone()))
    }

    async fn set_limits(&self, limits: &TenantLimits) -> Result<()> {
        self.limits.insert(limits.tenant_id.clone(), limits.clone());
        Ok(())
    }

    async fn is_suspended(&self, tenant_id: &str) -> Result<bool> {
        Ok(self
            .limits
            .get(tenant_id)
            .map(|e| e.suspended)
            .unwrap_or(false))
    }

    async fn increment_daily_run_count(&self, tenant_id: &str) -> Result<u64> {
        let key = format!("{}:{}", tenant_id, Utc::now().format("%Y-%m-%d"));
        let mut entry = self.daily_counts.entry(key).or_insert(0);
        *entry.value_mut() += 1;
        Ok(*entry.value())
    }

    async fn get_daily_run_count(&self, tenant_id: &str) -> Result<u64> {
        let key = format!("{}:{}", tenant_id, Utc::now().format("%Y-%m-%d"));
        Ok(self.daily_counts.get(&key).map(|e| *e.value()).unwrap_or(0))
    }
}
