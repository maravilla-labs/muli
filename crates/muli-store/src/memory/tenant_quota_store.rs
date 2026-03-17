// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory tenant quota tracking store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::registry::model::TenantQuota;
use muli_core::traits::TenantQuotaStore;

/// In-memory implementation of TenantQuotaStore for testing.
#[derive(Debug, Clone)]
pub struct MemoryTenantQuotaStore {
    /// Keyed by tenant_id.
    quotas: Arc<DashMap<String, TenantQuota>>,
}

impl MemoryTenantQuotaStore {
    pub fn new() -> Self {
        Self {
            quotas: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryTenantQuotaStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TenantQuotaStore for MemoryTenantQuotaStore {
    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>> {
        Ok(self
            .quotas
            .get(tenant_id)
            .map(|entry| entry.value().clone()))
    }

    async fn set_quota(&self, tenant_id: &str, max_storage_bytes: u64) -> Result<()> {
        self.quotas
            .entry(tenant_id.to_string())
            .and_modify(|q| q.max_storage_bytes = max_storage_bytes)
            .or_insert_with(|| TenantQuota::new(tenant_id.to_string(), max_storage_bytes));
        Ok(())
    }

    async fn update_usage(&self, tenant_id: &str, current_usage_bytes: u64) -> Result<()> {
        let mut entry = self
            .quotas
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantQuota {
                tenant_id: tenant_id.to_string(),
                max_storage_bytes: 0,
                current_usage_bytes: 0,
            });
        entry.value_mut().current_usage_bytes = current_usage_bytes;
        Ok(())
    }

    async fn adjust_usage(&self, tenant_id: &str, delta: i64) -> Result<()> {
        let mut entry = self
            .quotas
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantQuota {
                tenant_id: tenant_id.to_string(),
                max_storage_bytes: 0,
                current_usage_bytes: 0,
            });
        let current = entry.value().current_usage_bytes as i64;
        entry.value_mut().current_usage_bytes = (current + delta).max(0) as u64;
        Ok(())
    }

    async fn try_reserve(&self, tenant_id: &str, additional_bytes: u64) -> Result<bool> {
        let mut entry = self
            .quotas
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantQuota {
                tenant_id: tenant_id.to_string(),
                max_storage_bytes: 0,
                current_usage_bytes: 0,
            });
        let q = entry.value();
        if q.max_storage_bytes > 0
            && q.current_usage_bytes.saturating_add(additional_bytes) > q.max_storage_bytes
        {
            return Ok(false);
        }
        entry.value_mut().current_usage_bytes =
            q.current_usage_bytes.saturating_add(additional_bytes);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_and_get_quota() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("tenant-1", 1_000_000).await.unwrap();

        let quota = store.get_quota("tenant-1").await.unwrap().unwrap();
        assert_eq!(quota.max_storage_bytes, 1_000_000);
        assert_eq!(quota.current_usage_bytes, 0);
    }

    #[tokio::test]
    async fn test_get_nonexistent_quota() {
        let store = MemoryTenantQuotaStore::new();
        let result = store.get_quota("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_set_quota_upsert() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("tenant-1", 1_000_000).await.unwrap();
        store.set_quota("tenant-1", 2_000_000).await.unwrap();

        let quota = store.get_quota("tenant-1").await.unwrap().unwrap();
        assert_eq!(quota.max_storage_bytes, 2_000_000);
    }

    #[tokio::test]
    async fn test_update_usage() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("tenant-1", 1_000_000).await.unwrap();
        store.update_usage("tenant-1", 500_000).await.unwrap();

        let quota = store.get_quota("tenant-1").await.unwrap().unwrap();
        assert_eq!(quota.current_usage_bytes, 500_000);
    }

    #[tokio::test]
    async fn test_update_usage_upserts_when_no_quota() {
        let store = MemoryTenantQuotaStore::new();
        store.update_usage("nonexistent", 100).await.unwrap();
        let quota = store.get_quota("nonexistent").await.unwrap().unwrap();
        assert_eq!(quota.current_usage_bytes, 100);
        assert_eq!(quota.max_storage_bytes, 0);
    }

    #[tokio::test]
    async fn test_quota_would_exceed() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("tenant-1", 1000).await.unwrap();
        store.update_usage("tenant-1", 900).await.unwrap();

        let quota = store.get_quota("tenant-1").await.unwrap().unwrap();
        assert!(!quota.would_exceed(100));
        assert!(quota.would_exceed(101));
    }

    #[tokio::test]
    async fn test_adjust_usage_increment() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("t1", 1_000_000).await.unwrap();
        store.adjust_usage("t1", 100).await.unwrap();
        store.adjust_usage("t1", 200).await.unwrap();
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 300);
    }

    #[tokio::test]
    async fn test_adjust_usage_decrement() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("t1", 1_000_000).await.unwrap();
        store.update_usage("t1", 500).await.unwrap();
        store.adjust_usage("t1", -200).await.unwrap();
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 300);
    }

    #[tokio::test]
    async fn test_adjust_usage_floors_at_zero() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("t1", 1_000_000).await.unwrap();
        store.update_usage("t1", 100).await.unwrap();
        store.adjust_usage("t1", -500).await.unwrap();
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 0);
    }

    #[tokio::test]
    async fn test_adjust_usage_upserts() {
        let store = MemoryTenantQuotaStore::new();
        store.adjust_usage("new-tenant", 250).await.unwrap();
        let q = store.get_quota("new-tenant").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 250);
        assert_eq!(q.max_storage_bytes, 0);
    }

    #[tokio::test]
    async fn test_try_reserve_within_quota() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("t1", 1000).await.unwrap();
        assert!(store.try_reserve("t1", 500).await.unwrap());
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 500);
    }

    #[tokio::test]
    async fn test_try_reserve_exceeds_quota() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("t1", 1000).await.unwrap();
        store.update_usage("t1", 900).await.unwrap();
        assert!(!store.try_reserve("t1", 200).await.unwrap());
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 900);
    }

    #[tokio::test]
    async fn test_try_reserve_no_quota_row() {
        let store = MemoryTenantQuotaStore::new();
        assert!(store.try_reserve("new", 500).await.unwrap());
        let q = store.get_quota("new").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 500);
    }

    #[tokio::test]
    async fn test_try_reserve_concurrent_serialized() {
        let store = MemoryTenantQuotaStore::new();
        store.set_quota("t1", 1000).await.unwrap();
        assert!(store.try_reserve("t1", 600).await.unwrap());
        assert!(!store.try_reserve("t1", 500).await.unwrap());
        assert!(store.try_reserve("t1", 400).await.unwrap());
        let q = store.get_quota("t1").await.unwrap().unwrap();
        assert_eq!(q.current_usage_bytes, 1000);
    }
}
