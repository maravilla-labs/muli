// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Semaphore-based concurrency limiter with per-tenant and global limits.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use muli_core::error::MuliError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// RAII guard that releases both global and per-tenant concurrency slots on drop.
pub struct ConcurrencyPermit {
    _global_permit: OwnedSemaphorePermit,
    tenant_counter: Arc<AtomicUsize>,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        self.tenant_counter.fetch_sub(1, Ordering::Release);
    }
}

/// Limits concurrent job execution globally and per-tenant.
pub struct ConcurrencyLimiter {
    global: Arc<Semaphore>,
    per_tenant: DashMap<String, Arc<AtomicUsize>>,
    per_tenant_limit: usize,
}

impl ConcurrencyLimiter {
    pub fn new(global_limit: usize, per_tenant_limit: usize) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global_limit)),
            per_tenant: DashMap::new(),
            per_tenant_limit,
        }
    }

    /// Try to acquire both a global and per-tenant concurrency slot.
    pub fn try_acquire(&self, tenant_id: &str) -> muli_core::error::Result<ConcurrencyPermit> {
        // Acquire global slot first
        let global_permit =
            self.global.clone().try_acquire_owned().map_err(|_| {
                MuliError::ConcurrencyLimit("global concurrency limit reached".into())
            })?;

        // Get or create the tenant counter
        let counter = {
            let entry = self
                .per_tenant
                .entry(tenant_id.to_string())
                .or_insert_with(|| Arc::new(AtomicUsize::new(0)));
            entry.value().clone()
        };

        // CAS loop to atomically increment if under limit
        loop {
            let current = counter.load(Ordering::Acquire);
            if current >= self.per_tenant_limit {
                // Release global permit before returning error
                drop(global_permit);
                return Err(MuliError::ConcurrencyLimit(format!(
                    "tenant {tenant_id} concurrency limit reached"
                )));
            }
            if counter
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        Ok(ConcurrencyPermit {
            _global_permit: global_permit,
            tenant_counter: counter,
        })
    }

    /// Total number of active jobs across all tenants.
    pub fn active_count(&self) -> usize {
        self.per_tenant
            .iter()
            .map(|entry| entry.value().load(Ordering::Acquire))
            .sum()
    }

    /// Number of active jobs for a specific tenant.
    pub fn active_count_for_tenant(&self, tenant_id: &str) -> usize {
        self.per_tenant
            .get(tenant_id)
            .map(|counter| counter.load(Ordering::Acquire))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_and_release() {
        let limiter = ConcurrencyLimiter::new(10, 3);

        let permit = limiter.try_acquire("tenant1").unwrap();
        assert_eq!(limiter.active_count(), 1);
        assert_eq!(limiter.active_count_for_tenant("tenant1"), 1);

        drop(permit);
        assert_eq!(limiter.active_count(), 0);
        assert_eq!(limiter.active_count_for_tenant("tenant1"), 0);
    }

    #[test]
    fn test_global_limit() {
        let limiter = ConcurrencyLimiter::new(2, 10);

        let _p1 = limiter.try_acquire("t1").unwrap();
        let _p2 = limiter.try_acquire("t2").unwrap();

        // Third should fail (global limit = 2)
        let result = limiter.try_acquire("t3");
        assert!(result.is_err());
        assert_eq!(limiter.active_count(), 2);
    }

    #[test]
    fn test_per_tenant_limit() {
        let limiter = ConcurrencyLimiter::new(10, 2);

        let _p1 = limiter.try_acquire("tenant1").unwrap();
        let _p2 = limiter.try_acquire("tenant1").unwrap();

        // Third for same tenant should fail (per-tenant limit = 2)
        let result = limiter.try_acquire("tenant1");
        assert!(result.is_err());

        // Different tenant should still work
        let _p3 = limiter.try_acquire("tenant2").unwrap();
        assert_eq!(limiter.active_count(), 3);
    }

    #[test]
    fn test_release_frees_slot() {
        let limiter = ConcurrencyLimiter::new(10, 1);

        let permit = limiter.try_acquire("tenant1").unwrap();
        assert!(limiter.try_acquire("tenant1").is_err());

        drop(permit);
        // Slot freed, should succeed now
        let _permit = limiter.try_acquire("tenant1").unwrap();
        assert_eq!(limiter.active_count_for_tenant("tenant1"), 1);
    }

    #[test]
    fn test_active_count_for_unknown_tenant() {
        let limiter = ConcurrencyLimiter::new(10, 3);
        assert_eq!(limiter.active_count_for_tenant("unknown"), 0);
    }
}
