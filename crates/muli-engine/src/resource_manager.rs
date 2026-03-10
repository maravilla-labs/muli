// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resource tracking and allocation (CPU, memory, GPU).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::Semaphore;
use tracing::{debug, info};

use muli_core::error::{MuliError, Result};

/// Tracks a single resource allocation.
#[derive(Debug, Clone)]
pub struct ResourceAllocation {
    pub job_id: String,
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
}

/// Manages CPU/memory slot tracking for concurrent jobs.
pub struct ResourceManager {
    total_cpu: u64,
    total_memory: u64,
    used_cpu: AtomicU64,
    used_memory: AtomicU64,
    slot_semaphore: Arc<Semaphore>,
    allocations: DashMap<String, ResourceAllocation>,
}

impl ResourceManager {
    pub fn new(total_cpu_millicores: u64, total_memory_bytes: u64, max_slots: usize) -> Self {
        info!(
            total_cpu_millicores = total_cpu_millicores,
            total_memory_bytes = total_memory_bytes,
            max_slots = max_slots,
            "ResourceManager initialized"
        );

        Self {
            total_cpu: total_cpu_millicores,
            total_memory: total_memory_bytes,
            used_cpu: AtomicU64::new(0),
            used_memory: AtomicU64::new(0),
            slot_semaphore: Arc::new(Semaphore::new(max_slots)),
            allocations: DashMap::new(),
        }
    }

    /// Try to allocate resources for a job. Returns the allocation on success.
    pub fn try_allocate(
        &self,
        job_id: &str,
        cpu_millicores: u64,
        memory_bytes: u64,
    ) -> Result<ResourceAllocation> {
        // Try to acquire a slot
        let _permit = self
            .slot_semaphore
            .try_acquire()
            .map_err(|_| MuliError::ConcurrencyLimit("No available execution slots".to_string()))?;
        // We intentionally forget the permit; we manage slots via allocations map
        std::mem::forget(_permit);

        // Atomically check and reserve CPU
        loop {
            let current = self.used_cpu.load(Ordering::SeqCst);
            let new_val = current + cpu_millicores;
            if new_val > self.total_cpu {
                // Release the slot we acquired
                self.slot_semaphore.add_permits(1);
                return Err(MuliError::InsufficientResources {
                    needed: format!("{cpu_millicores}m CPU"),
                    available: format!("{}m CPU", self.total_cpu - current),
                });
            }
            if self
                .used_cpu
                .compare_exchange(current, new_val, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }

        // Atomically check and reserve memory
        loop {
            let current = self.used_memory.load(Ordering::SeqCst);
            let new_val = current + memory_bytes;
            if new_val > self.total_memory {
                // Rollback CPU
                self.used_cpu.fetch_sub(cpu_millicores, Ordering::SeqCst);
                self.slot_semaphore.add_permits(1);
                return Err(MuliError::InsufficientResources {
                    needed: format!("{memory_bytes} bytes memory"),
                    available: format!("{} bytes memory", self.total_memory - current),
                });
            }
            if self
                .used_memory
                .compare_exchange(current, new_val, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }

        let allocation = ResourceAllocation {
            job_id: job_id.to_string(),
            cpu_millicores,
            memory_bytes,
        };

        self.allocations
            .insert(job_id.to_string(), allocation.clone());

        debug!(
            job_id = %job_id,
            cpu = cpu_millicores,
            memory = memory_bytes,
            "Resources allocated"
        );

        Ok(allocation)
    }

    /// Release resources held by a job.
    pub fn release(&self, job_id: &str) {
        if let Some((_, alloc)) = self.allocations.remove(job_id) {
            self.used_cpu
                .fetch_sub(alloc.cpu_millicores, Ordering::SeqCst);
            self.used_memory
                .fetch_sub(alloc.memory_bytes, Ordering::SeqCst);
            self.slot_semaphore.add_permits(1);

            debug!(
                job_id = %job_id,
                cpu = alloc.cpu_millicores,
                memory = alloc.memory_bytes,
                "Resources released"
            );
        }
    }

    /// Get current resource usage.
    pub fn usage(&self) -> ResourceUsage {
        ResourceUsage {
            used_cpu_millicores: self.used_cpu.load(Ordering::SeqCst),
            total_cpu_millicores: self.total_cpu,
            used_memory_bytes: self.used_memory.load(Ordering::SeqCst),
            total_memory_bytes: self.total_memory,
            active_slots: self.allocations.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub used_cpu_millicores: u64,
    pub total_cpu_millicores: u64,
    pub used_memory_bytes: u64,
    pub total_memory_bytes: u64,
    pub active_slots: usize,
}
