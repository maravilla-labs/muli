// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory per-tenant registry visibility store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::registry::model::RegistryVisibilityLevel;
use muli_core::traits::RegistryVisibilityStore;

/// In-memory implementation of `RegistryVisibilityStore` for testing.
#[derive(Debug, Clone, Default)]
pub struct MemoryRegistryVisibilityStore {
    /// Keyed by tenant_id.
    visibility: Arc<DashMap<String, RegistryVisibilityLevel>>,
}

impl MemoryRegistryVisibilityStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RegistryVisibilityStore for MemoryRegistryVisibilityStore {
    async fn get_visibility(&self, tenant_id: &str) -> Result<Option<RegistryVisibilityLevel>> {
        Ok(self.visibility.get(tenant_id).map(|e| *e.value()))
    }

    async fn set_visibility(
        &self,
        tenant_id: &str,
        visibility: RegistryVisibilityLevel,
    ) -> Result<()> {
        self.visibility.insert(tenant_id.to_string(), visibility);
        Ok(())
    }
}
