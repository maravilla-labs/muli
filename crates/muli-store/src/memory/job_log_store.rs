// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory job log store.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use muli_core::error::Result;
use muli_core::job::model::StoredLogLine;
use muli_core::traits::JobLogStore;

/// In-memory log store for testing.
#[derive(Debug, Clone, Default)]
pub struct MemoryJobLogStore {
    logs: Arc<DashMap<String, Vec<StoredLogLine>>>,
}

impl MemoryJobLogStore {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl JobLogStore for MemoryJobLogStore {
    async fn append_logs(&self, job_id: &str, lines: Vec<StoredLogLine>) -> Result<()> {
        self.logs
            .entry(job_id.to_string())
            .or_default()
            .extend(lines);
        Ok(())
    }

    async fn get_logs(&self, job_id: &str, tail: usize) -> Result<Vec<StoredLogLine>> {
        let all = self
            .logs
            .get(job_id)
            .map(|v| v.value().clone())
            .unwrap_or_default();
        let start = all.len().saturating_sub(tail);
        Ok(all[start..].to_vec())
    }
}
