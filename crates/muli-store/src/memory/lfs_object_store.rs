// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory LFS object index (which oids belong to which repository).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::traits::LfsObjectStore;

#[derive(Debug, Clone, Default)]
pub struct MemoryLfsObjectStore {
    /// (tenant_id, repo_id, oid)
    rows: Arc<Mutex<HashSet<(String, String, String)>>>,
    migrations: Arc<Mutex<HashSet<String>>>,
}

impl MemoryLfsObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LfsObjectStore for MemoryLfsObjectStore {
    async fn add_lfs_object(&self, tenant_id: &str, repo_id: &str, oid: &str) -> Result<()> {
        self.rows.lock().unwrap().insert((
            tenant_id.to_string(),
            repo_id.to_string(),
            oid.to_string(),
        ));
        Ok(())
    }

    async fn add_lfs_objects(&self, tenant_id: &str, repo_id: &str, oids: &[String]) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        for oid in oids {
            rows.insert((tenant_id.to_string(), repo_id.to_string(), oid.clone()));
        }
        Ok(())
    }

    async fn has_lfs_object(&self, tenant_id: &str, repo_id: &str, oid: &str) -> Result<bool> {
        Ok(self.rows.lock().unwrap().contains(&(
            tenant_id.to_string(),
            repo_id.to_string(),
            oid.to_string(),
        )))
    }

    async fn copy_lfs_objects(
        &self,
        tenant_id: &str,
        from_repo_id: &str,
        to_repo_id: &str,
    ) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let copied: Vec<_> = rows
            .iter()
            .filter(|(t, r, _)| t == tenant_id && r == from_repo_id)
            .map(|(t, _, o)| (t.clone(), to_repo_id.to_string(), o.clone()))
            .collect();
        rows.extend(copied);
        Ok(rows
            .iter()
            .filter(|(t, r, _)| t == tenant_id && r == to_repo_id)
            .count() as u64)
    }

    async fn remove_repo_lfs_objects(&self, tenant_id: &str, repo_id: &str) -> Result<()> {
        self.rows
            .lock()
            .unwrap()
            .retain(|(t, r, _)| !(t == tenant_id && r == repo_id));
        Ok(())
    }

    async fn lfs_migration_done(&self, name: &str) -> Result<bool> {
        Ok(self.migrations.lock().unwrap().contains(name))
    }

    async fn mark_lfs_migration_done(&self, name: &str) -> Result<()> {
        self.migrations.lock().unwrap().insert(name.to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rows_are_per_repository() {
        let s = MemoryLfsObjectStore::new();
        s.add_lfs_object("t", "a", "o1").await.unwrap();
        assert!(s.has_lfs_object("t", "a", "o1").await.unwrap());
        assert!(!s.has_lfs_object("t", "b", "o1").await.unwrap());
        assert!(!s.has_lfs_object("u", "a", "o1").await.unwrap());
        assert_eq!(s.copy_lfs_objects("t", "a", "b").await.unwrap(), 1);
        assert!(s.has_lfs_object("t", "b", "o1").await.unwrap());
        s.remove_repo_lfs_objects("t", "a").await.unwrap();
        assert!(!s.has_lfs_object("t", "a", "o1").await.unwrap());
        assert!(s.has_lfs_object("t", "b", "o1").await.unwrap());
    }
}
