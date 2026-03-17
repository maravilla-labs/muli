// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Storage traits for pipeline entities.

use async_trait::async_trait;

use crate::error::Result;
use crate::pipeline::{
    Artifact, CacheEntry, Pipeline, PipelineRun, PipelineRunState, PipelineSecret, StepRun,
    StepRunState,
};

/// Storage for pipeline definitions.
#[async_trait]
pub trait PipelineStore: Send + Sync {
    /// Create or update a pipeline definition.
    async fn upsert_pipeline(&self, pipeline: &Pipeline) -> Result<()>;
    /// Get a pipeline by ID.
    async fn get_pipeline(&self, tenant_id: &str, pipeline_id: &str) -> Result<Option<Pipeline>>;
    /// Get all pipelines for a repository.
    async fn get_by_repo(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<Pipeline>>;
    /// Delete a pipeline.
    async fn delete_pipeline(&self, tenant_id: &str, pipeline_id: &str) -> Result<()>;
}

/// Storage for pipeline run records.
#[async_trait]
pub trait PipelineRunStore: Send + Sync {
    /// Create a new pipeline run.
    async fn create_run(&self, run: &PipelineRun) -> Result<String>;
    /// Get a run by ID.
    async fn get_run(&self, tenant_id: &str, run_id: &str) -> Result<Option<PipelineRun>>;
    /// Get a run by pipeline ID and run number.
    async fn get_run_by_number(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        run_number: u64,
    ) -> Result<Option<PipelineRun>>;
    /// List runs for a repository, newest first.
    async fn list_by_repo(
        &self,
        tenant_id: &str,
        repo_id: &str,
        state: Option<PipelineRunState>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PipelineRun>>;
    /// Update a run (state, timestamps).
    async fn update_run(&self, run: &PipelineRun) -> Result<()>;
    /// Get the next run number for a pipeline.
    async fn next_run_number(&self, tenant_id: &str, pipeline_id: &str) -> Result<u64>;
    /// Delete terminal runs older than the given timestamp. Returns number deleted.
    async fn delete_old_runs(&self, before: chrono::DateTime<chrono::Utc>) -> Result<u64>;
    /// Count active (non-terminal) pipeline runs for a tenant.
    async fn count_active(&self, tenant_id: &str) -> Result<u64>;
}

/// Storage for step run records.
#[async_trait]
pub trait StepRunStore: Send + Sync {
    /// Create a step run record.
    async fn create_step(&self, step: &StepRun) -> Result<String>;
    /// Get a step run by ID.
    async fn get_step(&self, tenant_id: &str, step_id: &str) -> Result<Option<StepRun>>;
    /// List all steps for a pipeline run.
    async fn list_by_run(&self, tenant_id: &str, run_id: &str) -> Result<Vec<StepRun>>;
    /// Update a step run (state, job_id, timestamps).
    async fn update_step(&self, step: &StepRun) -> Result<()>;
    /// Update only the state of a step.
    async fn update_step_state(
        &self,
        tenant_id: &str,
        step_id: &str,
        state: StepRunState,
    ) -> Result<()>;
    /// Delete all step runs for a given pipeline run. Returns number deleted.
    async fn delete_by_run(&self, tenant_id: &str, run_id: &str) -> Result<u64>;
}

/// Storage for build artifacts.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Create an artifact record.
    async fn create_artifact(&self, artifact: &Artifact) -> Result<String>;
    /// Get an artifact by ID.
    async fn get_artifact(&self, tenant_id: &str, artifact_id: &str) -> Result<Option<Artifact>>;
    /// List artifacts for a pipeline run.
    async fn list_by_run(&self, tenant_id: &str, run_id: &str) -> Result<Vec<Artifact>>;
    /// Delete expired artifacts older than the given timestamp.
    async fn delete_expired(&self, before: chrono::DateTime<chrono::Utc>) -> Result<u64>;
}

/// Storage for dependency cache entries.
#[async_trait]
pub trait CacheStore: Send + Sync {
    /// Get a cache entry by exact key.
    async fn get_cache(
        &self,
        tenant_id: &str,
        repo_id: &str,
        cache_key: &str,
    ) -> Result<Option<CacheEntry>>;
    /// Find cache entries matching a key prefix (for restore_keys fallback).
    async fn find_by_prefix(
        &self,
        tenant_id: &str,
        repo_id: &str,
        prefix: &str,
    ) -> Result<Vec<CacheEntry>>;
    /// Create or update a cache entry.
    async fn upsert_cache(&self, entry: &CacheEntry) -> Result<()>;
    /// Delete a specific cache entry.
    async fn delete_cache(&self, tenant_id: &str, repo_id: &str, cache_key: &str) -> Result<()>;
    /// Evict least-recently-used entries until total size is under the limit.
    async fn evict_lru(&self, tenant_id: &str, repo_id: &str, max_bytes: u64) -> Result<u64>;
    /// List all cache entries for a repository.
    async fn list_by_repo(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<CacheEntry>>;
}

/// Storage for pipeline secrets.
#[async_trait]
pub trait PipelineSecretStore: Send + Sync {
    /// Set (create or update) a secret.
    async fn set_secret(&self, secret: &PipelineSecret) -> Result<()>;
    /// Get a secret by name.
    async fn get_secret(
        &self,
        tenant_id: &str,
        repo_id: &str,
        name: &str,
    ) -> Result<Option<PipelineSecret>>;
    /// List secret names for a repository (values are never returned in listings).
    async fn list_names(&self, tenant_id: &str, repo_id: &str) -> Result<Vec<String>>;
    /// Delete a secret.
    async fn delete_secret(&self, tenant_id: &str, repo_id: &str, name: &str) -> Result<()>;
}
