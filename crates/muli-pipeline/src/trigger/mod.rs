// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod matcher;
pub mod reader;

pub use matcher::PipelineEvent;

use async_trait::async_trait;

/// Hook interface for triggering pipelines from git events.
#[async_trait]
pub trait PipelineTriggerHook: Send + Sync {
    /// Called after a successful push to a repository.
    async fn on_push(
        &self,
        tenant_id: &str,
        repo_id: &str,
        old_sha: &str,
        new_sha: &str,
        ref_name: &str,
    );
    /// Called when a pull request event occurs.
    async fn on_pr_event(&self, tenant_id: &str, repo_id: &str, pr_number: u64, event: &str);
}
