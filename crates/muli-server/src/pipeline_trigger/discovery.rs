// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Discovery phase: resolve the repository, read the pipeline configs from the
//! commit, and gather commit metadata plus the repo's known pipelines.

use std::path::PathBuf;

use tracing::{error, warn};

use muli_core::git::Repository;
use muli_core::pipeline::Pipeline;
use muli_pipeline::trigger::reader::{PipelineFile, read_pipeline_files};

use super::PipelineTriggerImpl;
use super::git_meta::resolve_commit_info;

/// Everything the per-pipeline planning loop needs, resolved once per trigger.
pub(crate) struct Discovery {
    pub repo: Repository,
    pub pipeline_files: Vec<PipelineFile>,
    pub commit_message: String,
    pub commit_author: String,
    pub known_pipelines: Vec<Pipeline>,
}

impl PipelineTriggerImpl {
    /// Look up the repository, read the pipeline YAML files from the commit, and
    /// collect commit metadata + the repo's registered pipelines.
    ///
    /// Returns `None` (drop the trigger) when the repo is missing, the YAML read
    /// fails, there are no pipeline files, or listing pipelines fails.
    pub(crate) async fn discover(
        &self,
        tenant_id: &str,
        repo_id: &str,
        commit_sha: &str,
    ) -> Option<Discovery> {
        // 1. Look up the repository to get namespace/name for the file path
        let repo = match self.repo_store.get_repository(repo_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                warn!(repo_id = %repo_id, "pipeline trigger: repository not found");
                return None;
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to look up repository");
                return None;
            }
        };

        // 2. Resolve the bare repo path on disk
        let repo_path: PathBuf = self
            .git_storage
            .repo_path(tenant_id, &repo.namespace, &repo.name);

        // 3. Read all supported pipeline configs from the commit
        let pipeline_files = match tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            let sha = commit_sha.to_string();
            move || read_pipeline_files(&repo_path, &sha)
        })
        .await
        {
            Ok(Ok(files)) if files.is_empty() => return None,
            Ok(Ok(files)) => files,
            Ok(Err(e)) => {
                warn!(error = %e, "pipeline trigger: failed to read pipeline YAML");
                return None;
            }
            Err(e) => {
                error!(error = %e, "pipeline trigger: spawn_blocking panicked");
                return None;
            }
        };

        // Extract commit metadata for display
        let (commit_message, commit_author) = (tokio::task::spawn_blocking({
            let rp = repo_path.clone();
            let sha = commit_sha.to_string();
            move || resolve_commit_info(&rp, &sha)
        })
        .await)
            .unwrap_or_default();

        let known_pipelines = match self.pipeline_store.get_by_repo(tenant_id, repo_id).await {
            Ok(pipelines) => pipelines,
            Err(e) => {
                error!(error = %e, "pipeline trigger: failed to list pipelines for repo");
                return None;
            }
        };

        Some(Discovery {
            repo,
            pipeline_files,
            commit_message,
            commit_author,
            known_pipelines,
        })
    }
}
