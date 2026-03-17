// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared helper functions for pipeline service RPCs.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Status;
use tracing::warn;

use muli_core::pipeline::{FailureStrategy as DomainFailureStrategy, StepRun as DomainStep};
use muli_core::traits::{PipelineSecretStore, StepRunStore};

/// Parse YAML, expand matrix, create StepRun records for a run.
pub async fn create_steps_from_yaml(
    yaml_content: &str,
    run_id: &str,
    tenant_id: &str,
    step_store: &Arc<dyn StepRunStore>,
) -> Result<Vec<DomainStep>, Status> {
    let pipeline_def = muli_pipeline::yaml::parser::parse_pipeline(yaml_content)
        .map_err(|e| Status::invalid_argument(format!("Invalid pipeline YAML: {e}")))?;

    muli_pipeline::yaml::validation::validate_pipeline(&pipeline_def)
        .map_err(|e| Status::invalid_argument(format!("Pipeline validation failed: {e}")))?;

    let mut all_steps = Vec::new();
    for step_def in &pipeline_def.steps {
        let expanded = muli_pipeline::dag::matrix::expand_matrix(step_def)
            .map_err(|e| Status::invalid_argument(format!("Matrix expansion failed: {e}")))?;

        for es in expanded {
            let failure_strategy = match es.failure_strategy.as_deref() {
                Some("continue") => DomainFailureStrategy::Continue,
                Some("ignore") => DomainFailureStrategy::Ignore,
                _ => DomainFailureStrategy::Stop,
            };
            let step = DomainStep::new(
                run_id.to_string(),
                tenant_id.to_string(),
                es.name.clone(),
                failure_strategy,
                None,
            );
            step_store
                .create_step(&step)
                .await
                .map_err(|e| Status::internal(format!("Failed to create step: {e}")))?;
            all_steps.push(step);
        }
    }
    Ok(all_steps)
}

/// Resolve muli-native pipeline secrets and merge with caller env_vars.
pub async fn resolve_env_vars(
    secret_store: &Arc<dyn PipelineSecretStore>,
    tenant_id: &str,
    repo_id: &str,
    caller_env_vars: HashMap<String, String>,
) -> Result<HashMap<String, String>, Status> {
    let mut env = HashMap::new();

    // Muli's own pipeline secrets (from PipelineSecretStore)
    let secret_names = secret_store
        .list_names(tenant_id, repo_id)
        .await
        .map_err(|e| Status::internal(format!("Failed to list secrets: {e}")))?;

    for name in secret_names {
        if let Ok(Some(_secret)) = secret_store.get_secret(tenant_id, repo_id, &name).await {
            // SECURITY: muli-native secrets are AES-256-GCM encrypted at rest.
            // Decryption requires `pipeline_secret_encryption_key` from server config,
            // which is not yet wired into this code path. Skip these secrets rather
            // than injecting raw ciphertext into step environments.
            warn!(
                secret_name = %name,
                repo_id = %repo_id,
                "skipping muli-native secret: decryption not yet implemented"
            );
        }
    }

    // Caller-provided env_vars override muli-native secrets
    env.extend(caller_env_vars);
    Ok(env)
}
