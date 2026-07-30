// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared helper functions for pipeline service RPCs.

use std::sync::Arc;

use tonic::Status;

use muli_core::pipeline::{FailureStrategy as DomainFailureStrategy, StepRun as DomainStep};
use muli_core::traits::StepRunStore;

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
    if !pipeline_def.jobs.is_empty() {
        for (job_name, job_def) in &pipeline_def.jobs {
            let expanded = muli_pipeline::dag::matrix::expand_job_matrix(job_name, job_def)
                .map_err(|e| Status::invalid_argument(format!("Matrix expansion failed: {e}")))?;

            for (step_name, matrix_values) in expanded {
                let failure_strategy = match job_def.failure_strategy.as_deref() {
                    Some("continue") => DomainFailureStrategy::Continue,
                    Some("ignore") => DomainFailureStrategy::Ignore,
                    _ => DomainFailureStrategy::Stop,
                };
                let mut step = DomainStep::new(
                    run_id.to_string(),
                    tenant_id.to_string(),
                    step_name,
                    failure_strategy,
                    matrix_values,
                );
                step.depends_on = job_def.needs.clone();
                step_store
                    .create_step(&step)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to create step: {e}")))?;
                all_steps.push(step);
            }
        }
    } else {
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
    }
    Ok(all_steps)
}
