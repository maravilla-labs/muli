// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Step expansion: turn a pipeline's jobs (or legacy steps) plus their matrix
//! variants into persisted `StepRun` records.

use std::collections::HashMap;

use tracing::{error, warn};

use muli_core::pipeline::{FailureStrategy, PipelineRun, StepRun};
use muli_pipeline::yaml::schema::PipelineDef;

use super::PipelineTriggerImpl;

impl PipelineTriggerImpl {
    /// 9. Expand steps/jobs (including matrix expansion) and create `StepRun`
    /// records for the run. Returns the created steps.
    pub(crate) async fn expand_steps(
        &self,
        tenant_id: &str,
        pipeline_def: &PipelineDef,
        run: &PipelineRun,
    ) -> Vec<StepRun> {
        let mut all_steps = Vec::new();

        if !pipeline_def.jobs.is_empty() {
            // Jobs mode: expand matrix job variants and record depends_on.
            for (job_name, job_def) in &pipeline_def.jobs {
                let expanded =
                    match muli_pipeline::dag::matrix::expand_job_matrix(job_name, job_def) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!(error = %e, job = %job_name, "job matrix expansion failed");
                            continue;
                        }
                    };
                for (step_name, matrix_values) in expanded {
                    let failure_strategy = match job_def.failure_strategy.as_deref() {
                        Some("continue") => FailureStrategy::Continue,
                        Some("ignore") => FailureStrategy::Ignore,
                        _ => FailureStrategy::Stop,
                    };
                    let mut step_run = StepRun::new(
                        run.id.clone(),
                        tenant_id.to_string(),
                        step_name.clone(),
                        failure_strategy,
                        matrix_values,
                    );
                    step_run.depends_on = job_def.needs.clone();
                    if let Err(e) = self.step_store.create_step(&step_run).await {
                        error!(error = %e, step = %step_name, "failed to create job step run");
                        continue;
                    }
                    all_steps.push(step_run);
                }
            }
        } else {
            // Steps mode (legacy): expand matrix steps.
            let mut conditions = HashMap::new();
            for step_def in &pipeline_def.steps {
                let expanded = match muli_pipeline::dag::matrix::expand_matrix(step_def) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(error = %e, step = %step_def.name, "matrix expansion failed");
                        continue;
                    }
                };
                for expanded_step in expanded {
                    let failure_strategy = match expanded_step.failure_strategy.as_deref() {
                        Some("continue") => FailureStrategy::Continue,
                        Some("ignore") => FailureStrategy::Ignore,
                        _ => FailureStrategy::Stop,
                    };
                    let mut step_run = StepRun::new(
                        run.id.clone(),
                        tenant_id.to_string(),
                        expanded_step.name.clone(),
                        failure_strategy,
                        None,
                    );
                    step_run.depends_on = expanded_step.needs.clone();
                    conditions.insert(expanded_step.name.clone(), expanded_step.condition.clone());
                    if let Err(e) = self.step_store.create_step(&step_run).await {
                        error!(
                            error = %e,
                            step = %expanded_step.name,
                            "failed to create step run"
                        );
                        continue;
                    }
                    all_steps.push(step_run);
                }
            }
            let _ = conditions; // used to evaluate conditions in executor
        }

        all_steps
    }
}
