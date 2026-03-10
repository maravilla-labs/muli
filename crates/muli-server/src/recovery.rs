// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job recovery after server restart.

use std::sync::Arc;

use muli_core::job::state_machine::JobState;
use muli_core::traits::JobStore;
use muli_queue::Scheduler;
use tracing::{info, warn};

/// Reset interrupted jobs back to Pending and re-enqueue all Pending jobs.
pub async fn recover_jobs(job_store: &Arc<dyn JobStore>, scheduler: &Arc<Scheduler>) {
    info!("Recovering non-terminal jobs from previous run");
    for state in [JobState::Scheduled, JobState::Pulling, JobState::Running] {
        match job_store.list_by_state(state).await {
            Ok(jobs) => {
                for job in jobs {
                    if let Err(e) = job_store
                        .update_state(&job.id, state, JobState::Pending)
                        .await
                    {
                        warn!(
                            job_id = %job.id,
                            error = %e,
                            "Failed to reset job to Pending during startup recovery"
                        );
                    }
                }
            }
            Err(e) => warn!(error = %e, "Failed to list {:?} jobs for recovery", state),
        }
    }
    match job_store.list_by_state(JobState::Pending).await {
        Ok(jobs) => {
            let count = jobs.len();
            for job in &jobs {
                scheduler.enqueue(
                    job.id.clone(),
                    job.spec.priority_tier,
                    job.spec.tenant_id.clone(),
                );
            }
            if count > 0 {
                info!(count, "Re-enqueued pending jobs after startup recovery");
            }
        }
        Err(e) => warn!(error = %e, "Failed to list Pending jobs for recovery"),
    }
}
