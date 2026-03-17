// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Stale job watchdog: detects and fails jobs stuck in non-terminal states.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use muli_core::job::model::JobResult;
use muli_core::job::state_machine::JobState;
use muli_core::traits::JobStore;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Spawn a background task that periodically checks for stale jobs.
///
/// A job is considered stale if it has been in `Running` or `Scheduled` state
/// for longer than `grace_period` without any updates.
pub fn spawn_stale_job_watchdog(
    job_store: Arc<dyn JobStore>,
    cancel: CancellationToken,
    interval: Duration,
    grace_period: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    check_stale_jobs(&job_store, grace_period).await;
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    });
}

async fn check_stale_jobs(job_store: &Arc<dyn JobStore>, grace_period: Duration) {
    let grace = chrono::Duration::from_std(grace_period).unwrap_or(chrono::Duration::seconds(300));

    for state in [JobState::Running, JobState::Scheduled] {
        let jobs = match job_store.list_by_state(state).await {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, state = ?state, "watchdog: failed to list jobs");
                continue;
            }
        };

        for job in jobs {
            let threshold = job.updated_at + grace;
            if Utc::now() < threshold {
                continue;
            }

            warn!(
                job_id = %job.id,
                state = ?state,
                updated_at = %job.updated_at,
                "watchdog: stale job detected, transitioning to Failed"
            );

            match job_store
                .update_state(&job.id, state, JobState::Failed)
                .await
            {
                Ok(()) => {
                    let mut updated = job.clone();
                    updated.state = JobState::Failed;
                    updated.result = Some(JobResult {
                        exit_code: None,
                        message: format!("Stale job detected by watchdog (stuck in {state:?})"),
                        container_id: None,
                    });
                    updated.finished_at = Some(Utc::now());
                    if let Err(e) = job_store.update_job(&updated).await {
                        warn!(job_id = %job.id, error = %e, "watchdog: failed to persist stale job result");
                    }
                }
                Err(e) => {
                    warn!(
                        job_id = %job.id,
                        error = %e,
                        "watchdog: failed to transition stale job to Failed"
                    );
                }
            }
        }
    }
}
