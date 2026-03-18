// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embedded job execution pipeline.

use std::sync::Arc;

use dashmap::DashMap;
use tracing::{error, info, warn};

use muli_core::job::model::JobResult;
use muli_core::job::state_machine::JobState;
use muli_core::traits::{JobLogStore, JobStore};
use muli_engine::docker::logs::LogCollector;
use muli_engine::executor::DockerExecutor;
use muli_queue::{RetryPolicy, Scheduler};

/// Execute a single job: update states, run container, collect logs, record result.
pub async fn execute_job(
    job_id: String,
    store: Arc<dyn JobStore>,
    executor: Arc<DockerExecutor>,
    log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    log_store: Arc<dyn JobLogStore>,
    scheduler: Arc<Scheduler>,
    retry_policy: Arc<RetryPolicy>,
) {
    // Get the job from store
    let job = match store.get_job(&job_id).await {
        Ok(Some(j)) => j,
        Ok(None) => {
            error!(job_id = %job_id, "Job not found in store");
            return;
        }
        Err(e) => {
            error!(job_id = %job_id, error = %e, "Failed to get job from store");
            return;
        }
    };

    // Transition to Scheduled
    if let Err(e) = store
        .update_state(&job_id, JobState::Pending, JobState::Scheduled)
        .await
    {
        error!(job_id = %job_id, error = %e, "Failed to transition to Scheduled");
        return;
    }

    // Create log collector for this job and share it with the executor
    let log_collector = Arc::new(LogCollector::new());
    log_collectors.insert(job_id.clone(), log_collector.clone());

    // Transition to Running
    if let Err(e) = store
        .update_state(&job_id, JobState::Scheduled, JobState::Running)
        .await
    {
        log_collectors.remove(&job_id);
        error!(job_id = %job_id, error = %e, "Failed to transition to Running");
        return;
    }

    // Record job start metric
    crate::metrics::JOBS_RUNNING.inc();
    let started_at = chrono::Utc::now();

    // Spawn a periodic log-flush task so that step logs are visible in the UI while the job runs.
    // Every 2s, lines buffered since the last flush are drained and written to durable storage.
    // The ring buffer is not cleared, so live `stream_logs` / `get_logs` still see all lines.
    let flush_collector = log_collector.clone();
    let flush_store = log_store.clone();
    let flush_job_id = job_id.clone();
    let flush_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            let lines = flush_collector.peek_unflushed().await;
            if !lines.is_empty() {
                let stored = lines_to_stored(lines);
                if let Err(e) = flush_store.append_logs(&flush_job_id, stored).await {
                    tracing::warn!(job_id = %flush_job_id, error = %e, "Periodic log flush failed");
                }
            }
        }
    });

    // Execute the job (passes the shared log_collector)
    let exec_result = executor.execute_job(&job, log_collector.clone()).await;

    // Stop the periodic flush task.
    flush_handle.abort();

    // Persist any lines produced after the last periodic flush tick.
    drain_and_persist_logs(&log_collector, &*log_store, &job_id).await;

    match exec_result {
        Ok(result) => {
            let final_state = match result.exit_code {
                Some(0) => JobState::Succeeded,
                _ => JobState::Failed,
            };

            // Use CAS update_state to avoid clobbering a concurrent cancellation.
            match store
                .update_state(&job_id, JobState::Running, final_state)
                .await
            {
                Ok(()) => {
                    // State transition succeeded; persist the result and timestamps.
                    let mut updated_job = job.clone();
                    updated_job.state = final_state;
                    updated_job.result = Some(result);
                    updated_job.finished_at = Some(chrono::Utc::now());

                    if let Err(e) = store.update_job(&updated_job).await {
                        error!(job_id = %job_id, error = %e, "Failed to persist job result");
                    }
                }
                Err(e) => {
                    warn!(
                        job_id = %job_id,
                        error = %e,
                        "State transition Running -> {:?} rejected (job may have been cancelled)",
                        final_state
                    );
                }
            }

            record_completion_metrics(
                &job.spec.tenant_id,
                &job.spec.priority_tier,
                final_state,
                started_at,
            );
            info!(job_id = %job_id, state = ?final_state, "Job execution completed");
        }
        Err(e) => {
            // Check if we should retry before going to terminal state
            if let Some(delay) = retry_policy.should_retry(&e, job.retry_count) {
                info!(
                    job_id = %job_id,
                    attempt = job.retry_count + 1,
                    delay_secs = delay.as_secs(),
                    error = %e,
                    "Retrying job after transient failure"
                );

                // Reset to Pending and increment retry count
                match store
                    .update_state(&job_id, JobState::Running, JobState::Pending)
                    .await
                {
                    Ok(()) => {
                        let mut updated_job = job.clone();
                        updated_job.state = JobState::Pending;
                        updated_job.retry_count += 1;
                        updated_job.updated_at = chrono::Utc::now();
                        if let Err(ue) = store.update_job(&updated_job).await {
                            error!(job_id = %job_id, error = %ue, "Failed to update retry count");
                        }

                        // Schedule delayed re-enqueue
                        let scheduler = scheduler.clone();
                        let jid = job_id.clone();
                        let tier = job.spec.priority_tier;
                        let tid = job.spec.tenant_id.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            scheduler.enqueue(jid, tier, tid);
                        });
                    }
                    Err(cas_err) => {
                        warn!(
                            job_id = %job_id,
                            error = %cas_err,
                            "Failed to reset job for retry (job may have been cancelled)"
                        );
                    }
                }

                record_completion_metrics(
                    &job.spec.tenant_id,
                    &job.spec.priority_tier,
                    JobState::Failed,
                    started_at,
                );
                log_collectors.remove(&job_id);
                return;
            }

            let final_state = if e.is_retryable() {
                JobState::Failed
            } else {
                match &e {
                    muli_core::error::MuliError::Timeout(_) => JobState::TimedOut,
                    muli_core::error::MuliError::Cancelled(_) => JobState::Cancelled,
                    _ => JobState::Failed,
                }
            };

            match store
                .update_state(&job_id, JobState::Running, final_state)
                .await
            {
                Ok(()) => {
                    let mut updated_job = job.clone();
                    updated_job.state = final_state;
                    updated_job.result = Some(JobResult {
                        exit_code: None,
                        message: format!("Execution error: {e}"),
                        container_id: None,
                    });
                    updated_job.finished_at = Some(chrono::Utc::now());

                    if let Err(update_err) = store.update_job(&updated_job).await {
                        error!(
                            job_id = %job_id,
                            error = %update_err,
                            "Failed to persist job error result"
                        );
                    }
                }
                Err(cas_err) => {
                    warn!(
                        job_id = %job_id,
                        error = %cas_err,
                        "State transition Running -> {:?} rejected (job may have been cancelled)",
                        final_state
                    );
                }
            }

            record_completion_metrics(
                &job.spec.tenant_id,
                &job.spec.priority_tier,
                final_state,
                started_at,
            );
            error!(job_id = %job_id, error = %e, state = ?final_state, "Job execution failed");
        }
    }

    // Remove from live map so log_service knows the job is complete
    log_collectors.remove(&job_id);
}

fn record_completion_metrics(
    tenant: &str,
    priority_tier: &muli_core::job::model::PriorityTier,
    final_state: JobState,
    started_at: chrono::DateTime<chrono::Utc>,
) {
    let tier = format!("{priority_tier:?}");
    let duration_secs = (chrono::Utc::now() - started_at).num_milliseconds().max(0) as f64 / 1000.0;
    crate::metrics::JOBS_RUNNING.dec();
    crate::metrics::JOBS_COMPLETED_TOTAL
        .with_label_values(&[tenant, &tier, &format!("{final_state:?}")])
        .inc();
    crate::metrics::JOB_DURATION_SECONDS
        .with_label_values(&[tenant, &tier])
        .observe(duration_secs);
}

/// Convert engine log lines to the stored representation.
fn lines_to_stored(
    lines: Vec<muli_engine::docker::logs::LogLine>,
) -> Vec<muli_core::job::model::StoredLogLine> {
    lines
        .into_iter()
        .map(|l| muli_core::job::model::StoredLogLine {
            sequence: l.sequence,
            timestamp: l.timestamp,
            stream: match l.stream {
                muli_engine::docker::logs::LogStream::Stdout => "stdout".to_string(),
                muli_engine::docker::logs::LogStream::Stderr => "stderr".to_string(),
            },
            message: l.message,
        })
        .collect()
}

/// Drain remaining unflushed log lines and persist them to durable storage.
async fn drain_and_persist_logs(
    collector: &LogCollector,
    log_store: &dyn JobLogStore,
    job_id: &str,
) {
    let lines = collector.drain().await;
    if lines.is_empty() {
        return;
    }
    let stored = lines_to_stored(lines);
    if let Err(e) = log_store.append_logs(job_id, stored).await {
        tracing::warn!(job_id = %job_id, error = %e, "Failed to persist job logs");
    }
}
