// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Scheduler that pulls jobs from the queue when concurrency slots are available.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use muli_core::job::model::PriorityTier;
use muli_core::job::priority::ScoredJob;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::concurrency::ConcurrencyLimiter;
use crate::queue::PriorityQueue;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const CONCURRENCY_BACKOFF: Duration = Duration::from_millis(100);

/// Scheduler that pulls jobs from the priority queue and dispatches them
/// when concurrency slots are available.
pub struct Scheduler {
    queue: Arc<PriorityQueue>,
    limiter: Arc<ConcurrencyLimiter>,
    notify: Arc<Notify>,
    poll_interval: Duration,
}

impl Scheduler {
    pub fn new(
        queue: Arc<PriorityQueue>,
        limiter: Arc<ConcurrencyLimiter>,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            queue,
            limiter,
            notify,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Set a custom poll interval (default 5s).
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Get a reference to the underlying priority queue.
    pub fn queue(&self) -> &Arc<PriorityQueue> {
        &self.queue
    }

    /// Add a job to the queue and wake the scheduler.
    pub fn enqueue(&self, job_id: String, tier: PriorityTier, tenant_id: String) {
        let job = ScoredJob::new(job_id, tier, Utc::now(), tenant_id);
        self.queue.push(job, tier);
    }

    /// Remove a job from the queue.
    pub fn cancel(&self, job_id: &str) {
        self.queue.remove(job_id);
    }

    /// Main scheduler loop. Dispatches jobs to the callback when concurrency
    /// slots are available. Runs until the cancellation token is triggered.
    pub async fn run<F, Fut>(&self, cancel: CancellationToken, job_callback: F)
    where
        F: Fn(String, String) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        info!("scheduler loop started");
        loop {
            if cancel.is_cancelled() {
                info!("scheduler received cancellation, stopping");
                break;
            }

            let mut concurrency_blocked = false;

            // Dispatch as many jobs as possible
            while let Some(job) = self.queue.pop() {
                match self.limiter.try_acquire(&job.tenant_id) {
                    Ok(permit) => {
                        let job_id = job.job_id.clone();
                        let tenant_id = job.tenant_id.clone();
                        let cb = job_callback.clone();
                        debug!(job_id = %job_id, tenant_id = %tenant_id, "dispatching job");
                        tokio::spawn(async move {
                            cb(job_id, tenant_id).await;
                            drop(permit);
                        });
                    }
                    Err(_) => {
                        debug!(job_id = %job.job_id, "concurrency limit hit, requeuing");
                        self.queue.requeue(job);
                        concurrency_blocked = true;
                        break;
                    }
                }
            }

            if concurrency_blocked {
                // Back off briefly when concurrency is full to avoid busy-looping.
                tokio::select! {
                    _ = cancel.cancelled() => {
                        info!("scheduler received cancellation during backoff, stopping");
                        break;
                    }
                    _ = tokio::time::sleep(CONCURRENCY_BACKOFF) => {}
                }
            } else {
                // Queue is empty - wait for new work, periodic refresh, or cancellation
                tokio::select! {
                    _ = cancel.cancelled() => {
                        info!("scheduler received cancellation while idle, stopping");
                        break;
                    }
                    _ = self.notify.notified() => {
                        debug!("scheduler woken by notify");
                    }
                    _ = tokio::time::sleep(self.poll_interval) => {
                        debug!("scheduler poll timeout, refreshing scores");
                        self.queue.refresh_scores();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[tokio::test]
    async fn test_enqueue_and_dispatch() {
        let notify = Arc::new(Notify::new());
        let queue = Arc::new(PriorityQueue::new(notify.clone()));
        let limiter = Arc::new(ConcurrencyLimiter::new(10, 3));
        let scheduler = Arc::new(Scheduler::new(queue, limiter, notify));

        let dispatched: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let dispatched_clone = dispatched.clone();

        let cancel = CancellationToken::new();
        let s = scheduler.clone();
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            s.run(cancel_clone, move |job_id, _tenant_id| {
                let d = dispatched_clone.clone();
                async move {
                    d.lock().unwrap().push(job_id);
                }
            })
            .await;
        });

        scheduler.enqueue("job1".into(), PriorityTier::Standard, "t1".into());
        scheduler.enqueue("job2".into(), PriorityTier::Enterprise, "t1".into());

        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let _ = handle.await;

        let jobs = dispatched.lock().unwrap();
        assert!(jobs.contains(&"job1".to_string()));
        assert!(jobs.contains(&"job2".to_string()));
    }

    #[tokio::test]
    async fn test_cancel() {
        let notify = Arc::new(Notify::new());
        let queue = Arc::new(PriorityQueue::new(notify.clone()));
        let limiter = Arc::new(ConcurrencyLimiter::new(10, 3));
        let scheduler = Scheduler::new(queue.clone(), limiter, notify);

        scheduler.enqueue("job1".into(), PriorityTier::Standard, "t1".into());
        assert_eq!(queue.len(), 1);

        scheduler.cancel("job1");
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn test_concurrency_limits_respected() {
        let notify = Arc::new(Notify::new());
        let queue = Arc::new(PriorityQueue::new(notify.clone()));
        // Global limit of 1, per-tenant limit of 1
        let limiter = Arc::new(ConcurrencyLimiter::new(1, 1));
        let scheduler = Arc::new(Scheduler::new(queue.clone(), limiter.clone(), notify));

        let dispatched: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let dispatched_clone = dispatched.clone();

        let cancel = CancellationToken::new();
        let s = scheduler.clone();
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            s.run(cancel_clone, move |job_id, _tenant_id| {
                let d = dispatched_clone.clone();
                async move {
                    d.lock().unwrap().push(job_id);
                    // Hold the permit for a bit
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
            .await;
        });

        // Enqueue two jobs - only one should run immediately due to limit=1
        scheduler.enqueue("job1".into(), PriorityTier::Standard, "t1".into());
        scheduler.enqueue("job2".into(), PriorityTier::Standard, "t1".into());

        // Wait for first job to be dispatched
        tokio::time::sleep(Duration::from_millis(50)).await;
        let count = dispatched.lock().unwrap().len();
        assert_eq!(count, 1);

        // Wait for first job to complete (200ms) + scheduler backoff (100ms) + margin
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
        let _ = handle.await;

        let count = dispatched.lock().unwrap().len();
        assert_eq!(count, 2);
    }
}
