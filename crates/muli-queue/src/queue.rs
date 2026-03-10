// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Thread-safe priority queue with scoring-based ordering.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use muli_core::job::model::PriorityTier;
use muli_core::job::priority::ScoredJob;
use tokio::sync::Notify;
use tracing::debug;

struct QueueInner {
    heap: BinaryHeap<ScoredJob>,
    tiers: HashMap<String, PriorityTier>,
    /// Tombstone set: job IDs that have been removed but may still exist in the heap.
    removed: HashSet<String>,
}

/// Thread-safe priority queue backed by a `BinaryHeap<ScoredJob>`.
pub struct PriorityQueue {
    inner: Mutex<QueueInner>,
    notify: Arc<Notify>,
}

impl PriorityQueue {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            inner: Mutex::new(QueueInner {
                heap: BinaryHeap::new(),
                tiers: HashMap::new(),
                removed: HashSet::new(),
            }),
            notify,
        }
    }

    /// Add a scored job to the queue and notify waiters.
    pub fn push(&self, job: ScoredJob, tier: PriorityTier) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        debug!(job_id = %job.job_id, score = job.score, "pushing job to queue");
        inner.tiers.insert(job.job_id.clone(), tier);
        inner.heap.push(job);
        drop(inner);
        self.notify.notify_one();
    }

    /// Pop the highest-priority job from the queue.
    /// Skips any entries that have been tombstoned via `remove`.
    pub fn pop(&self) -> Option<ScoredJob> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match inner.heap.pop() {
                None => return None,
                Some(job) if inner.removed.contains(&job.job_id) => {
                    inner.removed.remove(&job.job_id);
                    continue;
                }
                Some(job) => return Some(job),
            }
        }
    }

    /// Put a previously popped job back into the queue (tier already tracked).
    ///
    /// Does NOT notify waiters to prevent busy-looping when concurrency is full.
    /// The scheduler's poll interval handles picking up requeued jobs.
    pub fn requeue(&self, job: ScoredJob) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        debug!(job_id = %job.job_id, "requeuing job");
        inner.heap.push(job);
    }

    /// Remove a specific job by ID from the queue.
    /// Uses a tombstone so the heap is not drained and rebuilt — O(1) amortized.
    pub fn remove(&self, job_id: &str) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.tiers.remove(job_id);
        inner.removed.insert(job_id.to_string());
    }

    /// Number of live (non-tombstoned) jobs currently in the queue.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.heap.len().saturating_sub(inner.removed.len())
    }

    /// Whether the queue has no live jobs.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Recalculate all scores based on current time (call periodically).
    /// Also compacts tombstoned entries out of the heap.
    pub fn refresh_scores(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let removed = std::mem::take(&mut inner.removed);
        let mut jobs: Vec<ScoredJob> = inner
            .heap
            .drain()
            .filter(|j| !removed.contains(&j.job_id))
            .collect();
        for job in &mut jobs {
            if let Some(&tier) = inner.tiers.get(&job.job_id) {
                job.refresh_score(tier);
            }
        }
        inner.heap.extend(jobs);
        // removed set is now empty (was taken above)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_notify() -> Arc<Notify> {
        Arc::new(Notify::new())
    }

    #[test]
    fn test_push_pop_ordering() {
        let queue = PriorityQueue::new(make_notify());
        let now = Utc::now();

        let low = ScoredJob::new("low".into(), PriorityTier::Free, now, "t1".into());
        let high = ScoredJob::new("high".into(), PriorityTier::Enterprise, now, "t1".into());

        queue.push(low, PriorityTier::Free);
        queue.push(high, PriorityTier::Enterprise);

        let first = queue.pop().unwrap();
        assert_eq!(first.job_id, "high");

        let second = queue.pop().unwrap();
        assert_eq!(second.job_id, "low");

        assert!(queue.pop().is_none());
    }

    #[test]
    fn test_remove() {
        let queue = PriorityQueue::new(make_notify());
        let now = Utc::now();

        queue.push(
            ScoredJob::new("a".into(), PriorityTier::Standard, now, "t1".into()),
            PriorityTier::Standard,
        );
        queue.push(
            ScoredJob::new("b".into(), PriorityTier::Standard, now, "t1".into()),
            PriorityTier::Standard,
        );

        assert_eq!(queue.len(), 2);
        queue.remove("a");
        assert_eq!(queue.len(), 1);

        let job = queue.pop().unwrap();
        assert_eq!(job.job_id, "b");
    }

    #[test]
    fn test_len_and_is_empty() {
        let queue = PriorityQueue::new(make_notify());
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);

        let now = Utc::now();
        queue.push(
            ScoredJob::new("x".into(), PriorityTier::Free, now, "t1".into()),
            PriorityTier::Free,
        );

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_requeue() {
        let queue = PriorityQueue::new(make_notify());
        let now = Utc::now();

        queue.push(
            ScoredJob::new("a".into(), PriorityTier::Standard, now, "t1".into()),
            PriorityTier::Standard,
        );

        let job = queue.pop().unwrap();
        assert!(queue.is_empty());

        queue.requeue(job);
        assert_eq!(queue.len(), 1);

        let job = queue.pop().unwrap();
        assert_eq!(job.job_id, "a");
    }

    #[test]
    fn test_refresh_scores() {
        let queue = PriorityQueue::new(make_notify());
        let now = Utc::now();

        queue.push(
            ScoredJob::new("a".into(), PriorityTier::Free, now, "t1".into()),
            PriorityTier::Free,
        );
        queue.push(
            ScoredJob::new("b".into(), PriorityTier::Enterprise, now, "t2".into()),
            PriorityTier::Enterprise,
        );

        // Refresh should not change ordering for same-time jobs
        queue.refresh_scores();
        assert_eq!(queue.len(), 2);

        let first = queue.pop().unwrap();
        assert_eq!(first.job_id, "b"); // Enterprise still highest
    }
}
