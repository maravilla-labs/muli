// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Priority scoring with starvation prevention (wait-time boosting).

use chrono::{DateTime, Utc};

use super::model::PriorityTier;

/// Calculate the priority score for a job.
///
/// Formula: `tier_weight * (10 + queue_minutes) / 10`
///
/// This ensures:
/// - Higher-tier jobs are prioritized
/// - Long-waiting jobs of any tier gradually increase in score (prevents starvation)
pub fn calculate_score(tier: PriorityTier, enqueued_at: DateTime<Utc>) -> f64 {
    let wait_minutes = (Utc::now() - enqueued_at).num_minutes().max(0) as f64;
    tier.weight() as f64 * (10.0 + wait_minutes) / 10.0
}

/// A scored job entry for priority queue comparison.
#[derive(Debug, Clone)]
pub struct ScoredJob {
    pub job_id: String,
    pub score: f64,
    pub enqueued_at: DateTime<Utc>,
    pub tenant_id: String,
}

impl ScoredJob {
    pub fn new(
        job_id: String,
        tier: PriorityTier,
        enqueued_at: DateTime<Utc>,
        tenant_id: String,
    ) -> Self {
        let score = calculate_score(tier, enqueued_at);
        Self {
            job_id,
            score,
            enqueued_at,
            tenant_id,
        }
    }

    /// Refresh the score based on current time.
    pub fn refresh_score(&mut self, tier: PriorityTier) {
        self.score = calculate_score(tier, self.enqueued_at);
    }
}

impl PartialEq for ScoredJob {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for ScoredJob {}

impl PartialOrd for ScoredJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher score = higher priority (BinaryHeap is a max-heap)
        // Use total_cmp to handle NaN safely (NaN sorts to the end)
        // Tiebreak by job_id for deterministic ordering
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.job_id.cmp(&other.job_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_initial_scores() {
        let now = Utc::now();
        assert_eq!(PriorityTier::Free.weight(), 1);
        assert_eq!(PriorityTier::Standard.weight(), 10);
        assert_eq!(PriorityTier::Premium.weight(), 100);
        assert_eq!(PriorityTier::Enterprise.weight(), 1000);

        // At time 0, score = weight * 10 / 10 = weight
        let free_score = calculate_score(PriorityTier::Free, now);
        let enterprise_score = calculate_score(PriorityTier::Enterprise, now);
        assert!(enterprise_score > free_score);
    }

    #[test]
    fn test_score_increases_with_wait_time() {
        let ten_minutes_ago = Utc::now() - Duration::minutes(10);
        let now = Utc::now();

        let old_score = calculate_score(PriorityTier::Free, ten_minutes_ago);
        let new_score = calculate_score(PriorityTier::Free, now);

        assert!(old_score > new_score);
    }

    #[test]
    fn test_starvation_prevention() {
        // A free-tier job waiting 100 minutes should eventually beat a fresh standard job
        let long_wait = Utc::now() - Duration::minutes(100);
        let now = Utc::now();

        let old_free = calculate_score(PriorityTier::Free, long_wait);
        // Free: 1 * (10 + 100) / 10 = 11.0
        let fresh_standard = calculate_score(PriorityTier::Standard, now);
        // Standard: 10 * (10 + 0) / 10 = 10.0

        assert!(old_free > fresh_standard);
    }

    #[test]
    fn test_scored_job_ordering() {
        let now = Utc::now();
        let high = ScoredJob::new("high".into(), PriorityTier::Enterprise, now, "t1".into());
        let low = ScoredJob::new("low".into(), PriorityTier::Free, now, "t2".into());

        assert!(high > low);
    }
}
