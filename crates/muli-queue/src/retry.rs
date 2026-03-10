// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Retry policy with exponential backoff configuration.

use std::time::Duration;

use muli_core::error::MuliError;

/// Retry policy with exponential backoff.
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_retries,
            base_delay,
            max_delay,
        }
    }

    /// Returns the backoff duration if the error should be retried, or `None` if not.
    ///
    /// Uses exponential backoff: `base_delay * 2^attempt`, capped at `max_delay`.
    /// Only retries errors where `MuliError::is_retryable()` returns true.
    pub fn should_retry(&self, error: &MuliError, attempt: u32) -> Option<Duration> {
        if !error.is_retryable() || attempt >= self.max_retries {
            return None;
        }
        let multiplier = 2u32.checked_pow(attempt).unwrap_or(u32::MAX);
        let delay = self.base_delay.saturating_mul(multiplier);
        Some(delay.min(self.max_delay))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(2, Duration::from_secs(5), Duration::from_secs(300))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_error() {
        let policy = RetryPolicy::default();
        let err = MuliError::Docker("daemon down".into());

        // attempt 0: 5s * 2^0 = 5s
        let delay = policy.should_retry(&err, 0);
        assert_eq!(delay, Some(Duration::from_secs(5)));

        // attempt 1: 5s * 2^1 = 10s
        let delay = policy.should_retry(&err, 1);
        assert_eq!(delay, Some(Duration::from_secs(10)));

        // attempt 2: exceeds max_retries (2)
        let delay = policy.should_retry(&err, 2);
        assert_eq!(delay, None);
    }

    #[test]
    fn test_non_retryable_error() {
        let policy = RetryPolicy::default();
        let err = MuliError::JobNotFound("abc".into());

        let delay = policy.should_retry(&err, 0);
        assert_eq!(delay, None);
    }

    #[test]
    fn test_max_delay_cap() {
        let policy = RetryPolicy::new(10, Duration::from_secs(100), Duration::from_secs(300));
        let err = MuliError::Storage("connection lost".into());

        // attempt 3: 100s * 2^3 = 800s, capped at 300s
        let delay = policy.should_retry(&err, 3);
        assert_eq!(delay, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_default_values() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 2);
        assert_eq!(policy.base_delay, Duration::from_secs(5));
        assert_eq!(policy.max_delay, Duration::from_secs(300));
    }

    #[test]
    fn test_cancelled_is_not_retryable() {
        let policy = RetryPolicy::new(5, Duration::from_secs(1), Duration::from_secs(60));
        let err = MuliError::Cancelled("user request".into());

        assert_eq!(policy.should_retry(&err, 0), None);
    }
}
