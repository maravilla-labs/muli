// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Job state machine enforcing valid transitions (Pending -> Running -> Completed/Failed/Cancelled).

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::MuliError;

/// All possible states for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobState {
    Pending,
    Scheduled,
    Pulling,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl JobState {
    /// Terminal states cannot transition to anything else.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
        )
    }

    /// Check if a transition from this state to `target` is valid.
    pub fn can_transition_to(&self, target: &JobState) -> bool {
        if self.is_terminal() {
            return false;
        }

        // Any non-terminal state can be cancelled
        if *target == JobState::Cancelled {
            return true;
        }

        matches!(
            (self, target),
            (JobState::Pending, JobState::Scheduled)
                | (JobState::Scheduled, JobState::Pulling)
                | (JobState::Scheduled, JobState::Running) // Skip pull if image exists
                | (JobState::Pulling, JobState::Running)
                | (JobState::Pulling, JobState::Failed) // Pull failure
                | (JobState::Running, JobState::Succeeded)
                | (JobState::Running, JobState::Failed)
                | (JobState::Running, JobState::TimedOut)
        )
    }

    /// Perform a validated state transition, returning error if invalid.
    pub fn transition_to(&self, target: JobState) -> crate::error::Result<JobState> {
        if self.can_transition_to(&target) {
            Ok(target)
        } else {
            Err(MuliError::InvalidStateTransition {
                from: self.to_string(),
                to: target.to_string(),
            })
        }
    }

    /// All valid next states from current state.
    pub fn valid_transitions(&self) -> Vec<JobState> {
        if self.is_terminal() {
            return vec![];
        }

        let mut transitions = vec![JobState::Cancelled];

        match self {
            JobState::Pending => transitions.push(JobState::Scheduled),
            JobState::Scheduled => {
                transitions.push(JobState::Pulling);
                transitions.push(JobState::Running);
            }
            JobState::Pulling => {
                transitions.push(JobState::Running);
                transitions.push(JobState::Failed);
            }
            JobState::Running => {
                transitions.push(JobState::Succeeded);
                transitions.push(JobState::Failed);
                transitions.push(JobState::TimedOut);
            }
            _ => {}
        }

        transitions
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobState::Pending => write!(f, "Pending"),
            JobState::Scheduled => write!(f, "Scheduled"),
            JobState::Pulling => write!(f, "Pulling"),
            JobState::Running => write!(f, "Running"),
            JobState::Succeeded => write!(f, "Succeeded"),
            JobState::Failed => write!(f, "Failed"),
            JobState::Cancelled => write!(f, "Cancelled"),
            JobState::TimedOut => write!(f, "TimedOut"),
        }
    }
}

/// Simple status enum matching staticlab's JobStatus for compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimpleJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl From<JobState> for SimpleJobStatus {
    fn from(state: JobState) -> Self {
        match state {
            JobState::Pending | JobState::Scheduled => SimpleJobStatus::Pending,
            JobState::Pulling | JobState::Running => SimpleJobStatus::Running,
            JobState::Succeeded => SimpleJobStatus::Succeeded,
            JobState::Failed | JobState::Cancelled | JobState::TimedOut => SimpleJobStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_transitions() {
        assert!(JobState::Pending.can_transition_to(&JobState::Scheduled));
        assert!(JobState::Scheduled.can_transition_to(&JobState::Pulling));
        assert!(JobState::Scheduled.can_transition_to(&JobState::Running));
        assert!(JobState::Pulling.can_transition_to(&JobState::Running));
        assert!(JobState::Running.can_transition_to(&JobState::Succeeded));
        assert!(JobState::Running.can_transition_to(&JobState::Failed));
        assert!(JobState::Running.can_transition_to(&JobState::TimedOut));
    }

    #[test]
    fn test_invalid_transitions() {
        assert!(!JobState::Pending.can_transition_to(&JobState::Running));
        assert!(!JobState::Running.can_transition_to(&JobState::Pending));
        assert!(!JobState::Succeeded.can_transition_to(&JobState::Running));
        assert!(!JobState::Failed.can_transition_to(&JobState::Pending));
    }

    #[test]
    fn test_cancel_from_any_non_terminal() {
        assert!(JobState::Pending.can_transition_to(&JobState::Cancelled));
        assert!(JobState::Scheduled.can_transition_to(&JobState::Cancelled));
        assert!(JobState::Pulling.can_transition_to(&JobState::Cancelled));
        assert!(JobState::Running.can_transition_to(&JobState::Cancelled));
    }

    #[test]
    fn test_terminal_states_block_all_transitions() {
        for terminal in [
            JobState::Succeeded,
            JobState::Failed,
            JobState::Cancelled,
            JobState::TimedOut,
        ] {
            assert!(terminal.is_terminal());
            assert!(terminal.valid_transitions().is_empty());
            assert!(!terminal.can_transition_to(&JobState::Pending));
            assert!(!terminal.can_transition_to(&JobState::Cancelled));
        }
    }

    #[test]
    fn test_transition_to_returns_error() {
        let result = JobState::Pending.transition_to(JobState::Running);
        assert!(result.is_err());
    }

    #[test]
    fn test_transition_to_success() {
        let result = JobState::Pending.transition_to(JobState::Scheduled);
        assert_eq!(result.unwrap(), JobState::Scheduled);
    }

    #[test]
    fn test_simple_status_mapping() {
        assert_eq!(
            SimpleJobStatus::from(JobState::Pending),
            SimpleJobStatus::Pending
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::Scheduled),
            SimpleJobStatus::Pending
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::Pulling),
            SimpleJobStatus::Running
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::Running),
            SimpleJobStatus::Running
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::Succeeded),
            SimpleJobStatus::Succeeded
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::Failed),
            SimpleJobStatus::Failed
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::Cancelled),
            SimpleJobStatus::Failed
        );
        assert_eq!(
            SimpleJobStatus::from(JobState::TimedOut),
            SimpleJobStatus::Failed
        );
    }
}
