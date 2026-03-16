// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline and step run state machines.

use serde::{Deserialize, Serialize};

/// Overall state of a pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineRunState {
    /// Waiting to be scheduled.
    Pending,
    /// At least one step is executing.
    Running,
    /// All steps succeeded.
    Succeeded,
    /// At least one required step failed.
    Failed,
    /// Cancelled by user or system.
    Cancelled,
    /// Some steps failed but failure_strategy allowed continuation.
    Degraded,
}

impl PipelineRunState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Degraded,
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Running => "Running",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::Degraded => "Degraded",
        }
    }
}

/// State of an individual step within a pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepRunState {
    /// Waiting for dependencies to complete.
    Pending,
    /// Dependencies satisfied, eligible for scheduling.
    Ready,
    /// Currently executing as a Job.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Execution failed.
    Failed,
    /// Skipped due to `if` condition or dependency failure.
    Skipped,
    /// Cancelled by user or system.
    Cancelled,
}

impl StepRunState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled,
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Ready => "Ready",
            Self::Running => "Running",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Skipped => "Skipped",
            Self::Cancelled => "Cancelled",
        }
    }
}

/// How to handle step failure in the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureStrategy {
    /// Stop the entire pipeline on failure (default).
    Stop,
    /// Continue executing independent steps.
    Continue,
    /// Ignore the failure entirely.
    Ignore,
}

impl Default for FailureStrategy {
    fn default() -> Self {
        Self::Stop
    }
}
