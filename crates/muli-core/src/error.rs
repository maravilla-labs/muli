// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Error types for the Muli system with retry classification (retryable vs permanent).

use std::time::Duration;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum MuliError {
    #[error("Docker error: {0}")]
    Docker(String),

    #[error("Container not found: {0}")]
    ContainerNotFound(String),

    #[error("Image pull failed for {image}: {reason}")]
    ImagePullFailed { image: String, reason: String },

    #[error("Job not found: {0}")]
    JobNotFound(String),

    #[error("Invalid state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("Job timed out after {0:?}")]
    Timeout(Duration),

    #[error("Job cancelled: {0}")]
    Cancelled(String),

    #[error("Agent not found: {0}")]
    AgentNotFound(String),

    #[error("Insufficient resources: needed {needed}, available {available}")]
    InsufficientResources { needed: String, available: String },

    #[error("Concurrency limit reached: {0}")]
    ConcurrencyLimit(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("gRPC error: {0}")]
    Grpc(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Pipeline error: {0}")]
    Pipeline(String),

    #[error("Pipeline YAML error: {0}")]
    PipelineYamlError(String),

    #[error("Pipeline DAG cycle: {0}")]
    PipelineDagCycle(String),
}

impl MuliError {
    /// Whether this error is transient and the operation should be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            MuliError::Docker(_)
                | MuliError::ImagePullFailed { .. }
                | MuliError::Timeout(_)
                | MuliError::InsufficientResources { .. }
                | MuliError::ConcurrencyLimit(_)
                | MuliError::Storage(_)
                | MuliError::Grpc(_)
        )
    }

    /// Whether this error represents a permanent failure (no retry).
    pub fn is_permanent(&self) -> bool {
        !self.is_retryable()
    }
}

pub type Result<T> = std::result::Result<T, MuliError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_errors() {
        assert!(MuliError::Docker("daemon unavailable".into()).is_retryable());
        assert!(
            MuliError::ImagePullFailed {
                image: "test".into(),
                reason: "timeout".into()
            }
            .is_retryable()
        );
        assert!(MuliError::Timeout(Duration::from_secs(60)).is_retryable());
        assert!(MuliError::Storage("connection lost".into()).is_retryable());
    }

    #[test]
    fn test_permanent_errors() {
        assert!(MuliError::JobNotFound("abc".into()).is_permanent());
        assert!(
            MuliError::InvalidStateTransition {
                from: "Running".into(),
                to: "Pending".into()
            }
            .is_permanent()
        );
        assert!(MuliError::Cancelled("user request".into()).is_permanent());
        assert!(MuliError::ContainerNotFound("xyz".into()).is_permanent());
        assert!(MuliError::Validation("bad input".into()).is_permanent());
    }
}
