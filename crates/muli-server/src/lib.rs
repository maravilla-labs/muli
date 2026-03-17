// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Muli server: gRPC services, metrics, and embedded registry/git hosting.

pub mod cleanup;
pub mod config;
pub mod embedded_agent;
pub mod enforcement;
pub mod execution;
pub mod grpc;
pub mod metrics;
pub(crate) mod pipeline_job_submitter;
mod pipeline_trigger;
pub mod recovery;
pub mod shutdown;
mod start_grpc;
pub mod startup;
pub(crate) mod stores;
pub mod watchdog;

pub use startup::run;
