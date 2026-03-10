// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Muli server: gRPC services, metrics, and embedded registry/git hosting.

pub mod cleanup;
pub mod config;
pub mod embedded_agent;
pub mod execution;
pub mod grpc;
pub mod metrics;
pub mod recovery;
pub mod shutdown;
pub mod startup;
mod stores;

pub use startup::run;
