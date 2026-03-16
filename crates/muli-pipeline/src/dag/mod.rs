// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod graph;
pub mod executor;
pub mod matrix;

pub use graph::DagGraph;
pub use executor::{DagExecutor, JobSubmitter};
pub use matrix::expand_matrix;
