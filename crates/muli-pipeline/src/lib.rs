// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CI/CD pipeline orchestration: YAML parser, DAG engine, trigger system,
//! artifact/cache storage, and REST API.

pub mod yaml;
pub mod dag;
pub mod trigger;
pub mod artifact;
pub mod api;

pub use dag::JobSubmitter;
pub use trigger::PipelineTriggerHook;
