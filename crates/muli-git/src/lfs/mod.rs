// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git LFS 2.0 Batch API implementation.
//!
//! Provides content-addressable large file storage with pluggable backends
//! (filesystem, S3) and multi-tenant isolation.

pub mod api;
pub mod storage;
pub mod types;
