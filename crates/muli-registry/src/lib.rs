// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embedded multi-format package registry (OCI, npm, Cargo).

pub mod api;
pub mod auth;
pub mod cargo;
pub mod common;
pub mod garbage_collection;
pub mod maven;
pub mod metrics;
pub mod npm;
pub mod proxy_cache;
pub mod storage;
pub mod tenant;
pub mod validation;

pub use api::registry_router;
pub use auth::RegistryAuth;
pub use garbage_collection::GarbageCollector;
pub use metrics::RegistryMetrics;
pub use proxy_cache::{ProxyCache, UpstreamRegistry};
pub use storage::{FilesystemStorage, RegistryStorage};
pub use tenant::{TenantConfig, TenantContext};
