// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Multi-tenant git hosting service with HTTP and SSH protocol support.

pub mod api;
pub mod auth;
pub mod hooks;
pub mod protocol;
pub mod ssh;
pub mod storage;
pub mod tenant;

pub use api::{GitRouterConfig, git_router};
pub use auth::GitAuth;
pub use ssh::{SshConfig, SshServer};
pub use storage::check_git_available;
pub use tenant::TenantConfig;

/// Configuration for the git hosting service.
#[derive(Clone, Debug)]
pub struct GitConfig {
    pub git_root: String,
    pub domain: String,
    pub anonymous_pull: bool,
}
