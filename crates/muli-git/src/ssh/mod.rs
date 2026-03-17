// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH server for git clone/push via Ed25519 keys.

pub(crate) mod auth;
pub(crate) mod lfs_auth;
mod process;
mod ref_tracking;
mod server;
mod session;

pub use server::{SshConfig, SshServer, load_or_generate_host_key};
