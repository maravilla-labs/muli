// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH server for git clone/push via Ed25519 keys.

pub(crate) mod auth;
mod server;

pub use server::{SshConfig, SshServer, load_or_generate_host_key};
