// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI argument parsing for the server binary.

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "muli-server", about = "Muli job server")]
pub struct Cli {
    /// Enable registry: `docker` (OCI registry only) or `full` (+ npm + cargo)
    #[arg(long, value_enum)]
    pub registry: Option<RegistryMode>,

    /// Enable git HTTP hosting
    #[arg(long)]
    pub git: bool,

    /// Enable git SSH hosting (implies --git)
    #[arg(long)]
    pub git_ssh: bool,

    /// Spawn an in-process agent — no separate muli-agent process needed
    #[arg(long)]
    pub embedded_agent: bool,

    /// Override data directory (default: OS XDG path)
    #[arg(long)]
    pub data_dir: Option<String>,

    /// Override registry HTTP port (default: 5000; also: MULI_REGISTRY_PORT)
    #[arg(long, env = "MULI_REGISTRY_PORT")]
    pub registry_port: Option<u16>,

    /// Override git HTTP port (default: 7000; also: MULI_GIT_PORT)
    #[arg(long, env = "MULI_GIT_PORT")]
    pub git_port: Option<u16>,

    /// Override git SSH port (default: 2222; also: MULI_GIT_SSH_PORT)
    #[arg(long, env = "MULI_GIT_SSH_PORT")]
    pub git_ssh_port: Option<u16>,

    /// API key for gRPC authentication (also: MULI_API_KEY)
    #[arg(long, env = "MULI_API_KEY")]
    pub api_key: Option<String>,

    /// Default tenant: auto-provision and use as fallback x-tenant-id (also: MULI_DEFAULT_TENANT_ID)
    #[arg(long, env = "MULI_DEFAULT_TENANT_ID")]
    pub default_tenant: Option<String>,

    /// Allow localhost/private-IP webhook URLs (dev only; also: MULI_GIT_ALLOW_LOCALHOST_WEBHOOKS)
    #[arg(long, env = "MULI_GIT_ALLOW_LOCALHOST_WEBHOOKS")]
    pub allow_localhost_webhooks: bool,
}

#[derive(Clone, ValueEnum)]
pub enum RegistryMode {
    /// OCI/Docker registry only
    Docker,
    /// OCI/Docker registry + npm + cargo
    Full,
}

/// Apply CLI flag overrides to the loaded configuration.
pub fn apply_overrides(config: &mut muli_server::config::ServerConfig, cli: &Cli) {
    if let Some(mode) = &cli.registry {
        config.registry_enabled = true;
        if matches!(mode, RegistryMode::Full) {
            config.npm_enabled = true;
            config.cargo_enabled = true;
            config.maven_enabled = true;
        }
    }
    if cli.git || cli.git_ssh {
        config.git_enabled = true;
    }
    if cli.git_ssh {
        config.git_ssh_enabled = true;
    }
    if cli.embedded_agent {
        config.embedded_agent = true;
    }
    if let Some(dir) = &cli.data_dir {
        config.data_dir = dir.clone();
    }
    if let Some(port) = cli.registry_port {
        config.registry_port = port;
    }
    if let Some(port) = cli.git_port {
        config.git_port = port;
    }
    if let Some(port) = cli.git_ssh_port {
        config.git_ssh_port = port;
    }
    if let Some(key) = &cli.api_key {
        config.api_key = Some(key.clone());
    }
    if let Some(dt) = &cli.default_tenant {
        config.default_tenant_id = Some(dt.clone());
    }
    if cli.allow_localhost_webhooks {
        config.git_allow_localhost_webhooks = true;
    }
}
