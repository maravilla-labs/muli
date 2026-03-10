// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Muli server binary entry point.

mod cli;

use anyhow::Context;
use clap::Parser;
use muli_server::config::ServerConfig;
use rustls::crypto::ring;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Rustls 0.23 requires an explicit process-level crypto provider in some
    // feature combinations. Install ring provider at startup to avoid runtime panics.
    let _ = ring::default_provider().install_default();

    let _ = dotenvy::dotenv();

    // Parse CLI flags before tracing init so --help/--version exit cleanly
    let cli = cli::Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut config = ServerConfig::load().context("Failed to load server config")?;
    cli::apply_overrides(&mut config, &cli);

    muli_server::run(config).await
}
