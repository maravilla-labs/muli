// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Agent configuration from environment variables and CLI flags.

use clap::Parser;

/// Muli Agent - connects to a muli-server and executes container jobs.
#[derive(Debug, Clone, Parser)]
#[command(name = "muli-agent", about = "Muli build agent")]
pub struct AgentConfig {
    /// Name for this agent (must be unique per server)
    #[arg(long, default_value = "agent-1")]
    pub name: String,

    /// gRPC server URL to connect to
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    pub server_url: String,

    /// Heartbeat interval in seconds
    #[arg(long, default_value_t = 10)]
    pub heartbeat_interval_secs: u64,

    /// Maximum number of concurrent jobs
    #[arg(long, default_value_t = 4)]
    pub max_concurrent_jobs: u32,

    /// Total CPU millicores available for jobs
    #[arg(long, default_value_t = 4000)]
    pub total_cpu_millicores: u64,

    /// Total memory in bytes available for jobs
    #[arg(long, default_value_t = 8_589_934_592)]
    pub total_memory_bytes: u64,

    /// Labels for this agent (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub labels: Vec<String>,

    /// Graceful shutdown timeout in seconds (wait for running jobs to finish)
    #[arg(long, default_value_t = 60)]
    pub shutdown_timeout_secs: u64,

    /// API key for authenticating with the server (can also be set via MULI_API_KEY env var)
    #[arg(long, env = "MULI_API_KEY")]
    pub api_key: Option<String>,

    /// Path to a custom CA certificate (PEM) for verifying the server's TLS certificate
    #[arg(long, env = "MULI_TLS_CA_CERT_PATH")]
    pub tls_ca_cert: Option<String>,
}
