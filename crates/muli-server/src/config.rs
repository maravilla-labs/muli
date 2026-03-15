// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server configuration from environment variables.

use std::path::PathBuf;

use anyhow::Result;
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

/// Server configuration.  Priority (lowest → highest):
///   compiled-in defaults
///   < /etc/muli/config.toml          (system install)
///   < ~/.config/muli/config.toml     (XDG user install)
///   < ./muli.toml                    (local / dev)
///   < path in MULI_CONFIG env var    (explicit override)
///   < MULI_* environment variables   (always win)
///
/// Config-file keys match the env-var names minus the `MULI_` prefix, lowercased:
///   e.g. `MULI_GRPC_PORT` → `grpc_port = 50051` in the TOML file.
#[derive(Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub mongodb_url: String,
    pub mongodb_database: String,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub max_concurrent_jobs: usize,
    pub max_jobs_per_tenant: usize,
    pub total_cpu_millicores: u64,
    pub total_memory_bytes: u64,
    pub cleanup_interval_seconds: u64,
    pub cleanup_max_age_seconds: u64,
    pub registry_enabled: bool,
    pub registry_port: u16,
    pub registry_domain: String,
    /// Explicit registry data path. When `None`, derived as `{data_dir}/registry`.
    pub registry_root: Option<String>,
    pub registry_max_size_gb: f64,
    pub registry_max_blob_size_mb: u64,
    pub npm_enabled: bool,
    pub cargo_enabled: bool,
    pub maven_enabled: bool,
    pub registry_tls_cert_path: Option<String>,
    pub registry_tls_key_path: Option<String>,
    pub shutdown_timeout_seconds: u64,
    pub api_key: Option<String>,
    pub require_auth: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub max_log_lines: usize,
    pub git_enabled: bool,
    pub git_port: u16,
    pub git_domain: String,
    /// Explicit git root path. When `None`, derived as `{data_dir}/git`.
    pub git_root: Option<String>,
    pub git_ssh_enabled: bool,
    pub git_ssh_port: u16,
    pub git_ssh_host_key_path: Option<String>,
    /// Single root directory for all persistent data.
    /// SQLite databases are stored here directly; registry and git sub-paths are
    /// derived as `{data_dir}/registry` and `{data_dir}/git` unless overridden.
    pub data_dir: String,
    /// When true, spawn an in-process agent alongside the server (single-command mode).
    /// Set via `MULI_EMBEDDED_AGENT=true` or `embedded_agent = true` in muli.toml.
    pub embedded_agent: bool,
    /// Default tenant ID: auto-provisioned at startup and injected as fallback x-tenant-id.
    /// Set via `MULI_DEFAULT_TENANT_ID` or `--default-tenant` CLI flag.
    pub default_tenant_id: Option<String>,
    /// Allow localhost and private-IP webhook URLs (development only).
    /// Set via `MULI_GIT_ALLOW_LOCALHOST_WEBHOOKS` or `--allow-localhost-webhooks`.
    pub git_allow_localhost_webhooks: bool,
    /// Maximum LFS object size in megabytes (default: 5120 = 5 GB).
    pub lfs_max_object_size_mb: u64,
}

impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("mongodb_url", &"[REDACTED]")
            .field("mongodb_database", &self.mongodb_database)
            .field("grpc_port", &self.grpc_port)
            .field("metrics_port", &self.metrics_port)
            .field("max_concurrent_jobs", &self.max_concurrent_jobs)
            .field("max_jobs_per_tenant", &self.max_jobs_per_tenant)
            .field("total_cpu_millicores", &self.total_cpu_millicores)
            .field("total_memory_bytes", &self.total_memory_bytes)
            .field("cleanup_interval_seconds", &self.cleanup_interval_seconds)
            .field("cleanup_max_age_seconds", &self.cleanup_max_age_seconds)
            .field("registry_enabled", &self.registry_enabled)
            .field("registry_port", &self.registry_port)
            .field("registry_domain", &self.registry_domain)
            .field("registry_root", &self.registry_root)
            .field("registry_max_size_gb", &self.registry_max_size_gb)
            .field("registry_max_blob_size_mb", &self.registry_max_blob_size_mb)
            .field("npm_enabled", &self.npm_enabled)
            .field("cargo_enabled", &self.cargo_enabled)
            .field("maven_enabled", &self.maven_enabled)
            .field("registry_tls_cert_path", &self.registry_tls_cert_path)
            .field(
                "registry_tls_key_path",
                &self.registry_tls_key_path.as_ref().map(|_| "[REDACTED]"),
            )
            .field("shutdown_timeout_seconds", &self.shutdown_timeout_seconds)
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field("require_auth", &self.require_auth)
            .field("tls_cert_path", &self.tls_cert_path)
            .field(
                "tls_key_path",
                &self.tls_key_path.as_ref().map(|_| "[REDACTED]"),
            )
            .field("max_log_lines", &self.max_log_lines)
            .field("git_enabled", &self.git_enabled)
            .field("git_port", &self.git_port)
            .field("git_domain", &self.git_domain)
            .field("git_root", &self.git_root)
            .field("git_ssh_enabled", &self.git_ssh_enabled)
            .field("git_ssh_port", &self.git_ssh_port)
            .field(
                "git_ssh_host_key_path",
                &self.git_ssh_host_key_path.as_ref().map(|_| "[REDACTED]"),
            )
            .field("data_dir", &self.data_dir)
            .field("embedded_agent", &self.embedded_agent)
            .field("default_tenant_id", &self.default_tenant_id)
            .field(
                "git_allow_localhost_webhooks",
                &self.git_allow_localhost_webhooks,
            )
            .field("lfs_max_object_size_mb", &self.lfs_max_object_size_mb)
            .finish()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mongodb_url: "mongodb://localhost:27017".to_string(),
            mongodb_database: "muli".to_string(),
            grpc_port: 50051,
            metrics_port: 9090,
            max_concurrent_jobs: 10,
            max_jobs_per_tenant: 3,
            total_cpu_millicores: 8000,
            total_memory_bytes: 17_179_869_184,
            cleanup_interval_seconds: 300,
            cleanup_max_age_seconds: 3600,
            registry_enabled: false,
            registry_port: 5000,
            registry_domain: "localhost".to_string(),
            registry_root: None,
            registry_max_size_gb: 50.0,
            registry_max_blob_size_mb: 5120,
            npm_enabled: false,
            cargo_enabled: false,
            maven_enabled: false,
            registry_tls_cert_path: None,
            registry_tls_key_path: None,
            shutdown_timeout_seconds: 30,
            api_key: None,
            require_auth: false,
            tls_cert_path: None,
            tls_key_path: None,
            max_log_lines: 10000,
            git_enabled: false,
            git_port: 7000,
            git_domain: "localhost".to_string(),
            git_root: None,
            git_ssh_enabled: false,
            git_ssh_port: 2222,
            git_ssh_host_key_path: None,
            data_dir: dirs::data_local_dir()
                .map(|d| d.join("muli").to_string_lossy().into_owned())
                .unwrap_or_else(|| "./data".to_string()),
            embedded_agent: false,
            default_tenant_id: None,
            git_allow_localhost_webhooks: false,
            lfs_max_object_size_mb: 5120,
        }
    }
}

impl ServerConfig {
    /// Load configuration from all sources in priority order.
    pub fn load() -> Result<Self> {
        let mut fig = Figment::new()
            .merge(Serialized::defaults(Self::default()))
            .merge(Toml::file("/etc/muli/config.toml"))
            .merge(Toml::file(xdg_config_path()))
            .merge(Toml::file("muli.toml"));

        if let Ok(path) = std::env::var("MULI_CONFIG") {
            fig = fig.merge(Toml::file(path));
        }

        fig = fig.merge(Env::prefixed("MULI_").lowercase(true));

        fig.extract().map_err(|e| anyhow::anyhow!("Config: {e}"))
    }

    /// Returns the effective registry root path.
    /// Uses `registry_root` if explicitly set; otherwise derives `{data_dir}/registry`.
    pub fn effective_registry_root(&self) -> PathBuf {
        self.registry_root
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&self.data_dir).join("registry"))
    }

    /// Returns the effective git root path.
    /// Uses `git_root` if explicitly set; otherwise derives `{data_dir}/git`.
    pub fn effective_git_root(&self) -> PathBuf {
        self.git_root
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&self.data_dir).join("git"))
    }
}

fn xdg_config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from(".config"))
        .join("muli")
        .join("config.toml")
}
