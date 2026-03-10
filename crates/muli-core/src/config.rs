// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server configuration loaded from environment variables.

use serde::{Deserialize, Serialize};

/// Shared configuration types used across crates.
#[derive(Clone, Serialize, Deserialize)]
pub struct MuliConfig {
    pub mongodb_url: String,
    pub mongodb_database: String,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub max_concurrent_jobs: usize,
    pub max_jobs_per_tenant: usize,
    pub container_timeout_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub cleanup_max_age_seconds: u64,
    pub total_cpu_millicores: u64,
    pub total_memory_bytes: u64,
    pub default_registry: Option<RegistryConfig>,
    pub registry: Option<EmbeddedRegistryConfig>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub server: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedRegistryConfig {
    pub enabled: bool,
    pub port: u16,
    pub root: String,
    pub max_size_gb: u64,
}

impl std::fmt::Debug for MuliConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MuliConfig")
            .field("mongodb_url", &"[REDACTED]")
            .field("mongodb_database", &self.mongodb_database)
            .field("grpc_port", &self.grpc_port)
            .field("metrics_port", &self.metrics_port)
            .field("max_concurrent_jobs", &self.max_concurrent_jobs)
            .field("max_jobs_per_tenant", &self.max_jobs_per_tenant)
            .field("container_timeout_seconds", &self.container_timeout_seconds)
            .field("cleanup_interval_seconds", &self.cleanup_interval_seconds)
            .field("cleanup_max_age_seconds", &self.cleanup_max_age_seconds)
            .field("total_cpu_millicores", &self.total_cpu_millicores)
            .field("total_memory_bytes", &self.total_memory_bytes)
            .field("default_registry", &self.default_registry)
            .field("registry", &self.registry)
            .finish()
    }
}

impl std::fmt::Debug for RegistryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryConfig")
            .field("server", &self.server)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl Default for MuliConfig {
    fn default() -> Self {
        Self {
            mongodb_url: "mongodb://localhost:27017".to_string(),
            mongodb_database: "muli".to_string(),
            grpc_port: 50051,
            metrics_port: 9090,
            max_concurrent_jobs: 10,
            max_jobs_per_tenant: 3,
            container_timeout_seconds: 3600,
            cleanup_interval_seconds: 300,
            cleanup_max_age_seconds: 3600,
            total_cpu_millicores: 8000,
            total_memory_bytes: 17_179_869_184, // 16 GiB
            default_registry: None,
            registry: None,
        }
    }
}
