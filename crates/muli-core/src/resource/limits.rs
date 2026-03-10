// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Parsing and validation of resource limits (CPU millicores, memory bytes).

use serde::{Deserialize, Serialize};

use crate::error::{MuliError, Result};

/// Resource specification for a job (Kubernetes-compatible format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSpec {
    pub cpu_request: String,
    pub cpu_limit: String,
    pub memory_request: String,
    pub memory_limit: String,
    pub timeout_seconds: u64,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            cpu_request: "500m".to_string(),
            cpu_limit: "1000m".to_string(),
            memory_request: "512Mi".to_string(),
            memory_limit: "1Gi".to_string(),
            timeout_seconds: 1800,
        }
    }
}

/// Maximum CPU in millicores (1,000,000 = 1000 cores).
const MAX_CPU_MILLICORES: u64 = 1_000_000;

/// Maximum memory in bytes (1 TB).
const MAX_MEMORY_BYTES: u64 = 1_099_511_627_776;

/// Safely convert a non-negative finite f64 to u64, returning an error on overflow.
fn safe_f64_to_u64(value: f64, label: &str, original: &str) -> Result<u64> {
    if value.is_nan() || value.is_infinite() || value < 0.0 || value > u64::MAX as f64 {
        return Err(MuliError::Internal(format!(
            "Invalid {label} value '{original}': cannot convert to integer"
        )));
    }
    Ok(value as u64)
}

/// Parse Kubernetes-style CPU value (e.g., "1000m", "2", "500m") to millicores.
pub fn parse_cpu_millicores(cpu: &str) -> Result<u64> {
    let cpu = cpu.trim();
    let millicores = if let Some(stripped) = cpu.strip_suffix('m') {
        stripped
            .parse::<u64>()
            .map_err(|e| MuliError::Internal(format!("Invalid CPU value '{cpu}': {e}")))?
    } else {
        let v = cpu
            .parse::<f64>()
            .map_err(|e| MuliError::Internal(format!("Invalid CPU value '{cpu}': {e}")))?;
        if v < 0.0 || v.is_nan() || v.is_infinite() {
            return Err(MuliError::Internal(format!(
                "Invalid CPU value '{cpu}': must be a positive finite number"
            )));
        }
        let result = v * 1000.0;
        if result > MAX_CPU_MILLICORES as f64 {
            return Err(MuliError::Internal(format!(
                "CPU value '{cpu}' exceeds maximum of {MAX_CPU_MILLICORES} millicores"
            )));
        }
        safe_f64_to_u64(result, "CPU", cpu)?
    };
    if millicores > MAX_CPU_MILLICORES {
        return Err(MuliError::Internal(format!(
            "CPU value '{cpu}' exceeds maximum of {MAX_CPU_MILLICORES} millicores"
        )));
    }
    Ok(millicores)
}

/// Parse Kubernetes-style memory value (e.g., "512Mi", "1Gi", "256M") to bytes.
pub fn parse_memory_bytes(mem: &str) -> Result<u64> {
    let mem = mem.trim();

    let (num_str, multiplier) = if let Some(s) = mem.strip_suffix("Gi") {
        (s, 1024.0 * 1024.0 * 1024.0)
    } else if let Some(s) = mem.strip_suffix("Mi") {
        (s, 1024.0 * 1024.0)
    } else if let Some(s) = mem.strip_suffix("Ki") {
        (s, 1024.0)
    } else if let Some(s) = mem.strip_suffix('G') {
        (s, 1_000_000_000.0)
    } else if let Some(s) = mem.strip_suffix('M') {
        (s, 1_000_000.0)
    } else if let Some(s) = mem.strip_suffix('K') {
        (s, 1000.0)
    } else {
        let bytes = mem
            .parse::<u64>()
            .map_err(|e| MuliError::Internal(format!("Invalid memory value '{mem}': {e}")))?;
        if bytes > MAX_MEMORY_BYTES {
            return Err(MuliError::Internal(format!(
                "Memory value '{mem}' exceeds maximum of {MAX_MEMORY_BYTES} bytes"
            )));
        }
        return Ok(bytes);
    };

    let val: f64 = num_str
        .parse()
        .map_err(|e| MuliError::Internal(format!("Invalid memory value '{mem}': {e}")))?;
    if val < 0.0 || val.is_nan() || val.is_infinite() {
        return Err(MuliError::Internal(format!(
            "Invalid memory value '{mem}': must be a positive finite number"
        )));
    }
    let bytes = val * multiplier;
    if bytes > MAX_MEMORY_BYTES as f64 {
        return Err(MuliError::Internal(format!(
            "Memory value '{mem}' exceeds maximum of {MAX_MEMORY_BYTES} bytes"
        )));
    }
    safe_f64_to_u64(bytes, "memory", mem)
}

/// Convert CPU millicores to Docker's NanoCPUs value.
pub fn millicores_to_nano_cpus(millicores: u64) -> Result<i64> {
    let m = i64::try_from(millicores)
        .map_err(|_| MuliError::Internal(format!("CPU millicores {millicores} overflows i64")))?;
    m.checked_mul(1_000_000).ok_or_else(|| {
        MuliError::Internal(format!(
            "CPU millicores {millicores} overflows when converting to nano CPUs"
        ))
    })
}

/// Convert memory bytes to Docker's memory limit.
pub fn memory_bytes_to_docker(bytes: u64) -> Result<i64> {
    i64::try_from(bytes)
        .map_err(|_| MuliError::Internal(format!("Memory bytes {bytes} overflows i64")))
}

impl ResourceSpec {
    /// Parse limits for Docker container configuration.
    pub fn to_docker_limits(&self) -> Result<DockerResourceLimits> {
        Ok(DockerResourceLimits {
            nano_cpus: millicores_to_nano_cpus(parse_cpu_millicores(&self.cpu_limit)?)?,
            memory_bytes: memory_bytes_to_docker(parse_memory_bytes(&self.memory_limit)?)?,
            cpu_request_millicores: parse_cpu_millicores(&self.cpu_request)?,
            memory_request_bytes: parse_memory_bytes(&self.memory_request)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DockerResourceLimits {
    pub nano_cpus: i64,
    pub memory_bytes: i64,
    pub cpu_request_millicores: u64,
    pub memory_request_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_millicores() {
        assert_eq!(parse_cpu_millicores("1000m").unwrap(), 1000);
        assert_eq!(parse_cpu_millicores("500m").unwrap(), 500);
        assert_eq!(parse_cpu_millicores("2").unwrap(), 2000);
        assert_eq!(parse_cpu_millicores("0.5").unwrap(), 500);
    }

    #[test]
    fn test_parse_memory_bytes() {
        assert_eq!(parse_memory_bytes("512Mi").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_bytes("1Gi").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_bytes("256Mi").unwrap(), 256 * 1024 * 1024);
        assert_eq!(parse_memory_bytes("128Ki").unwrap(), 128 * 1024);
        assert_eq!(parse_memory_bytes("1G").unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_millicores_to_nano() {
        assert_eq!(millicores_to_nano_cpus(1000).unwrap(), 1_000_000_000);
        assert_eq!(millicores_to_nano_cpus(500).unwrap(), 500_000_000);
    }

    #[test]
    fn test_resource_spec_to_docker() {
        let spec = ResourceSpec {
            cpu_request: "500m".to_string(),
            cpu_limit: "1000m".to_string(),
            memory_request: "512Mi".to_string(),
            memory_limit: "1Gi".to_string(),
            timeout_seconds: 1800,
        };
        let limits = spec.to_docker_limits().unwrap();
        assert_eq!(limits.nano_cpus, 1_000_000_000);
        assert_eq!(limits.memory_bytes, 1024 * 1024 * 1024);
    }
}
