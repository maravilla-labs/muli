// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Default resource presets for common ML and web frameworks.

use super::limits::ResourceSpec;

/// Get default resource limits for a given framework.
/// Ported from staticlab's K8sJobManager::get_framework_defaults.
pub fn get_framework_defaults(framework: &str) -> ResourceSpec {
    match framework.to_lowercase().as_str() {
        "nextjs" | "next" => ResourceSpec {
            cpu_request: "1000m".to_string(),
            cpu_limit: "4000m".to_string(),
            memory_request: "1Gi".to_string(),
            memory_limit: "4Gi".to_string(),
            timeout_seconds: 2400,
        },
        "gatsby" => ResourceSpec {
            cpu_request: "1000m".to_string(),
            cpu_limit: "4000m".to_string(),
            memory_request: "1Gi".to_string(),
            memory_limit: "4Gi".to_string(),
            timeout_seconds: 3600,
        },
        "hugo" => ResourceSpec {
            cpu_request: "250m".to_string(),
            cpu_limit: "1000m".to_string(),
            memory_request: "256Mi".to_string(),
            memory_limit: "1Gi".to_string(),
            timeout_seconds: 600,
        },
        "jekyll" => ResourceSpec {
            cpu_request: "500m".to_string(),
            cpu_limit: "2000m".to_string(),
            memory_request: "512Mi".to_string(),
            memory_limit: "2Gi".to_string(),
            timeout_seconds: 1200,
        },
        "astro" => ResourceSpec {
            cpu_request: "500m".to_string(),
            cpu_limit: "2000m".to_string(),
            memory_request: "512Mi".to_string(),
            memory_limit: "2Gi".to_string(),
            timeout_seconds: 1800,
        },
        "vite" => ResourceSpec {
            cpu_request: "500m".to_string(),
            cpu_limit: "2000m".to_string(),
            memory_request: "512Mi".to_string(),
            memory_limit: "2Gi".to_string(),
            timeout_seconds: 1200,
        },
        "docusaurus" => ResourceSpec {
            cpu_request: "500m".to_string(),
            cpu_limit: "2000m".to_string(),
            memory_request: "512Mi".to_string(),
            memory_limit: "2Gi".to_string(),
            timeout_seconds: 1800,
        },
        "static" | "html" => ResourceSpec {
            cpu_request: "100m".to_string(),
            cpu_limit: "500m".to_string(),
            memory_request: "128Mi".to_string(),
            memory_limit: "512Mi".to_string(),
            timeout_seconds: 300,
        },
        _ => ResourceSpec::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nextjs_defaults() {
        let spec = get_framework_defaults("nextjs");
        assert_eq!(spec.cpu_limit, "4000m");
        assert_eq!(spec.memory_limit, "4Gi");
        assert_eq!(spec.timeout_seconds, 2400);
    }

    #[test]
    fn test_static_defaults() {
        let spec = get_framework_defaults("static");
        assert_eq!(spec.cpu_limit, "500m");
        assert_eq!(spec.memory_limit, "512Mi");
        assert_eq!(spec.timeout_seconds, 300);
    }

    #[test]
    fn test_unknown_framework_defaults() {
        let spec = get_framework_defaults("unknown_framework");
        let default = ResourceSpec::default();
        assert_eq!(spec.cpu_limit, default.cpu_limit);
        assert_eq!(spec.memory_limit, default.memory_limit);
    }

    #[test]
    fn test_case_insensitive() {
        let spec = get_framework_defaults("NextJS");
        assert_eq!(spec.timeout_seconds, 2400);
    }
}
