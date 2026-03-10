// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Input validation utilities.

mod resource;

pub use resource::validate_resource_spec;

use std::fmt;

/// Validation error with a descriptive message suitable for gRPC Status::invalid_argument.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for ValidationError {}

pub type ValidationResult = Result<(), ValidationError>;

fn err(field: &str, message: impl Into<String>) -> ValidationResult {
    Err(ValidationError {
        field: field.to_string(),
        message: message.into(),
    })
}

/// Validate an identifier field (tenant_id, project_id, workspace_id, deployment_id).
/// Must be non-empty, max 256 chars, alphanumeric + hyphens + underscores.
pub fn validate_identifier(field_name: &str, value: &str) -> ValidationResult {
    if value.is_empty() {
        return err(field_name, "must not be empty");
    }
    if value.len() > 256 {
        return err(field_name, "must be at most 256 characters");
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return err(
            field_name,
            "must contain only alphanumeric characters, hyphens, and underscores",
        );
    }
    Ok(())
}

/// Validate a Docker image reference (e.g., "nginx:latest", "registry.io/org/image:tag",
/// "image@sha256:abc123...").
pub fn validate_docker_image(field_name: &str, value: &str) -> ValidationResult {
    if value.is_empty() {
        return err(field_name, "must not be empty");
    }
    if value.len() > 512 {
        return err(field_name, "must be at most 512 characters");
    }

    // Check for digest reference
    if let Some(at_pos) = value.find('@') {
        let digest = &value[at_pos + 1..];
        if !digest.starts_with("sha256:") || digest.len() != 71 {
            return err(
                field_name,
                "invalid digest format, expected sha256:<64 hex chars>",
            );
        }
        let hex = &digest[7..];
        if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return err(field_name, "digest must contain only hex characters");
        }
        let name = &value[..at_pos];
        if name.is_empty() {
            return err(field_name, "image name must not be empty");
        }
        return Ok(());
    }

    // Split name:tag
    let (name, tag) = if let Some(colon_pos) = value.rfind(':') {
        // Ensure the colon is not inside a registry port (e.g., "registry:5000/image")
        let after_colon = &value[colon_pos + 1..];
        if after_colon.contains('/') {
            // The colon is part of the registry, no explicit tag
            (value, "")
        } else {
            (&value[..colon_pos], after_colon)
        }
    } else {
        (value, "")
    };

    if name.is_empty() {
        return err(field_name, "image name must not be empty");
    }

    for c in name.chars() {
        if !(c.is_ascii_alphanumeric() || c == '/' || c == '.' || c == '-' || c == '_' || c == ':')
        {
            return err(field_name, format!("invalid character '{c}' in image name"));
        }
    }

    if !tag.is_empty() {
        if tag.len() > 128 {
            return err(field_name, "tag must be at most 128 characters");
        }
        for c in tag.chars() {
            if !(c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_') {
                return err(field_name, format!("invalid character '{c}' in tag"));
            }
        }
    }

    Ok(())
}

/// Validate an environment variable name. Must match ^[a-zA-Z_][a-zA-Z0-9_]*$.
pub fn validate_env_var_name(value: &str) -> ValidationResult {
    if value.is_empty() {
        return err("env_var_name", "must not be empty");
    }
    if value.len() > 256 {
        return err("env_var_name", "must be at most 256 characters");
    }
    let first = value.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return err("env_var_name", "must start with a letter or underscore");
    }
    if !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return err(
            "env_var_name",
            "must contain only alphanumeric characters and underscores",
        );
    }
    Ok(())
}

/// Validate an agent name. Non-empty, max 255 chars, alphanumeric + hyphens + underscores.
pub fn validate_agent_name(value: &str) -> ValidationResult {
    if value.is_empty() {
        return err("name", "must not be empty");
    }
    if value.len() > 255 {
        return err("name", "must be at most 255 characters");
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return err(
            "name",
            "must contain only alphanumeric characters, hyphens, and underscores",
        );
    }
    Ok(())
}

/// Validate a label in key=value format.
pub fn validate_label(value: &str) -> ValidationResult {
    if value.is_empty() {
        return err("label", "must not be empty");
    }
    if value.len() > 512 {
        return err("label", "must be at most 512 characters");
    }
    if !value.contains('=') {
        return err("label", "must be in key=value format");
    }
    let (key, _) = value.split_once('=').unwrap();
    if key.is_empty() {
        return err("label", "key must not be empty");
    }
    Ok(())
}

/// Validate a string field is non-empty with a max length.
pub fn validate_non_empty(field_name: &str, value: &str, max_len: usize) -> ValidationResult {
    if value.is_empty() {
        return err(field_name, "must not be empty");
    }
    if value.len() > max_len {
        return err(field_name, format!("must be at most {max_len} characters"));
    }
    Ok(())
}

/// Validate a string field length (may be empty).
pub fn validate_max_length(field_name: &str, value: &str, max_len: usize) -> ValidationResult {
    if value.len() > max_len {
        return err(field_name, format!("must be at most {max_len} characters"));
    }
    Ok(())
}

/// Cap a list limit to a maximum value. Zero means "use default" (100, capped to max).
pub fn cap_limit(limit: u32, max: u32) -> u32 {
    if limit == 0 {
        100.min(max)
    } else {
        limit.min(max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("tenant_id", "my-tenant_123").is_ok());
        assert!(validate_identifier("tenant_id", "a").is_ok());
    }

    #[test]
    fn test_validate_identifier_empty() {
        assert!(validate_identifier("tenant_id", "").is_err());
    }

    #[test]
    fn test_validate_identifier_too_long() {
        let long = "a".repeat(257);
        assert!(validate_identifier("tenant_id", &long).is_err());
    }

    #[test]
    fn test_validate_identifier_invalid_chars() {
        assert!(validate_identifier("tenant_id", "my tenant").is_err());
        assert!(validate_identifier("tenant_id", "my/tenant").is_err());
        assert!(validate_identifier("tenant_id", "my@tenant").is_err());
    }

    #[test]
    fn test_validate_docker_image_valid() {
        assert!(validate_docker_image("image", "nginx:latest").is_ok());
        assert!(validate_docker_image("image", "nginx").is_ok());
        assert!(validate_docker_image("image", "registry.io/org/image:v1.2.3").is_ok());
        assert!(validate_docker_image("image", "registry:5000/image").is_ok());
        let digest = format!("image@sha256:{}", "a".repeat(64));
        assert!(validate_docker_image("image", &digest).is_ok());
    }

    #[test]
    fn test_validate_docker_image_invalid() {
        assert!(validate_docker_image("image", "").is_err());
        assert!(validate_docker_image("image", "image@sha256:short").is_err());
    }

    #[test]
    fn test_validate_env_var_name_valid() {
        assert!(validate_env_var_name("MY_VAR").is_ok());
        assert!(validate_env_var_name("_PRIVATE").is_ok());
        assert!(validate_env_var_name("a123").is_ok());
    }

    #[test]
    fn test_validate_env_var_name_invalid() {
        assert!(validate_env_var_name("").is_err());
        assert!(validate_env_var_name("123abc").is_err());
        assert!(validate_env_var_name("MY-VAR").is_err());
    }

    #[test]
    fn test_validate_agent_name() {
        assert!(validate_agent_name("my-agent_1").is_ok());
        assert!(validate_agent_name("").is_err());
        assert!(validate_agent_name("agent name").is_err());
    }

    #[test]
    fn test_validate_label() {
        assert!(validate_label("env=production").is_ok());
        assert!(validate_label("key=").is_ok());
        assert!(validate_label("").is_err());
        assert!(validate_label("noequals").is_err());
        assert!(validate_label("=value").is_err());
    }

    #[test]
    fn test_cap_limit() {
        assert_eq!(cap_limit(0, 1000), 100);
        assert_eq!(cap_limit(50, 1000), 50);
        assert_eq!(cap_limit(2000, 1000), 1000);
    }
}
