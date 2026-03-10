// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resource limit validation (CPU, memory ranges).

use super::{ValidationResult, err};

/// Validate a resource spec string (cpu or memory) is syntactically valid.
/// Rejects negative values and invalid formats.
pub fn validate_resource_spec(field_name: &str, value: &str) -> ValidationResult {
    if value.is_empty() {
        return err(field_name, "must not be empty");
    }
    if value.len() > 32 {
        return err(field_name, "must be at most 32 characters");
    }
    let valid_suffixes = ["m", "Mi", "Gi", "Ki", "M", "G", "K"];
    let trimmed = value.trim();
    let has_valid_suffix = valid_suffixes.iter().any(|s| trimmed.ends_with(s));
    if has_valid_suffix {
        // Strip the suffix and check the numeric part
        let num_part = valid_suffixes
            .iter()
            .filter_map(|s| trimmed.strip_suffix(s))
            .next()
            .unwrap_or(trimmed);
        if let Ok(v) = num_part.parse::<f64>() {
            if v < 0.0 {
                return err(field_name, "must not be negative");
            }
        } else {
            return err(field_name, "invalid resource format");
        }
    } else {
        // Must be a plain number
        match trimmed.parse::<f64>() {
            Ok(v) if v < 0.0 => return err(field_name, "must not be negative"),
            Err(_) => return err(field_name, "invalid resource format"),
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::validation::*;

    #[test]
    fn test_validate_resource_spec_valid() {
        assert!(validate_resource_spec("cpu", "500m").is_ok());
        assert!(validate_resource_spec("cpu", "1000m").is_ok());
        assert!(validate_resource_spec("mem", "512Mi").is_ok());
        assert!(validate_resource_spec("mem", "1Gi").is_ok());
        assert!(validate_resource_spec("cpu", "2").is_ok());
        assert!(validate_resource_spec("cpu", "0.5").is_ok());
    }

    #[test]
    fn test_validate_resource_spec_invalid() {
        assert!(validate_resource_spec("cpu", "").is_err());
        assert!(validate_resource_spec("cpu", "abc").is_err());
    }

    #[test]
    fn test_validate_resource_spec_negative() {
        assert!(validate_resource_spec("cpu", "-100m").is_err());
        assert!(validate_resource_spec("cpu", "-1").is_err());
        assert!(validate_resource_spec("mem", "-512Mi").is_err());
    }
}
