// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo crate name and version validation.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CargoValidationError {
    #[error("invalid crate name: {0}")]
    InvalidCrateName(String),
    #[error("invalid version: {0}")]
    InvalidVersion(String),
}

/// Validate a crate name per Cargo registry conventions.
/// - Must be non-empty and at most 64 characters
/// - Must start with an ASCII letter
/// - May contain ASCII alphanumerics, `-`, or `_`
pub fn validate_crate_name(name: &str) -> Result<(), CargoValidationError> {
    if name.is_empty() || name.len() > 64 {
        return Err(CargoValidationError::InvalidCrateName(name.to_string()));
    }
    let first = name.as_bytes()[0];
    if !first.is_ascii_alphabetic() {
        return Err(CargoValidationError::InvalidCrateName(name.to_string()));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(CargoValidationError::InvalidCrateName(name.to_string()));
    }
    Ok(())
}

/// Validate a version string as valid semver.
pub fn validate_version(version: &str) -> Result<(), CargoValidationError> {
    semver::Version::parse(version)
        .map_err(|_| CargoValidationError::InvalidVersion(version.to_string()))?;
    Ok(())
}

/// Compute the sparse index path prefix for a crate name.
/// - 1 char: `1/{name}`
/// - 2 chars: `2/{name}`
/// - 3 chars: `3/{first_char}/{name}`
/// - 4+ chars: `{first_two}/{next_two}/{name}`
pub fn index_prefix(name: &str) -> String {
    let lower = name.to_lowercase();
    match lower.len() {
        1 => format!("1/{lower}"),
        2 => format!("2/{lower}"),
        3 => format!("3/{}/{}", &lower[..1], lower),
        _ => format!("{}/{}/{}", &lower[..2], &lower[2..4], lower),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate_crate_name ---

    #[test]
    fn valid_crate_names() {
        assert!(validate_crate_name("serde").is_ok());
        assert!(validate_crate_name("my-crate").is_ok());
        assert!(validate_crate_name("my_crate").is_ok());
        assert!(validate_crate_name("a").is_ok());
        assert!(validate_crate_name("Tokio").is_ok());
        assert!(validate_crate_name("a1b2c3").is_ok());
    }

    #[test]
    fn invalid_crate_names() {
        assert!(validate_crate_name("").is_err());
        assert!(validate_crate_name("1start").is_err());
        assert!(validate_crate_name("-start").is_err());
        assert!(validate_crate_name("has space").is_err());
        assert!(validate_crate_name("has.dot").is_err());
        assert!(validate_crate_name(&"a".repeat(65)).is_err());
    }

    #[test]
    fn valid_versions() {
        assert!(validate_version("1.0.0").is_ok());
        assert!(validate_version("0.0.1").is_ok());
        assert!(validate_version("1.2.3-beta.1").is_ok());
        assert!(validate_version("1.0.0+build.123").is_ok());
    }

    #[test]
    fn invalid_versions() {
        assert!(validate_version("").is_err());
        assert!(validate_version("1.0").is_err());
        assert!(validate_version("not-a-version").is_err());
    }

    // --- index_prefix ---

    #[test]
    fn index_prefix_1_char() {
        assert_eq!(index_prefix("a"), "1/a");
        assert_eq!(index_prefix("A"), "1/a");
    }

    #[test]
    fn index_prefix_2_chars() {
        assert_eq!(index_prefix("ab"), "2/ab");
        assert_eq!(index_prefix("AB"), "2/ab");
    }

    #[test]
    fn index_prefix_3_chars() {
        assert_eq!(index_prefix("abc"), "3/a/abc");
        assert_eq!(index_prefix("ABC"), "3/a/abc");
    }

    #[test]
    fn index_prefix_4plus_chars() {
        assert_eq!(index_prefix("serde"), "se/rd/serde");
        assert_eq!(index_prefix("tokio"), "to/ki/tokio");
        assert_eq!(index_prefix("SERDE"), "se/rd/serde");
        assert_eq!(index_prefix("abcd"), "ab/cd/abcd");
    }
}
