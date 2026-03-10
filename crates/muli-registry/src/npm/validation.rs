// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm package name and version validation.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NpmValidationError {
    #[error("invalid package name: {0}")]
    InvalidPackageName(String),
    #[error("invalid version: {0}")]
    InvalidVersion(String),
}

/// Validate an npm package name.
///
/// Rules:
/// - 1-214 characters
/// - Scoped packages: @scope/name (scope must be at least 1 char, name non-empty)
/// - Unscoped packages: lowercase, no leading dots or underscores, only [a-z0-9._~-]
pub fn validate_package_name(name: &str) -> Result<(), NpmValidationError> {
    if name.is_empty() || name.len() > 214 {
        return Err(NpmValidationError::InvalidPackageName(name.to_string()));
    }

    // Allow scoped packages
    if name.starts_with('@') {
        let parts: Vec<&str> = name.splitn(2, '/').collect();
        if parts.len() != 2 || parts[0].len() < 2 || parts[1].is_empty() {
            return Err(NpmValidationError::InvalidPackageName(name.to_string()));
        }
        return Ok(());
    }

    // Unscoped: must be lowercase, no leading dots or underscores
    if name.starts_with('.') || name.starts_with('_') {
        return Err(NpmValidationError::InvalidPackageName(name.to_string()));
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "-._~".contains(c))
    {
        return Err(NpmValidationError::InvalidPackageName(name.to_string()));
    }

    Ok(())
}

/// Validate a version string as valid semver.
pub fn validate_version(version: &str) -> Result<(), NpmValidationError> {
    semver::Version::parse(version)
        .map_err(|_| NpmValidationError::InvalidVersion(version.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_unscoped_names() {
        assert!(validate_package_name("my-package").is_ok());
        assert!(validate_package_name("my.package").is_ok());
        assert!(validate_package_name("my_package").is_ok());
        assert!(validate_package_name("my~package").is_ok());
        assert!(validate_package_name("a").is_ok());
        assert!(validate_package_name("pkg123").is_ok());
    }

    #[test]
    fn valid_scoped_names() {
        assert!(validate_package_name("@scope/name").is_ok());
        assert!(validate_package_name("@my-org/my-package").is_ok());
    }

    #[test]
    fn invalid_names() {
        assert!(validate_package_name("").is_err());
        assert!(validate_package_name(".leading-dot").is_err());
        assert!(validate_package_name("_leading-underscore").is_err());
        assert!(validate_package_name("UPPERCASE").is_err());
        assert!(validate_package_name("@/missing-scope").is_err());
        assert!(validate_package_name("@scope/").is_err());
        assert!(validate_package_name(&"a".repeat(215)).is_err());
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
}
