// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat package name, version and requirement validation.
//!
//! Names are always scoped: `@scope/name`, both parts matching
//! `[a-z0-9][a-z0-9._-]{0,63}`. That rule also makes each part a safe single
//! path component on disk (it can never be empty, `.` or `..`).

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LuatValidationError {
    #[error(
        "invalid package name `{0}`: expected @scope/name, each part matching [a-z0-9][a-z0-9._-]{{0,63}}"
    )]
    InvalidName(String),
    #[error("invalid version `{0}`: expected semver 2.0")]
    InvalidVersion(String),
    #[error("invalid version requirement `{0}`")]
    InvalidRequirement(String),
}

/// Validate one part of a package name (the scope or the bare name).
pub fn is_valid_part(part: &str) -> bool {
    let bytes = part.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    let first_ok = bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit();
    first_ok
        && bytes[1..].iter().all(|&b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

/// Split and validate a full `@scope/name`, returning `(scope, name)`.
pub fn parse_full_name(full: &str) -> Result<(&str, &str), LuatValidationError> {
    let err = || LuatValidationError::InvalidName(full.to_string());
    let rest = full.strip_prefix('@').ok_or_else(err)?;
    let (scope, name) = rest.split_once('/').ok_or_else(err)?;
    if is_valid_part(scope) && is_valid_part(name) {
        Ok((scope, name))
    } else {
        Err(err())
    }
}

/// Validate the `{scope}` and `{name}` path parameters (scope without `@`).
pub fn validate_parts(scope: &str, name: &str) -> Result<(), LuatValidationError> {
    if is_valid_part(scope) && is_valid_part(name) {
        Ok(())
    } else {
        Err(LuatValidationError::InvalidName(format!("@{scope}/{name}")))
    }
}

/// Parse a version as semver 2.0.
pub fn parse_version(version: &str) -> Result<semver::Version, LuatValidationError> {
    semver::Version::parse(version)
        .map_err(|_| LuatValidationError::InvalidVersion(version.to_string()))
}

/// Validate a semver requirement (`^1.2`, `>=0.2`, `~1.0.3`, ...).
pub fn validate_requirement(req: &str) -> Result<(), LuatValidationError> {
    semver::VersionReq::parse(req)
        .map(|_| ())
        .map_err(|_| LuatValidationError::InvalidRequirement(req.to_string()))
}

/// Two versions denote the same release when they have equal precedence
/// (build metadata is ignored, as semver and every package manager do).
pub fn same_release(a: &str, b: &str) -> bool {
    match (semver::Version::parse(a), semver::Version::parse(b)) {
        (Ok(a), Ok(b)) => a.cmp_precedence(&b).is_eq(),
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_names() {
        assert_eq!(parse_full_name("@acme/ui"), Ok(("acme", "ui")));
        assert!(parse_full_name("@a/b").is_ok());
        assert!(parse_full_name("@0x/my.pkg_name-2").is_ok());
        assert!(parse_full_name(&format!("@{}/{}", "a".repeat(64), "b".repeat(64))).is_ok());
    }

    #[test]
    fn invalid_names() {
        for bad in [
            "acme/ui",
            "@acme",
            "@/ui",
            "@acme/",
            "@Acme/ui",
            "@acme/UI",
            "@.acme/ui",
            "@acme/-ui",
            "@acme/_ui",
            "@acme/u i",
            "@acme/ui/extra",
            "@acme/..",
            "@../ui",
        ] {
            assert!(parse_full_name(bad).is_err(), "{bad} should be rejected");
        }
        assert!(parse_full_name(&format!("@{}/b", "a".repeat(65))).is_err());
    }

    #[test]
    fn versions_and_requirements() {
        assert!(parse_version("1.2.0").is_ok());
        assert!(parse_version("1.2.0-beta.1+build.5").is_ok());
        assert!(parse_version("1.2").is_err());
        assert!(parse_version("v1.2.0").is_err());
        assert!(validate_requirement("^2.0").is_ok());
        assert!(validate_requirement(">=0.2").is_ok());
        assert!(validate_requirement("not a req").is_err());
    }

    #[test]
    fn same_release_ignores_build_metadata() {
        assert!(same_release("1.0.0", "1.0.0+abc"));
        assert!(!same_release("1.0.0", "1.0.1"));
        assert!(!same_release("1.0.0", "1.0.0-rc.1"));
    }
}
