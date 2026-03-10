// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Input validation for OCI registry operations.

/// Validation errors for registry inputs.
#[derive(Debug, Clone)]
pub enum ValidationError {
    InvalidRepositoryName(String),
    InvalidReference(String),
    InvalidDigest(String),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRepositoryName(n) => write!(f, "invalid repository name: {n}"),
            Self::InvalidReference(r) => write!(f, "invalid reference: {r}"),
            Self::InvalidDigest(d) => write!(f, "invalid digest format: {d}"),
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validate a repository name per OCI Distribution Spec.
/// Pattern: `[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)*`
/// Max length: 256 characters.
pub fn validate_repository_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() || name.len() > 256 {
        return Err(ValidationError::InvalidRepositoryName(name.to_string()));
    }
    for component in name.split('/') {
        if !is_valid_name_component(component) {
            return Err(ValidationError::InvalidRepositoryName(name.to_string()));
        }
    }
    Ok(())
}

/// Validate a tag or digest reference.
/// Tags: `[a-zA-Z0-9_.-]+` (max 128 chars)
/// Digests: validated via `validate_digest`.
pub fn validate_reference(reference: &str) -> Result<(), ValidationError> {
    if reference.is_empty() {
        return Err(ValidationError::InvalidReference(reference.to_string()));
    }
    // If it looks like a digest, validate as digest
    if reference.contains(':') {
        return validate_digest(reference)
            .map_err(|_| ValidationError::InvalidReference(reference.to_string()));
    }
    // Tag: [a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127} (must not start with . or -)
    if reference.len() > 128 {
        return Err(ValidationError::InvalidReference(reference.to_string()));
    }
    let first = reference.as_bytes()[0];
    if !first.is_ascii_alphanumeric() && first != b'_' {
        return Err(ValidationError::InvalidReference(reference.to_string()));
    }
    if !reference
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
    {
        return Err(ValidationError::InvalidReference(reference.to_string()));
    }
    Ok(())
}

/// Validate a digest format: `(sha256|sha512):[a-f0-9]{64,128}`
pub fn validate_digest(digest: &str) -> Result<(), ValidationError> {
    let (algo, hash) = digest
        .split_once(':')
        .ok_or_else(|| ValidationError::InvalidDigest(digest.to_string()))?;

    let expected_len = match algo {
        "sha256" => 64,
        "sha512" => 128,
        _ => return Err(ValidationError::InvalidDigest(digest.to_string())),
    };

    if hash.len() != expected_len {
        return Err(ValidationError::InvalidDigest(digest.to_string()));
    }

    if !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(ValidationError::InvalidDigest(digest.to_string()));
    }

    Ok(())
}

/// Check that a name component matches `[a-z0-9]+([._-][a-z0-9]+)*`.
fn is_valid_name_component(component: &str) -> bool {
    if component.is_empty() {
        return false;
    }
    let bytes = component.as_bytes();
    let mut i = 0;

    // First segment: [a-z0-9]+
    if !matches!(bytes[i], b'a'..=b'z' | b'0'..=b'9') {
        return false;
    }
    i += 1;
    while i < bytes.len() && matches!(bytes[i], b'a'..=b'z' | b'0'..=b'9') {
        i += 1;
    }

    // Optional: ([._-][a-z0-9]+)*
    while i < bytes.len() {
        if !matches!(bytes[i], b'.' | b'_' | b'-') {
            return false;
        }
        i += 1;
        if i >= bytes.len() || !matches!(bytes[i], b'a'..=b'z' | b'0'..=b'9') {
            return false;
        }
        i += 1;
        while i < bytes.len() && matches!(bytes[i], b'a'..=b'z' | b'0'..=b'9') {
            i += 1;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_repository_names() {
        assert!(validate_repository_name("myrepo").is_ok());
        assert!(validate_repository_name("my-repo").is_ok());
        assert!(validate_repository_name("my.repo").is_ok());
        assert!(validate_repository_name("my_repo").is_ok());
        assert!(validate_repository_name("library/nginx").is_ok());
        assert!(validate_repository_name("org/team/app").is_ok());
        assert!(validate_repository_name("abc123").is_ok());
        assert!(validate_repository_name("a0").is_ok());
    }

    #[test]
    fn invalid_repository_names() {
        assert!(validate_repository_name("").is_err());
        assert!(validate_repository_name("..").is_err());
        assert!(validate_repository_name("../etc/passwd").is_err());
        assert!(validate_repository_name("MyRepo").is_err());
        assert!(validate_repository_name("-repo").is_err());
        assert!(validate_repository_name("repo/").is_err());
        assert!(validate_repository_name("/repo").is_err());
        assert!(validate_repository_name("re po").is_err());
        assert!(validate_repository_name("repo//name").is_err());
    }

    #[test]
    fn valid_references() {
        assert!(validate_reference("latest").is_ok());
        assert!(validate_reference("v1.0.0").is_ok());
        assert!(validate_reference("my-tag_v2").is_ok());
        let digest_ref = format!("sha256:{}", "a".repeat(64));
        assert!(validate_reference(&digest_ref).is_ok());
    }

    #[test]
    fn invalid_references() {
        assert!(validate_reference("").is_err());
        assert!(validate_reference("..").is_err());
        assert!(validate_reference("tag with spaces").is_err());
        assert!(validate_reference("tag/slash").is_err());
    }

    #[test]
    fn valid_digests() {
        let sha256 = format!("sha256:{}", "a".repeat(64));
        assert!(validate_digest(&sha256).is_ok());
        let sha512 = format!("sha512:{}", "a".repeat(128));
        assert!(validate_digest(&sha512).is_ok());
        let sha256_mixed = format!("sha256:{}", "0123456789abcdef".repeat(4));
        assert!(validate_digest(&sha256_mixed).is_ok());
    }

    #[test]
    fn invalid_digests() {
        assert!(validate_digest("").is_err());
        assert!(validate_digest("nocolon").is_err());
        assert!(validate_digest("md5:abc123").is_err());
        assert!(validate_digest("sha256:tooshort").is_err());
        assert!(validate_digest(&format!("sha256:{}", "G".repeat(64))).is_err());
        assert!(validate_digest(&format!("sha256:{}", "A".repeat(64))).is_err());
        assert!(validate_digest(&format!("sha256:{}", "a".repeat(63))).is_err());
        assert!(validate_digest(&format!("sha256:{}", "a".repeat(65))).is_err());
    }
}
