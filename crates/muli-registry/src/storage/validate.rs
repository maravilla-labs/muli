// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Digest and path validation for storage operations.

use std::path::{Component, Path};

use super::{StorageError, StorageResult};

/// Validate digest format strictly: algorithm must be sha256 or sha512,
/// hash must be lowercase hex of the correct length.
pub fn parse_digest(digest: &str) -> StorageResult<(&str, &str)> {
    crate::validation::validate_digest(digest)
        .map_err(|_| StorageError::InvalidDigest(digest.to_string()))?;
    // Safe to unwrap: validate_digest already confirmed the format
    Ok(digest.split_once(':').unwrap())
}

/// Verify no path component is ".." to prevent path traversal.
pub fn ensure_no_traversal(path: &Path) -> StorageResult<()> {
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(StorageError::InvalidInput(
                "path traversal detected".to_string(),
            ));
        }
    }
    Ok(())
}

/// Validate upload ID is a UUID (hex digits and hyphens, correct format).
pub fn validate_upload_id(id: &str) -> StorageResult<()> {
    if id.len() != 36 {
        return Err(StorageError::InvalidInput(format!(
            "invalid upload id: {id}"
        )));
    }
    let parts: Vec<&str> = id.split('-').collect();
    if parts.len() != 5
        || parts[0].len() != 8
        || parts[1].len() != 4
        || parts[2].len() != 4
        || parts[3].len() != 4
        || parts[4].len() != 12
    {
        return Err(StorageError::InvalidInput(format!(
            "invalid upload id: {id}"
        )));
    }
    if !parts
        .iter()
        .all(|p| p.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err(StorageError::InvalidInput(format!(
            "invalid upload id: {id}"
        )));
    }
    Ok(())
}
