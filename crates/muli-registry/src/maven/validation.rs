// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Maven coordinate parsing and path validation.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MavenValidationError {
    #[error("invalid maven path: {0}")]
    InvalidPath(String),
    #[error("path traversal attempt: {0}")]
    PathTraversal(String),
    #[error("invalid group id: {0}")]
    InvalidGroupId(String),
    #[error("invalid artifact id: {0}")]
    InvalidArtifactId(String),
    #[error("invalid version: {0}")]
    InvalidVersion(String),
}

/// Parsed Maven coordinate from a URL path.
#[derive(Debug, Clone)]
pub struct MavenCoordinate {
    /// e.g., "org.apache.commons" (dots represent the group hierarchy)
    pub group_id: String,
    /// e.g., "commons-lang3"
    pub artifact_id: String,
    /// e.g., "3.14.0" or "1.0.0-SNAPSHOT"
    pub version: Option<String>,
    /// e.g., "commons-lang3-3.14.0.jar"
    pub filename: Option<String>,
}

impl MavenCoordinate {
    /// Returns true if this is a snapshot version.
    pub fn is_snapshot(&self) -> bool {
        self.version
            .as_ref()
            .is_some_and(|v| v.ends_with("-SNAPSHOT"))
    }
}

/// Classification of a Maven path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MavenPathKind {
    /// `groupId/artifactId/maven-metadata.xml`
    ArtifactLevelMetadata,
    /// `groupId/artifactId/version/maven-metadata.xml`
    VersionLevelMetadata,
    /// `groupId/artifactId/version/filename`
    Artifact,
    /// Any of the above with a checksum extension (`.sha1`, `.md5`, `.sha256`, `.sha512`)
    Checksum {
        /// The checksum algorithm extension (e.g., "sha1", "md5")
        algorithm: String,
        /// The kind of the base path (without the checksum extension)
        base_kind: Box<MavenPathKind>,
    },
}

/// Checksum file extensions we recognise.
const CHECKSUM_EXTENSIONS: &[&str] = &[".sha1", ".md5", ".sha256", ".sha512"];

/// Parse a Maven URL path into a coordinate and path kind.
///
/// Expected path formats (already URL-decoded by axum):
/// - `org/apache/commons/commons-lang3/maven-metadata.xml`
/// - `org/apache/commons/commons-lang3/3.14.0/commons-lang3-3.14.0.jar`
/// - `org/apache/commons/commons-lang3/3.14.0/maven-metadata.xml`
/// - Any of the above with `.sha1` / `.md5` / `.sha256` / `.sha512` appended
pub fn parse_maven_path(
    path: &str,
) -> Result<(MavenCoordinate, MavenPathKind), MavenValidationError> {
    // Defence: reject dangerous patterns before any further processing.
    validate_raw_path(path)?;

    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Minimum: groupId(1+) / artifactId / maven-metadata.xml  → at least 3 segments
    if segments.len() < 3 {
        return Err(MavenValidationError::InvalidPath(
            "path too short".to_string(),
        ));
    }

    // Validate every segment individually.
    for seg in &segments {
        validate_path_segment(seg)?;
    }

    let last = segments[segments.len() - 1];

    // Check for checksum suffix — strip it and recurse on the base path.
    for ext in CHECKSUM_EXTENSIONS {
        if let Some(base_filename) = last.strip_suffix(ext) {
            let algo = ext.trim_start_matches('.').to_string();
            // Rebuild the base path with the stripped filename.
            let mut base_segments: Vec<&str> = segments[..segments.len() - 1].to_vec();
            // Only push if the base_filename is non-empty (e.g. "maven-metadata.xml.sha1" → "maven-metadata.xml")
            if !base_filename.is_empty() {
                base_segments.push(base_filename);
            }
            let base_path = base_segments.join("/");
            let (mut coord, base_kind) = parse_maven_path(&base_path)?;
            // Restore the original filename (with checksum extension) so that
            // `artifact_path` generates the sidecar file path, not the base artifact path.
            coord.filename = Some(last.to_string());
            return Ok((
                coord,
                MavenPathKind::Checksum {
                    algorithm: algo,
                    base_kind: Box::new(base_kind),
                },
            ));
        }
    }

    // Is the last segment `maven-metadata.xml`?
    if last == "maven-metadata.xml" {
        return parse_metadata_path(&segments);
    }

    // Otherwise it's an artifact file: groupId(1+) / artifactId / version / filename
    parse_artifact_path(&segments)
}

/// Parse a path ending in `maven-metadata.xml`.
fn parse_metadata_path(
    segments: &[&str],
) -> Result<(MavenCoordinate, MavenPathKind), MavenValidationError> {
    // Remove the trailing `maven-metadata.xml`
    let parts = &segments[..segments.len() - 1];

    if parts.len() < 2 {
        return Err(MavenValidationError::InvalidPath(
            "metadata path too short".to_string(),
        ));
    }

    // Try to determine if this is artifact-level or version-level metadata.
    // Heuristic: if the last segment looks like a version, it's version-level.
    let maybe_version = parts[parts.len() - 1];
    if looks_like_version(maybe_version) && parts.len() >= 3 {
        // Version-level: groupId(1+) / artifactId / version / maven-metadata.xml
        let artifact_id = parts[parts.len() - 2];
        let group_parts = &parts[..parts.len() - 2];

        validate_group_id_parts(group_parts)?;
        validate_artifact_id(artifact_id)?;
        validate_version(maybe_version)?;

        let coord = MavenCoordinate {
            group_id: group_parts.join("."),
            artifact_id: artifact_id.to_string(),
            version: Some(maybe_version.to_string()),
            filename: Some("maven-metadata.xml".to_string()),
        };
        Ok((coord, MavenPathKind::VersionLevelMetadata))
    } else {
        // Artifact-level: groupId(1+) / artifactId / maven-metadata.xml
        let artifact_id = parts[parts.len() - 1];
        let group_parts = &parts[..parts.len() - 1];

        validate_group_id_parts(group_parts)?;
        validate_artifact_id(artifact_id)?;

        let coord = MavenCoordinate {
            group_id: group_parts.join("."),
            artifact_id: artifact_id.to_string(),
            version: None,
            filename: Some("maven-metadata.xml".to_string()),
        };
        Ok((coord, MavenPathKind::ArtifactLevelMetadata))
    }
}

/// Parse a path to an artifact file.
fn parse_artifact_path(
    segments: &[&str],
) -> Result<(MavenCoordinate, MavenPathKind), MavenValidationError> {
    // groupId(1+) / artifactId / version / filename  → at least 4 segments
    if segments.len() < 4 {
        return Err(MavenValidationError::InvalidPath(
            "artifact path too short".to_string(),
        ));
    }

    let filename = segments[segments.len() - 1];
    let version = segments[segments.len() - 2];
    let artifact_id = segments[segments.len() - 3];
    let group_parts = &segments[..segments.len() - 3];

    validate_group_id_parts(group_parts)?;
    validate_artifact_id(artifact_id)?;
    validate_version(version)?;

    let coord = MavenCoordinate {
        group_id: group_parts.join("."),
        artifact_id: artifact_id.to_string(),
        version: Some(version.to_string()),
        filename: Some(filename.to_string()),
    };
    Ok((coord, MavenPathKind::Artifact))
}

// ── Security validation ─────────────────────────────────────────────

/// Reject dangerous raw path patterns before splitting.
fn validate_raw_path(path: &str) -> Result<(), MavenValidationError> {
    if path.contains("..") {
        return Err(MavenValidationError::PathTraversal(
            "path contains '..'".to_string(),
        ));
    }
    if path.contains('\\') {
        return Err(MavenValidationError::PathTraversal(
            "path contains backslash".to_string(),
        ));
    }
    if path.contains('\0') {
        return Err(MavenValidationError::PathTraversal(
            "path contains null byte".to_string(),
        ));
    }
    if path.contains("//") {
        return Err(MavenValidationError::PathTraversal(
            "path contains double slash".to_string(),
        ));
    }
    // Reject percent-encoded traversal sequences (%2e = '.', %2f = '/')
    let lower = path.to_ascii_lowercase();
    if lower.contains("%2e")
        || lower.contains("%2f")
        || lower.contains("%5c")
        || lower.contains("%00")
    {
        return Err(MavenValidationError::PathTraversal(
            "path contains encoded traversal sequence".to_string(),
        ));
    }
    Ok(())
}

/// Validate a single path segment.
fn validate_path_segment(seg: &str) -> Result<(), MavenValidationError> {
    if seg.is_empty() {
        return Err(MavenValidationError::PathTraversal(
            "empty path segment".to_string(),
        ));
    }
    if seg == "." || seg == ".." {
        return Err(MavenValidationError::PathTraversal(format!(
            "illegal segment: {seg:?}"
        )));
    }
    Ok(())
}

/// Validate each part of a groupId (split by `/` in the path, joined with `.`).
fn validate_group_id_parts(parts: &[&str]) -> Result<(), MavenValidationError> {
    if parts.is_empty() {
        return Err(MavenValidationError::InvalidGroupId(
            "empty group id".to_string(),
        ));
    }
    for part in parts {
        if part.is_empty() {
            return Err(MavenValidationError::InvalidGroupId(
                "empty group id segment".to_string(),
            ));
        }
        if !part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(MavenValidationError::InvalidGroupId(format!(
                "invalid group id segment: {part:?}"
            )));
        }
    }
    Ok(())
}

/// Validate an artifactId.
pub fn validate_artifact_id(id: &str) -> Result<(), MavenValidationError> {
    if id.is_empty() {
        return Err(MavenValidationError::InvalidArtifactId(
            "empty artifact id".to_string(),
        ));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
    {
        return Err(MavenValidationError::InvalidArtifactId(format!(
            "invalid artifact id: {id:?}"
        )));
    }
    Ok(())
}

/// Validate a version string.
pub fn validate_version(v: &str) -> Result<(), MavenValidationError> {
    if v.is_empty() {
        return Err(MavenValidationError::InvalidVersion(
            "empty version".to_string(),
        ));
    }
    if !v
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
    {
        return Err(MavenValidationError::InvalidVersion(format!(
            "invalid version: {v:?}"
        )));
    }
    Ok(())
}

/// Heuristic: does this segment look like a Maven version?
/// Versions typically start with a digit or contain a digit.
fn looks_like_version(s: &str) -> bool {
    // A version string typically starts with a digit, or is a snapshot like "1.0-SNAPSHOT"
    s.chars().next().is_some_and(|c| c.is_ascii_digit()) || s.ends_with("-SNAPSHOT")
}

/// Returns the checksum algorithm if the filename ends with a known checksum extension.
pub fn checksum_extension(filename: &str) -> Option<&'static str> {
    for ext in CHECKSUM_EXTENSIONS {
        if filename.ends_with(ext) {
            return Some(&ext[1..]); // strip leading dot
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_artifact_jar() {
        let (coord, kind) =
            parse_maven_path("org/apache/commons/commons-lang3/3.14.0/commons-lang3-3.14.0.jar")
                .unwrap();
        assert_eq!(coord.group_id, "org.apache.commons");
        assert_eq!(coord.artifact_id, "commons-lang3");
        assert_eq!(coord.version.as_deref(), Some("3.14.0"));
        assert_eq!(coord.filename.as_deref(), Some("commons-lang3-3.14.0.jar"));
        assert_eq!(kind, MavenPathKind::Artifact);
        assert!(!coord.is_snapshot());
    }

    #[test]
    fn parse_artifact_level_metadata() {
        let (coord, kind) =
            parse_maven_path("org/apache/commons/commons-lang3/maven-metadata.xml").unwrap();
        assert_eq!(coord.group_id, "org.apache.commons");
        assert_eq!(coord.artifact_id, "commons-lang3");
        assert!(coord.version.is_none());
        assert_eq!(kind, MavenPathKind::ArtifactLevelMetadata);
    }

    #[test]
    fn parse_version_level_metadata() {
        let (coord, kind) =
            parse_maven_path("org/apache/commons/commons-lang3/1.0.0-SNAPSHOT/maven-metadata.xml")
                .unwrap();
        assert_eq!(coord.group_id, "org.apache.commons");
        assert_eq!(coord.artifact_id, "commons-lang3");
        assert_eq!(coord.version.as_deref(), Some("1.0.0-SNAPSHOT"));
        assert_eq!(kind, MavenPathKind::VersionLevelMetadata);
        assert!(coord.is_snapshot());
    }

    #[test]
    fn parse_checksum_file() {
        let (coord, kind) = parse_maven_path(
            "org/apache/commons/commons-lang3/3.14.0/commons-lang3-3.14.0.jar.sha1",
        )
        .unwrap();
        assert_eq!(coord.group_id, "org.apache.commons");
        assert_eq!(coord.artifact_id, "commons-lang3");
        if let MavenPathKind::Checksum {
            algorithm,
            base_kind,
        } = kind
        {
            assert_eq!(algorithm, "sha1");
            assert_eq!(*base_kind, MavenPathKind::Artifact);
        } else {
            panic!("expected Checksum kind");
        }
    }

    #[test]
    fn parse_metadata_checksum() {
        let (_, kind) =
            parse_maven_path("org/apache/commons/commons-lang3/maven-metadata.xml.sha1").unwrap();
        if let MavenPathKind::Checksum {
            algorithm,
            base_kind,
        } = kind
        {
            assert_eq!(algorithm, "sha1");
            assert_eq!(*base_kind, MavenPathKind::ArtifactLevelMetadata);
        } else {
            panic!("expected Checksum kind");
        }
    }

    #[test]
    fn parse_snapshot_artifact() {
        let (coord, kind) =
            parse_maven_path("com/example/mylib/1.0.0-SNAPSHOT/mylib-1.0.0-20220709.063105-3.jar")
                .unwrap();
        assert_eq!(coord.group_id, "com.example");
        assert_eq!(coord.artifact_id, "mylib");
        assert_eq!(coord.version.as_deref(), Some("1.0.0-SNAPSHOT"));
        assert!(coord.is_snapshot());
        assert_eq!(kind, MavenPathKind::Artifact);
    }

    #[test]
    fn reject_path_traversal() {
        assert!(parse_maven_path("org/../etc/passwd").is_err());
        assert!(parse_maven_path("org/apache\\commons/foo/1.0/bar.jar").is_err());
        assert!(parse_maven_path("org/foo//bar/1.0/x.jar").is_err());
    }

    #[test]
    fn reject_encoded_traversal() {
        assert!(parse_maven_path("org/%2e%2e/etc/passwd").is_err());
    }

    #[test]
    fn reject_null_bytes() {
        assert!(parse_maven_path("org/foo\0bar/1.0/x.jar").is_err());
    }

    #[test]
    fn reject_too_short() {
        assert!(parse_maven_path("org/foo").is_err());
        assert!(parse_maven_path("x").is_err());
    }

    #[test]
    fn reject_invalid_group_id_chars() {
        assert!(parse_maven_path("org/apa+che/foo/maven-metadata.xml").is_err());
    }

    #[test]
    fn simple_group() {
        let (coord, _) = parse_maven_path("com/mylib/1.0/mylib-1.0.jar").unwrap();
        assert_eq!(coord.group_id, "com");
        assert_eq!(coord.artifact_id, "mylib");
    }
}
