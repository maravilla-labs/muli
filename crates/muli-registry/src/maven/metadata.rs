// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Maven metadata XML generation and parsing.

use std::fmt;
use std::sync::Arc;

use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;
use tracing::warn;

use crate::storage::FilesystemStorage;

use super::storage;

/// Parsed artifact-level maven-metadata.xml.
#[derive(Debug, Clone, Default)]
pub struct MavenMetadata {
    pub group_id: String,
    pub artifact_id: String,
    pub version: Option<String>,
    pub versioning: Versioning,
}

/// Versioning block inside maven-metadata.xml.
#[derive(Debug, Clone, Default)]
pub struct Versioning {
    pub latest: Option<String>,
    pub release: Option<String>,
    pub versions: Vec<String>,
    pub last_updated: Option<String>,
    pub snapshot: Option<SnapshotInfo>,
    pub snapshot_versions: Vec<SnapshotVersion>,
}

/// Snapshot timestamp/build info.
#[derive(Debug, Clone, Default)]
pub struct SnapshotInfo {
    pub timestamp: String,
    pub build_number: u32,
}

/// Individual snapshot version entry.
#[derive(Debug, Clone, Default)]
pub struct SnapshotVersion {
    pub classifier: Option<String>,
    pub extension: String,
    pub value: String,
    pub updated: String,
}

/// Error type for metadata XML generation.
#[derive(Debug)]
pub struct MetadataWriteError(quick_xml::Error);

impl fmt::Display for MetadataWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metadata XML write error: {}", self.0)
    }
}

impl std::error::Error for MetadataWriteError {}

impl From<std::io::Error> for MetadataWriteError {
    fn from(e: std::io::Error) -> Self {
        Self(quick_xml::Error::Io(Arc::new(e)))
    }
}

impl From<quick_xml::Error> for MetadataWriteError {
    fn from(e: quick_xml::Error) -> Self {
        Self(e)
    }
}

// ── Generation ──────────────────────────────────────────────────────

/// Generate artifact-level maven-metadata.xml.
pub fn generate_artifact_metadata(
    group_id: &str,
    artifact_id: &str,
    versions: &[String],
) -> String {
    generate_artifact_metadata_inner(group_id, artifact_id, versions)
        .expect("failed to generate artifact metadata XML")
}

fn generate_artifact_metadata_inner(
    group_id: &str,
    artifact_id: &str,
    versions: &[String],
) -> Result<String, MetadataWriteError> {
    let mut buf = Vec::new();
    let mut writer = Writer::new_with_indent(&mut buf, b' ', 2);

    // XML declaration
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    // <metadata>
    writer.write_event(Event::Start(BytesStart::new("metadata")))?;

    write_text_element(&mut writer, "groupId", group_id)?;
    write_text_element(&mut writer, "artifactId", artifact_id)?;

    // Determine latest and release
    let release = versions
        .iter()
        .filter(|v| !v.ends_with("-SNAPSHOT"))
        .next_back();
    let latest = versions.last();

    // <versioning>
    writer.write_event(Event::Start(BytesStart::new("versioning")))?;

    if let Some(latest) = latest {
        write_text_element(&mut writer, "latest", latest)?;
    }
    if let Some(release) = release {
        write_text_element(&mut writer, "release", release)?;
    }

    // <versions>
    writer.write_event(Event::Start(BytesStart::new("versions")))?;
    for v in versions {
        write_text_element(&mut writer, "version", v)?;
    }
    writer.write_event(Event::End(BytesEnd::new("versions")))?;

    let now = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    write_text_element(&mut writer, "lastUpdated", &now)?;

    writer.write_event(Event::End(BytesEnd::new("versioning")))?;

    // </metadata>
    writer.write_event(Event::End(BytesEnd::new("metadata")))?;

    Ok(String::from_utf8(buf).expect("XML writer produced invalid UTF-8"))
}

/// Generate version-level snapshot maven-metadata.xml.
pub fn generate_snapshot_metadata(
    group_id: &str,
    artifact_id: &str,
    version: &str,
    snapshot: &SnapshotInfo,
    snapshot_versions: &[SnapshotVersion],
) -> String {
    generate_snapshot_metadata_inner(group_id, artifact_id, version, snapshot, snapshot_versions)
        .expect("failed to generate snapshot metadata XML")
}

fn generate_snapshot_metadata_inner(
    group_id: &str,
    artifact_id: &str,
    version: &str,
    snapshot: &SnapshotInfo,
    snapshot_versions: &[SnapshotVersion],
) -> Result<String, MetadataWriteError> {
    let mut buf = Vec::new();
    let mut writer = Writer::new_with_indent(&mut buf, b' ', 2);

    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    writer.write_event(Event::Start(BytesStart::new("metadata")))?;

    write_text_element(&mut writer, "groupId", group_id)?;
    write_text_element(&mut writer, "artifactId", artifact_id)?;
    write_text_element(&mut writer, "version", version)?;

    // <versioning>
    writer.write_event(Event::Start(BytesStart::new("versioning")))?;

    // <snapshot>
    writer.write_event(Event::Start(BytesStart::new("snapshot")))?;
    write_text_element(&mut writer, "timestamp", &snapshot.timestamp)?;
    write_text_element(
        &mut writer,
        "buildNumber",
        &snapshot.build_number.to_string(),
    )?;
    writer.write_event(Event::End(BytesEnd::new("snapshot")))?;

    let updated = snapshot.timestamp.replace('.', "");
    write_text_element(&mut writer, "lastUpdated", &updated)?;

    // <snapshotVersions>
    if !snapshot_versions.is_empty() {
        writer.write_event(Event::Start(BytesStart::new("snapshotVersions")))?;

        for sv in snapshot_versions {
            writer.write_event(Event::Start(BytesStart::new("snapshotVersion")))?;

            if let Some(ref classifier) = sv.classifier {
                write_text_element(&mut writer, "classifier", classifier)?;
            }
            write_text_element(&mut writer, "extension", &sv.extension)?;
            write_text_element(&mut writer, "value", &sv.value)?;
            write_text_element(&mut writer, "updated", &sv.updated)?;

            writer.write_event(Event::End(BytesEnd::new("snapshotVersion")))?;
        }

        writer.write_event(Event::End(BytesEnd::new("snapshotVersions")))?;
    }

    writer.write_event(Event::End(BytesEnd::new("versioning")))?;

    writer.write_event(Event::End(BytesEnd::new("metadata")))?;

    Ok(String::from_utf8(buf).expect("XML writer produced invalid UTF-8"))
}

/// Parse a maven-metadata.xml file.
pub fn parse_metadata(xml: &[u8]) -> Result<MavenMetadata, String> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut metadata = MavenMetadata::default();
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                path.push(String::from_utf8_lossy(e.name().as_ref()).to_string());
            }
            Ok(Event::End(_)) => {
                path.pop();
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().map_err(|e| e.to_string())?.to_string();
                let tag_path = path.join("/");
                match tag_path.as_str() {
                    "metadata/groupId" => metadata.group_id = text,
                    "metadata/artifactId" => metadata.artifact_id = text,
                    "metadata/version" => metadata.version = Some(text),
                    "metadata/versioning/latest" => metadata.versioning.latest = Some(text),
                    "metadata/versioning/release" => metadata.versioning.release = Some(text),
                    "metadata/versioning/versions/version" => {
                        metadata.versioning.versions.push(text);
                    }
                    "metadata/versioning/lastUpdated" => {
                        metadata.versioning.last_updated = Some(text);
                    }
                    "metadata/versioning/snapshot/timestamp" => {
                        metadata
                            .versioning
                            .snapshot
                            .get_or_insert_with(SnapshotInfo::default)
                            .timestamp = text;
                    }
                    "metadata/versioning/snapshot/buildNumber" => {
                        metadata
                            .versioning
                            .snapshot
                            .get_or_insert_with(SnapshotInfo::default)
                            .build_number = text.parse().unwrap_or(0);
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(format!("XML parse error: {e}")),
            _ => {}
        }
        buf.clear();
    }

    Ok(metadata)
}

/// Scan version directories and generate artifact-level metadata.
/// Used as a fallback when the metadata file is missing.
pub async fn regenerate_artifact_metadata(
    storage_ref: &Arc<FilesystemStorage>,
    tenant_id: &str,
    group_id: &str,
    artifact_id: &str,
) -> Option<String> {
    match storage::list_versions(storage_ref, tenant_id, group_id, artifact_id).await {
        Ok(versions) if !versions.is_empty() => {
            Some(generate_artifact_metadata(group_id, artifact_id, &versions))
        }
        Ok(_) => None,
        Err(e) => {
            warn!(
                group_id,
                artifact_id,
                error = %e,
                "failed to scan versions for metadata regeneration"
            );
            None
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn write_text_element(
    writer: &mut Writer<&mut Vec<u8>>,
    tag: &str,
    text: &str,
) -> Result<(), quick_xml::Error> {
    writer.write_event(Event::Start(BytesStart::new(tag)))?;
    writer.write_event(Event::Text(BytesText::new(text)))?;
    writer.write_event(Event::End(BytesEnd::new(tag)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_parse_artifact_metadata() {
        let versions = vec![
            "3.13.0".to_string(),
            "3.14.0".to_string(),
            "1.0.0-SNAPSHOT".to_string(),
        ];
        let xml = generate_artifact_metadata("org.apache.commons", "commons-lang3", &versions);

        let parsed = parse_metadata(xml.as_bytes()).unwrap();
        assert_eq!(parsed.group_id, "org.apache.commons");
        assert_eq!(parsed.artifact_id, "commons-lang3");
        assert_eq!(parsed.versioning.versions.len(), 3);
        assert_eq!(parsed.versioning.release.as_deref(), Some("3.14.0"));
        assert_eq!(parsed.versioning.latest.as_deref(), Some("1.0.0-SNAPSHOT"));
    }

    #[test]
    fn generate_and_parse_snapshot_metadata() {
        let snapshot = SnapshotInfo {
            timestamp: "20220709.063105".to_string(),
            build_number: 3,
        };
        let sv = vec![
            SnapshotVersion {
                classifier: None,
                extension: "jar".to_string(),
                value: "1.0.0-20220709.063105-3".to_string(),
                updated: "20220709063105".to_string(),
            },
            SnapshotVersion {
                classifier: None,
                extension: "pom".to_string(),
                value: "1.0.0-20220709.063105-3".to_string(),
                updated: "20220709063105".to_string(),
            },
        ];
        let xml =
            generate_snapshot_metadata("com.example", "mylib", "1.0.0-SNAPSHOT", &snapshot, &sv);

        let parsed = parse_metadata(xml.as_bytes()).unwrap();
        assert_eq!(parsed.group_id, "com.example");
        assert_eq!(parsed.artifact_id, "mylib");
        assert_eq!(parsed.version.as_deref(), Some("1.0.0-SNAPSHOT"));
        let snap = parsed.versioning.snapshot.unwrap();
        assert_eq!(snap.timestamp, "20220709.063105");
        assert_eq!(snap.build_number, 3);
    }

    #[test]
    fn parse_empty_versions() {
        let xml = generate_artifact_metadata("com.example", "mylib", &[]);
        let parsed = parse_metadata(xml.as_bytes()).unwrap();
        assert!(parsed.versioning.versions.is_empty());
        assert!(parsed.versioning.release.is_none());
    }

    #[test]
    fn parse_only_snapshots() {
        let versions = vec!["1.0.0-SNAPSHOT".to_string()];
        let xml = generate_artifact_metadata("com.example", "mylib", &versions);
        let parsed = parse_metadata(xml.as_bytes()).unwrap();
        assert!(parsed.versioning.release.is_none());
        assert_eq!(parsed.versioning.latest.as_deref(), Some("1.0.0-SNAPSHOT"));
    }
}
