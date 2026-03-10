// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo registry wire format types.

/// Cargo publish wire format:
///   4 bytes (u32 LE): JSON metadata length
///   N bytes: JSON metadata
///   4 bytes (u32 LE): .crate file length
///   M bytes: .crate file data
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CrateMetadata {
    pub name: String,
    #[serde(rename = "vers")]
    pub version: String,
    #[serde(default)]
    pub deps: Vec<CrateDependency>,
    #[serde(default)]
    pub features: std::collections::HashMap<String, Vec<String>>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub documentation: Option<String>,
    #[serde(default)]
    pub homepage: Option<String>,
    #[serde(default)]
    pub readme: Option<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub license: Option<String>,
    #[serde(default)]
    pub license_file: Option<String>,
    #[serde(default)]
    pub repository: Option<String>,
    #[serde(default)]
    pub links: Option<String>,
}

#[derive(Debug, Deserialize, serde::Serialize, Clone)]
pub struct CrateDependency {
    pub name: String,
    #[serde(rename = "version_req")]
    pub req: String,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub optional: bool,
    #[serde(default = "default_true")]
    pub default_features: bool,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub registry: Option<String>,
    #[serde(default)]
    pub package: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_kind() -> String {
    "normal".to_string()
}

#[derive(Debug)]
pub struct ParsedPublish {
    pub metadata: CrateMetadata,
    pub crate_data: Vec<u8>,
}

/// Maximum allowed JSON metadata length (10 MB).
const MAX_JSON_LEN: usize = 10 * 1024 * 1024;

/// Parse the Cargo publish wire format from raw bytes.
pub fn parse_publish_body(body: &[u8]) -> Result<ParsedPublish, String> {
    if body.len() < 4 {
        return Err("body too short for JSON length".to_string());
    }

    // Read JSON metadata length (u32 LE)
    let json_len = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    if json_len > MAX_JSON_LEN {
        return Err(format!(
            "JSON metadata too large: {json_len} bytes (max {MAX_JSON_LEN})"
        ));
    }
    let json_start = 4;
    let json_end = json_start + json_len;

    if body.len() < json_end + 4 {
        return Err("body too short for JSON metadata + crate length".to_string());
    }

    // Parse JSON metadata
    let metadata: CrateMetadata = serde_json::from_slice(&body[json_start..json_end])
        .map_err(|e| format!("invalid JSON metadata: {e}"))?;

    // Read .crate file length (u32 LE)
    let crate_len = u32::from_le_bytes([
        body[json_end],
        body[json_end + 1],
        body[json_end + 2],
        body[json_end + 3],
    ]) as usize;
    let crate_start = json_end + 4;
    let crate_end = crate_start + crate_len;

    if body.len() < crate_end {
        return Err(format!(
            "body too short for .crate data: expected {} bytes, got {}",
            crate_len,
            body.len() - crate_start,
        ));
    }

    let crate_data = body[crate_start..crate_end].to_vec();

    Ok(ParsedPublish {
        metadata,
        crate_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_publish_body() {
        let metadata = r#"{"name":"test-crate","vers":"0.1.0"}"#;
        let crate_data = b"fake crate data";

        let mut body = Vec::new();
        body.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        body.extend_from_slice(metadata.as_bytes());
        body.extend_from_slice(&(crate_data.len() as u32).to_le_bytes());
        body.extend_from_slice(crate_data);

        let parsed = parse_publish_body(&body).unwrap();
        assert_eq!(parsed.metadata.name, "test-crate");
        assert_eq!(parsed.metadata.version, "0.1.0");
        assert_eq!(parsed.crate_data, crate_data);
    }

    #[test]
    fn parse_body_too_short() {
        assert!(parse_publish_body(&[0, 0]).is_err());
    }

    #[test]
    fn parse_body_truncated_json() {
        let mut body = Vec::new();
        body.extend_from_slice(&100u32.to_le_bytes()); // claims 100 bytes of JSON
        body.extend_from_slice(b"short"); // only 5 bytes
        assert!(parse_publish_body(&body).is_err());
    }

    #[test]
    fn parse_body_truncated_crate() {
        let metadata = r#"{"name":"x","vers":"1.0.0"}"#;
        let mut body = Vec::new();
        body.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        body.extend_from_slice(metadata.as_bytes());
        body.extend_from_slice(&100u32.to_le_bytes()); // claims 100 bytes of crate data
        body.extend_from_slice(b"short"); // only 5 bytes
        assert!(parse_publish_body(&body).is_err());
    }
}
