// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git LFS Batch API protocol types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Content type for Git LFS JSON requests and responses.
pub const APPLICATION_VND_GIT_LFS_JSON: &str = "application/vnd.git-lfs+json";

/// LFS Batch API request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRequest {
    pub operation: Operation,
    pub objects: Vec<ObjectSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfers: Option<Vec<String>>,
    #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
    pub ref_spec: Option<RefSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_algo: Option<String>,
}

/// LFS operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Upload,
    Download,
}

/// An LFS object identifier (SHA-256 oid + size).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSpec {
    pub oid: String,
    pub size: u64,
}

/// A Git ref context for the batch request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefSpec {
    pub name: String,
}

/// LFS Batch API response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponse {
    pub transfer: String,
    pub objects: Vec<ObjectResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_algo: Option<String>,
}

/// Per-object response in a batch result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectResponse {
    pub oid: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authenticated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Actions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ObjectError>,
}

/// Available transfer actions for an object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Actions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload: Option<Action>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download: Option<Action>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify: Option<Action>,
}

/// A single transfer action (URL + optional headers + expiry).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub href: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<u64>,
}

/// Per-object error in a batch response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectError {
    pub code: u16,
    pub message: String,
}

/// Verify request body (POST after upload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyRequest {
    pub oid: String,
    pub size: u64,
}

/// Response from `git-lfs-authenticate` over SSH.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshAuthResponse {
    pub href: String,
    pub header: HashMap<String, String>,
    pub expires_in: u64,
}

/// Validate an LFS oid: must be exactly 64 lowercase hex characters (SHA-256).
pub fn validate_oid(oid: &str) -> bool {
    oid.len() == 64 && oid.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_oid_accepts_valid() {
        let oid = "a" .repeat(64);
        assert!(validate_oid(&oid));
        let oid = "0123456789abcdef".repeat(4);
        assert!(validate_oid(&oid));
    }

    #[test]
    fn validate_oid_rejects_invalid() {
        assert!(!validate_oid("too_short"));
        assert!(!validate_oid(&"A".repeat(64))); // uppercase
        assert!(!validate_oid(&"g".repeat(64))); // not hex
        assert!(!validate_oid(&"a".repeat(63))); // too short
        assert!(!validate_oid(&"a".repeat(65))); // too long
    }

    #[test]
    fn batch_request_serde_roundtrip() {
        let req = BatchRequest {
            operation: Operation::Download,
            objects: vec![ObjectSpec {
                oid: "a".repeat(64),
                size: 1024,
            }],
            transfers: Some(vec!["basic".to_string()]),
            ref_spec: Some(RefSpec {
                name: "refs/heads/main".to_string(),
            }),
            hash_algo: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: BatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.objects.len(), 1);
        assert_eq!(parsed.operation, Operation::Download);
    }

    #[test]
    fn batch_response_serde_roundtrip() {
        let resp = BatchResponse {
            transfer: "basic".to_string(),
            objects: vec![ObjectResponse {
                oid: "b".repeat(64),
                size: 2048,
                authenticated: Some(true),
                actions: Some(Actions {
                    download: Some(Action {
                        href: "https://example.com/download".to_string(),
                        header: None,
                        expires_in: Some(3600),
                    }),
                    upload: None,
                    verify: None,
                }),
                error: None,
            }],
            hash_algo: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: BatchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.objects[0].size, 2048);
        assert!(parsed.objects[0].actions.as_ref().unwrap().download.is_some());
    }
}
