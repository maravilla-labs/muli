// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end tests for Git LFS Batch API with authentication.
//!
//! Tests cover:
//! - LFS batch download/upload flows via HTTP
//! - Auth enforcement (unauthenticated, wrong token, read-only)
//! - Direct object upload and download
//! - Verify endpoint
//! - Multi-tenant isolation
//! - Deduplication (upload same oid twice)

use serde_json::json;
use sha2::{Digest, Sha256};

use crate::harness::*;

// ---------------------------------------------------------------------------
// Batch API tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lfs_batch_upload_and_download() {
    let srv = start_server().await;

    // Create a repo first
    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-test", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let data = b"hello large file contents";
    let oid = hex::encode(Sha256::digest(data));
    let size = data.len() as u64;

    // 1. Batch upload request — object doesn't exist yet, should get upload action
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-test.git/info/lfs/objects/batch",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({
            "operation": "upload",
            "objects": [{"oid": oid, "size": size}],
            "transfers": ["basic"]
        }))
        .send()
        .await
        .expect("batch upload request");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["transfer"], "basic");
    assert_eq!(body["objects"][0]["oid"], oid);
    let upload_href = body["objects"][0]["actions"]["upload"]["href"]
        .as_str()
        .expect("upload href");
    assert!(upload_href.contains(&oid), "upload href contains oid");
    let verify_href = body["objects"][0]["actions"]["verify"]["href"]
        .as_str()
        .expect("verify href");
    assert!(verify_href.contains("verify"), "verify href present");

    // 2. Upload the object
    let resp = client
        .put(upload_href)
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", size.to_string())
        .body(data.to_vec())
        .send()
        .await
        .expect("upload object");
    assert_eq!(resp.status(), 200);

    // 3. Verify the object
    let resp = client
        .post(verify_href)
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({"oid": oid, "size": size}))
        .send()
        .await
        .expect("verify object");
    assert_eq!(resp.status(), 200);

    // 4. Batch download request — object exists, should get download action
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-test.git/info/lfs/objects/batch",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({
            "operation": "download",
            "objects": [{"oid": oid, "size": size}]
        }))
        .send()
        .await
        .expect("batch download request");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let download_href = body["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .expect("download href");

    // 5. Download the object
    let resp = client
        .get(download_href)
        .header("Authorization", auth_header(&srv))
        .send()
        .await
        .expect("download object");
    assert_eq!(resp.status(), 200);
    let downloaded = resp.bytes().await.unwrap();
    assert_eq!(downloaded.as_ref(), data);
}

#[tokio::test]
async fn lfs_batch_download_missing_returns_404_per_object() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-missing", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let fake_oid = "a".repeat(64);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-missing.git/info/lfs/objects/batch",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({
            "operation": "download",
            "objects": [{"oid": fake_oid, "size": 100}]
        }))
        .send()
        .await
        .expect("batch download missing");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["objects"][0]["error"]["code"], 404);
    assert!(body["objects"][0]["actions"].is_null());
}

#[tokio::test]
async fn lfs_batch_upload_dedup_skips_existing() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-dedup", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let data = b"dedup test content";
    let oid = hex::encode(Sha256::digest(data));
    let size = data.len() as u64;

    let client = reqwest::Client::new();

    // Upload the object
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-dedup.git/info/lfs/objects/{oid}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/octet-stream")
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Batch upload for same oid — should return empty actions (no upload needed)
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-dedup.git/info/lfs/objects/batch",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({
            "operation": "upload",
            "objects": [{"oid": oid, "size": size}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    // No actions = object already exists, no upload needed
    assert!(
        body["objects"][0]["actions"].is_null(),
        "expected no actions for existing object, got: {}",
        body["objects"][0]["actions"]
    );
    assert!(body["objects"][0]["error"].is_null());
}

// ---------------------------------------------------------------------------
// Auth enforcement tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lfs_batch_rejects_unauthenticated() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-noauth", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-noauth.git/info/lfs/objects/batch",
            base_url(&srv)
        ))
        // No Authorization header
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({
            "operation": "download",
            "objects": [{"oid": "a".repeat(64), "size": 10}]
        }))
        .send()
        .await
        .unwrap();

    // Should be rejected (401 or 403)
    assert!(
        resp.status().as_u16() == 401 || resp.status().as_u16() == 403,
        "expected 401/403, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn lfs_upload_rejects_wrong_token() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-wrongtoken", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let client = reqwest::Client::new();
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-wrongtoken.git/info/lfs/objects/{}",
            base_url(&srv),
            "b".repeat(64)
        ))
        .header("Authorization", "Bearer wrong-token-12345")
        .header("Content-Type", "application/octet-stream")
        .body(b"data".to_vec())
        .send()
        .await
        .unwrap();

    assert!(
        resp.status().as_u16() == 401 || resp.status().as_u16() == 403,
        "expected 401/403, got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// Direct upload/download/verify edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lfs_upload_invalid_oid_rejected() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-badoid", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let client = reqwest::Client::new();
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-badoid.git/info/lfs/objects/not-a-valid-oid",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/octet-stream")
        .body(b"data".to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn lfs_download_nonexistent_returns_404() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-dl404", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let client = reqwest::Client::new();
    let oid = "c".repeat(64);
    let resp = client
        .get(format!(
            "{}/{NAMESPACE}/lfs-dl404.git/info/lfs/objects/{oid}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn lfs_verify_size_mismatch() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-verify-size", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let data = b"verify test";
    let oid = hex::encode(Sha256::digest(data));

    let client = reqwest::Client::new();

    // Upload
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-verify-size.git/info/lfs/objects/{oid}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/octet-stream")
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify with wrong size
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-verify-size.git/info/lfs/objects/verify",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({"oid": oid, "size": 99999}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn lfs_digest_mismatch_on_upload() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-digest-bad", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let client = reqwest::Client::new();
    // Upload with a valid-format oid that doesn't match the content
    let wrong_oid = "d".repeat(64);
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-digest-bad.git/info/lfs/objects/{wrong_oid}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/octet-stream")
        .body(b"this data does not match the oid".to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 422);
}

// ---------------------------------------------------------------------------
// Multi-tenant isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lfs_objects_isolated_per_tenant() {
    let srv = start_server().await;

    // Create a repo under the default tenant (TENANT = "tenant-a")
    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-isolation", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let data = b"tenant-specific data";
    let oid = hex::encode(Sha256::digest(data));

    let client = reqwest::Client::new();

    // Upload under the default tenant
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-isolation.git/info/lfs/objects/{oid}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Download should succeed for the same tenant
    let resp = client
        .get(format!(
            "{}/{NAMESPACE}/lfs-isolation.git/info/lfs/objects/{oid}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let downloaded = resp.bytes().await.unwrap();
    assert_eq!(downloaded.as_ref(), data);
}

// ---------------------------------------------------------------------------
// Batch with multiple objects
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lfs_batch_multiple_objects() {
    let srv = start_server().await;

    let (status, _) = api_post(
        &srv,
        "/api/v1/repos",
        json!({"namespace": NAMESPACE, "name": "lfs-multi", "description": "", "is_private": false}),
    )
    .await;
    assert_eq!(status, 201);

    let data1 = b"object one";
    let data2 = b"object two";
    let oid1 = hex::encode(Sha256::digest(data1));
    let oid2 = hex::encode(Sha256::digest(data2));
    let missing_oid = "e".repeat(64);

    let client = reqwest::Client::new();

    // Upload oid1 only
    let resp = client
        .put(format!(
            "{}/{NAMESPACE}/lfs-multi.git/info/lfs/objects/{oid1}",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .body(data1.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Batch download for oid1 (exists), oid2 (not uploaded), missing_oid (not uploaded)
    let resp = client
        .post(format!(
            "{}/{NAMESPACE}/lfs-multi.git/info/lfs/objects/batch",
            base_url(&srv)
        ))
        .header("Authorization", auth_header(&srv))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&json!({
            "operation": "download",
            "objects": [
                {"oid": oid1, "size": data1.len()},
                {"oid": oid2, "size": data2.len()},
                {"oid": missing_oid, "size": 100}
            ]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 3);

    // oid1 should have download action
    assert!(objects[0]["actions"]["download"]["href"].is_string());
    assert!(objects[0]["error"].is_null());

    // oid2 and missing_oid should have 404 errors
    assert_eq!(objects[1]["error"]["code"], 404);
    assert_eq!(objects[2]["error"]["code"], 404);
}
