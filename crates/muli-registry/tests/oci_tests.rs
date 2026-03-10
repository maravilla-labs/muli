// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI / Docker registry integration tests (no Docker CLI required).

mod common;

use axum::body::Body;
use common::{TestRegistry, sha256_hex};

#[tokio::test]
async fn test_oci_push_pull() {
    let reg = TestRegistry::new().await;

    let blob_data = b"hello world blob";
    let blob_digest = format!("sha256:{}", sha256_hex(blob_data));

    // --- Step 1: Start a blob upload (POST) ---------------------------------
    let req = reg
        .request("POST", "/v2/myapp/blobs/uploads/")
        .body(Body::empty())
        .unwrap();
    let (status, headers, _) = reg.send(req).await;
    assert_eq!(status, 202, "start upload should return 202 Accepted");
    let location = headers
        .get("Location")
        .expect("Location header")
        .to_str()
        .unwrap()
        .to_string();

    // --- Step 2: Complete the upload (PUT with digest) -----------------------
    let put_url = format!("{location}?digest={blob_digest}");
    let req = reg
        .request("PUT", &put_url)
        .body(Body::from(blob_data.to_vec()))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 201, "complete upload should return 201 Created");

    // --- Step 3: HEAD blob (verify exists) ----------------------------------
    let req = reg
        .request("HEAD", &format!("/v2/myapp/blobs/{blob_digest}"))
        .body(Body::empty())
        .unwrap();
    let (status, headers, _) = reg.send(req).await;
    assert_eq!(status, 200, "HEAD blob should return 200");
    assert_eq!(
        headers
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap(),
        blob_digest
    );

    // --- Step 4: PUT manifest -----------------------------------------------
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": blob_digest,
            "size": blob_data.len(),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": blob_digest,
            "size": blob_data.len(),
        }]
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let req = reg
        .request("PUT", "/v2/myapp/manifests/v1.0")
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(Body::from(manifest_bytes.clone()))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 201, "PUT manifest should return 201 Created");

    // --- Step 5: GET manifest -----------------------------------------------
    let req = reg
        .request("GET", "/v2/myapp/manifests/v1.0")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "GET manifest should return 200");
    assert_eq!(body, manifest_bytes, "manifest body must match");

    // --- Step 6: GET blob ---------------------------------------------------
    let req = reg
        .request("GET", &format!("/v2/myapp/blobs/{blob_digest}"))
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "GET blob should return 200");
    assert_eq!(body, blob_data, "blob body must match");

    // --- Step 7: GET tags/list ----------------------------------------------
    let req = reg
        .request("GET", "/v2/myapp/tags/list")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let tags: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tag_list = tags["tags"].as_array().unwrap();
    assert!(
        tag_list.iter().any(|t| t.as_str() == Some("v1.0")),
        "tags/list should include v1.0: {tag_list:?}"
    );

    // --- Step 8: GET catalog ------------------------------------------------
    let req = reg
        .request("GET", "/v2/_catalog")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let catalog: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let repos = catalog["repositories"].as_array().unwrap();
    assert!(
        repos.iter().any(|r| r.as_str() == Some("myapp")),
        "catalog should include myapp: {repos:?}"
    );
}

#[tokio::test]
async fn test_unauthenticated_rejected() {
    let reg = TestRegistry::new().await;

    // OCI: GET tags/list without auth → 401
    let req = axum::http::Request::builder()
        .uri("/v2/myapp/tags/list")
        .method("GET")
        .header("Host", "test-tenant.registry.test")
        .body(Body::empty())
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 401, "OCI without auth should return 401");

    // npm: PUT publish without auth → 401
    let req = axum::http::Request::builder()
        .uri("/-/npm/evil-package")
        .method("PUT")
        .header("Host", "test-tenant.registry.test")
        .header("Content-Type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 401, "npm publish without auth should return 401");

    // Cargo: PUT publish without auth → 401
    let req = axum::http::Request::builder()
        .uri("/api/v1/crates/new")
        .method("PUT")
        .header("Host", "test-tenant.registry.test")
        .body(Body::from(vec![0u8; 8]))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 401, "cargo publish without auth should return 401");
}
