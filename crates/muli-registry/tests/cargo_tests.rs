// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cargo registry integration tests (API-level + CLI-level).

mod common;

use axum::body::Body;
use common::{TestRegistry, TestServer, has_command, sha256_hex};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Cargo API-level tests (via tower oneshot)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cargo_publish_download() {
    let reg = TestRegistry::new().await;

    // --- config.json --------------------------------------------------------
    let req = reg
        .request("GET", "/index/config.json")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "config.json should return 200");
    let config: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(config["dl"].as_str().unwrap().contains("test-tenant"));
    assert!(config["api"].as_str().unwrap().contains("test-tenant"));

    // --- Publish crate ------------------------------------------------------
    let crate_data = b"fake .crate file content for testing";
    let metadata = serde_json::json!({
        "name": "my-crate",
        "vers": "0.1.0",
        "deps": [],
        "features": {},
        "authors": ["test"],
        "description": "a test crate",
        "license": "MIT"
    });
    let metadata_bytes = serde_json::to_vec(&metadata).unwrap();

    let mut wire_body: Vec<u8> = Vec::new();
    wire_body.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
    wire_body.extend_from_slice(&metadata_bytes);
    wire_body.extend_from_slice(&(crate_data.len() as u32).to_le_bytes());
    wire_body.extend_from_slice(crate_data);

    let req = reg
        .request("PUT", "/api/v1/crates/new")
        .body(Body::from(wire_body))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        200,
        "cargo publish should return 200: {}",
        String::from_utf8_lossy(&body)
    );
    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(resp["warnings"].is_object());

    // --- Sparse index lookup ------------------------------------------------
    let req = reg
        .request("GET", "/index/my/-c/my-crate")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "index lookup should return 200");
    let index_content = String::from_utf8(body).unwrap();
    let line: serde_json::Value =
        serde_json::from_str(index_content.trim()).expect("should be valid NDJSON");
    assert_eq!(line["name"], "my-crate");
    assert_eq!(line["vers"], "0.1.0");
    assert!(!line["cksum"].as_str().unwrap().is_empty());
    assert_eq!(line["yanked"], false);

    let expected_cksum = sha256_hex(crate_data);
    assert_eq!(line["cksum"].as_str().unwrap(), expected_cksum);

    // --- Download .crate ----------------------------------------------------
    let req = reg
        .request("GET", "/api/v1/crates/my-crate/0.1.0/download")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "crate download should return 200");
    assert_eq!(body, crate_data, ".crate bytes must match");

    // --- Yank ---------------------------------------------------------------
    let req = reg
        .request("DELETE", "/api/v1/crates/my-crate/0.1.0/yank")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        200,
        "yank should return 200: {}",
        String::from_utf8_lossy(&body)
    );

    let req = reg
        .request("GET", "/index/my/-c/my-crate")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let line: serde_json::Value =
        serde_json::from_str(String::from_utf8(body).unwrap().trim()).unwrap();
    assert_eq!(line["yanked"], true, "should be yanked");

    // --- Unyank -------------------------------------------------------------
    let req = reg
        .request("PUT", "/api/v1/crates/my-crate/0.1.0/unyank")
        .body(Body::empty())
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 200, "unyank should return 200");

    let req = reg
        .request("GET", "/index/my/-c/my-crate")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let line: serde_json::Value =
        serde_json::from_str(String::from_utf8(body).unwrap().trim()).unwrap();
    assert_eq!(line["yanked"], false, "should be unyanked");
}

#[tokio::test]
async fn test_cargo_duplicate_publish() {
    let reg = TestRegistry::new().await;

    let crate_data = b"fake crate";
    let metadata = serde_json::json!({
        "name": "my-crate",
        "vers": "0.1.0",
        "deps": [],
        "features": {},
        "authors": [],
        "description": null,
        "license": "MIT"
    });
    let metadata_bytes = serde_json::to_vec(&metadata).unwrap();

    let build_wire = || {
        let mut body: Vec<u8> = Vec::new();
        body.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(&metadata_bytes);
        body.extend_from_slice(&(crate_data.len() as u32).to_le_bytes());
        body.extend_from_slice(crate_data);
        body
    };

    let req = reg
        .request("PUT", "/api/v1/crates/new")
        .body(Body::from(build_wire()))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 200, "first publish should succeed");

    let req = reg
        .request("PUT", "/api/v1/crates/new")
        .body(Body::from(build_wire()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        409,
        "duplicate publish should return 409 Conflict: {}",
        String::from_utf8_lossy(&body)
    );
}

// ---------------------------------------------------------------------------
// Cargo CLI integration test
// ---------------------------------------------------------------------------

fn write_cargo_config(dir: &std::path::Path, base_url: &str, token: &str) {
    std::fs::create_dir_all(dir.join(".cargo")).unwrap();
    std::fs::write(
        dir.join(".cargo/config.toml"),
        format!(
            r#"[registries.test-registry]
index = "sparse+{base_url}/index/"
token = "{token}"

[registry]
global-credential-providers = ["cargo:token"]
"#,
        ),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cargo_cli_publish_fetch() {
    if !has_command("cargo") {
        eprintln!("SKIP: cargo not found");
        return;
    }

    let server = TestServer::start().await;
    let crate_dir = TempDir::new().expect("crate_dir");
    let base_url = server.base_url();

    std::fs::write(
        crate_dir.path().join("Cargo.toml"),
        r#"[package]
name = "cli-test-crate"
version = "0.1.0"
edition = "2021"
description = "integration test crate"
license = "MIT"
"#,
    )
    .unwrap();

    std::fs::create_dir_all(crate_dir.path().join("src")).unwrap();
    std::fs::write(
        crate_dir.path().join("src/lib.rs"),
        "pub fn hello() -> &'static str { \"hello from cli-test-crate\" }\n",
    )
    .unwrap();

    write_cargo_config(crate_dir.path(), &base_url, &server.plaintext_token);

    let dir = crate_dir.path().to_path_buf();
    let cargo_home = crate_dir.path().join(".cargo-home");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("cargo")
            .args([
                "publish",
                "--registry",
                "test-registry",
                "--allow-dirty",
                "--no-verify",
            ])
            .current_dir(&dir)
            .env("CARGO_HOME", &cargo_home)
            .output()
            .expect("cargo publish")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "cargo publish failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // Verify index entry via HTTP
    let client = reqwest::Client::new();
    let index_url = format!("{base_url}/index/cl/i-/cli-test-crate");
    let resp = client
        .get(&index_url)
        .header("Authorization", &server.plaintext_token)
        .send()
        .await
        .expect("index GET");
    assert_eq!(resp.status(), 200, "index entry should exist");

    let body = resp.text().await.unwrap();
    let line: serde_json::Value = serde_json::from_str(body.trim()).expect("valid NDJSON");
    assert_eq!(line["name"], "cli-test-crate");
    assert_eq!(line["vers"], "0.1.0");

    // Consume: create a project that depends on cli-test-crate
    let consumer_dir = TempDir::new().expect("consumer_dir");
    std::fs::write(
        consumer_dir.path().join("Cargo.toml"),
        r#"[package]
name = "consumer-crate"
version = "0.1.0"
edition = "2021"

[dependencies]
cli-test-crate = { version = "0.1.0", registry = "test-registry" }
"#,
    )
    .unwrap();

    std::fs::create_dir_all(consumer_dir.path().join("src")).unwrap();
    std::fs::write(
        consumer_dir.path().join("src/lib.rs"),
        "use cli_test_crate::hello;\npub fn greet() -> &'static str { hello() }\n",
    )
    .unwrap();

    write_cargo_config(consumer_dir.path(), &base_url, &server.plaintext_token);

    let dir = consumer_dir.path().to_path_buf();
    let cargo_home = consumer_dir.path().join(".cargo-home");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("cargo")
            .args(["check"])
            .current_dir(&dir)
            .env("CARGO_HOME", &cargo_home)
            .output()
            .expect("cargo check")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "cargo check (consume) failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
