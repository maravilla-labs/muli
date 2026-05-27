// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! npm registry integration tests (API-level + CLI-level).

mod common;

use axum::body::Body;
use common::{TestRegistry, TestServer, has_command};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// npm API-level tests (via tower oneshot)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_npm_publish_install() {
    let reg = TestRegistry::new().await;

    // --- Ping ---------------------------------------------------------------
    let req = reg.request("GET", "/-/ping").body(Body::empty()).unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 200, "ping should return 200");

    // --- Whoami -------------------------------------------------------------
    let req = reg.request("GET", "/-/whoami").body(Body::empty()).unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "whoami should return 200");
    let whoami: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(whoami["username"], "test-tenant");

    // --- Publish package ----------------------------------------------------
    let tarball_data = b"fake-tarball-content-for-testing";
    let tarball_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, tarball_data);

    let publish_body = serde_json::json!({
        "name": "my-package",
        "description": "A test package",
        "dist-tags": { "latest": "1.0.0" },
        "versions": {
            "1.0.0": {
                "name": "my-package",
                "version": "1.0.0",
                "description": "A test package",
                "dist": { "tarball": "", "shasum": "" }
            }
        },
        "_attachments": {
            "my-package-1.0.0.tgz": {
                "data": tarball_b64,
                "length": tarball_data.len()
            }
        }
    });

    let req = reg
        .request("PUT", "/-/npm/my-package")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&publish_body).unwrap()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        200,
        "publish should return 200: {}",
        String::from_utf8_lossy(&body)
    );
    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resp["ok"], true);

    // --- Get packument (full) -----------------------------------------------
    let req = reg
        .request("GET", "/-/npm/my-package")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "get packument should return 200");
    let packument: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(packument["name"], "my-package");
    assert!(
        packument["versions"]["1.0.0"].is_object(),
        "version 1.0.0 should exist"
    );
    let dist = &packument["versions"]["1.0.0"]["dist"];
    assert!(!dist["shasum"].as_str().unwrap().is_empty());
    assert!(dist["integrity"].as_str().unwrap().starts_with("sha512-"));

    // --- Get abbreviated packument ------------------------------------------
    let req = reg
        .request("GET", "/-/npm/my-package")
        .header("Accept", "application/vnd.npm.install-v1+json")
        .body(Body::empty())
        .unwrap();
    let (status, headers, _) = reg.send(req).await;
    assert_eq!(status, 200);
    assert_eq!(
        headers.get("Content-Type").unwrap().to_str().unwrap(),
        "application/vnd.npm.install-v1+json"
    );

    // --- Download tarball ---------------------------------------------------
    let tarball_url = dist["tarball"].as_str().unwrap();
    let req = reg.request("GET", tarball_url).body(Body::empty()).unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "tarball download should return 200");
    assert_eq!(body, tarball_data, "tarball bytes must match");

    // --- Search -------------------------------------------------------------
    let req = reg
        .request("GET", "/-/v1/search?text=my-package")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let search: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let objects = search["objects"].as_array().unwrap();
    assert!(
        objects
            .iter()
            .any(|o| o["package"]["name"].as_str() == Some("my-package")),
        "search should return my-package: {objects:?}"
    );
}

#[tokio::test]
async fn test_npm_scoped_publish() {
    let reg = TestRegistry::new().await;

    let tarball_data = b"scoped-tarball-bytes";
    let tarball_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, tarball_data);

    let publish_body = serde_json::json!({
        "name": "@myorg/my-lib",
        "description": "A scoped package",
        "dist-tags": { "latest": "2.0.0" },
        "versions": {
            "2.0.0": {
                "name": "@myorg/my-lib",
                "version": "2.0.0",
                "description": "A scoped package",
                "dist": { "tarball": "", "shasum": "" }
            }
        },
        // npm/pnpm key scoped attachments by the full package name,
        // including the scope and its '/'. Use the realistic key here.
        "_attachments": {
            "@myorg/my-lib-2.0.0.tgz": {
                "data": tarball_b64,
                "length": tarball_data.len()
            }
        }
    });

    let req = reg
        .request("PUT", "/-/npm/@myorg/my-lib")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&publish_body).unwrap()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        200,
        "scoped publish should return 200: {}",
        String::from_utf8_lossy(&body)
    );

    let req = reg
        .request("GET", "/-/npm/@myorg/my-lib")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let packument: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(packument["name"], "@myorg/my-lib");
    let tarball_url = packument["versions"]["2.0.0"]["dist"]["tarball"]
        .as_str()
        .unwrap();

    let req = reg.request("GET", tarball_url).body(Body::empty()).unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    assert_eq!(body, tarball_data);

    // Search must surface the real latest version (from the packument), not a
    // hardcoded "0.0.0" placeholder.
    let req = reg
        .request("GET", "/-/v1/search?text=my-lib")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let search: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let pkg = &search["objects"][0]["package"];
    assert_eq!(pkg["name"], "@myorg/my-lib");
    assert_eq!(
        pkg["version"], "2.0.0",
        "search should report the real latest version, got: {}",
        pkg["version"]
    );
}

// ---------------------------------------------------------------------------
// npm CLI integration test
// ---------------------------------------------------------------------------

fn npm_cmd(
    dir: &std::path::Path,
    npmrc: &std::path::Path,
    home: &std::path::Path,
) -> std::process::Command {
    let mut cmd = std::process::Command::new("npm");
    cmd.current_dir(dir)
        .env("HOME", home)
        .env("NPM_CONFIG_USERCONFIG", npmrc);
    cmd
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_npm_cli_publish_install() {
    if !has_command("npm") {
        eprintln!("SKIP: npm not found");
        return;
    }

    let server = TestServer::start().await;
    let pkg_dir = TempDir::new().expect("pkg_dir");
    let registry_url = format!("{}/-/npm/", server.base_url());
    let port = server.addr.port();

    std::fs::write(
        pkg_dir.path().join("package.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "name": "cli-test-pkg",
            "version": "1.0.0",
            "description": "integration test package",
            "main": "index.js"
        }))
        .unwrap(),
    )
    .unwrap();

    let npmrc_content = format!(
        "registry={registry}\n//127.0.0.1:{port}/-/npm/:_authToken={token}\nalways-auth=true\n",
        registry = registry_url,
        port = port,
        token = server.plaintext_token,
    );
    std::fs::write(pkg_dir.path().join(".npmrc"), &npmrc_content).unwrap();
    std::fs::write(
        pkg_dir.path().join("index.js"),
        "module.exports = { name: 'cli-test-pkg' };",
    )
    .unwrap();
    std::fs::write(pkg_dir.path().join(".npmignore"), ".npmrc\n.npm\n").unwrap();

    // --- PUBLISH ---
    let dir = pkg_dir.path().to_path_buf();
    let reg = registry_url.clone();
    let npmrc = pkg_dir.path().join(".npmrc");
    let home = pkg_dir.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        npm_cmd(&dir, &npmrc, &home)
            .args(["publish", "--registry", &reg])
            .output()
            .expect("npm publish")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "npm publish failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- VIEW ---
    let dir = pkg_dir.path().to_path_buf();
    let reg = registry_url.clone();
    let npmrc = pkg_dir.path().join(".npmrc");
    let home = pkg_dir.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        npm_cmd(&dir, &npmrc, &home)
            .args(["view", "cli-test-pkg", "--registry", &reg, "--json"])
            .output()
            .expect("npm view")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "npm view failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let view: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse npm view JSON");
    assert_eq!(view["name"], "cli-test-pkg");
    assert_eq!(view["version"], "1.0.0");

    // --- INSTALL ---
    let consumer_dir = TempDir::new().expect("consumer_dir");
    std::fs::write(
        consumer_dir.path().join("package.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "name": "consumer",
            "version": "1.0.0",
            "dependencies": { "cli-test-pkg": "1.0.0" }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(consumer_dir.path().join(".npmrc"), &npmrc_content).unwrap();

    let dir = consumer_dir.path().to_path_buf();
    let reg = registry_url.clone();
    let npmrc = consumer_dir.path().join(".npmrc");
    let home = consumer_dir.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        npm_cmd(&dir, &npmrc, &home)
            .args(["install", "--registry", &reg])
            .output()
            .expect("npm install")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "npm install failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let installed_pkg_json = consumer_dir
        .path()
        .join("node_modules/cli-test-pkg/package.json");
    assert!(installed_pkg_json.exists());
    let installed: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&installed_pkg_json).unwrap()).unwrap();
    assert_eq!(installed["name"], "cli-test-pkg");
    assert_eq!(installed["version"], "1.0.0");

    let installed_index = consumer_dir
        .path()
        .join("node_modules/cli-test-pkg/index.js");
    assert!(installed_index.exists());
    let content = std::fs::read_to_string(&installed_index).unwrap();
    assert!(content.contains("cli-test-pkg"));
}
