// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Docker CLI push/pull/tag integration test (Docker daemon required, auto-skip).

mod common;

use common::{TestServer, has_command};
use tempfile::TempDir;

fn docker_daemon_running() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn is_docker_desktop() -> bool {
    std::process::Command::new("docker")
        .args(["context", "show"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .starts_with("desktop")
        })
        .unwrap_or(false)
}

fn docker_registry_host() -> &'static str {
    if is_docker_desktop() {
        "host.docker.internal"
    } else {
        "127.0.0.1"
    }
}

fn docker_has_insecure_registry_for_host_internal() -> bool {
    let output = std::process::Command::new("docker")
        .args(["info", "--format", "{{json .RegistryConfig}}"])
        .output()
        .ok();
    match output {
        Some(o) if o.status.success() => {
            let text = String::from_utf8_lossy(&o.stdout);
            text.contains("host.docker.internal") || text.contains("192.168.65.")
        }
        _ => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_docker_cli_push_pull_tag() {
    if !has_command("docker") {
        eprintln!("SKIP: docker not found");
        return;
    }
    if !docker_daemon_running() {
        eprintln!("SKIP: docker daemon not running");
        return;
    }

    let docker_host = docker_registry_host();

    if docker_host == "host.docker.internal" && !docker_has_insecure_registry_for_host_internal() {
        eprintln!(
            "SKIP: Docker Desktop detected but 'host.docker.internal' is not in \
             insecure-registries.  Add it via Docker Desktop Settings > Docker Engine."
        );
        return;
    }

    let server = TestServer::start().await;
    let port = server.addr.port();
    let registry = format!("{docker_host}:{port}");
    let image_ref_v1 = format!("{registry}/test-image:v1.0");
    let image_ref_v2 = format!("{registry}/test-image:v2.0");
    let base_url = server.base_url();

    // --- Setup Docker auth config ---
    let docker_cfg_dir = TempDir::new().expect("docker config dir");
    let auth_string = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(format!("user:{}", server.plaintext_token))
    };
    let config_json = serde_json::json!({
        "auths": {
            registry.clone(): {
                "auth": auth_string
            }
        }
    });
    std::fs::write(
        docker_cfg_dir.path().join("config.json"),
        serde_json::to_string_pretty(&config_json).unwrap(),
    )
    .unwrap();
    let docker_config = docker_cfg_dir.path().to_path_buf();

    // --- Build a minimal image ---
    let build_dir = TempDir::new().expect("build dir");
    std::fs::write(build_dir.path().join("testfile"), "hello from docker test").unwrap();
    std::fs::write(
        build_dir.path().join("Dockerfile"),
        "FROM scratch\nCOPY testfile /testfile\n",
    )
    .unwrap();

    let build_ctx = build_dir.path().to_path_buf();
    let tag = image_ref_v1.clone();
    let cfg = docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["build", "-t", &tag, "."])
            .current_dir(&build_ctx)
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker build")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker build failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Push v1.0 ---
    let img = image_ref_v1.clone();
    let cfg = docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["push", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker push")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker push v1.0 failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Remove local image ---
    let img = image_ref_v1.clone();
    let cfg = docker_config.clone();
    let _ = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["rmi", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
    })
    .await;

    // --- Pull v1.0 ---
    let img = image_ref_v1.clone();
    let cfg = docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["pull", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker pull")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker pull v1.0 failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Inspect pulled image ---
    let img = image_ref_v1.clone();
    let cfg = docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["inspect", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker inspect")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker inspect failed — image not found after pull:\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Tag as v2.0 ---
    let src = image_ref_v1.clone();
    let dst = image_ref_v2.clone();
    let cfg = docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["tag", &src, &dst])
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker tag")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker tag failed:\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Push v2.0 ---
    let img = image_ref_v2.clone();
    let cfg = docker_config.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["push", &img])
            .env("DOCKER_CONFIG", &cfg)
            .output()
            .expect("docker push v2")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "docker push v2.0 failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Verify tags via HTTP ---
    let client = reqwest::Client::new();
    let tags_url = format!("{base_url}/v2/test-image/tags/list");
    let resp = client
        .get(&tags_url)
        .header(
            "Authorization",
            format!("Bearer {}", server.plaintext_token),
        )
        .send()
        .await
        .expect("tags/list GET");
    assert_eq!(resp.status(), 200, "tags/list should return 200");
    let tags: serde_json::Value = resp.json().await.unwrap();
    let tag_list = tags["tags"].as_array().expect("tags should be array");
    assert!(
        tag_list.iter().any(|t| t.as_str() == Some("v1.0")),
        "tags should include v1.0: {tag_list:?}"
    );
    assert!(
        tag_list.iter().any(|t| t.as_str() == Some("v2.0")),
        "tags should include v2.0: {tag_list:?}"
    );

    // --- Verify catalog via HTTP ---
    let catalog_url = format!("{base_url}/v2/_catalog");
    let resp = client
        .get(&catalog_url)
        .header(
            "Authorization",
            format!("Bearer {}", server.plaintext_token),
        )
        .send()
        .await
        .expect("catalog GET");
    assert_eq!(resp.status(), 200, "catalog should return 200");
    let catalog: serde_json::Value = resp.json().await.unwrap();
    let repos = catalog["repositories"]
        .as_array()
        .expect("repos should be array");
    assert!(
        repos.iter().any(|r| r.as_str() == Some("test-image")),
        "catalog should include test-image: {repos:?}"
    );

    // --- Cleanup ---
    let v1 = image_ref_v1.clone();
    let v2 = image_ref_v2.clone();
    let cfg = docker_config.clone();
    let _ = tokio::task::spawn_blocking(move || {
        let _ = std::process::Command::new("docker")
            .args(["rmi", &v1, &v2])
            .env("DOCKER_CONFIG", &cfg)
            .output();
    })
    .await;
}
