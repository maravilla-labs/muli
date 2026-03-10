// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared test harness and helpers for muli-registry integration tests.
#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http;
use http_body_util::BodyExt;
use muli_core::registry::model::{RegistryPermission, RegistryToken};
use muli_core::traits::RegistryTokenStore;
use muli_registry::api::{RegistryConfig, registry_router};
use muli_registry::auth::{RegistryAuth, hash_token, token_prefix};
use muli_registry::storage::FilesystemStorage;
use muli_registry::tenant::TenantConfig;
use muli_store::memory::MemoryRegistryTokenStore;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;

/// Shared test harness that builds a full registry router backed by a temp directory.
pub struct TestRegistry {
    pub router: axum::Router,
    pub tenant_id: String,
    pub base_domain: String,
    pub plaintext_token: String,
    pub _tmp: TempDir,
}

impl TestRegistry {
    pub async fn new() -> Self {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let storage = Arc::new(
            FilesystemStorage::new(tmp.path())
                .await
                .expect("storage init"),
        );

        let plaintext_token = "test-token-secret-123";
        let token_hash = hash_token(plaintext_token);
        let prefix = token_prefix(plaintext_token);

        let token_store = Arc::new(MemoryRegistryTokenStore::new());
        let token = RegistryToken::new(
            "test-tenant".to_string(),
            token_hash,
            prefix,
            vec![
                RegistryPermission::Pull,
                RegistryPermission::Push,
                RegistryPermission::Admin,
            ],
            "integration-test token".to_string(),
            None,
        );
        token_store
            .create_token(&token)
            .await
            .expect("insert token");

        let auth = RegistryAuth::new(token_store);
        let tenant_config = TenantConfig::new("registry.test");

        let router = registry_router(
            storage,
            Some(auth),
            tenant_config,
            None,
            RegistryConfig {
                npm_enabled: true,
                cargo_enabled: true,
                maven_enabled: true,
            },
        );

        Self {
            router,
            tenant_id: "test-tenant".to_string(),
            base_domain: "registry.test".to_string(),
            plaintext_token: plaintext_token.to_string(),
            _tmp: tmp,
        }
    }

    /// Build an HTTP request with the correct Host and Authorization headers.
    pub fn request(&self, method: &str, path: &str) -> http::request::Builder {
        http::Request::builder()
            .uri(path)
            .method(method)
            .header("Host", "test-tenant.registry.test")
            .header("Authorization", format!("Bearer {}", self.plaintext_token))
    }

    /// Send a request through the router and collect the full response body.
    pub async fn send(
        &self,
        req: http::Request<Body>,
    ) -> (http::StatusCode, http::HeaderMap, Vec<u8>) {
        let resp = self
            .router
            .clone()
            .oneshot(req)
            .await
            .expect("oneshot failed");
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("body collect")
            .to_bytes()
            .to_vec();
        (status, headers, body)
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(data))
}

pub fn has_command(cmd: &str) -> bool {
    std::process::Command::new("which")
        .arg(cmd)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A real TCP test server for CLI integration tests.
pub struct TestServer {
    pub addr: std::net::SocketAddr,
    pub plaintext_token: String,
    pub _tmp: TempDir,
    pub shutdown: tokio::sync::watch::Sender<bool>,
}

impl TestServer {
    pub async fn start() -> Self {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let storage = Arc::new(
            FilesystemStorage::new(tmp.path())
                .await
                .expect("storage init"),
        );

        let plaintext_token = "cli-test-token-secret-456".to_string();
        let token_hash = hash_token(&plaintext_token);
        let prefix = token_prefix(&plaintext_token);

        let token_store = Arc::new(MemoryRegistryTokenStore::new());
        let token = RegistryToken::new(
            "test-tenant".to_string(),
            token_hash,
            prefix,
            vec![
                RegistryPermission::Pull,
                RegistryPermission::Push,
                RegistryPermission::Admin,
            ],
            "cli-integration-test token".to_string(),
            None,
        );
        token_store
            .create_token(&token)
            .await
            .expect("insert token");

        let auth = RegistryAuth::new(token_store);
        let tenant_config = TenantConfig::new("localhost").with_default_tenant("test-tenant");

        let router = registry_router(
            storage,
            Some(auth),
            tenant_config,
            None,
            RegistryConfig {
                npm_enabled: true,
                cargo_enabled: true,
                maven_enabled: true,
            },
        );

        let listener = TcpListener::bind("0.0.0.0:0").await.expect("bind TCP");
        let addr = listener.local_addr().expect("local addr");

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let mut rx = shutdown_rx;
                    let _ = rx.changed().await;
                })
                .await
                .ok();
        });

        Self {
            addr,
            plaintext_token,
            _tmp: tmp,
            shutdown: shutdown_tx,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.addr.port())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}
