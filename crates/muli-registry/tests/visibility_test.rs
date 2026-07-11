// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-tenant registry read visibility: a `Public` tenant serves reads with no
//! token; `Private` still requires one.

use std::sync::Arc;

use axum::body::Body;
use axum::http;
use muli_core::registry::model::RegistryVisibilityLevel;
use muli_core::traits::RegistryVisibilityStore;
use muli_registry::api::{RegistryConfig, registry_router};
use muli_registry::auth::RegistryAuth;
use muli_registry::storage::FilesystemStorage;
use muli_registry::tenant::TenantConfig;
use muli_store::memory::{MemoryRegistryTokenStore, MemoryRegistryVisibilityStore};
use tempfile::TempDir;
use tower::ServiceExt;

const TENANT: &str = "acme";
const HOST: &str = "acme.registry.test";
// A scoped packument path; the package does not exist, so a request that PASSES
// auth reaches the handler and gets 404 — never 401. That distinguishes
// "auth allowed the anonymous read" (404) from "auth rejected it" (401).
const PATH: &str = "/-/npm/@acme%2fnothing";

async fn router_with_visibility(level: RegistryVisibilityLevel) -> (axum::Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(FilesystemStorage::new(tmp.path()).await.unwrap());
    let token_store = Arc::new(MemoryRegistryTokenStore::new());
    let vis = Arc::new(MemoryRegistryVisibilityStore::new());
    vis.set_visibility(TENANT, level).await.unwrap();

    let auth = RegistryAuth::new(token_store)
        .with_visibility(vis, RegistryVisibilityLevel::Private);
    let router = registry_router(
        storage,
        Some(auth),
        TenantConfig::new("registry.test"),
        None,
        RegistryConfig {
            npm_enabled: true,
            cargo_enabled: true,
            maven_enabled: true,
        },
    );
    (router, tmp)
}

fn anon_get() -> http::Request<Body> {
    http::Request::builder()
        .uri(PATH)
        .method("GET")
        .header("Host", HOST)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn public_tenant_serves_anonymous_read() {
    let (router, _tmp) = router_with_visibility(RegistryVisibilityLevel::Public).await;
    let resp = router.oneshot(anon_get()).await.unwrap();
    // Passed auth with NO token → handler reached (404 missing package), not 401.
    assert_ne!(resp.status(), http::StatusCode::UNAUTHORIZED);
    assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn private_tenant_rejects_anonymous_read() {
    let (router, _tmp) = router_with_visibility(RegistryVisibilityLevel::Private).await;
    let resp = router.oneshot(anon_get()).await.unwrap();
    assert_eq!(resp.status(), http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authenticated_tenant_rejects_anonymous_read() {
    // `authenticated` is enforced at token issuance, so from muli's side an
    // anonymous (token-less) read must still be rejected — same as private.
    let (router, _tmp) = router_with_visibility(RegistryVisibilityLevel::Authenticated).await;
    let resp = router.oneshot(anon_get()).await.unwrap();
    assert_eq!(resp.status(), http::StatusCode::UNAUTHORIZED);
}
