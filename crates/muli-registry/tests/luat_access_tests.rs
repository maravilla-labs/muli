// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat registry access: `me`, CORS on anonymous reads, private tenants and
//! coexistence with cargo's routes.

mod luat_common;

use axum::body::Body;
use luat_common::{ALICE, BOB, LuatRegistry, UNBOUND, assert_error, json};
use muli_core::registry::model::RegistryVisibilityLevel;

#[tokio::test]
async fn me_returns_user_and_publishable_scopes() {
    let reg = LuatRegistry::new().await;
    let me = |token: Option<&'static str>| {
        reg.send(
            reg.request("GET", "/api/v1/me", token)
                .body(Body::empty())
                .unwrap(),
        )
    };
    let (status, _, body) = me(Some(ALICE)).await;
    assert_eq!(status, 200);
    assert_eq!(
        json(&body),
        serde_json::json!({"user": "alice", "scopes": ["acme", "alice", "maravilla"]})
    );
    // A viewer of acme may not publish there.
    assert_eq!(
        json(&me(Some(BOB)).await.2)["scopes"],
        serde_json::json!(["bob"])
    );
    // Read-only tokens may still ask who they are.
    let (status, _, _) = me(Some(luat_common::ALICE_RO)).await;
    assert_eq!(status, 200);
    // Public registry or not, `me` needs a token.
    assert_error(&me(None).await, 401);
    assert_error(&me(Some("not-a-real-token-at-all")).await, 401);
    assert_error(&me(Some(UNBOUND)).await, 403);
    assert_error(&me(Some(luat_common::FOREIGN)).await, 403);
}

#[tokio::test]
async fn cors_only_on_anonymous_reads() {
    let reg = LuatRegistry::new().await;
    reg.publish_ok("@acme/ui", "1.0.0").await;
    let preflight = |path: &str| {
        reg.request("OPTIONS", path, None)
            .header("Origin", "https://packages.example")
            .header("Access-Control-Request-Method", "GET")
            .body(Body::empty())
            .unwrap()
    };
    for path in [
        "/index/@acme/ui",
        "/api/v1/packages",
        "/api/v1/packages/@acme/ui",
        "/api/v1/packages/@acme/ui/1.0.0",
        "/api/v1/packages/@acme/ui/1.0.0/download",
    ] {
        let (status, headers, _) = reg.send(preflight(path)).await;
        assert_eq!(status, 204, "{path}");
        assert_eq!(headers["access-control-allow-origin"], "*");
        assert_eq!(
            headers["access-control-allow-methods"],
            "GET, HEAD, OPTIONS"
        );
        let (status, headers, _) = reg.get(path).await;
        assert_eq!(status, 200, "{path}");
        assert_eq!(headers["access-control-allow-origin"], "*", "{path}");
        assert_eq!(headers["access-control-expose-headers"], "ETag");
    }
    // Writes and identity get no CORS.
    let (_, headers, _) = reg
        .publish(
            Some(ALICE),
            "@acme/ui",
            "2.0.0",
            luat_common::tarball::simple("@acme/ui", "2.0.0"),
        )
        .await;
    assert!(headers.get("access-control-allow-origin").is_none());
    let (status, headers, _) = reg.send(preflight("/api/v1/me")).await;
    assert_ne!(status, 204);
    assert!(headers.get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn private_tenant_requires_a_token_to_read() {
    let reg = LuatRegistry::with_visibility(RegistryVisibilityLevel::Private).await;
    reg.publish_ok("@acme/ui", "1.0.0").await;
    let reply = reg.get("/index/@acme/ui").await;
    assert_error(&reply, 401);
    let req = reg
        .request("GET", "/index/@acme/ui", Some(ALICE))
        .body(Body::empty())
        .unwrap();
    assert_eq!(reg.send(req).await.0, 200);
}

#[tokio::test]
async fn cargo_routes_still_work_next_to_luat() {
    let reg = LuatRegistry::new().await;
    let (status, _, body) = reg.get("/index/config.json").await;
    assert_eq!(status, 200);
    assert!(json(&body)["dl"].is_string());
    // A cargo index lookup for a missing crate is cargo's 404, not Luat's.
    assert_eq!(reg.get("/index/se/rd/serde").await.0, 404);
}
