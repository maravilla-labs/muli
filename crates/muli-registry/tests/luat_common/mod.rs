// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared harness for the Luat registry integration tests: a full registry
//! router (every format enabled, so route overlaps with cargo are exercised)
//! for tenant `luat`, with users, orgs and tokens:
//!
//! | token       | user  | permissions      | may publish to                 |
//! |-------------|-------|------------------|--------------------------------|
//! | `alice`     | alice | pull+push        | alice, acme (member), maravilla (owner) |
//! | `bob`       | bob   | pull+push        | bob (viewer of acme: no)       |
//! | `alice_ro`  | alice | pull             | nothing (no push permission)   |
//! | `unbound`   | —     | pull+push+admin  | nothing (not bound to a user)  |
//! | `foreign`   | carol | pull+push        | tenant `other`, not `luat`     |
#![allow(dead_code)]

pub mod tarball;

use std::sync::Arc;

use axum::body::Body;
use axum::http;
use http_body_util::BodyExt;
use muli_core::org::{OrgMember, OrgRole, Organization};
use muli_core::registry::model::{RegistryPermission, RegistryToken, RegistryVisibilityLevel};
use muli_core::traits::{
    OrgMemberStore, OrgStore, RegistryTokenStore, RegistryVisibilityStore, UserStore,
};
use muli_core::user::TenantUser;
use muli_registry::api::{RegistryConfig, registry_router};
use muli_registry::auth::{RegistryAuth, hash_token, token_prefix};
use muli_registry::luat::LuatConfig;
use muli_registry::storage::FilesystemStorage;
use muli_registry::tenant::TenantConfig;
use muli_store::memory::{
    MemoryOrgMemberStore, MemoryOrgStore, MemoryRegistryTokenStore, MemoryRegistryVisibilityStore,
    MemoryUserStore,
};
use tempfile::TempDir;
use tower::ServiceExt;

pub const TENANT: &str = "luat";
pub const HOST: &str = "luat.registry.test";

pub const ALICE: &str = "alice-luat-token-0001";
pub const BOB: &str = "bobby-luat-token-0002";
pub const ALICE_RO: &str = "readonly-luat-token-0003";
pub const UNBOUND: &str = "unbound-luat-token-0004";
pub const FOREIGN: &str = "foreign-luat-token-0005";
/// A user whose handle is also an org's handle (`acme`), not a member of it.
pub const NAMESAKE: &str = "namesake-luat-token-0006";

pub struct LuatRegistry {
    pub router: axum::Router,
    pub _tmp: TempDir,
}

pub type Reply = (http::StatusCode, http::HeaderMap, Vec<u8>);

async fn add_token(
    store: &MemoryRegistryTokenStore,
    tenant: &str,
    plaintext: &str,
    perms: Vec<RegistryPermission>,
    user: Option<&TenantUser>,
) {
    let mut token = RegistryToken::new(
        tenant.to_string(),
        hash_token(plaintext),
        token_prefix(plaintext),
        perms,
        "luat test".to_string(),
        None,
    );
    if let Some(user) = user {
        token = token.with_user(user.id.clone());
    }
    store.create_token(&token).await.unwrap();
}

impl LuatRegistry {
    pub async fn new() -> Self {
        Self::with_visibility(RegistryVisibilityLevel::Public).await
    }

    pub async fn with_visibility(level: RegistryVisibilityLevel) -> Self {
        use RegistryPermission::{Admin, Pull, Push};
        let tmp = TempDir::new().unwrap();
        let storage = Arc::new(FilesystemStorage::new(tmp.path()).await.unwrap());

        let users = Arc::new(MemoryUserStore::new());
        let orgs = Arc::new(MemoryOrgStore::new());
        let members = Arc::new(MemoryOrgMemberStore::new());
        let user = |handle: &str, tenant: &str| {
            TenantUser::new(
                tenant.into(),
                handle.into(),
                format!("ext-{handle}"),
                format!("{handle}@example.com"),
            )
        };
        let (alice, bob, carol, namesake) = (
            user("alice", TENANT),
            user("bob", TENANT),
            user("carol", "other"),
            user("acme", TENANT),
        );
        for u in [&alice, &bob, &carol, &namesake] {
            users.create_user(u).await.unwrap();
        }
        let acme = Organization::new(TENANT.into(), "acme".into(), "Acme".into(), String::new());
        let maravilla = Organization::new(
            TENANT.into(),
            "maravilla".into(),
            "Maravilla".into(),
            String::new(),
        );
        orgs.create_org(&acme).await.unwrap();
        orgs.create_org(&maravilla).await.unwrap();
        for (org, u, role) in [
            (&acme, &alice, OrgRole::Member),
            (&acme, &bob, OrgRole::Viewer),
            (&maravilla, &alice, OrgRole::Owner),
        ] {
            members
                .add_member(&OrgMember::new(org.id.clone(), u.id.clone(), role))
                .await
                .unwrap();
        }

        let tokens = Arc::new(MemoryRegistryTokenStore::new());
        add_token(&tokens, TENANT, ALICE, vec![Pull, Push], Some(&alice)).await;
        add_token(&tokens, TENANT, BOB, vec![Pull, Push], Some(&bob)).await;
        add_token(&tokens, TENANT, ALICE_RO, vec![Pull], Some(&alice)).await;
        add_token(&tokens, TENANT, UNBOUND, vec![Pull, Push, Admin], None).await;
        add_token(&tokens, "other", FOREIGN, vec![Pull, Push], Some(&carol)).await;
        add_token(&tokens, TENANT, NAMESAKE, vec![Pull, Push], Some(&namesake)).await;

        let visibility = Arc::new(MemoryRegistryVisibilityStore::new());
        visibility.set_visibility(TENANT, level).await.unwrap();
        let auth =
            RegistryAuth::new(tokens).with_visibility(visibility, RegistryVisibilityLevel::Private);

        let router = registry_router(
            storage,
            Some(auth),
            TenantConfig::new("registry.test"),
            None,
            RegistryConfig {
                npm_enabled: true,
                cargo_enabled: true,
                maven_enabled: true,
                luat: Some(LuatConfig {
                    user_store: users,
                    org_store: orgs,
                    org_member_store: members,
                }),
            },
        );
        Self { router, _tmp: tmp }
    }

    /// A request to the luat tenant, with a bearer token when given.
    pub fn request(&self, method: &str, path: &str, token: Option<&str>) -> http::request::Builder {
        let builder = http::Request::builder()
            .uri(path)
            .method(method)
            .header("Host", HOST);
        match token {
            Some(t) => builder.header("Authorization", format!("Bearer {t}")),
            None => builder,
        }
    }

    pub async fn send(&self, req: http::Request<Body>) -> Reply {
        let resp = self.router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, headers, body)
    }

    /// Anonymous GET.
    pub async fn get(&self, path: &str) -> Reply {
        self.send(self.request("GET", path, None).body(Body::empty()).unwrap())
            .await
    }

    pub async fn publish(
        &self,
        token: Option<&str>,
        name: &str,
        version: &str,
        tgz: Vec<u8>,
    ) -> Reply {
        let req = self
            .request("PUT", &format!("/api/v1/packages/{name}/{version}"), token)
            .header("Content-Type", "application/gzip")
            .body(Body::from(tgz))
            .unwrap();
        self.send(req).await
    }

    /// Publish a minimal valid package as alice and assert 201.
    pub async fn publish_ok(&self, name: &str, version: &str) -> serde_json::Value {
        let (status, _, body) = self
            .publish(Some(ALICE), name, version, tarball::simple(name, version))
            .await;
        assert_eq!(
            status,
            201,
            "publish {name}@{version}: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).unwrap()
    }

    pub async fn yank(&self, token: Option<&str>, name: &str, version: &str, yank: bool) -> Reply {
        let (method, action) = if yank {
            ("DELETE", "yank")
        } else {
            ("PUT", "unyank")
        };
        let path = format!("/api/v1/packages/{name}/{version}/{action}");
        self.send(
            self.request(method, &path, token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
    }
}

pub fn json(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body)
        .unwrap_or_else(|_| panic!("not JSON: {}", String::from_utf8_lossy(body)))
}

/// Assert a Luat-style error: the status and a `{"error": "..."}` body.
pub fn assert_error(reply: &Reply, status: u16) {
    assert_eq!(
        reply.0,
        status,
        "body: {}",
        String::from_utf8_lossy(&reply.2)
    );
    let body = json(&reply.2);
    assert!(
        body["error"].is_string(),
        "expected {{\"error\": ...}}, got {body}"
    );
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(data))
}
