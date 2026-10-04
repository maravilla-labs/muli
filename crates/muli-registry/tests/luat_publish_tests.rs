// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat publish: authentication, scope ownership and manifest checks.

mod luat_common;

use luat_common::tarball::{manifest, simple, with_toml};
use luat_common::{
    ALICE, ALICE_RO, BOB, FOREIGN, LuatRegistry, NAMESAKE, UNBOUND, assert_error, json,
};

#[tokio::test]
async fn publish_needs_a_valid_token() {
    let reg = LuatRegistry::new().await;
    let tgz = || simple("@acme/ui", "1.0.0");
    let reply = reg.publish(None, "@acme/ui", "1.0.0", tgz()).await;
    assert_error(&reply, 401);
    assert!(reply.1.contains_key("www-authenticate"));
    assert_error(
        &reg.publish(Some("bogus-token-value-xyz"), "@acme/ui", "1.0.0", tgz())
            .await,
        401,
    );
    // Nothing was stored.
    assert_eq!(reg.get("/index/@acme/ui").await.0, 404);
}

#[tokio::test]
async fn publish_is_limited_to_owned_scopes() {
    let reg = LuatRegistry::new().await;
    // Not a member of `other`.
    assert_error(
        &reg.publish(
            Some(ALICE),
            "@other/ui",
            "1.0.0",
            simple("@other/ui", "1.0.0"),
        )
        .await,
        403,
    );
    // Someone else's personal scope.
    assert_error(
        &reg.publish(Some(ALICE), "@bob/ui", "1.0.0", simple("@bob/ui", "1.0.0"))
            .await,
        403,
    );
    // Viewer of acme.
    assert_error(
        &reg.publish(Some(BOB), "@acme/ui", "1.0.0", simple("@acme/ui", "1.0.0"))
            .await,
        403,
    );
    // Token without push permission.
    assert_error(
        &reg.publish(
            Some(ALICE_RO),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.0"),
        )
        .await,
        403,
    );
    // Token not bound to a user.
    assert_error(
        &reg.publish(
            Some(UNBOUND),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.0"),
        )
        .await,
        403,
    );
    // Token of another tenant.
    assert_error(
        &reg.publish(
            Some(FOREIGN),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.0"),
        )
        .await,
        403,
    );

    // A user whose handle equals an org's handle does not own the org's scope.
    assert_error(
        &reg.publish(
            Some(NAMESAKE),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.0"),
        )
        .await,
        403,
    );
    let me_request = reg
        .request("GET", "/api/v1/me", Some(NAMESAKE))
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(me_request).await;
    assert_eq!(status, 200);
    let me: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        me["scopes"],
        serde_json::json!([]),
        "namesake must not get the org's scope"
    );

    // Member, owner and personal scopes are fine.
    reg.publish_ok("@acme/ui", "1.0.0").await;
    reg.publish_ok("@maravilla/core", "0.1.0").await;
    reg.publish_ok("@alice/tools", "0.1.0").await;
    let (status, _, _) = reg
        .publish(Some(BOB), "@bob/x", "1.0.0", simple("@bob/x", "1.0.0"))
        .await;
    assert_eq!(status, 201);
}

#[tokio::test]
async fn duplicate_version_is_a_conflict() {
    let reg = LuatRegistry::new().await;
    reg.publish_ok("@acme/ui", "1.0.0").await;
    assert_error(
        &reg.publish(
            Some(ALICE),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.0"),
        )
        .await,
        409,
    );
    // Build metadata does not make a new release.
    let tgz = simple("@acme/ui", "1.0.0+rebuild");
    assert_error(
        &reg.publish(Some(ALICE), "@acme/ui", "1.0.0+rebuild", tgz)
            .await,
        409,
    );
    // Yanked versions still exist.
    reg.yank(Some(ALICE), "@acme/ui", "1.0.0", true).await;
    assert_error(
        &reg.publish(
            Some(ALICE),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.0"),
        )
        .await,
        409,
    );
    let index = String::from_utf8(reg.get("/index/@acme/ui").await.2).unwrap();
    assert_eq!(index.lines().count(), 1);
}

#[tokio::test]
async fn manifest_must_match_the_url() {
    let reg = LuatRegistry::new().await;
    // Name mismatch.
    assert_error(
        &reg.publish(
            Some(ALICE),
            "@acme/ui",
            "1.0.0",
            simple("@acme/other", "1.0.0"),
        )
        .await,
        400,
    );
    // Version mismatch.
    assert_error(
        &reg.publish(
            Some(ALICE),
            "@acme/ui",
            "1.0.0",
            simple("@acme/ui", "1.0.1"),
        )
        .await,
        400,
    );
    assert_eq!(reg.get("/index/@acme/ui").await.0, 404);
}

#[tokio::test]
async fn invalid_names_versions_and_manifests_are_rejected() {
    let reg = LuatRegistry::new().await;
    let bad_url = [
        ("@Acme/ui", "1.0.0"),
        ("@acme/-ui", "1.0.0"),
        ("@acme/ui", "1.0"),
        ("@acme/ui", "latest"),
    ];
    for (name, version) in bad_url {
        let reply = reg
            .publish(Some(ALICE), name, version, simple(name, version))
            .await;
        assert_error(&reply, 400);
    }

    let base = manifest("@acme/ui", "1.0.0");
    let bad_tomls = [
        "this is = not [toml".to_string(),
        "[dependencies]\n".to_string(),
        "[package]\nversion = \"1.0.0\"\n".to_string(),
        format!("{base}luat = \"definitely not a requirement\"\n"),
        format!("{base}\n[dependencies]\n\"BadName\" = \"^1.0\"\n"),
        format!("{base}\n[dependencies]\n\"@acme/Icons\" = \"^1.0\"\n"),
        format!("{base}\n[dependencies]\n\"@acme/icons\" = \"one point oh\"\n"),
        format!("{base}\n[dependencies]\n\"@acme/icons\" = {{ version = \"^1\" }}\n"),
    ];
    for toml in bad_tomls {
        let reply = reg
            .publish(Some(ALICE), "@acme/ui", "1.0.0", with_toml(&toml, &[]))
            .await;
        assert_error(&reply, 400);
    }
    assert_eq!(reg.get("/index/@acme/ui").await.0, 404);
}

#[tokio::test]
async fn plain_readme_and_prerelease_latest() {
    let reg = LuatRegistry::new().await;
    let tgz = with_toml(
        &manifest("@acme/ui", "1.0.0"),
        &[("package/README", b"plain readme")],
    );
    assert_eq!(
        reg.publish(Some(ALICE), "@acme/ui", "1.0.0", tgz).await.0,
        201
    );
    reg.publish_ok("@acme/ui", "2.0.0-beta.1").await;
    let info = json(&reg.get("/api/v1/packages/@acme/ui").await.2);
    // A pre-release does not become "latest" while a release exists.
    assert_eq!(info["latest"], "1.0.0");
    assert_eq!(info["readme"], "plain readme");
    let beta = json(&reg.get("/api/v1/packages/@acme/ui/2.0.0-beta.1").await.2);
    assert_eq!(beta["vers"], "2.0.0-beta.1");
    assert!(beta["readme"].is_null());
}
