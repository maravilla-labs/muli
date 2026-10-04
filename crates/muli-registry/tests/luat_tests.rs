// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat registry: anonymous reads, index format + ETag, package/version info,
//! yank/unyank and search.

mod luat_common;

use axum::body::Body;
use luat_common::tarball::{file, tgz};
use luat_common::{ALICE, BOB, LuatRegistry, UNBOUND, assert_error, json, sha256_hex};

const UI_TOML: &str = r#"[package]
name = "@acme/ui"
version = "1.2.0"
description = "Cards, buttons and layouts"
license = "MIT"
repository = "https://example.com/acme/ui"
keywords = ["ui", "components"]
luat = ">=0.2"

[dependencies]
"@acme/icons" = "^2.0"
"#;

fn ui_tarball() -> Vec<u8> {
    tgz(&[
        file("package/luat.toml", UI_TOML.as_bytes()),
        file("package/README.md", b"# @acme/ui\nhello"),
        file("package/src/Card.luat", b"<div class=\"card\"></div>"),
    ])
}

#[tokio::test]
async fn anonymous_read_of_a_published_package() {
    let reg = LuatRegistry::new().await;
    let tgz = ui_tarball();
    let (status, _, body) = reg
        .publish(Some(ALICE), "@acme/ui", "1.2.0", tgz.clone())
        .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let cksum = format!("sha256:{}", sha256_hex(&tgz));
    assert_eq!(
        json(&body),
        serde_json::json!({"name": "@acme/ui", "vers": "1.2.0", "cksum": cksum})
    );

    // Index: exactly the spec's line, no token needed.
    let (status, headers, body) = reg.get("/index/@acme/ui").await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "application/x-ndjson");
    let text = String::from_utf8(body).unwrap();
    assert_eq!(
        text,
        format!(
            "{{\"name\":\"@acme/ui\",\"vers\":\"1.2.0\",\"deps\":{{\"@acme/icons\":\"^2.0\"}},\"cksum\":\"{cksum}\",\"luat\":\">=0.2\",\"yanked\":false}}\n"
        )
    );

    // Download: the exact bytes, as gzip.
    let (status, headers, body) = reg.get("/api/v1/packages/@acme/ui/1.2.0/download").await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "application/gzip");
    assert_eq!(body, tgz);

    // Package page data.
    let (status, _, body) = reg.get("/api/v1/packages/@acme/ui").await;
    assert_eq!(status, 200);
    let info = json(&body);
    assert_eq!(info["name"], "@acme/ui");
    assert_eq!(info["description"], "Cards, buttons and layouts");
    assert_eq!(info["latest"], "1.2.0");
    assert_eq!(info["license"], "MIT");
    assert_eq!(info["repository"], "https://example.com/acme/ui");
    assert_eq!(info["keywords"], serde_json::json!(["ui", "components"]));
    assert_eq!(info["readme"], "# @acme/ui\nhello");
    assert_eq!(
        info["dependencies"],
        serde_json::json!({"@acme/icons": "^2.0"})
    );
    assert_eq!(
        info["files"],
        serde_json::json!(["README.md", "luat.toml", "src/Card.luat"])
    );
    let v = &info["versions"][0];
    assert_eq!(v["vers"], "1.2.0");
    assert_eq!(v["yanked"], false);
    assert_eq!(v["size"], tgz.len());
    let published = v["published_at"].as_str().unwrap();
    assert!(
        published.ends_with('Z') && !published.contains('.'),
        "{published}"
    );

    // Version page data: same shape.
    let (status, _, body) = reg.get("/api/v1/packages/@acme/ui/1.2.0").await;
    assert_eq!(status, 200);
    assert_eq!(json(&body)["readme"], info["readme"]);
    assert_eq!(json(&body)["files"], info["files"]);
}

#[tokio::test]
async fn missing_things_are_json_404s() {
    let reg = LuatRegistry::new().await;
    assert_error(&reg.get("/index/@acme/nothing").await, 404);
    assert_error(&reg.get("/api/v1/packages/@acme/nothing").await, 404);
    reg.publish_ok("@acme/ui", "1.0.0").await;
    assert_error(&reg.get("/api/v1/packages/@acme/ui/9.9.9").await, 404);
    assert_error(
        &reg.get("/api/v1/packages/@acme/ui/9.9.9/download").await,
        404,
    );
    assert_error(&reg.get("/index/@Acme/ui").await, 400);
}

#[tokio::test]
async fn index_is_oldest_first_with_etag() {
    let reg = LuatRegistry::new().await;
    for v in ["1.0.0", "1.1.0", "0.9.0"] {
        reg.publish_ok("@acme/ui", v).await;
    }
    let (status, headers, body) = reg.get("/index/@acme/ui").await;
    assert_eq!(status, 200);
    let vers: Vec<String> = String::from_utf8(body)
        .unwrap()
        .lines()
        .map(|l| json(l.as_bytes())["vers"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        vers,
        ["1.0.0", "1.1.0", "0.9.0"],
        "publish order, oldest first"
    );
    let etag = headers["etag"].to_str().unwrap().to_string();
    assert!(etag.starts_with('"') && etag.ends_with('"'));

    let cached = |tag: &str| {
        reg.request("GET", "/index/@acme/ui", None)
            .header("If-None-Match", tag)
            .body(Body::empty())
            .unwrap()
    };
    let (status, headers, body) = reg.send(cached(&etag)).await;
    assert_eq!(status, 304);
    assert!(body.is_empty());
    assert_eq!(headers["etag"].to_str().unwrap(), etag);
    assert_eq!(reg.send(cached("\"stale\"")).await.0, 200);

    // Any change to the index changes the ETag.
    reg.yank(Some(ALICE), "@acme/ui", "1.1.0", true).await;
    let (status, _, _) = reg.send(cached(&etag)).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn yank_and_unyank_flip_the_flag_only() {
    let reg = LuatRegistry::new().await;
    reg.publish_ok("@acme/ui", "1.0.0").await;
    reg.publish_ok("@acme/ui", "1.1.0").await;

    assert_error(&reg.yank(None, "@acme/ui", "1.1.0", true).await, 401);
    assert_error(&reg.yank(Some(BOB), "@acme/ui", "1.1.0", true).await, 403);
    assert_error(
        &reg.yank(Some(UNBOUND), "@acme/ui", "1.1.0", true).await,
        403,
    );
    assert_error(&reg.yank(Some(ALICE), "@acme/ui", "7.0.0", true).await, 404);

    let (status, _, body) = reg.yank(Some(ALICE), "@acme/ui", "1.1.0", true).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let index = String::from_utf8(reg.get("/index/@acme/ui").await.2).unwrap();
    let lines: Vec<_> = index.lines().map(|l| json(l.as_bytes())).collect();
    assert_eq!(lines.len(), 2, "nothing is deleted");
    assert_eq!(lines[0]["yanked"], false);
    assert_eq!(lines[1]["yanked"], true);
    // Still downloadable (lockfiles may pin it); latest moves back.
    assert_eq!(
        reg.get("/api/v1/packages/@acme/ui/1.1.0/download").await.0,
        200
    );
    let info = json(&reg.get("/api/v1/packages/@acme/ui").await.2);
    assert_eq!(info["latest"], "1.0.0");
    assert_eq!(info["versions"][1]["yanked"], true);

    let (status, _, _) = reg.yank(Some(ALICE), "@acme/ui", "1.1.0", false).await;
    assert_eq!(status, 200);
    let index = String::from_utf8(reg.get("/index/@acme/ui").await.2).unwrap();
    assert!(index.lines().all(|l| json(l.as_bytes())["yanked"] == false));
    assert_eq!(
        json(&reg.get("/api/v1/packages/@acme/ui").await.2)["latest"],
        "1.1.0"
    );
}

#[tokio::test]
async fn search_and_listing() {
    let reg = LuatRegistry::new().await;
    let toml = |name: &str, desc: &str, kw: &str| {
        format!(
            "[package]\nname = \"{name}\"\nversion = \"1.0.0\"\ndescription = \"{desc}\"\nkeywords = [\"{kw}\"]\n"
        )
    };
    for (name, desc, kw) in [
        ("@acme/ui", "Cards and buttons", "components"),
        ("@acme/icons", "SVG icon set", "graphics"),
        ("@maravilla/forms", "Form helpers", "Input"),
    ] {
        let tgz = luat_common::tarball::with_toml(&toml(name, desc, kw), &[]);
        assert_eq!(reg.publish(Some(ALICE), name, "1.0.0", tgz).await.0, 201);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let names = |body: &[u8]| -> Vec<String> {
        json(body)["packages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap().to_string())
            .collect()
    };
    let (status, _, body) = reg.get("/api/v1/packages").await;
    assert_eq!(status, 200);
    assert_eq!(json(&body)["total"], 3);
    assert_eq!(
        names(&body),
        ["@maravilla/forms", "@acme/icons", "@acme/ui"]
    );
    let first = &json(&body)["packages"][0];
    assert_eq!(first["description"], "Form helpers");
    assert_eq!(first["latest"], "1.0.0");
    assert!(first["updated_at"].as_str().unwrap().ends_with('Z'));

    assert_eq!(
        names(&reg.get("/api/v1/packages?q=ICON").await.2),
        ["@acme/icons"]
    );
    assert_eq!(
        names(&reg.get("/api/v1/packages?q=buttons").await.2),
        ["@acme/ui"]
    );
    assert_eq!(
        names(&reg.get("/api/v1/packages?q=input").await.2),
        ["@maravilla/forms"]
    );
    assert_eq!(
        json(&reg.get("/api/v1/packages?q=acme").await.2)["total"],
        2
    );

    let (_, _, body) = reg.get("/api/v1/packages?page=2&per_page=2").await;
    assert_eq!(json(&body)["total"], 3);
    assert_eq!(names(&body), ["@acme/ui"]);
    assert_eq!(
        names(&reg.get("/api/v1/packages?per_page=1000").await.2).len(),
        3
    );
    assert_error(&reg.get("/api/v1/packages?page=x").await, 400);
}
