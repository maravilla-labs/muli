// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat publish: hostile and oversized tarballs.

mod luat_common;

use luat_common::tarball::{Entry, dir, file, gzip, manifest, pax, special, tar, tgz};
use luat_common::{ALICE, LuatRegistry, assert_error};

const NAME: &str = "@acme/ui";
const VERSION: &str = "1.0.0";

fn toml_entry() -> Entry {
    file("package/luat.toml", manifest(NAME, VERSION).as_bytes())
}

/// A tarball with a valid manifest plus `extra`.
fn package_with(extra: Vec<Entry>) -> Vec<u8> {
    let mut entries = vec![dir("package/"), toml_entry()];
    entries.extend(extra);
    tgz(&entries)
}

async fn expect_rejected(reg: &LuatRegistry, tgz: Vec<u8>, status: u16, case: &str) {
    expect_rejected_because(reg, tgz, status, case, "").await;
}

/// Like `expect_rejected`, also checking the error message names the reason.
async fn expect_rejected_because(
    reg: &LuatRegistry,
    tgz: Vec<u8>,
    status: u16,
    case: &str,
    reason: &str,
) {
    let reply = reg.publish(Some(ALICE), NAME, VERSION, tgz).await;
    let message = String::from_utf8_lossy(&reply.2).to_string();
    assert!(
        message.contains(reason),
        "{case}: expected `{reason}` in {message}"
    );
    assert_eq!(
        reply.0,
        status,
        "{case}: {}",
        String::from_utf8_lossy(&reply.2)
    );
    assert_error(&reply, status);
    assert_eq!(
        reg.get("/index/@acme/ui").await.0,
        404,
        "{case}: nothing stored"
    );
}

#[tokio::test]
async fn hostile_entries_are_rejected() {
    let reg = LuatRegistry::new().await;
    let link = "not a regular file or directory";
    let escape = "path components are not allowed";
    let outside = "must live under package/";
    let absolute = "absolute paths";
    let cases: Vec<(&str, &str, Vec<Entry>)> = vec![
        (
            "symlink",
            link,
            vec![special("package/src/evil", b'2', "/etc/passwd")],
        ),
        (
            "hard link",
            link,
            vec![special("package/src/evil", b'1', "package/luat.toml")],
        ),
        ("char device", link, vec![special("package/dev", b'3', "")]),
        ("block device", link, vec![special("package/blk", b'4', "")]),
        ("fifo", link, vec![special("package/fifo", b'6', "")]),
        (
            "absolute path",
            absolute,
            vec![file("/etc/cron.d/evil", b"x")],
        ),
        (
            "dot-dot escape",
            escape,
            vec![file("package/../evil.lua", b"x")],
        ),
        (
            "dot-dot inside",
            escape,
            vec![file("package/src/../../evil", b"x")],
        ),
        ("outside package/", outside, vec![file("evil.lua", b"x")]),
        (
            "sibling root",
            outside,
            vec![file("packagex/evil.lua", b"x")],
        ),
        (
            "backslash path",
            absolute,
            vec![file("package\\..\\evil", b"x")],
        ),
        (
            "duplicate file",
            "duplicate entry",
            vec![file("package/luat.toml", b"[package]\n")],
        ),
        (
            "pax path override escaping package/",
            escape,
            vec![
                pax(&[("path", "package/../../evil")]),
                file("package/innocent", b"x"),
            ],
        ),
        (
            "pax linkpath on a symlink",
            link,
            vec![
                pax(&[("linkpath", "/etc/passwd")]),
                special("package/l", b'2', "x"),
            ],
        ),
    ];
    for (case, reason, extra) in cases {
        expect_rejected_because(&reg, package_with(extra), 400, case, reason).await;
    }
}

#[tokio::test]
async fn malformed_archives_are_rejected() {
    let reg = LuatRegistry::new().await;
    let entries = [toml_entry()];
    expect_rejected(&reg, tar(&entries), 400, "plain tar, not gzip").await;
    expect_rejected(
        &reg,
        b"\x1f\x8bnot really gzip".to_vec(),
        400,
        "gzip magic only",
    )
    .await;
    expect_rejected(&reg, Vec::new(), 400, "empty body").await;
    expect_rejected(
        &reg,
        gzip(b"just some text, not a tar"),
        400,
        "gzip of non-tar",
    )
    .await;
    expect_rejected(
        &reg,
        tgz(&[file("package/src/init.lua", b"x")]),
        400,
        "no luat.toml",
    )
    .await;
    expect_rejected(
        &reg,
        tgz(&[file(
            "package/sub/luat.toml",
            manifest(NAME, VERSION).as_bytes(),
        )]),
        400,
        "luat.toml not at root",
    )
    .await;

    // A header that under-declares its size desynchronises the stream.
    let mut lying = file("package/src/a.lua", &[b'a'; 4096]);
    lying.declared_size = Some(10);
    expect_rejected(&reg, tgz(&[toml_entry(), lying]), 400, "lying size header").await;
    // A header that over-declares runs off the end of the archive.
    let mut short = file("package/src/b.lua", b"tiny");
    short.declared_size = Some(1 << 20);
    expect_rejected(&reg, tgz(&[toml_entry(), short]), 400, "truncated entry").await;
}

#[tokio::test]
async fn size_and_count_limits() {
    let reg = LuatRegistry::new().await;

    // > 10 MiB compressed (incompressible bytes).
    let mut noise = Vec::with_capacity(11 << 20);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    while noise.len() < (10 << 20) + 1024 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        noise.extend_from_slice(&x.to_le_bytes());
    }
    let big = package_with(vec![file("package/blob.bin", &noise)]);
    assert!(big.len() > 10 << 20);
    expect_rejected_because(&reg, big, 413, "compressed size", "bytes compressed").await;

    // Gzip bomb: tiny compressed, > 50 MiB uncompressed.
    let bomb = package_with(vec![file("package/zeros.bin", &vec![0u8; (50 << 20) + 1])]);
    assert!(bomb.len() < 1 << 20);
    expect_rejected_because(&reg, bomb, 413, "uncompressed size", "unpacks to more than").await;

    // Many files that are individually small but together exceed 50 MiB.
    let chunk = vec![0u8; 11 << 20];
    let files = (0..5)
        .map(|i| file(&format!("package/z{i}.bin"), &chunk))
        .collect();
    expect_rejected_because(
        &reg,
        package_with(files),
        413,
        "total uncompressed size",
        "unpacks to more than",
    )
    .await;

    // More than 5000 files.
    let many = (0..5001)
        .map(|i| file(&format!("package/f/{i}.lua"), b"x"))
        .collect();
    expect_rejected_because(
        &reg,
        package_with(many),
        413,
        "file count",
        "more than 5000 files",
    )
    .await;
}

#[tokio::test]
async fn limits_are_inclusive() {
    let reg = LuatRegistry::new().await;
    // Exactly 5000 files (luat.toml counts) and exactly 50 MiB of content.
    let toml = manifest(NAME, VERSION);
    let mut entries = vec![file("package/luat.toml", toml.as_bytes())];
    entries.extend((0..4998).map(|i| file(&format!("package/f/{i}.lua"), b"")));
    let filler = (50usize << 20) - toml.len();
    entries.push(file("package/zeros.bin", &vec![0u8; filler]));
    let (status, _, body) = reg.publish(Some(ALICE), NAME, VERSION, tgz(&entries)).await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
}
