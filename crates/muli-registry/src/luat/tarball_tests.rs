// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Unit tests for tarball inspection. Hostile archives (links, `..`, absolute
//! paths, size bombs) are covered end to end in `tests/luat_tarball_tests.rs`.

use std::io::Write;

use super::*;

fn gzip(tar: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(tar).unwrap();
    gz.finish().unwrap()
}

fn build(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (path, data) in files {
        let mut header = tar::Header::new_ustar();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        builder.append_data(&mut header, path, *data).unwrap();
    }
    gzip(&builder.into_inner().unwrap())
}

const TOML: &[u8] = b"[package]\nname = \"@a/b\"\nversion = \"1.0.0\"\n";

#[test]
fn valid_package_is_inspected() {
    let tgz = build(&[
        ("package/luat.toml", TOML),
        ("package/README.md", b"# hi"),
        ("package/src/init.lua", b"return {}"),
    ]);
    let got = inspect(&tgz).unwrap();
    assert_eq!(got.manifest_source.as_bytes(), TOML);
    assert_eq!(got.readme.as_deref(), Some("# hi"));
    assert_eq!(got.files, vec!["README.md", "luat.toml", "src/init.lua"]);
    assert_eq!(got.unpacked_bytes, (TOML.len() + 4 + 9) as u64);
}

#[test]
fn readme_md_wins_over_plain_readme() {
    let tgz = build(&[
        ("package/README", b"plain"),
        ("package/README.md", b"markdown"),
        ("package/luat.toml", TOML),
    ]);
    assert_eq!(inspect(&tgz).unwrap().readme.as_deref(), Some("markdown"));
}

#[test]
fn nested_readme_is_not_the_package_readme() {
    let tgz = build(&[
        ("package/luat.toml", TOML),
        ("package/docs/README.md", b"x"),
    ]);
    assert!(inspect(&tgz).unwrap().readme.is_none());
}

#[test]
fn missing_manifest_is_invalid() {
    let tgz = build(&[("package/src/init.lua", b"return {}")]);
    assert!(matches!(inspect(&tgz), Err(TarballError::Invalid(_))));
}

#[test]
fn plain_tar_is_rejected() {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_ustar();
    header.set_size(TOML.len() as u64);
    builder
        .append_data(&mut header, "package/luat.toml", TOML)
        .unwrap();
    let tar = builder.into_inner().unwrap();
    assert!(matches!(inspect(&tar), Err(TarballError::Invalid(_))));
}

#[test]
fn garbage_after_gzip_magic_is_invalid() {
    let mut bytes = vec![0x1f, 0x8b];
    bytes.extend_from_slice(b"definitely not deflate");
    assert!(matches!(inspect(&bytes), Err(TarballError::Invalid(_))));
}

#[test]
fn entry_paths_are_checked() {
    assert_eq!(package_relative("package/").unwrap(), None);
    assert_eq!(
        package_relative("package/src/a.lua").unwrap().as_deref(),
        Some("src/a.lua")
    );
    for bad in [
        "/package/a",
        "other/a",
        "package/../a",
        "package/./a",
        "package//a",
        "package\\a",
        "./package/a",
        "packages/a",
    ] {
        assert!(package_relative(bad).is_err(), "{bad} should be rejected");
    }
}
