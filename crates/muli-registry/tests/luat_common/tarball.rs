// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Hand-rolled tar writer for tests. `tar::Builder` refuses to write the
//! hostile entries the registry must reject (`..`, absolute paths), so the
//! headers are assembled byte by byte here.

use std::io::Write;

/// One raw tar entry.
pub struct Entry {
    pub path: String,
    pub kind: u8,
    pub data: Vec<u8>,
    pub link: String,
    /// Size written in the header (defaults to `data.len()`).
    pub declared_size: Option<u64>,
}

pub fn file(path: &str, data: &[u8]) -> Entry {
    Entry {
        path: path.into(),
        kind: b'0',
        data: data.to_vec(),
        link: String::new(),
        declared_size: None,
    }
}

pub fn dir(path: &str) -> Entry {
    Entry {
        path: path.into(),
        kind: b'5',
        data: Vec::new(),
        link: String::new(),
        declared_size: None,
    }
}

pub fn special(path: &str, kind: u8, link: &str) -> Entry {
    Entry {
        path: path.into(),
        kind,
        data: Vec::new(),
        link: link.into(),
        declared_size: None,
    }
}

/// A pax extended header whose records override the next entry's metadata.
pub fn pax(records: &[(&str, &str)]) -> Entry {
    let mut data = Vec::new();
    for (k, v) in records {
        let body = format!(" {k}={v}\n");
        // The length prefix counts itself.
        let mut len = body.len() + 1;
        while format!("{len}{body}").len() != len {
            len += 1;
        }
        data.extend(format!("{len}{body}").into_bytes());
    }
    Entry {
        path: "PaxHeaders/x".into(),
        kind: b'x',
        data,
        link: String::new(),
        declared_size: None,
    }
}

fn put(field: &mut [u8], value: &[u8]) {
    let n = value.len().min(field.len());
    field[..n].copy_from_slice(&value[..n]);
}

fn octal(field: &mut [u8], value: u64) {
    let s = format!("{:0width$o}", value, width = field.len() - 1);
    put(field, s.as_bytes());
}

fn header(entry: &Entry) -> [u8; 512] {
    let mut h = [0u8; 512];
    put(&mut h[0..100], entry.path.as_bytes());
    octal(&mut h[100..108], 0o644);
    octal(&mut h[108..116], 0);
    octal(&mut h[116..124], 0);
    octal(
        &mut h[124..136],
        entry.declared_size.unwrap_or(entry.data.len() as u64),
    );
    octal(&mut h[136..148], 1_700_000_000);
    h[156] = entry.kind;
    put(&mut h[157..257], entry.link.as_bytes());
    put(&mut h[257..263], b"ustar\0");
    put(&mut h[263..265], b"00");
    h[148..156].copy_from_slice(b"        ");
    let sum: u32 = h.iter().map(|&b| u32::from(b)).sum();
    put(&mut h[148..156], format!("{sum:06o}\0 ").as_bytes());
    h
}

/// Raw (uncompressed) tar bytes.
pub fn tar(entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        out.extend_from_slice(&header(e));
        out.extend_from_slice(&e.data);
        let pad = (512 - e.data.len() % 512) % 512;
        out.extend(std::iter::repeat_n(0u8, pad));
    }
    out.extend([0u8; 1024]);
    out
}

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(bytes).unwrap();
    gz.finish().unwrap()
}

/// A gzip tarball of `entries`.
pub fn tgz(entries: &[Entry]) -> Vec<u8> {
    gzip(&tar(entries))
}

pub fn manifest(name: &str, version: &str) -> String {
    format!("[package]\nname = \"{name}\"\nversion = \"{version}\"\n")
}

/// A minimal valid package.
pub fn simple(name: &str, version: &str) -> Vec<u8> {
    tgz(&[
        dir("package/"),
        file("package/luat.toml", manifest(name, version).as_bytes()),
        file("package/src/init.lua", b"return {}\n"),
    ])
}

/// A valid package with the given `luat.toml` and extra files.
pub fn with_toml(toml: &str, extra: &[(&str, &[u8])]) -> Vec<u8> {
    let mut entries = vec![file("package/luat.toml", toml.as_bytes())];
    entries.extend(extra.iter().map(|(p, d)| file(p, d)));
    tgz(&entries)
}
