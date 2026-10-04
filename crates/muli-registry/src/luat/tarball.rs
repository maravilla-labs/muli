// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Luat package tarball inspection.
//!
//! A package is a gzip-compressed tar whose entries all live under `package/`:
//! regular files and directories only (no links, devices or FIFOs), no
//! absolute paths, no `..`. At most 10 MB compressed and 50 MB / 5000 files
//! uncompressed. Sizes are measured on the bytes actually read from the
//! stream — header-declared sizes are never trusted.

use std::collections::HashSet;
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tar::EntryType;

/// Maximum compressed tarball size (10 MiB).
pub const MAX_COMPRESSED_BYTES: usize = 10 * 1024 * 1024;
/// Maximum total size of the files once unpacked (50 MiB).
pub const MAX_UNPACKED_BYTES: u64 = 50 * 1024 * 1024;
/// Maximum number of regular files.
pub const MAX_FILES: usize = 5000;
/// Maximum number of entries of any kind (bounds directory-only archives).
const MAX_ENTRIES: usize = 2 * MAX_FILES;
/// Hard cap on the decompressed tar stream (file data + headers + padding).
const MAX_STREAM_BYTES: u64 = MAX_UNPACKED_BYTES + 16 * 1024 * 1024;
/// Largest `luat.toml` / README the registry keeps in memory.
const MAX_TEXT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum TarballError {
    /// The tarball breaks a packaging rule (HTTP 400).
    Invalid(String),
    /// The tarball exceeds a size or count limit (HTTP 413).
    TooLarge(String),
}

/// What the registry keeps from a valid tarball.
#[derive(Debug)]
pub struct Inspected {
    pub manifest_source: String,
    pub readme: Option<String>,
    /// Regular files, relative to the package root, sorted.
    pub files: Vec<String>,
    pub unpacked_bytes: u64,
}

/// Inspect a tarball, enforcing every packaging rule.
pub fn inspect(bytes: &[u8]) -> Result<Inspected, TarballError> {
    if bytes.len() > MAX_COMPRESSED_BYTES {
        return Err(too_large_compressed());
    }
    if !bytes.starts_with(&[0x1f, 0x8b]) {
        return Err(TarballError::Invalid(
            "tarball is not gzip-compressed".into(),
        ));
    }
    let exceeded = Arc::new(AtomicBool::new(false));
    let reader = CappedReader {
        inner: flate2::read::GzDecoder::new(bytes),
        remaining: MAX_STREAM_BYTES,
        exceeded: exceeded.clone(),
    };
    let mut archive = tar::Archive::new(reader);
    let mut state = State::default();
    let result = walk(&mut archive, &mut state);
    if exceeded.load(Ordering::Relaxed) {
        return Err(too_large_unpacked());
    }
    result?;

    let manifest_source = state
        .manifest
        .ok_or_else(|| TarballError::Invalid("tarball has no package/luat.toml".into()))?;
    let readme = state.readme_md.or(state.readme_plain);
    state.files.sort();
    Ok(Inspected {
        manifest_source,
        readme,
        files: state.files,
        unpacked_bytes: state.unpacked,
    })
}

#[derive(Default)]
struct State {
    manifest: Option<String>,
    readme_md: Option<String>,
    readme_plain: Option<String>,
    files: Vec<String>,
    seen: HashSet<String>,
    entries: usize,
    unpacked: u64,
}

fn walk<R: Read>(archive: &mut tar::Archive<R>, state: &mut State) -> Result<(), TarballError> {
    let entries = archive.entries().map_err(corrupt)?;
    for entry in entries {
        let mut entry = entry.map_err(corrupt)?;
        state.entries += 1;
        if state.entries > MAX_ENTRIES {
            return Err(TarballError::TooLarge(format!(
                "tarball has more than {MAX_ENTRIES} entries"
            )));
        }
        let entry_type = entry.header().entry_type();
        // A pax global header carries archive-wide metadata, never a file.
        if entry_type == EntryType::XGlobalHeader {
            continue;
        }
        let raw_path = entry.path_bytes().into_owned();
        let path = std::str::from_utf8(&raw_path)
            .map_err(|_| TarballError::Invalid("tarball entry path is not UTF-8".into()))?;
        let relative = package_relative(path)?;
        match entry_type {
            EntryType::Directory => continue,
            EntryType::Regular | EntryType::Continuous => {}
            other => {
                return Err(TarballError::Invalid(format!(
                    "`{path}` is not a regular file or directory ({other:?})"
                )));
            }
        }
        let Some(relative) = relative else {
            return Err(TarballError::Invalid(
                "`package` must be a directory".into(),
            ));
        };
        if !state.seen.insert(relative.clone()) {
            return Err(TarballError::Invalid(format!("duplicate entry `{path}`")));
        }
        if state.files.len() >= MAX_FILES {
            return Err(TarballError::TooLarge(format!(
                "tarball has more than {MAX_FILES} files"
            )));
        }
        let keep_text = matches!(relative.as_str(), "luat.toml" | "README.md" | "README");
        read_file(&mut entry, state, keep_text, &relative)?;
        state.files.push(relative);
    }
    Ok(())
}

/// Read one file's bytes, counting what is actually in the stream.
fn read_file<R: Read>(
    entry: &mut R,
    state: &mut State,
    keep_text: bool,
    relative: &str,
) -> Result<(), TarballError> {
    let budget = MAX_UNPACKED_BYTES - state.unpacked;
    let mut limited = entry.take(budget + 1);
    let read = if keep_text {
        let mut buf = Vec::new();
        let n = (&mut limited)
            .take(MAX_TEXT_BYTES + 1)
            .read_to_end(&mut buf)
            .map_err(corrupt)? as u64;
        if n > MAX_TEXT_BYTES {
            if relative == "luat.toml" {
                return Err(TarballError::Invalid(
                    "luat.toml is larger than 1 MiB".into(),
                ));
            }
            // An oversized README is shipped but not shown on the package page.
            n + io::copy(&mut limited, &mut io::sink()).map_err(corrupt)?
        } else {
            let text = String::from_utf8(buf);
            match (relative, text) {
                ("luat.toml", Ok(t)) => state.manifest = Some(t),
                ("luat.toml", Err(_)) => {
                    return Err(TarballError::Invalid("luat.toml is not UTF-8".into()));
                }
                ("README.md", t) => state.readme_md = Some(lossy(t)),
                (_, t) => state.readme_plain = Some(lossy(t)),
            }
            n
        }
    } else {
        io::copy(&mut limited, &mut io::sink()).map_err(corrupt)?
    };
    if read > budget {
        return Err(too_large_unpacked());
    }
    state.unpacked += read;
    Ok(())
}

fn lossy(text: Result<String, std::string::FromUtf8Error>) -> String {
    text.unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Check an entry path and return it relative to `package/`
/// (`None` for the `package/` directory itself).
fn package_relative(path: &str) -> Result<Option<String>, TarballError> {
    let bad = |why: &str| TarballError::Invalid(format!("tarball entry `{path}`: {why}"));
    if path.starts_with('/') || path.contains('\\') {
        return Err(bad("absolute paths and backslashes are not allowed"));
    }
    let trimmed = path.strip_suffix('/').unwrap_or(path);
    let mut parts = trimmed.split('/');
    if parts.next() != Some("package") {
        return Err(bad("every entry must live under package/"));
    }
    let rest: Vec<&str> = parts.collect();
    for part in &rest {
        if part.is_empty() || *part == "." || *part == ".." {
            return Err(bad("empty, `.` and `..` path components are not allowed"));
        }
    }
    Ok((!rest.is_empty()).then(|| rest.join("/")))
}

fn corrupt(e: io::Error) -> TarballError {
    TarballError::Invalid(format!("tarball is not a valid gzip tar: {e}"))
}

fn too_large_compressed() -> TarballError {
    TarballError::TooLarge(format!(
        "tarball is larger than {MAX_COMPRESSED_BYTES} bytes compressed"
    ))
}

fn too_large_unpacked() -> TarballError {
    TarballError::TooLarge(format!(
        "tarball unpacks to more than {MAX_UNPACKED_BYTES} bytes"
    ))
}

/// Reader that fails once more than `remaining` bytes have been produced,
/// recording that it did so (the tar crate rewraps the I/O error).
struct CappedReader<R> {
    inner: R,
    remaining: u64,
    exceeded: Arc<AtomicBool>,
}

impl<R: Read> Read for CappedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            // Probe one byte: a stream ending exactly at the cap is fine.
            let mut probe = [0u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => {
                    self.exceeded.store(true, Ordering::Relaxed);
                    Err(io::Error::other(
                        "decompressed tarball exceeds the size limit",
                    ))
                }
            };
        }
        let max = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..max])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
#[path = "tarball_tests.rs"]
mod tests;
