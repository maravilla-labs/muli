// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Read a single file out of a job's artifact archive.
//!
//! Job artifacts are stored as a plain (uncompressed) `tar` by
//! [`super::manager::ArtifactManager`]. The release-notes `changelog` source
//! reads one entry back out of that archive server-side (the sandboxed job
//! checkout is not reachable from the engine).

use std::io::Read;

/// Return the UTF-8 text of `file_path` inside a plain tar `archive`, or `None`
/// if the entry is absent, unreadable, or not valid UTF-8.
///
/// Matching is lenient about a leading `./` and a trailing `/`, since tar
/// entries are commonly stored with a `./` prefix.
pub fn read_text_from_archive(archive: &[u8], file_path: &str) -> Option<String> {
    let wanted = normalize(file_path);
    let mut tar = tar::Archive::new(std::io::Cursor::new(archive));
    for entry in tar.entries().ok()? {
        let mut entry = entry.ok()?;
        let path = entry.path().ok()?.to_string_lossy().into_owned();
        if normalize(&path) == wanted {
            let mut buf = String::new();
            entry.read_to_string(&mut buf).ok()?;
            return Some(buf);
        }
    }
    None
}

/// Strip a leading `./` and trailing `/` so equivalent tar paths compare equal.
fn normalize(p: &str) -> String {
    p.trim_start_matches("./").trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar_with(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, content) in entries {
            let bytes = content.as_bytes();
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, name, bytes).unwrap();
        }
        builder.into_inner().unwrap()
    }

    #[test]
    fn reads_named_file() {
        let archive = tar_with(&[("CHANGELOG.md", "# v1.0\n- thing")]);
        assert_eq!(
            read_text_from_archive(&archive, "CHANGELOG.md").as_deref(),
            Some("# v1.0\n- thing")
        );
    }

    #[test]
    fn reads_nested_file_with_dot_slash_prefix() {
        let archive = tar_with(&[("./docs/NOTES.md", "hello")]);
        assert_eq!(
            read_text_from_archive(&archive, "docs/NOTES.md").as_deref(),
            Some("hello")
        );
    }

    #[test]
    fn missing_file_is_none() {
        let archive = tar_with(&[("other.txt", "x")]);
        assert!(read_text_from_archive(&archive, "CHANGELOG.md").is_none());
    }
}
