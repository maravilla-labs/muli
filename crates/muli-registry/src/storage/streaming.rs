// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Streaming helpers for storage I/O.

use std::path::{Path, PathBuf};

use futures::stream;
use tokio::fs;
use tokio::io::AsyncReadExt;

use super::{ByteStream, StorageResult};

/// Convert a file path into an async byte stream, reading in 64 KiB chunks.
pub fn file_to_stream(path: PathBuf) -> ByteStream {
    const CHUNK_SIZE: usize = 64 * 1024;
    Box::pin(stream::unfold(None::<fs::File>, move |file_opt| {
        let path = path.clone();
        async move {
            let mut file = match file_opt {
                Some(f) => f,
                None => match fs::File::open(&path).await {
                    Ok(f) => f,
                    Err(e) => return Some((Err(e), None)),
                },
            };
            let mut buf = vec![0u8; CHUNK_SIZE];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok(buf), Some(file)))
                }
                Err(e) => Some((Err(e), None)),
            }
        }
    }))
}

/// Recursively collect repository names (directories that directly contain files).
pub async fn collect_repositories(
    base: &Path,
    current: &Path,
    repos: &mut Vec<String>,
) -> StorageResult<()> {
    let mut entries = fs::read_dir(current).await?;
    let mut has_files = false;
    let mut subdirs = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let ft = entry.file_type().await?;
        if ft.is_file() {
            has_files = true;
        } else if ft.is_dir() {
            subdirs.push(entry.path());
        }
    }

    if has_files && let Ok(rel) = current.strip_prefix(base) {
        repos.push(rel.to_string_lossy().to_string());
    }

    for subdir in subdirs {
        Box::pin(collect_repositories(base, &subdir, repos)).await?;
    }

    Ok(())
}

/// Recursively collect all manifest data from a directory tree.
pub async fn collect_manifest_data(dir: &Path, result: &mut Vec<Vec<u8>>) -> StorageResult<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let ft = entry.file_type().await?;
        if ft.is_file() {
            let data = fs::read(entry.path()).await?;
            result.push(data);
        } else if ft.is_dir() {
            Box::pin(collect_manifest_data(&entry.path(), result)).await?;
        }
    }
    Ok(())
}

/// Recursively sum file sizes in a directory.
pub async fn collect_dir_size(dir: &Path, total: &mut u64) -> StorageResult<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let ft = entry.file_type().await?;
        if ft.is_file() {
            let meta = fs::metadata(entry.path()).await?;
            *total += meta.len();
        } else if ft.is_dir() {
            Box::pin(collect_dir_size(&entry.path(), total)).await?;
        }
    }
    Ok(())
}
