// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Read pipeline YAML from bare git repositories.

use std::path::{Path, PathBuf};

use muli_core::error::{MuliError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineFile {
    pub path: String,
    pub content: String,
}

/// Read all pipeline YAML files for a commit.
///
/// Supported locations:
/// - `.maravilla/pipeline.yml`
/// - `.maravilla/pipeline/*.yml`
/// - `.maravilla/pipeline/*.yaml`
pub fn read_pipeline_files(repo_path: &Path, commit_sha: &str) -> Result<Vec<PipelineFile>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;

    let oid = git2::Oid::from_str(commit_sha)
        .map_err(|e| MuliError::Pipeline(format!("invalid commit SHA: {e}")))?;

    let commit = repo
        .find_commit(oid)
        .map_err(|e| MuliError::Pipeline(format!("commit not found: {e}")))?;

    let tree = commit
        .tree()
        .map_err(|e| MuliError::Pipeline(format!("cannot read tree: {e}")))?;

    let mut files = Vec::new();

    if let Ok(entry) = tree.get_path(Path::new(".maravilla/pipeline.yml")) {
        files.push(read_pipeline_blob(
            &repo,
            entry.id(),
            PathBuf::from(".maravilla/pipeline.yml"),
        )?);
    }

    if let Ok(entry) = tree.get_path(Path::new(".maravilla/pipeline")) {
        let subtree = repo
            .find_tree(entry.id())
            .map_err(|e| MuliError::Pipeline(format!("cannot read pipeline directory: {e}")))?;

        for child in &subtree {
            if child.kind() != Some(git2::ObjectType::Blob) {
                continue;
            }
            let Some(name) = child.name() else {
                continue;
            };
            if !(name.ends_with(".yml") || name.ends_with(".yaml")) {
                continue;
            }
            files.push(read_pipeline_blob(
                &repo,
                child.id(),
                PathBuf::from(".maravilla/pipeline").join(name),
            )?);
        }
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

/// Read the first available pipeline YAML, preserving the legacy API shape.
pub fn read_pipeline_yaml(repo_path: &Path, commit_sha: &str) -> Result<Option<String>> {
    Ok(read_pipeline_files(repo_path, commit_sha)?
        .into_iter()
        .next()
        .map(|file| file.content))
}

/// Read pipeline YAML from HEAD of a bare repository.
pub fn read_pipeline_yaml_from_head(repo_path: &Path) -> Result<Option<String>> {
    Ok(read_pipeline_files_from_head(repo_path)?
        .into_iter()
        .next()
        .map(|file| file.content))
}

/// Read all pipeline files from HEAD of a bare repository.
pub fn read_pipeline_files_from_head(repo_path: &Path) -> Result<Vec<PipelineFile>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| MuliError::Pipeline(format!("cannot open repo: {e}")))?;

    let head = match repo.head() {
        Ok(h) => h,
        Err(_) => return Ok(Vec::new()),
    };

    let commit = head
        .peel_to_commit()
        .map_err(|e| MuliError::Pipeline(format!("cannot peel HEAD to commit: {e}")))?;

    read_pipeline_files(repo_path, &commit.id().to_string())
}

fn read_pipeline_blob(
    repo: &git2::Repository,
    blob_id: git2::Oid,
    path: PathBuf,
) -> Result<PipelineFile> {
    let blob = repo
        .find_blob(blob_id)
        .map_err(|e| MuliError::Pipeline(format!("cannot read pipeline blob: {e}")))?;

    let content = std::str::from_utf8(blob.content())
        .map_err(|e| MuliError::Pipeline(format!("{} is not valid UTF-8: {e}", path.display())))?;

    Ok(PipelineFile {
        path: path.to_string_lossy().to_string(),
        content: content.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    fn commit_repo(files: &[(&str, &str)]) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let origin = dir.path().join("origin.git");
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();

        std::process::Command::new("git")
            .args(["init", "--bare", "-b", "main", origin.to_str().unwrap()])
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(&work)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.email", "test@muli.dev"])
            .current_dir(&work)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.name", "Muli Test"])
            .current_dir(&work)
            .output()
            .unwrap();

        for (path, contents) in files {
            write(&work.join(path), contents);
        }

        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(&work)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&work)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["remote", "add", "origin", origin.to_str().unwrap()])
            .current_dir(&work)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["push", "origin", "main"])
            .current_dir(&work)
            .output()
            .unwrap();

        let sha = String::from_utf8(
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&work)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        (dir, sha)
    }

    #[test]
    fn reads_legacy_pipeline_file() {
        let (dir, sha) = commit_repo(&[(
            ".maravilla/pipeline.yml",
            "name: legacy\nsteps:\n  - name: a\n    image: alpine\n",
        )]);
        let files = read_pipeline_files(&dir.path().join("origin.git"), &sha).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, ".maravilla/pipeline.yml");
    }

    #[test]
    fn reads_pipeline_directory_files_sorted() {
        let (dir, sha) = commit_repo(&[
            (
                ".maravilla/pipeline/build.yaml",
                "name: build\njobs:\n  test:\n    image: alpine\n    commands: [echo build]\n",
            ),
            (
                ".maravilla/pipeline/lint.yml",
                "name: lint\njobs:\n  test:\n    image: alpine\n    commands: [echo lint]\n",
            ),
            (".maravilla/ignore.txt", "skip"),
        ]);
        let files = read_pipeline_files(&dir.path().join("origin.git"), &sha).unwrap();
        let paths: Vec<_> = files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                ".maravilla/pipeline/build.yaml",
                ".maravilla/pipeline/lint.yml"
            ]
        );
    }
}
