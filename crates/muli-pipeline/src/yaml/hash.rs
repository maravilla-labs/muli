// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Template resolution for cache keys with `{{ hash('filename') }}` syntax.

use std::path::Path;

use muli_core::error::{MuliError, Result};
use sha2::{Digest, Sha256};

/// Resolve `{{ hash('filename') }}` templates in a string.
pub fn resolve_hash_templates(template: &str, repo_path: &Path) -> Result<String> {
    let mut result = template.to_string();
    while let Some(start) = result.find("{{ hash('") {
        let after = &result[start + 9..];
        let end = after.find("') }}").ok_or_else(|| {
            MuliError::PipelineYamlError("unclosed hash template".into())
        })?;
        let filename = &after[..end];
        let file_path = repo_path.join(filename);
        let hash = hash_file(&file_path)?;
        let full_match = format!("{{{{ hash('{filename}') }}}}");
        result = result.replace(&full_match, &hash);
    }
    Ok(result)
}

fn hash_file(path: &Path) -> Result<String> {
    let content = std::fs::read(path).map_err(|e| {
        MuliError::PipelineYamlError(format!(
            "cannot read file for hash '{}': {e}",
            path.display()
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(&content);
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_no_templates() {
        let result = resolve_hash_templates("cargo-lock", Path::new("/tmp")).unwrap();
        assert_eq!(result, "cargo-lock");
    }

    #[test]
    fn test_hash_template() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.lock");
        let mut f = std::fs::File::create(&file_path).unwrap();
        f.write_all(b"hello").unwrap();

        let result =
            resolve_hash_templates("cache-{{ hash('test.lock') }}", dir.path()).unwrap();
        assert!(result.starts_with("cache-"));
        assert_eq!(result.len(), "cache-".len() + 64);
    }
}
