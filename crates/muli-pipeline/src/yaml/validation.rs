// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! DAG cycle detection, name uniqueness, and security validation.

use std::collections::{HashMap, HashSet};

use muli_core::error::{MuliError, Result};

use super::schema::PipelineDef;

const MAX_MATRIX_SIZE: usize = 25;

pub fn validate_pipeline(def: &PipelineDef) -> Result<()> {
    validate_unique_names(def)?;
    validate_dag(def)?;
    validate_matrix_sizes(def)?;
    validate_artifact_names(def)?;
    Ok(())
}

fn validate_unique_names(def: &PipelineDef) -> Result<()> {
    let mut seen = HashSet::new();
    for step in &def.steps {
        if !seen.insert(&step.name) {
            return Err(MuliError::PipelineYamlError(format!(
                "duplicate step name: '{}'",
                step.name
            )));
        }
    }
    Ok(())
}

fn validate_dag(def: &PipelineDef) -> Result<()> {
    let names: HashSet<&str> = def.steps.iter().map(|s| s.name.as_str()).collect();
    for step in &def.steps {
        for dep in &step.needs {
            if !names.contains(dep.as_str()) {
                return Err(MuliError::PipelineYamlError(format!(
                    "step '{}' depends on unknown step '{dep}'",
                    step.name
                )));
            }
        }
    }

    let adj: HashMap<&str, Vec<&str>> = def
        .steps
        .iter()
        .map(|s| {
            (
                s.name.as_str(),
                s.needs.iter().map(|n| n.as_str()).collect(),
            )
        })
        .collect();

    let mut visited = HashSet::new();
    let mut in_stack = HashSet::new();
    for step in &def.steps {
        if !visited.contains(step.name.as_str())
            && has_cycle(&adj, step.name.as_str(), &mut visited, &mut in_stack)
        {
            return Err(MuliError::PipelineDagCycle(format!(
                "cycle detected involving step '{}'",
                step.name
            )));
        }
    }
    Ok(())
}

fn has_cycle<'a>(
    adj: &HashMap<&'a str, Vec<&'a str>>,
    node: &'a str,
    visited: &mut HashSet<&'a str>,
    in_stack: &mut HashSet<&'a str>,
) -> bool {
    visited.insert(node);
    in_stack.insert(node);
    if let Some(deps) = adj.get(node) {
        for &dep in deps {
            if !visited.contains(dep) {
                if has_cycle(adj, dep, visited, in_stack) {
                    return true;
                }
            } else if in_stack.contains(dep) {
                return true;
            }
        }
    }
    in_stack.remove(node);
    false
}

fn validate_matrix_sizes(def: &PipelineDef) -> Result<()> {
    for step in &def.steps {
        if let Some(matrix) = &step.matrix {
            let combinations: usize = matrix.values().map(|v| v.len().max(1)).product();
            if combinations > MAX_MATRIX_SIZE {
                return Err(MuliError::PipelineYamlError(format!(
                    "step '{}' matrix produces {combinations} combinations (max {MAX_MATRIX_SIZE})",
                    step.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_artifact_names(def: &PipelineDef) -> Result<()> {
    for step in &def.steps {
        if let Some(arts) = &step.artifacts {
            if let Some(upload) = &arts.upload {
                validate_path_component(&upload.name, "artifact name")?;
            }
        }
        if let Some(cache) = &step.cache {
            validate_path_component(&cache.key, "cache key")?;
        }
    }
    Ok(())
}

fn validate_path_component(s: &str, label: &str) -> Result<()> {
    if s.is_empty() || s.contains("..") || s.contains('/') || s.contains('\\') || s.contains('\0') {
        return Err(MuliError::PipelineYamlError(format!(
            "invalid {label}: '{s}' (path traversal not allowed)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yaml::parser::parse_pipeline;

    #[test]
    fn test_cycle_detection() {
        let yaml = r#"
name: test
steps:
  - name: a
    image: x
    needs: [b]
  - name: b
    image: x
    needs: [a]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_valid_dag() {
        let yaml = r#"
name: test
steps:
  - name: a
    image: x
  - name: b
    image: x
    needs: [a]
  - name: c
    image: x
    needs: [a, b]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_ok());
    }

    #[test]
    fn test_duplicate_names() {
        let yaml = r#"
name: test
steps:
  - name: a
    image: x
  - name: a
    image: y
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_self_dependency_rejected() {
        let yaml = r#"
name: test
steps:
  - name: a
    image: x
    needs: [a]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_unknown_dependency_rejected() {
        let yaml = r#"
name: test
steps:
  - name: a
    image: x
    needs: [nonexistent]
"#;
        let def = parse_pipeline(yaml).unwrap();
        let result = validate_pipeline(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown step"));
    }

    #[test]
    fn test_matrix_over_25_rejected() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    matrix:
      a: ["1","2","3","4","5","6"]
      b: ["1","2","3","4","5"]
"#;
        let def = parse_pipeline(yaml).unwrap();
        let result = validate_pipeline(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("30 combinations"));
    }

    #[test]
    fn test_path_traversal_in_artifact_name() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    artifacts:
      upload:
        name: "../../../etc/passwd"
        paths: [out]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_path_traversal_in_cache_key() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    cache:
      key: "../../secrets"
      paths: [cache]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_null_byte_in_artifact_name() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    artifacts:
      upload:
        name: "binary\u0000evil"
        paths: [out]
"#;
        // Note: YAML parser may or may not pass the null byte through.
        // If it does, validation should catch it.
        if let Ok(def) = parse_pipeline(yaml) {
            if def.steps[0]
                .artifacts
                .as_ref()
                .unwrap()
                .upload
                .as_ref()
                .unwrap()
                .name
                .contains('\0')
            {
                assert!(validate_pipeline(&def).is_err());
            }
        }
    }

    #[test]
    fn test_slash_in_cache_key() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    cache:
      key: "path/to/cache"
      paths: [cache]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_backslash_in_artifact_name() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    artifacts:
      upload:
        name: "path\\to\\file"
        paths: [out]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_three_node_cycle_detected() {
        let yaml = r#"
name: test
steps:
  - name: a
    image: x
    needs: [c]
  - name: b
    image: x
    needs: [a]
  - name: c
    image: x
    needs: [b]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_err());
    }

    #[test]
    fn test_valid_pipeline_with_artifacts_and_cache() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: x
    cache:
      key: cargo-lock
      paths: [target]
    artifacts:
      upload:
        name: binary
        paths: [target/release]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert!(validate_pipeline(&def).is_ok());
    }
}
