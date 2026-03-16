// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! YAML pipeline definition parser.

use muli_core::error::{MuliError, Result};

use super::schema::PipelineDef;

const MAX_STEPS: usize = 100;

pub fn parse_pipeline(yaml_str: &str) -> Result<PipelineDef> {
    let def: PipelineDef = serde_yaml::from_str(yaml_str)
        .map_err(|e| MuliError::PipelineYamlError(format!("YAML parse error: {e}")))?;

    if def.name.is_empty() {
        return Err(MuliError::PipelineYamlError(
            "pipeline name is required".into(),
        ));
    }
    if def.steps.is_empty() {
        return Err(MuliError::PipelineYamlError(
            "pipeline must have at least one step".into(),
        ));
    }
    if def.steps.len() > MAX_STEPS {
        return Err(MuliError::PipelineYamlError(format!(
            "pipeline has {} steps (max {MAX_STEPS})",
            def.steps.len()
        )));
    }
    for step in &def.steps {
        if step.name.is_empty() {
            return Err(MuliError::PipelineYamlError(
                "step name is required".into(),
            ));
        }
        if step.image.is_empty() {
            return Err(MuliError::PipelineYamlError(format!(
                "step '{}' must specify an image",
                step.name
            )));
        }
    }
    Ok(def)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        let yaml = r#"
name: test
steps:
  - name: build
    image: rust:1.82
    commands:
      - cargo build
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert_eq!(def.name, "test");
        assert_eq!(def.steps.len(), 1);
        assert_eq!(def.steps[0].name, "build");
    }

    #[test]
    fn test_parse_full() {
        let yaml = r#"
name: build-and-deploy
on:
  push:
    branches: [main]
  manual: true
env:
  CARGO_TERM_COLOR: always
steps:
  - name: test
    image: rust:1.82
    commands:
      - cargo test
    cache:
      key: "cargo-lock"
      paths: [target]
  - name: build
    image: rust:1.82
    needs: [test]
    commands:
      - cargo build --release
    artifacts:
      upload:
        name: binary
        paths: [target/release/app]
  - name: deploy
    image: alpine:latest
    needs: [build]
    commands:
      - ./deploy.sh
    artifacts:
      download: [binary]
    if: "branch == 'main'"
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert_eq!(def.steps.len(), 3);
        assert_eq!(def.steps[1].needs, vec!["test"]);
    }

    #[test]
    fn test_empty_name_rejected() {
        let yaml = "name: \"\"\nsteps:\n  - name: x\n    image: y\n";
        assert!(parse_pipeline(yaml).is_err());
    }

    #[test]
    fn test_no_steps_rejected() {
        let yaml = "name: test\nsteps: []\n";
        assert!(parse_pipeline(yaml).is_err());
    }

    #[test]
    fn test_max_steps_exceeded() {
        let mut steps = String::new();
        for i in 0..101 {
            steps.push_str(&format!("  - name: step{i}\n    image: rust:1.82\n"));
        }
        let yaml = format!("name: test\nsteps:\n{steps}");
        let result = parse_pipeline(&yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("101 steps"));
    }

    #[test]
    fn test_yaml_bomb_rejected() {
        // 200 steps > MAX_STEPS (100)
        let mut steps = String::new();
        for i in 0..200 {
            steps.push_str(&format!("  - name: s{i}\n    image: x\n"));
        }
        let yaml = format!("name: test\nsteps:\n{steps}");
        assert!(parse_pipeline(&yaml).is_err());
    }

    #[test]
    fn test_step_with_all_fields() {
        let yaml = r#"
name: full
on:
  push:
    branches: [main]
env:
  CI: "true"
services:
  postgres:
    image: postgres:16
    env:
      POSTGRES_PASSWORD: test
secrets: [DB_URL, API_KEY]
steps:
  - name: test
    image: rust:1.82
    commands:
      - cargo test
    needs: []
    env:
      RUST_LOG: debug
    cache:
      key: cargo-lock
      restore_keys: [cargo-]
      paths: [target]
    artifacts:
      upload:
        name: results
        paths: [test-results]
      download: [previous]
    matrix:
      version: ["1.80", "1.82"]
    if: "branch == 'main'"
    timeout: 3600
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert_eq!(def.name, "full");
        assert_eq!(def.services.len(), 1);
        assert_eq!(def.secrets, vec!["DB_URL", "API_KEY"]);
        let step = &def.steps[0];
        assert_eq!(step.timeout, Some(3600));
        assert!(step.cache.is_some());
        assert!(step.artifacts.is_some());
        assert!(step.matrix.is_some());
        assert_eq!(step.condition, Some("branch == 'main'".into()));
    }

    #[test]
    fn test_step_missing_image_rejected() {
        let yaml = "name: test\nsteps:\n  - name: build\n    image: \"\"\n";
        assert!(parse_pipeline(yaml).is_err());
    }

    #[test]
    fn test_step_missing_name_rejected() {
        let yaml = "name: test\nsteps:\n  - name: \"\"\n    image: rust:1.82\n";
        assert!(parse_pipeline(yaml).is_err());
    }
}
