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

    if !def.jobs.is_empty() {
        // Jobs-based format validation
        if def.jobs.len() > MAX_STEPS {
            return Err(MuliError::PipelineYamlError(format!(
                "pipeline has {} jobs (max {MAX_STEPS})",
                def.jobs.len()
            )));
        }
        let job_names: std::collections::HashSet<&str> =
            def.jobs.keys().map(|s| s.as_str()).collect();
        for (job_name, job_def) in &def.jobs {
            // Validate effective image exists
            if job_def.effective_image(def.image.as_deref()).is_none() {
                return Err(MuliError::PipelineYamlError(format!(
                    "job '{job_name}' must specify an image (or set a pipeline-level image)"
                )));
            }
            // Validate needs references
            for dep in &job_def.needs {
                if !job_names.contains(dep.as_str()) {
                    return Err(MuliError::PipelineYamlError(format!(
                        "job '{job_name}' needs unknown job '{dep}'"
                    )));
                }
            }
        }
    } else {
        // Legacy steps-based format validation
        if def.steps.is_empty() {
            return Err(MuliError::PipelineYamlError(
                "pipeline must have at least one step or job".into(),
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
                return Err(MuliError::PipelineYamlError("step name is required".into()));
            }
            if step.image.is_empty() {
                return Err(MuliError::PipelineYamlError(format!(
                    "step '{}' must specify an image",
                    step.name
                )));
            }
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

    #[test]
    fn test_parse_jobs_format() {
        let yaml = r#"
name: fullstack-ci
image: node:22-alpine
on:
  push:
    branches: [main]
jobs:
  install:
    commands: [npm ci]
    artifacts:
      paths: [node_modules/]
  lint:
    needs: install
    commands:
      - npx eslint src/
    failure_strategy: continue
  build:
    needs: [lint, install]
    commands: [npm run build]
    artifacts:
      paths: [dist/]
  deploy:
    image: alpine/kubectl
    needs: build
    commands: [kubectl apply -f k8s/]
"#;
        let def = parse_pipeline(yaml).unwrap();
        assert_eq!(def.name, "fullstack-ci");
        assert_eq!(def.image, Some("node:22-alpine".to_string()));
        assert_eq!(def.jobs.len(), 4);
        let install = def.jobs.get("install").unwrap();
        assert_eq!(install.needs, Vec::<String>::new());
        assert_eq!(
            install.artifact_upload_paths(),
            vec!["node_modules/".to_string()]
        );
        let lint = def.jobs.get("lint").unwrap();
        assert_eq!(lint.needs, vec!["install"]);
        let build = def.jobs.get("build").unwrap();
        assert_eq!(build.needs, vec!["lint", "install"]);
        let deploy = def.jobs.get("deploy").unwrap();
        assert_eq!(deploy.image, Some("alpine/kubectl".to_string()));
        assert_eq!(deploy.needs, vec!["build"]);
    }

    #[test]
    fn test_jobs_missing_image_rejected() {
        let yaml = "name: test\njobs:\n  build:\n    commands: [echo hi]\n";
        assert!(parse_pipeline(yaml).is_err());
    }

    #[test]
    fn test_jobs_pipeline_level_image_ok() {
        let yaml =
            "name: test\nimage: node:18\njobs:\n  build:\n    commands: [npm run build]\n";
        assert!(parse_pipeline(yaml).is_ok());
    }

    #[test]
    fn test_jobs_unknown_needs_rejected() {
        let yaml = r#"
name: test
image: node:18
jobs:
  build:
    needs: nonexistent
    commands: [echo hi]
"#;
        assert!(parse_pipeline(yaml).is_err());
    }
}
