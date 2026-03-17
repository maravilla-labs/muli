// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end pipeline test: parse YAML -> validate -> expand matrix -> trigger matching -> conditions

use muli_pipeline::dag::matrix::expand_matrix;
use muli_pipeline::trigger::matcher::{PipelineEvent, matches_trigger};
use muli_pipeline::yaml::expression::{ExpressionContext, evaluate_condition};
use muli_pipeline::yaml::parser::parse_pipeline;
use muli_pipeline::yaml::validation::validate_pipeline;

#[test]
fn test_full_pipeline_flow() {
    let yaml = r#"
name: ci
on:
  push:
    branches: [main, develop]
env:
  CI: "true"
steps:
  - name: test
    image: rust:1.82
    commands:
      - cargo test
    cache:
      key: cargo-lock
      paths: [target]
  - name: lint
    image: rust:1.82
    commands:
      - cargo clippy
  - name: build
    needs: [test, lint]
    image: rust:1.82
    commands:
      - cargo build --release
    artifacts:
      upload:
        name: binary
        paths: [target/release/app]
    matrix:
      rust_version: ["1.80", "1.82"]
  - name: deploy
    needs: [build]
    image: alpine:latest
    commands:
      - ./deploy.sh
    if: "branch == 'main' && event == 'push'"
"#;

    // 1. Parse
    let def = parse_pipeline(yaml).unwrap();
    assert_eq!(def.name, "ci");
    assert_eq!(def.steps.len(), 4);

    // 2. Validate
    validate_pipeline(&def).unwrap();

    // 3. Check triggers match
    let event = PipelineEvent::Push {
        branch: "main".into(),
        changed_paths: vec![],
    };
    assert!(matches_trigger(&def.on, &event));

    let event_feature = PipelineEvent::Push {
        branch: "feature".into(),
        changed_paths: vec![],
    };
    assert!(!matches_trigger(&def.on, &event_feature));

    // 4. Expand matrix
    let build_step = &def.steps[2];
    let expanded = expand_matrix(build_step).unwrap();
    assert_eq!(expanded.len(), 2);

    // 5. Verify total steps after expansion = 5 (test + lint + 2x build + deploy)
    let mut total_steps = 0;
    for step in &def.steps {
        total_steps += expand_matrix(step).unwrap().len();
    }
    assert_eq!(total_steps, 5);

    // 6. Verify conditions
    let ctx_main = ExpressionContext {
        branch: "main".into(),
        event: "push".into(),
        tag: None,
    };
    assert!(evaluate_condition(
        "branch == 'main' && event == 'push'",
        &ctx_main
    ));

    let ctx_dev = ExpressionContext {
        branch: "develop".into(),
        event: "push".into(),
        tag: None,
    };
    assert!(!evaluate_condition(
        "branch == 'main' && event == 'push'",
        &ctx_dev
    ));
}

#[test]
fn test_pipeline_with_services_and_secrets() {
    let yaml = r#"
name: integration
on:
  push:
    branches: [main]
services:
  postgres:
    image: postgres:16
    env:
      POSTGRES_PASSWORD: test
secrets: [DB_URL, API_KEY]
steps:
  - name: integration-test
    image: rust:1.82
    commands:
      - cargo test --features integration
    env:
      DATABASE_URL: postgres://postgres:test@localhost:5432/test
    timeout: 1800
"#;
    let def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&def).unwrap();
    assert_eq!(def.services.len(), 1);
    assert!(def.services.contains_key("postgres"));
    assert_eq!(def.secrets, vec!["DB_URL", "API_KEY"]);
    assert_eq!(def.steps[0].timeout, Some(1800));
}

#[test]
fn test_security_path_traversal_variants() {
    // Artifact name with path traversal
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

    // Cache key with path traversal
    let yaml2 = r#"
name: test
steps:
  - name: build
    image: x
    cache:
      key: "../../secrets"
      paths: [cache]
"#;
    let def2 = parse_pipeline(yaml2).unwrap();
    assert!(validate_pipeline(&def2).is_err());
}

#[test]
fn test_security_cycle_detection_complex() {
    // A -> B -> C -> A (3-node cycle)
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
fn test_security_matrix_bomb() {
    // 6 x 5 = 30 > 25 limit
    let yaml = r#"
name: test
steps:
  - name: build
    image: x
    matrix:
      a: ["1","2","3","4","5"]
      b: ["1","2","3","4","5","6"]
"#;
    let def = parse_pipeline(yaml).unwrap();
    assert!(validate_pipeline(&def).is_err());
}

#[test]
fn test_complex_dag_with_diamond() {
    // Diamond: A -> B, A -> C, B -> D, C -> D
    let yaml = r#"
name: diamond
steps:
  - name: a
    image: x
  - name: b
    image: x
    needs: [a]
  - name: c
    image: x
    needs: [a]
  - name: d
    image: x
    needs: [b, c]
"#;
    let def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&def).unwrap();

    // Verify DAG levels
    use muli_pipeline::dag::DagGraph;
    let steps: Vec<(String, Vec<String>)> = def
        .steps
        .iter()
        .map(|s| (s.name.clone(), s.needs.clone()))
        .collect();
    let dag = DagGraph::from_steps(&steps);
    let levels = dag.topological_levels();
    assert_eq!(levels.len(), 3); // [a], [b, c], [d]
    assert_eq!(levels[0], vec!["a"]);
    assert!(levels[1].contains(&"b"));
    assert!(levels[1].contains(&"c"));
    assert_eq!(levels[2], vec!["d"]);
}

#[test]
fn test_pipeline_pr_and_manual_triggers() {
    let yaml = r#"
name: pr-ci
on:
  pull_request:
    branches: [main]
    events: [opened, synchronize]
  manual: true
steps:
  - name: test
    image: rust:1.82
    commands:
      - cargo test
"#;
    let def = parse_pipeline(yaml).unwrap();
    validate_pipeline(&def).unwrap();

    // PR trigger
    assert!(matches_trigger(
        &def.on,
        &PipelineEvent::PullRequest {
            target_branch: "main".into(),
            event: "opened".into()
        }
    ));
    assert!(!matches_trigger(
        &def.on,
        &PipelineEvent::PullRequest {
            target_branch: "main".into(),
            event: "closed".into()
        }
    ));

    // Manual trigger
    assert!(matches_trigger(&def.on, &PipelineEvent::Manual));

    // Push should NOT match
    assert!(!matches_trigger(
        &def.on,
        &PipelineEvent::Push {
            branch: "main".into(),
            changed_paths: vec![]
        }
    ));
}

#[test]
fn test_condition_evaluation_in_context() {
    // Simulates deploy step condition: only on main + push
    let ctx_main_push = ExpressionContext {
        branch: "main".into(),
        event: "push".into(),
        tag: None,
    };
    let ctx_main_pr = ExpressionContext {
        branch: "main".into(),
        event: "pull_request".into(),
        tag: None,
    };
    let ctx_dev_push = ExpressionContext {
        branch: "develop".into(),
        event: "push".into(),
        tag: None,
    };

    let condition = "branch == 'main' && event == 'push'";
    assert!(evaluate_condition(condition, &ctx_main_push));
    assert!(!evaluate_condition(condition, &ctx_main_pr));
    assert!(!evaluate_condition(condition, &ctx_dev_push));

    // Tag-based condition
    let ctx_tag = ExpressionContext {
        branch: "main".into(),
        event: "push".into(),
        tag: Some("v1.0.0".into()),
    };
    assert!(evaluate_condition("tag == 'v1.0.0'", &ctx_tag));
    assert!(!evaluate_condition("tag == 'v2.0.0'", &ctx_tag));
}
