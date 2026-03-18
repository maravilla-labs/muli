// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Matrix expansion for pipeline steps.

use muli_core::error::{MuliError, Result};

use crate::yaml::schema::{JobDef, StepDef};

const MAX_MATRIX_COMBINATIONS: usize = 25;

/// Expand a step with a matrix into multiple step copies.
pub fn expand_matrix(step: &StepDef) -> Result<Vec<StepDef>> {
    let matrix = match &step.matrix {
        Some(m) if !m.is_empty() => m,
        _ => return Ok(vec![step.clone()]),
    };

    let keys: Vec<&String> = matrix.keys().collect();
    let values: Vec<&Vec<String>> = keys.iter().map(|k| &matrix[*k]).collect();

    let mut combinations = vec![vec![]];
    for vals in &values {
        let mut new_combos = Vec::new();
        for combo in &combinations {
            for v in *vals {
                let mut c = combo.clone();
                c.push(v.clone());
                new_combos.push(c);
            }
        }
        combinations = new_combos;
    }

    if combinations.len() > MAX_MATRIX_COMBINATIONS {
        return Err(MuliError::PipelineYamlError(format!(
            "step '{}' matrix produces {} combinations (max {MAX_MATRIX_COMBINATIONS})",
            step.name,
            combinations.len()
        )));
    }

    let mut expanded = Vec::new();
    for combo in &combinations {
        let mut expanded_step = step.clone();
        let suffix: Vec<String> = keys
            .iter()
            .zip(combo.iter())
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        expanded_step.name = format!("{} ({})", step.name, suffix.join(", "));
        for (key, val) in keys.iter().zip(combo.iter()) {
            expanded_step
                .env
                .insert(key.to_uppercase().to_string(), val.clone());
        }
        expanded_step.matrix = None;
        expanded.push(expanded_step);
    }

    Ok(expanded)
}

/// Expand a job with a matrix into `(step_name, matrix_values_json)` pairs.
/// Returns a single `(job_name, None)` pair if the job has no matrix.
pub fn expand_job_matrix(
    job_name: &str,
    job_def: &JobDef,
) -> Result<Vec<(String, Option<serde_json::Value>)>> {
    let matrix = match &job_def.matrix {
        Some(m) if !m.is_empty() => m,
        _ => return Ok(vec![(job_name.to_string(), None)]),
    };

    let keys: Vec<&String> = matrix.keys().collect();
    let values: Vec<&Vec<String>> = keys.iter().map(|k| &matrix[*k]).collect();

    let mut combinations = vec![vec![]];
    for vals in &values {
        let mut new_combos = Vec::new();
        for combo in &combinations {
            for v in *vals {
                let mut c = combo.clone();
                c.push(v.clone());
                new_combos.push(c);
            }
        }
        combinations = new_combos;
    }

    if combinations.len() > MAX_MATRIX_COMBINATIONS {
        return Err(MuliError::PipelineYamlError(format!(
            "job '{}' matrix produces {} combinations (max {MAX_MATRIX_COMBINATIONS})",
            job_name,
            combinations.len()
        )));
    }

    let mut expanded = Vec::new();
    for combo in &combinations {
        let suffix: Vec<String> = keys
            .iter()
            .zip(combo.iter())
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        let name = format!("{} ({})", job_name, suffix.join(", "));
        let mut mv = serde_json::Map::new();
        for (key, val) in keys.iter().zip(combo.iter()) {
            mv.insert((*key).clone(), serde_json::Value::String(val.clone()));
        }
        expanded.push((name, Some(serde_json::Value::Object(mv))));
    }

    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_step(name: &str, matrix: HashMap<String, Vec<String>>) -> StepDef {
        StepDef {
            name: name.into(),
            image: "rust:1.82".into(),
            commands: vec![],
            needs: vec![],
            env: HashMap::new(),
            cache: None,
            artifacts: None,
            resources: None,
            matrix: Some(matrix),
            condition: None,
            failure_strategy: None,
            timeout: None,
        }
    }

    #[test]
    fn test_expand_2x2() {
        let mut matrix = HashMap::new();
        matrix.insert("version".into(), vec!["1.80".into(), "1.82".into()]);
        let step = make_step("build", matrix);
        let expanded = expand_matrix(&step).unwrap();
        assert_eq!(expanded.len(), 2);
        assert!(expanded[0].name.contains("version=1.80"));
        assert!(expanded[1].name.contains("version=1.82"));
    }

    #[test]
    fn test_no_matrix() {
        let step = StepDef {
            name: "test".into(),
            image: "rust:1.82".into(),
            commands: vec![],
            needs: vec![],
            env: HashMap::new(),
            cache: None,
            artifacts: None,
            resources: None,
            matrix: None,
            condition: None,
            failure_strategy: None,
            timeout: None,
        };
        let expanded = expand_matrix(&step).unwrap();
        assert_eq!(expanded.len(), 1);
    }

    #[test]
    fn test_matrix_3x3() {
        let mut matrix = HashMap::new();
        matrix.insert(
            "os".into(),
            vec!["linux".into(), "macos".into(), "windows".into()],
        );
        matrix.insert(
            "version".into(),
            vec!["1.80".into(), "1.81".into(), "1.82".into()],
        );
        let step = make_step("build", matrix);
        let expanded = expand_matrix(&step).unwrap();
        assert_eq!(expanded.len(), 9);
        // Each expanded step should have a unique name
        let names: std::collections::HashSet<&str> =
            expanded.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names.len(), 9);
    }

    #[test]
    fn test_matrix_over_limit() {
        // 6 x 5 = 30 > 25
        let mut matrix = HashMap::new();
        matrix.insert(
            "a".into(),
            vec![
                "1".into(),
                "2".into(),
                "3".into(),
                "4".into(),
                "5".into(),
                "6".into(),
            ],
        );
        matrix.insert(
            "b".into(),
            vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()],
        );
        let step = make_step("build", matrix);
        let result = expand_matrix(&step);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("30 combinations"));
    }

    #[test]
    fn test_matrix_env_injection() {
        let mut matrix = HashMap::new();
        matrix.insert("rust_version".into(), vec!["1.80".into(), "1.82".into()]);
        let step = make_step("build", matrix);
        let expanded = expand_matrix(&step).unwrap();
        assert_eq!(expanded.len(), 2);
        // Matrix values should be injected as uppercase env vars
        assert_eq!(expanded[0].env.get("RUST_VERSION").unwrap(), "1.80");
        assert_eq!(expanded[1].env.get("RUST_VERSION").unwrap(), "1.82");
        // Matrix should be cleared after expansion
        assert!(expanded[0].matrix.is_none());
        assert!(expanded[1].matrix.is_none());
    }

    #[test]
    fn test_matrix_empty_map() {
        let step = make_step("build", HashMap::new());
        let expanded = expand_matrix(&step).unwrap();
        // Empty matrix should return single step unchanged
        assert_eq!(expanded.len(), 1);
    }

    #[test]
    fn test_matrix_single_dimension() {
        let mut matrix = HashMap::new();
        matrix.insert("target".into(), vec!["x86_64".into(), "aarch64".into()]);
        let step = make_step("build", matrix);
        let expanded = expand_matrix(&step).unwrap();
        assert_eq!(expanded.len(), 2);
        assert!(expanded[0].name.contains("target=x86_64"));
        assert!(expanded[1].name.contains("target=aarch64"));
    }

    #[test]
    fn test_matrix_exactly_25() {
        // 5 x 5 = 25, exactly at the limit, should succeed
        let mut matrix = HashMap::new();
        matrix.insert(
            "a".into(),
            vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()],
        );
        matrix.insert(
            "b".into(),
            vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()],
        );
        let step = make_step("build", matrix);
        let expanded = expand_matrix(&step).unwrap();
        assert_eq!(expanded.len(), 25);
    }
}
