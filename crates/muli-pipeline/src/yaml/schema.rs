// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Serde types for the pipeline YAML DSL.

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    pub name: String,
    /// Pipeline-level default image (inherited by all jobs when not overridden).
    pub image: Option<String>,
    /// Optional checkout configuration.
    #[serde(default)]
    pub checkout: CheckoutConfig,
    #[serde(default)]
    pub on: TriggerDef,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub services: HashMap<String, ServiceDef>,
    #[serde(default)]
    pub secrets: Vec<String>,
    /// New jobs-based format: map of job_name → JobDef.
    #[serde(default)]
    pub jobs: HashMap<String, JobDef>,
    /// Legacy steps-based format (backward compat).
    #[serde(default)]
    pub steps: Vec<StepDef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckoutConfig {
    #[serde(default)]
    pub submodules: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TriggerDef {
    pub push: Option<PushTrigger>,
    pub pull_request: Option<PrTrigger>,
    #[serde(default)]
    pub manual: bool,
    #[serde(default)]
    pub schedule: Vec<ScheduleTrigger>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushTrigger {
    #[serde(default)]
    pub branches: Vec<String>,
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrTrigger {
    #[serde(default)]
    pub branches: Vec<String>,
    #[serde(default)]
    pub events: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleTrigger {
    pub cron: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDef {
    pub image: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// A job in the new jobs-based pipeline format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDef {
    /// Image to use; inherits from pipeline-level image if absent.
    pub image: Option<String>,
    /// Jobs this job depends on (runs after they succeed).
    #[serde(default, deserialize_with = "deserialize_needs")]
    pub needs: Vec<String>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub steps: Vec<JobStepDef>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub cache: Option<CacheDef>,
    pub artifacts: Option<ArtifactsDef>,
    pub resources: Option<ResourceDef>,
    pub matrix: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "if")]
    pub condition: Option<String>,
    pub failure_strategy: Option<String>,
    pub timeout: Option<u64>,
}

impl JobDef {
    /// Return the effective image: job-level or pipeline-level default.
    pub fn effective_image<'a>(&'a self, pipeline_image: Option<&'a str>) -> Option<&'a str> {
        self.image.as_deref().or(pipeline_image)
    }

    /// Return the paths to upload as artifacts (from artifacts.paths or artifacts.upload.paths).
    pub fn artifact_upload_paths(&self) -> Vec<String> {
        match &self.artifacts {
            None => vec![],
            Some(a) => {
                if !a.paths.is_empty() {
                    a.paths.clone()
                } else if let Some(upload) = &a.upload {
                    upload.paths.clone()
                } else {
                    vec![]
                }
            }
        }
    }

    /// Return the full command list for this job: pre-commands followed by
    /// named step commands with log-visible headers.
    pub fn execution_commands(&self) -> Vec<String> {
        let mut commands = self.commands.clone();
        for step in &self.steps {
            commands.push(format!(
                "printf '%s\\n' {}",
                shell_single_quote(&format!("==> {}", step.name))
            ));
            commands.extend(step.commands.clone());
        }
        commands
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStepDef {
    pub name: String,
    #[serde(default)]
    pub commands: Vec<String>,
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// `needs:` accepts either a single string or an array of strings.
fn deserialize_needs<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NeedsHelper {
        Single(String),
        Multiple(Vec<String>),
    }

    let helper = Option::<NeedsHelper>::deserialize(d)?;
    Ok(match helper {
        None => Vec::new(),
        Some(NeedsHelper::Single(s)) => vec![s],
        Some(NeedsHelper::Multiple(v)) => v,
    })
}

/// Legacy step definition (steps-based format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub cache: Option<CacheDef>,
    pub artifacts: Option<ArtifactsDef>,
    pub resources: Option<ResourceDef>,
    pub matrix: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "if")]
    pub condition: Option<String>,
    #[serde(default)]
    pub failure_strategy: Option<String>,
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheDef {
    pub key: String,
    #[serde(default)]
    pub restore_keys: Vec<String>,
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactsDef {
    /// Legacy: artifacts.upload.{name, paths}
    pub upload: Option<ArtifactUploadDef>,
    /// New shorthand: artifacts.paths (for jobs mode).
    #[serde(default)]
    pub paths: Vec<String>,
    /// Explicit download list (backward compat).
    #[serde(default)]
    pub download: Vec<String>,
    /// Expiry duration string, e.g. "1 week", "1 day".
    pub expire_in: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactUploadDef {
    pub name: Option<String>,
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDef {
    pub cpu: Option<String>,
    pub memory: Option<String>,
}
