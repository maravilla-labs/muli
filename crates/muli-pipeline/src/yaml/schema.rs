// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Serde types for the pipeline YAML DSL.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    pub name: String,
    #[serde(default)]
    pub on: TriggerDef,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub services: HashMap<String, ServiceDef>,
    #[serde(default)]
    pub secrets: Vec<String>,
    pub steps: Vec<StepDef>,
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
    pub upload: Option<ArtifactUploadDef>,
    #[serde(default)]
    pub download: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactUploadDef {
    pub name: String,
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDef {
    pub cpu: Option<String>,
    pub memory: Option<String>,
}
