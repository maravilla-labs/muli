// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Serde types for the pipeline YAML DSL.

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

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
    /// Arbitrary key-value data included in all webhook payloads for this pipeline.
    /// Muli does not interpret the values — consumers (e.g. Flightdeck) read them.
    #[serde(default)]
    pub webhook: HashMap<String, Value>,
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
    /// Tag name globs (GitHub-style `on.push.tags`). When non-empty, the trigger
    /// fires on a tag push whose name matches one of these globs.
    #[serde(default)]
    pub tags: Vec<String>,
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
    #[serde(default)]
    pub paths: Vec<String>,
    pub cache: Option<CacheDef>,
    pub artifacts: Option<ArtifactsDef>,
    pub resources: Option<ResourceDef>,
    pub matrix: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "if")]
    pub condition: Option<String>,
    pub failure_strategy: Option<String>,
    pub timeout: Option<u64>,
    /// Declarative release: when the run succeeds, record a repository release
    /// (tag + notes + an archive asset) server-side. No release credential is
    /// injected into the job container — the release is created by the engine.
    pub release: Option<ReleaseDef>,
    /// Opt-in ambient registry credentials. `registry: write` makes the engine
    /// mint a short-lived, Push-scoped token for the run and inject it into this
    /// job as `MULI_REGISTRY_TOKEN`, so it can publish to the handle's embedded
    /// registry with no manual token setup. Omitted → no registry credential.
    #[serde(default)]
    pub registry: Option<RegistryAccess>,
}

/// Level of registry access a job opts into. A `write` job receives an ambient
/// `MULI_REGISTRY_TOKEN` (Push-scoped); `read` is reserved for a future
/// pull-only credential and grants no publish token today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryAccess {
    Read,
    Write,
}

/// Declarative `release:` block on a job. Executed in-process after the run
/// succeeds; see `pipeline_trigger/release.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseDef {
    /// Release tag. Interpolated (`$PIPELINE_*`). Defaults to the run's
    /// `refs/tags/*` ref when the run was a tag push.
    pub tag: Option<String>,
    /// Display name. Interpolated; defaults to the tag when absent.
    pub name: Option<String>,
    /// Where the release notes come from.
    pub notes: Option<NotesDef>,
    /// Create the release as an unpublished draft.
    pub draft: Option<bool>,
    /// Mark as a prerelease (e.g. `v1.0.0-rc1`).
    pub prerelease: Option<bool>,
    /// Create the git tag at the run's commit if the run wasn't a tag push.
    pub create_tag: Option<bool>,
    /// Globs matched against job names; each matching job's artifact archive is
    /// attached as a single release asset. Per-file distribution is the
    /// registry's job, not a release concern.
    #[serde(default)]
    pub assets: Vec<String>,
}

/// Source of a release's notes. Internally tagged by `from:`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum NotesDef {
    /// Read a changelog file the job produced (from its artifact archive).
    Changelog { file: String },
    /// Server-computed `git log` since the previous tag.
    GitLog,
    /// Literal text, with `$PIPELINE_*` interpolation.
    Inline { text: String },
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

    /// Return the structured execution substeps for this job.
    pub fn execution_substeps(&self) -> Vec<JobStepDef> {
        if self.steps.is_empty() {
            return vec![JobStepDef {
                name: "Commands".to_string(),
                commands: self.commands.clone(),
            }];
        }

        let mut substeps = Vec::new();
        if !self.commands.is_empty() {
            substeps.push(JobStepDef {
                name: "Preparation".to_string(),
                commands: self.commands.clone(),
            });
        }
        substeps.extend(self.steps.clone());
        substeps
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The `from:` internal tag must round-trip for every notes variant.
    #[test]
    fn notes_changelog_roundtrip() {
        let notes = NotesDef::Changelog {
            file: "CHANGELOG.md".to_string(),
        };
        let json = serde_json::to_value(&notes).unwrap();
        assert_eq!(json, serde_json::json!({ "from": "changelog", "file": "CHANGELOG.md" }));
        let back: NotesDef = serde_json::from_value(json).unwrap();
        assert!(matches!(back, NotesDef::Changelog { file } if file == "CHANGELOG.md"));
    }

    #[test]
    fn notes_git_log_roundtrip() {
        let notes = NotesDef::GitLog;
        let json = serde_json::to_value(&notes).unwrap();
        assert_eq!(json, serde_json::json!({ "from": "git_log" }));
        let back: NotesDef = serde_json::from_value(json).unwrap();
        assert!(matches!(back, NotesDef::GitLog));
    }

    #[test]
    fn notes_inline_roundtrip() {
        let notes = NotesDef::Inline {
            text: "Release $PIPELINE_TAG".to_string(),
        };
        let json = serde_json::to_value(&notes).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "from": "inline", "text": "Release $PIPELINE_TAG" })
        );
        let back: NotesDef = serde_json::from_value(json).unwrap();
        assert!(matches!(back, NotesDef::Inline { text } if text == "Release $PIPELINE_TAG"));
    }

    /// A job carrying a `release:` block parses from real pipeline YAML.
    #[test]
    fn job_release_parses_from_yaml() {
        let yaml = r#"
name: release-pipeline
on:
  push:
    tags: ["v*"]
jobs:
  publish:
    image: alpine
    commands: ["true"]
    release:
      tag: "$PIPELINE_TAG"
      name: "Release $PIPELINE_TAG"
      draft: false
      prerelease: false
      create_tag: true
      assets: ["build"]
      notes:
        from: changelog
        file: CHANGELOG.md
"#;
        let def: PipelineDef = serde_yaml::from_str(yaml).unwrap();
        let job = def.jobs.get("publish").expect("publish job");
        let rel = job.release.as_ref().expect("release def");
        assert_eq!(rel.tag.as_deref(), Some("$PIPELINE_TAG"));
        assert_eq!(rel.create_tag, Some(true));
        assert_eq!(rel.assets, vec!["build".to_string()]);
        assert!(matches!(&rel.notes, Some(NotesDef::Changelog { file }) if file == "CHANGELOG.md"));
    }

    /// A job without a `release:` block leaves the field `None`.
    #[test]
    fn job_without_release_is_none() {
        let yaml = r#"
name: p
jobs:
  build:
    image: alpine
    commands: ["make"]
"#;
        let def: PipelineDef = serde_yaml::from_str(yaml).unwrap();
        assert!(def.jobs.get("build").unwrap().release.is_none());
    }

    /// `RegistryAccess` round-trips through serde with snake_case values.
    #[test]
    fn registry_access_roundtrip() {
        assert_eq!(
            serde_json::to_value(RegistryAccess::Write).unwrap(),
            serde_json::json!("write")
        );
        assert_eq!(
            serde_json::to_value(RegistryAccess::Read).unwrap(),
            serde_json::json!("read")
        );
        let back: RegistryAccess = serde_json::from_value(serde_json::json!("read")).unwrap();
        assert_eq!(back, RegistryAccess::Read);
        let back: RegistryAccess = serde_json::from_value(serde_json::json!("write")).unwrap();
        assert_eq!(back, RegistryAccess::Write);
    }

    /// A job opts into ambient publish credentials with `registry: write`;
    /// a job that omits `registry:` leaves the field `None`.
    #[test]
    fn job_registry_write_parses_from_yaml() {
        let yaml = r#"
name: p
jobs:
  publish:
    image: node
    commands: ["npm publish"]
    registry: write
  build:
    image: node
    commands: ["npm run build"]
"#;
        let def: PipelineDef = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            def.jobs.get("publish").unwrap().registry,
            Some(RegistryAccess::Write)
        );
        assert_eq!(def.jobs.get("build").unwrap().registry, None);
    }
}
