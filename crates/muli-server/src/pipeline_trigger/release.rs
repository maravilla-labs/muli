// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Declarative `release:` executor.
//!
//! Runs in-process after a run succeeds (called from [`super::run`]), so it can
//! hold the release stores directly — no release credential ever enters a job
//! container. For each job carrying a `release:` block it resolves the tag,
//! optionally creates the git tag, builds notes, records the release (idempotent
//! on re-runs), and attaches each selected job's artifact archive as a single
//! release asset.

use std::collections::HashMap;

use tracing::{info, warn};

use muli_core::error::Result;
use muli_core::git::Repository;
use muli_core::pipeline::PipelineRun;
use muli_core::release::{NewRelease, Release as CoreRelease, ReleaseAsset as CoreAsset};
use muli_pipeline::artifact::archive::read_text_from_archive;
use muli_pipeline::trigger::matcher::matches_glob;
use muli_pipeline::yaml::schema::{JobDef, NotesDef, PipelineDef, ReleaseDef};

use super::PipelineTriggerImpl;
use crate::release_storage::ReleaseAssetStorage;

impl PipelineTriggerImpl {
    /// Execute every job's `release:` block after a successful run. Returns a
    /// JSON summary (`{ id, tag, asset_ids }`) per created-or-existing release,
    /// in stable job-name order, for the completion webhook.
    pub(crate) async fn run_releases(
        &self,
        tenant_id: &str,
        repo: &Repository,
        run: &PipelineRun,
        pipeline_def: &PipelineDef,
    ) -> Vec<serde_json::Value> {
        let mut jobs: Vec<(&String, &JobDef)> = pipeline_def.jobs.iter().collect();
        jobs.sort_by(|a, b| a.0.cmp(b.0));

        let mut summaries = Vec::new();
        for (job_name, job) in jobs {
            let Some(rel) = &job.release else { continue };
            match self
                .execute_release(tenant_id, repo, run, job_name, rel)
                .await
            {
                Ok(Some(summary)) => summaries.push(summary),
                Ok(None) => {}
                Err(e) => {
                    warn!(job = %job_name, error = %e, "release: failed to create release");
                }
            }
        }
        summaries
    }

    /// Create (or find) the release declared by a single job. `Ok(None)` means
    /// the block was intentionally skipped (e.g. no tag to release).
    async fn execute_release(
        &self,
        tenant_id: &str,
        repo: &Repository,
        run: &PipelineRun,
        job_name: &str,
        rel: &ReleaseDef,
    ) -> Result<Option<serde_json::Value>> {
        // (a) Resolve the tag. `$PIPELINE_TAG` in the tag field is the ref-derived
        // tag; an omitted tag falls back to the run's `refs/tags/*` ref.
        let ref_tag = run
            .ref_name
            .strip_prefix("refs/tags/")
            .map(str::to_string);
        let tag_field_vars = pipeline_vars(run, repo, ref_tag.as_deref().unwrap_or(""));
        let tag = rel
            .tag
            .as_ref()
            .map(|t| interpolate(t, &tag_field_vars))
            .filter(|t| !t.is_empty())
            .or(ref_tag);
        let Some(tag) = tag else {
            warn!(
                job = %job_name,
                "release: no tag to release (run is not a tag push and no release.tag set); skipping"
            );
            return Ok(None);
        };

        // (b) Idempotency: a re-run must not duplicate an existing release.
        if let Some(existing) = self.release_store.get_release_by_tag(&repo.id, &tag).await?
            && existing.tenant_id == tenant_id
        {
            info!(tag = %tag, release_id = %existing.id, "release: already exists; skipping (idempotent)");
            return Ok(Some(release_summary(&existing.id, &existing.tag, existing.assets.iter().map(|a| a.id.clone()).collect())));
        }

        // (c) Create the git tag if requested and absent (idempotent helper).
        if rel.create_tag.unwrap_or(false)
            && let Err(e) = self
                .git_storage
                .create_tag(tenant_id, &repo.namespace, &repo.name, &tag, &run.commit_sha)
                .await
        {
            warn!(tag = %tag, error = %e, "release: failed to create git tag (recording release anyway)");
        }

        // (d) Notes. Final interpolation context sees the resolved tag.
        let vars = pipeline_vars(run, repo, &tag);
        let notes = self
            .build_notes(tenant_id, repo, run, job_name, &tag, rel.notes.as_ref(), &vars)
            .await;
        let name = rel
            .name
            .as_ref()
            .map(|n| interpolate(n, &vars))
            .unwrap_or_default();

        // (e) Record the release.
        let release = CoreRelease::new(NewRelease {
            tenant_id: tenant_id.to_string(),
            repo_id: repo.id.clone(),
            tag: tag.clone(),
            target_commitish: run.commit_sha.clone(),
            name,
            body: notes,
            draft: rel.draft.unwrap_or(false),
            prerelease: rel.prerelease.unwrap_or(false),
            created_by: "pipeline".to_string(),
        })?;
        self.release_store.create_release(&release).await?;

        // (f) Attach selected job archives as single release assets.
        let asset_ids = self.attach_assets(tenant_id, run, &release, &rel.assets).await;

        info!(
            tag = %tag,
            release_id = %release.id,
            assets = asset_ids.len(),
            "release: created from pipeline run"
        );
        Ok(Some(release_summary(&release.id, &release.tag, asset_ids)))
    }

    /// Build the release body from the configured notes source. Never fabricates
    /// content: a missing changelog file or a failed git-log falls back to empty
    /// notes with a logged warning.
    async fn build_notes(
        &self,
        tenant_id: &str,
        repo: &Repository,
        run: &PipelineRun,
        job_name: &str,
        tag: &str,
        notes: Option<&NotesDef>,
        vars: &HashMap<String, String>,
    ) -> String {
        match notes {
            None => String::new(),
            Some(NotesDef::Inline { text }) => interpolate(text, vars),
            Some(NotesDef::Changelog { file }) => {
                match self.artifact_storage.download(tenant_id, &run.id, job_name).await {
                    Ok(archive) => read_text_from_archive(&archive, file).unwrap_or_else(|| {
                        warn!(
                            file = %file,
                            job = %job_name,
                            "release: changelog file not found in job artifact archive; empty notes"
                        );
                        String::new()
                    }),
                    Err(e) => {
                        warn!(
                            job = %job_name,
                            error = %e,
                            "release: no artifact archive for changelog job; empty notes"
                        );
                        String::new()
                    }
                }
            }
            Some(NotesDef::GitLog) => {
                match self
                    .git_storage
                    .log_since_previous_tag(
                        tenant_id,
                        &repo.namespace,
                        &repo.name,
                        tag,
                        &run.commit_sha,
                    )
                    .await
                {
                    Ok(body) => body,
                    Err(e) => {
                        warn!(error = %e, "release: git_log notes computation failed; empty notes");
                        String::new()
                    }
                }
            }
        }
    }

    /// For every job whose name matches an `assets:` glob, download its artifact
    /// archive and attach it as a single release asset. Returns the created asset
    /// ids. Per-file distribution is intentionally out of scope (the registry's
    /// job); a release attaches whole archives only.
    async fn attach_assets(
        &self,
        tenant_id: &str,
        run: &PipelineRun,
        release: &CoreRelease,
        globs: &[String],
    ) -> Vec<String> {
        if globs.is_empty() {
            return Vec::new();
        }
        let artifacts = match self.artifact_store.list_by_run(tenant_id, &run.id).await {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "release: failed to list run artifacts for assets");
                return Vec::new();
            }
        };

        let mut asset_ids = Vec::new();
        for artifact in artifacts {
            // Artifact `name` equals the producing job's name.
            if !globs.iter().any(|g| matches_glob(g, &artifact.name)) {
                continue;
            }
            let bytes = match self
                .artifact_storage
                .download(tenant_id, &run.id, &artifact.name)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(job = %artifact.name, error = %e, "release: failed to download artifact archive; skipping asset");
                    continue;
                }
            };

            let mut asset = CoreAsset::new(
                tenant_id.to_string(),
                release.id.clone(),
                format!("{}.tar", artifact.name),
                0,
                String::new(),
                "application/x-tar".to_string(),
                String::new(),
            );
            match self
                .release_asset_storage
                .upload(tenant_id, &release.id, &asset.id, &bytes)
                .await
            {
                Ok((size, sha256)) => {
                    asset.size = size;
                    asset.sha256 = sha256;
                }
                Err(e) => {
                    warn!(job = %artifact.name, error = %e, "release: failed to store release asset; skipping");
                    continue;
                }
            }
            asset.storage_key = ReleaseAssetStorage::key(tenant_id, &release.id, &asset.id);
            if let Err(e) = self.release_store.add_asset(&asset).await {
                warn!(job = %artifact.name, error = %e, "release: failed to record release asset; rolling back bytes");
                let _ = self
                    .release_asset_storage
                    .delete(tenant_id, &release.id, &asset.id)
                    .await;
                continue;
            }
            asset_ids.push(asset.id);
        }
        asset_ids
    }
}

/// The webhook summary object for one release.
fn release_summary(id: &str, tag: &str, asset_ids: Vec<String>) -> serde_json::Value {
    serde_json::json!({ "id": id, "tag": tag, "asset_ids": asset_ids })
}

/// `$PIPELINE_*` variables available to `release.tag`, `release.name`, and
/// inline notes.
fn pipeline_vars(run: &PipelineRun, repo: &Repository, tag: &str) -> HashMap<String, String> {
    let branch = run
        .ref_name
        .strip_prefix("refs/heads/")
        .unwrap_or("")
        .to_string();
    HashMap::from([
        ("PIPELINE_TAG".to_string(), tag.to_string()),
        ("PIPELINE_SHA".to_string(), run.commit_sha.clone()),
        ("PIPELINE_REF".to_string(), run.ref_name.clone()),
        ("PIPELINE_BRANCH".to_string(), branch),
        ("PIPELINE_RUN_ID".to_string(), run.id.clone()),
        ("PIPELINE_RUN_NUMBER".to_string(), run.run_number.to_string()),
        ("PIPELINE_COMMIT_MESSAGE".to_string(), run.commit_message.clone()),
        ("PIPELINE_COMMIT_AUTHOR".to_string(), run.commit_author.clone()),
        (
            "PIPELINE_REPO".to_string(),
            format!("{}/{}", repo.namespace, repo.name),
        ),
    ])
}

/// Substitute `$VAR` and `${VAR}` from `vars`. Unknown variables are left
/// verbatim so a stray `$` in prose survives untouched.
fn interpolate(template: &str, vars: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        match chars.peek() {
            Some('{') => {
                chars.next(); // consume '{'
                let mut name = String::new();
                let mut closed = false;
                while let Some(&nc) = chars.peek() {
                    chars.next();
                    if nc == '}' {
                        closed = true;
                        break;
                    }
                    name.push(nc);
                }
                if closed {
                    push_var(&mut out, &name, vars, &format!("${{{name}}}"));
                } else {
                    out.push_str("${");
                    out.push_str(&name);
                }
            }
            Some(&nc) if nc == '_' || nc.is_ascii_alphabetic() => {
                let mut name = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc == '_' || nc.is_ascii_alphanumeric() {
                        name.push(nc);
                        chars.next();
                    } else {
                        break;
                    }
                }
                push_var(&mut out, &name, vars, &format!("${name}"));
            }
            _ => out.push('$'),
        }
    }
    out
}

fn push_var(out: &mut String, name: &str, vars: &HashMap<String, String>, literal: &str) {
    match vars.get(name) {
        Some(v) => out.push_str(v),
        None => out.push_str(literal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> HashMap<String, String> {
        HashMap::from([
            ("PIPELINE_TAG".to_string(), "v1.2.3".to_string()),
            ("PIPELINE_SHA".to_string(), "abc123".to_string()),
        ])
    }

    #[test]
    fn interpolates_bare_and_braced() {
        assert_eq!(interpolate("Release $PIPELINE_TAG", &vars()), "Release v1.2.3");
        assert_eq!(interpolate("[${PIPELINE_TAG}]", &vars()), "[v1.2.3]");
        assert_eq!(
            interpolate("$PIPELINE_TAG@$PIPELINE_SHA", &vars()),
            "v1.2.3@abc123"
        );
    }

    #[test]
    fn leaves_unknown_and_lone_dollar() {
        assert_eq!(interpolate("$NOPE and ${MISSING}", &vars()), "$NOPE and ${MISSING}");
        assert_eq!(interpolate("cost is $5", &vars()), "cost is $5");
        assert_eq!(interpolate("trailing $", &vars()), "trailing $");
    }

    #[test]
    fn preserves_utf8_prose() {
        assert_eq!(
            interpolate("café — $PIPELINE_TAG ✨", &vars()),
            "café — v1.2.3 ✨"
        );
    }
}
