// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pre-push snapshot and ref-diffing logic for SSH post-push hooks.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::hooks::{PostPushHooks, RefUpdate};

/// Snapshot captured before git-receive-pack runs so that post-push hooks
/// can compute ref diffs and fire pipeline/webhook/cache/quota events.
pub(super) struct SshPrePushSnapshot {
    pub hooks: PostPushHooks,
    pub tenant_id: String,
    pub repo_id: String,
    pub repo_name: String,
    pub old_refs: HashMap<String, String>,
    pub repo_path: PathBuf,
    /// Repo directory size before push, for quota delta calculation.
    pub repo_size_before: Option<u64>,
}

/// Read all refs from a bare git repository.
pub(super) async fn read_refs(repo_path: &std::path::Path) -> HashMap<String, String> {
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(repo_path)
        .args(["for-each-ref", "--format=%(objectname) %(refname)"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .await;

    let mut refs = HashMap::new();
    if let Ok(output) = output {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                if let Some((sha, refname)) = line.split_once(' ') {
                    refs.insert(refname.to_string(), sha.to_string());
                }
            }
        }
    }
    refs
}

/// Compute ref updates by diffing old and new ref snapshots.
pub(super) fn compute_ref_updates(
    old_refs: &HashMap<String, String>,
    new_refs: &HashMap<String, String>,
) -> Vec<RefUpdate> {
    let zero_sha = "0".repeat(40);
    let mut updates = Vec::new();
    for (ref_name, new_sha) in new_refs {
        let old_sha = old_refs
            .get(ref_name)
            .cloned()
            .unwrap_or_else(|| zero_sha.clone());
        if old_sha != *new_sha {
            updates.push(RefUpdate {
                old_sha,
                new_sha: new_sha.clone(),
                ref_name: ref_name.clone(),
            });
        }
    }
    updates
}
