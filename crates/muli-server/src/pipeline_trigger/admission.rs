// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Admission control for pipeline triggers: per-ref rate limiting and tenant
//! suspension / daily-run / concurrent-pipeline enforcement.

use std::time::Instant;

use tracing::warn;

use super::PipelineTriggerImpl;

/// Minimum interval between pipeline triggers for the same ref (rate limit).
pub(crate) const MIN_TRIGGER_INTERVAL_SECS: u64 = 5;

impl PipelineTriggerImpl {
    /// Rate-limit key. Scoped to the **ref**, not the repo: a single
    /// `git push --follow-tags` fires one concurrent `on_push` per ref, so a
    /// repo-wide key made the branch push and the tag push cancel each other out
    /// (whichever lost the race was silently dropped).
    fn rate_limit_key(tenant_id: &str, repo_id: &str, ref_name: &str) -> String {
        format!("{tenant_id}/{repo_id}/{ref_name}")
    }

    /// Record that this ref produced a run, starting its rate-limit window.
    ///
    /// Called only once a pipeline actually matched the event — a push to a ref
    /// no pipeline cares about must not consume that ref's budget.
    pub(crate) fn note_trigger(&self, tenant_id: &str, repo_id: &str, ref_name: &str) {
        self.last_trigger.insert(
            Self::rate_limit_key(tenant_id, repo_id, ref_name),
            Instant::now(),
        );
    }

    /// Run admission checks (rate limit + tenant enforcement). Returns `true` when
    /// the trigger is allowed to proceed, `false` when it should be dropped.
    pub(crate) async fn admission_allows(
        &self,
        tenant_id: &str,
        repo_id: &str,
        ref_name: &str,
    ) -> bool {
        // 0. Rate limit: skip if this ref was triggered too recently.
        let key = Self::rate_limit_key(tenant_id, repo_id, ref_name);
        if let Some(last) = self.last_trigger.get(&key)
            && last.elapsed().as_secs() < MIN_TRIGGER_INTERVAL_SECS
        {
            warn!(
                repo_id = %repo_id,
                ref_name = %ref_name,
                "pipeline trigger rate-limited (< {}s since last trigger for this ref)",
                MIN_TRIGGER_INTERVAL_SECS,
            );
            return false;
        }

        // 0b. Tenant enforcement checks
        if let Some(ref limits_store) = self.tenant_limits_store {
            if limits_store.is_suspended(tenant_id).await.unwrap_or(false) {
                warn!(tenant_id = %tenant_id, "pipeline trigger: tenant is suspended");
                return false;
            }
            if let Ok(Some(limits)) = limits_store.get_limits(tenant_id).await {
                // Check daily run limit
                if limits.max_pipeline_runs_per_day > 0 {
                    if let Ok(count) = limits_store.get_daily_run_count(tenant_id).await {
                        if count >= limits.max_pipeline_runs_per_day as u64 {
                            warn!(
                                tenant_id = %tenant_id,
                                count = count,
                                limit = limits.max_pipeline_runs_per_day,
                                "pipeline trigger: daily run limit exceeded"
                            );
                            return false;
                        }
                    }
                }
                // Check concurrent pipeline limit
                if limits.max_concurrent_pipelines > 0 {
                    if let Ok(active) = self.run_store.count_active(tenant_id).await {
                        if active >= limits.max_concurrent_pipelines as u64 {
                            warn!(
                                tenant_id = %tenant_id,
                                active = active,
                                limit = limits.max_concurrent_pipelines,
                                "pipeline trigger: concurrent pipeline limit exceeded"
                            );
                            return false;
                        }
                    }
                }
            }
        }

        true
    }
}
