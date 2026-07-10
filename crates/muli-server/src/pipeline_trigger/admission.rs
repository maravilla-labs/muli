// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Admission control for pipeline triggers: per-repo rate limiting and tenant
//! suspension / daily-run / concurrent-pipeline enforcement.

use std::time::Instant;

use tracing::warn;

use super::PipelineTriggerImpl;

/// Minimum interval between pipeline triggers for the same repo (rate limit).
pub(crate) const MIN_TRIGGER_INTERVAL_SECS: u64 = 5;

impl PipelineTriggerImpl {
    /// Run admission checks (rate limit + tenant enforcement). Returns `true` when
    /// the trigger is allowed to proceed, `false` when it should be dropped.
    pub(crate) async fn admission_allows(&self, tenant_id: &str, repo_id: &str) -> bool {
        // 0. Rate limit: skip if triggered too recently for this repo
        let repo_key = format!("{tenant_id}/{repo_id}");
        if let Some(last) = self.last_trigger.get(&repo_key) {
            if last.elapsed().as_secs() < MIN_TRIGGER_INTERVAL_SECS {
                warn!(
                    repo_id = %repo_id,
                    "pipeline trigger rate-limited (< {}s since last trigger)",
                    MIN_TRIGGER_INTERVAL_SECS,
                );
                return false;
            }
        }
        self.last_trigger.insert(repo_key, Instant::now());

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
