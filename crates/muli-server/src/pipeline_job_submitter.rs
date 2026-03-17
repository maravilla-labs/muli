// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! JobSubmitter implementation that creates Jobs in the store and enqueues them in the scheduler.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use muli_core::error::Result;
use muli_core::job::model::Job;
use muli_core::traits::{JobStore, TenantLimitsStore};
use muli_pipeline::dag::executor::JobSubmitter;
use muli_queue::Scheduler;

/// Submits pipeline step Jobs to the existing job engine.
pub struct SchedulerJobSubmitter {
    pub job_store: Arc<dyn JobStore>,
    pub scheduler: Arc<Scheduler>,
    pub tenant_limits_store: Option<Arc<dyn TenantLimitsStore>>,
}

#[async_trait]
impl JobSubmitter for SchedulerJobSubmitter {
    async fn submit(&self, mut job: Job) -> Result<String> {
        let job_id = job.id.clone();
        let tenant_id = job.spec.tenant_id.clone();

        // Look up tenant's priority tier from limits, override job default
        if let Some(ref limits_store) = self.tenant_limits_store {
            if let Ok(Some(limits)) = limits_store.get_limits(&tenant_id).await {
                job.spec.priority_tier = limits.priority_tier;
            }
        }

        let tier = job.spec.priority_tier;

        self.job_store.create_job(&job).await?;
        self.scheduler.enqueue(job_id.clone(), tier, tenant_id);

        info!(job_id = %job_id, "pipeline step job submitted to scheduler");
        Ok(job_id)
    }
}
