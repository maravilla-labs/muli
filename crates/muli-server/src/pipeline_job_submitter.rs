// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! JobSubmitter implementation that creates Jobs in the store and enqueues them in the scheduler.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use muli_core::error::Result;
use muli_core::job::model::Job;
use muli_core::traits::JobStore;
use muli_pipeline::dag::executor::JobSubmitter;
use muli_queue::Scheduler;

/// Submits pipeline step Jobs to the existing job engine.
pub struct SchedulerJobSubmitter {
    pub job_store: Arc<dyn JobStore>,
    pub scheduler: Arc<Scheduler>,
}

#[async_trait]
impl JobSubmitter for SchedulerJobSubmitter {
    async fn submit(&self, job: Job) -> Result<String> {
        let job_id = job.id.clone();
        let tenant_id = job.spec.tenant_id.clone();
        let tier = job.spec.priority_tier;

        self.job_store.create_job(&job).await?;
        self.scheduler.enqueue(job_id.clone(), tier, tenant_id);

        info!(job_id = %job_id, "pipeline step job submitted to scheduler");
        Ok(job_id)
    }
}
