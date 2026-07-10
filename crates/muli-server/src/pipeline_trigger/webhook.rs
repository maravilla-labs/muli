// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline webhook delivery and payload construction.

use muli_core::git::WebhookEvent;
use muli_core::pipeline::{Pipeline, PipelineRun};
use muli_git::hooks::deliver_webhooks;

use super::PipelineTriggerImpl;

impl PipelineTriggerImpl {
    pub(crate) async fn deliver_pipeline_webhook(
        &self,
        tenant_id: &str,
        repo_id: &str,
        event: WebhookEvent,
        payload: serde_json::Value,
    ) {
        deliver_webhooks(
            self.webhook_store.clone(),
            self.webhook_http_client.clone(),
            tenant_id,
            repo_id,
            &muli_git::hooks::HookDelivery {
                repo_id: repo_id.to_string(),
                event,
                payload,
            },
            self.allow_localhost_webhooks,
        )
        .await;
    }

    /// Deliver the `pipeline.started` webhook for a newly-created run.
    pub(crate) async fn deliver_started(
        &self,
        tenant_id: &str,
        repo_id: &str,
        run: &PipelineRun,
        pipeline: &Pipeline,
    ) {
        self.deliver_pipeline_webhook(
            tenant_id,
            repo_id,
            WebhookEvent::PipelineStarted,
            serde_json::json!({
                "run_id": run.id,
                "pipeline_id": pipeline.id,
                "pipeline_name": pipeline.name,
                "run_number": run.run_number,
                "commit_sha": run.commit_sha,
                "ref_name": run.ref_name,
                "state": "pending",
                "webhook": run.webhook_data,
            }),
        )
        .await;
    }

    /// Deliver the `pipeline.completed` webhook. `state` is the lowercased
    /// execution state (or `"failed"`); `error` carries the failure message when
    /// execution errored. `release` is the `{ id, tag, asset_ids }` summary of a
    /// release recorded by a declarative `release:` block, when one ran.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn deliver_completed(
        &self,
        tenant_id: &str,
        repo_id: &str,
        run: &PipelineRun,
        pipeline: &Pipeline,
        state: &str,
        error: Option<&str>,
        artifacts_json: &serde_json::Value,
        release: Option<&serde_json::Value>,
    ) {
        let mut payload = serde_json::json!({
            "run_id": run.id,
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.name,
            "run_number": run.run_number,
            "commit_sha": run.commit_sha,
            "ref_name": run.ref_name,
            "state": state,
            "webhook": run.webhook_data,
            "artifacts": artifacts_json,
        });
        if let Some(obj) = payload.as_object_mut() {
            if let Some(err) = error {
                obj.insert("error".to_string(), serde_json::json!(err));
            }
            if let Some(rel) = release {
                obj.insert("release".to_string(), rel.clone());
            }
        }
        self.deliver_pipeline_webhook(
            tenant_id,
            repo_id,
            WebhookEvent::PipelineCompleted,
            payload,
        )
        .await;
    }
}
