// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server-side git hook execution and webhook delivery.

use std::sync::Arc;

use muli_core::git::{WebhookEvent, validate_webhook_target};
use muli_core::traits::WebhookStore;

use crate::api::webhooks::sign_payload;

/// A webhook delivery payload for a repository event.
pub struct HookDelivery {
    pub repo_id: String,
    pub event: WebhookEvent,
    pub payload: serde_json::Value,
}

/// Deliver a webhook event to all active, matching webhooks for a repository.
///
/// Failures are logged but do not propagate — delivery is best-effort.
/// The `http_client` should be a long-lived shared instance to allow connection reuse.
pub async fn deliver_webhooks(
    webhook_store: Arc<dyn WebhookStore>,
    http_client: Arc<reqwest::Client>,
    tenant_id: &str,
    repo_id: &str,
    delivery: &HookDelivery,
    allow_localhost_webhooks: bool,
) {
    let hooks = match webhook_store.list_webhooks(tenant_id, repo_id).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, repo_id, "failed to list webhooks for delivery");
            return;
        }
    };

    let client = http_client.as_ref();

    for hook in hooks {
        if !hook.active {
            continue;
        }
        if !hook.events.contains(&delivery.event) {
            continue;
        }

        if !allow_localhost_webhooks && let Err(e) = validate_webhook_target(&hook.url).await {
            tracing::warn!(
                hook_id = %hook.id,
                url = %hook.url,
                error = %e,
                "webhook target rejected by SSRF safeguards"
            );
            continue;
        }

        let payload_bytes = match serde_json::to_vec(&delivery.payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, hook_id = %hook.id, "failed to serialize webhook payload");
                continue;
            }
        };

        let signature = sign_payload(&hook.secret, &payload_bytes);
        let event_name = event_name(&delivery.event);

        match client
            .post(&hook.url)
            .header("Content-Type", "application/json")
            .header("X-Muli-Event", event_name)
            .header("X-Hub-Signature-256", &signature)
            .body(payload_bytes)
            .send()
            .await
        {
            Ok(resp) => {
                tracing::debug!(
                    hook_id = %hook.id,
                    url = %hook.url,
                    status = %resp.status(),
                    "webhook delivered"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    hook_id = %hook.id,
                    url = %hook.url,
                    "webhook delivery failed"
                );
            }
        }
    }
}

fn event_name(event: &WebhookEvent) -> &'static str {
    match event {
        WebhookEvent::Push => "push",
        WebhookEvent::Create => "create",
        WebhookEvent::Delete => "delete",
        WebhookEvent::PrOpened => "pr.opened",
        WebhookEvent::PrMerged => "pr.merged",
        WebhookEvent::PrClosed => "pr.closed",
    }
}
