// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server-side git hook execution and webhook delivery.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use muli_core::git::{WebhookEvent, validate_webhook_target};
use muli_core::traits::{TenantQuotaStore, TreeCommitCacheStore, WebhookStore};

use crate::api::webhooks::sign_payload;

/// Hook for triggering pipeline runs from git events (push, PR).
#[async_trait::async_trait]
pub trait PipelineTriggerHook: Send + Sync {
    async fn on_push(&self, tenant_id: &str, repo_id: &str, commit_sha: &str, ref_name: &str);
    async fn on_pr_event(&self, tenant_id: &str, repo_id: &str, pr_number: u64, event: &str);
}

/// A single ref update from a push operation.
#[derive(Clone, Debug)]
pub struct RefUpdate {
    pub old_sha: String,
    pub new_sha: String,
    pub ref_name: String,
}

/// Shared infrastructure for firing post-push hooks (pipeline triggers,
/// webhooks, cache invalidation, quota tracking). Cloneable — shared across transports.
#[derive(Clone)]
pub struct PostPushHooks {
    pub pipeline_trigger: Option<Arc<dyn PipelineTriggerHook>>,
    pub webhook_store: Arc<dyn WebhookStore>,
    pub http_client: Arc<reqwest::Client>,
    pub webhook_semaphore: Arc<tokio::sync::Semaphore>,
    pub allow_localhost_webhooks: bool,
    pub cache_store: Option<Arc<dyn TreeCommitCacheStore>>,
    pub quota_store: Option<Arc<dyn TenantQuotaStore>>,
}

impl PostPushHooks {
    /// Fire all post-push hooks in background tasks. Non-blocking.
    pub fn fire(
        &self,
        tenant_id: String,
        repo_id: String,
        repo_name: String,
        ref_updates: Vec<RefUpdate>,
        repo_size_before: Option<u64>,
        repo_path: PathBuf,
    ) {
        let zero_sha = "0".repeat(40);

        // 1. Pipeline triggers (spawn per non-deletion ref)
        if let Some(trigger) = self.pipeline_trigger.as_ref() {
            for update in &ref_updates {
                if update.new_sha != zero_sha {
                    let trigger = trigger.clone();
                    let tid = tenant_id.clone();
                    let rid = repo_id.clone();
                    let sha = update.new_sha.clone();
                    let rn = update.ref_name.clone();
                    tokio::spawn(async move {
                        trigger.on_push(&tid, &rid, &sha, &rn).await;
                    });
                }
            }
        }

        // 2. Webhook delivery (single spawned task, semaphore-gated)
        let webhook_store = self.webhook_store.clone();
        let http_client = self.http_client.clone();
        let semaphore = self.webhook_semaphore.clone();
        let allow_localhost = self.allow_localhost_webhooks;
        let tid = tenant_id.clone();
        let rid = repo_id.clone();
        let rname = repo_name;
        tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            if ref_updates.is_empty() {
                deliver_webhooks(
                    webhook_store,
                    http_client,
                    &tid,
                    &rid,
                    &HookDelivery {
                        repo_id: rid.clone(),
                        event: WebhookEvent::Push,
                        payload: serde_json::json!({
                            "repository": rname,
                        }),
                    },
                    allow_localhost,
                )
                .await;
            } else {
                for update in &ref_updates {
                    if update.new_sha == zero_sha {
                        continue;
                    }
                    deliver_webhooks(
                        webhook_store.clone(),
                        http_client.clone(),
                        &tid,
                        &rid,
                        &HookDelivery {
                            repo_id: rid.clone(),
                            event: WebhookEvent::Push,
                            payload: serde_json::json!({
                                "repository": rname,
                                "ref": update.ref_name,
                                "before": update.old_sha,
                                "after": update.new_sha,
                            }),
                        },
                        allow_localhost,
                    )
                    .await;
                }
            }
        });

        // 3. Cache invalidation (spawn if cache_store is Some)
        if let Some(cache) = self.cache_store.clone() {
            let tid = tenant_id.clone();
            let rid = repo_id.clone();
            tokio::spawn(async move {
                let _ = cache.invalidate_repo(&tid, &rid).await;
            });
        }

        // 4. Quota tracking — compute repo size delta and adjust usage
        if let (Some(store), Some(size_before)) = (self.quota_store.clone(), repo_size_before) {
            let tid = tenant_id;
            tokio::spawn(async move {
                let size_after = compute_dir_size(&repo_path).await.unwrap_or(size_before);
                let delta = size_after as i64 - size_before as i64;
                if delta != 0 {
                    if let Err(e) = store.adjust_usage(&tid, delta).await {
                        // Retry once after a short delay
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        if let Err(e2) = store.adjust_usage(&tid, delta).await {
                            tracing::warn!(
                                tenant_id = %tid, delta,
                                error = %e, retry_error = %e2,
                                "failed to adjust git quota usage after push (retried)"
                            );
                        }
                    }
                }
            });
        }
    }

    /// Fire a single webhook event (for PR events etc). Non-blocking.
    pub fn fire_webhook(
        &self,
        tenant_id: String,
        repo_id: String,
        event: WebhookEvent,
        payload: serde_json::Value,
    ) {
        let webhook_store = self.webhook_store.clone();
        let http_client = self.http_client.clone();
        let semaphore = self.webhook_semaphore.clone();
        let allow_localhost = self.allow_localhost_webhooks;
        tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            deliver_webhooks(
                webhook_store,
                http_client,
                &tenant_id,
                &repo_id,
                &HookDelivery {
                    repo_id: repo_id.clone(),
                    event,
                    payload,
                },
                allow_localhost,
            )
            .await;
        });
    }
}

/// Build a shared HTTP client configured for webhook delivery.
pub fn webhook_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build webhook HTTP client")
}

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

/// Recursively sum file sizes in a directory.
pub async fn compute_dir_size(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let ft = entry.file_type().await?;
            if ft.is_file() {
                total += entry.metadata().await?.len();
            } else if ft.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}

fn event_name(event: &WebhookEvent) -> &'static str {
    match event {
        WebhookEvent::Push => "push",
        WebhookEvent::Create => "create",
        WebhookEvent::Delete => "delete",
        WebhookEvent::PrOpened => "pr.opened",
        WebhookEvent::PrMerged => "pr.merged",
        WebhookEvent::PrClosed => "pr.closed",
        WebhookEvent::PipelineCompleted => "pipeline.completed",
    }
}
