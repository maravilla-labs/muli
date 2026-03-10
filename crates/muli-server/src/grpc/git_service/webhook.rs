// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Webhook management RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::git::{WebhookEvent, validate_webhook_target};
use muli_proto::{
    CreateWebhookRequest, DeleteWebhookRequest, DeleteWebhookResponse, GitWebhook,
    ListWebhooksRequest, ListWebhooksResponse,
};

use super::GitServiceImpl;
use super::helpers::webhook_to_proto;
use crate::grpc::conversions::proto_webhook_event_to_core;
use crate::grpc::util::extract_tenant_id;

impl GitServiceImpl {
    pub async fn create_webhook_impl(
        &self,
        request: Request<CreateWebhookRequest>,
    ) -> Result<Response<GitWebhook>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.url.is_empty() {
            return Err(Status::invalid_argument("url is required"));
        }
        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if !self.allow_localhost_webhooks {
            validate_webhook_target(&req.url)
                .await
                .map_err(|e| Status::invalid_argument(format!("invalid webhook URL: {e}")))?;
        }

        let repo = self
            .repo_store
            .get_repository(&req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up repository: {e}")))?
            .ok_or_else(|| Status::not_found(format!("repository {} not found", req.repo_id)))?;

        if repo.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "repository does not belong to tenant",
            ));
        }

        let events: Vec<WebhookEvent> = req
            .events
            .iter()
            .map(|&e| proto_webhook_event_to_core(e))
            .collect::<Result<Vec<_>, _>>()?;

        let webhook = muli_core::git::Webhook::new(
            req.tenant_id.clone(),
            req.repo_id.clone(),
            req.url.clone(),
            req.secret.clone(),
            events,
        );

        self.webhook_store
            .create_webhook(&webhook)
            .await
            .map_err(|e| Status::internal(format!("Failed to create webhook: {e}")))?;

        info!(
            operation = "create_webhook",
            tenant_id = %req.tenant_id,
            repo_id = %req.repo_id,
            webhook_id = %webhook.id,
            "audit: webhook created"
        );

        Ok(Response::new(webhook_to_proto(&webhook)))
    }

    pub async fn list_webhooks_impl(
        &self,
        request: Request<ListWebhooksRequest>,
    ) -> Result<Response<ListWebhooksResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let webhooks = self
            .webhook_store
            .list_webhooks(&req.tenant_id, &req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list webhooks: {e}")))?;

        Ok(Response::new(ListWebhooksResponse {
            webhooks: webhooks.iter().map(webhook_to_proto).collect(),
        }))
    }

    pub async fn delete_webhook_impl(
        &self,
        request: Request<DeleteWebhookRequest>,
    ) -> Result<Response<DeleteWebhookResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.webhook_id.is_empty() {
            return Err(Status::invalid_argument("webhook_id is required"));
        }

        let webhook = self
            .webhook_store
            .get_webhook(&req.webhook_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up webhook: {e}")))?
            .ok_or_else(|| Status::not_found(format!("webhook {} not found", req.webhook_id)))?;

        if webhook.tenant_id != caller_tenant {
            return Err(Status::permission_denied(format!(
                "webhook {} does not belong to tenant {}",
                req.webhook_id, caller_tenant
            )));
        }

        self.webhook_store
            .delete_webhook(&req.webhook_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete webhook: {e}")))?;

        info!(
            operation = "delete_webhook",
            tenant_id = %caller_tenant,
            webhook_id = %req.webhook_id,
            "audit: webhook deleted"
        );

        Ok(Response::new(DeleteWebhookResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use muli_core::traits::RepositoryStore;
    use muli_store::sqlite::{SqliteCollaboratorStore, SqliteStoreFactory};
    use tempfile::TempDir;
    use tonic::Request;

    use muli_core::git::Repository;
    use muli_proto::CreateWebhookRequest;
    use muli_store::memory::{MemoryRepositoryStore, MemoryWebhookStore};

    use super::GitServiceImpl;

    #[tokio::test]
    async fn create_webhook_rejects_private_url_when_localhost_disabled() {
        let repo_store = Arc::new(MemoryRepositoryStore::new());
        let webhook_store = Arc::new(MemoryWebhookStore::new());
        let tmp = TempDir::new().expect("temp dir");
        let collaborator_store = Arc::new(SqliteCollaboratorStore::new(
            SqliteStoreFactory::new(tmp.path())
                .await
                .expect("sqlite factory"),
        ));

        let git_storage = Arc::new(
            muli_git::storage::FilesystemStorage::new(tmp.path().to_str().unwrap())
                .await
                .expect("git storage"),
        );

        let repo = Repository::new(
            "tenant-1".into(),
            "acme".into(),
            "repo".into(),
            "desc".into(),
            false,
        )
        .expect("repo");
        repo_store
            .create_repository(&repo)
            .await
            .expect("create repo");

        let service = GitServiceImpl {
            repo_store,
            token_store: Arc::new(muli_store::memory::MemoryGitTokenStore::new()),
            ssh_key_store: Arc::new(muli_store::memory::MemorySshKeyStore::new()),
            webhook_store,
            collaborator_store,
            git_storage,
            allow_localhost_webhooks: false,
        };

        let mut req = Request::new(CreateWebhookRequest {
            tenant_id: "tenant-1".into(),
            repo_id: repo.id.clone(),
            url: "http://127.0.0.1/hook".into(),
            secret: "s".into(),
            events: vec![],
        });
        req.metadata_mut().insert(
            "x-tenant-id",
            tonic::metadata::MetadataValue::from_static("tenant-1"),
        );

        let result = service.create_webhook_impl(req).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected invalid_argument").code(),
            tonic::Code::InvalidArgument
        );
    }
}
