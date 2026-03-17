// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git hosting gRPC service.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use muli_core::traits::{
    CollaboratorStore, GitTokenStore, RepositoryStore, SshKeyStore, TenantLimitsStore, WebhookStore,
};
use muli_proto::git_service_server::GitService;
use muli_proto::{
    AddCollaboratorRequest, AddSshKeyRequest, CollaboratorResponse, CreateGitTokenRequest,
    CreateGitTokenResponse, CreateImpersonationTokenRequest, CreateRepositoryRequest,
    CreateWebhookRequest, DeleteRepositoryRequest, DeleteRepositoryResponse, DeleteWebhookRequest,
    DeleteWebhookResponse, ForkRepositoryRequest, GetRepositoryRequest, GitRepository, GitWebhook,
    ListCollaboratorsRequest, ListCollaboratorsResponse, ListGitTokensByUserRequest,
    ListGitTokensRequest, ListGitTokensResponse, ListRepositoriesRequest, ListRepositoriesResponse,
    ListSshKeysByUserRequest, ListSshKeysRequest, ListSshKeysResponse, ListWebhooksRequest,
    ListWebhooksResponse, RemoveCollaboratorRequest, RemoveSshKeyRequest, RemoveSshKeyResponse,
    RevokeGitTokenRequest, RevokeGitTokenResponse, SshKey as ProtoSshKey,
    TransferRepositoryRequest, UpdateVisibilityRequest,
};

mod collaborator;
mod helpers;
mod repo;
mod ssh;
mod token;
mod webhook;

pub struct GitServiceImpl {
    pub repo_store: Arc<dyn RepositoryStore>,
    pub token_store: Arc<dyn GitTokenStore>,
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    pub webhook_store: Arc<dyn WebhookStore>,
    pub collaborator_store: Arc<dyn CollaboratorStore>,
    pub git_storage: Arc<muli_git::storage::FilesystemStorage>,
    pub allow_localhost_webhooks: bool,
    pub repo_service: Arc<muli_core::service::RepositoryService>,
    pub tenant_limits_store: Option<Arc<dyn TenantLimitsStore>>,
}

#[tonic::async_trait]
impl GitService for GitServiceImpl {
    async fn create_repository(
        &self,
        request: Request<CreateRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        self.create_repository_impl(request).await
    }

    async fn get_repository(
        &self,
        request: Request<GetRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        self.get_repository_impl(request).await
    }

    async fn delete_repository(
        &self,
        request: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        self.delete_repository_impl(request).await
    }

    async fn list_repositories(
        &self,
        request: Request<ListRepositoriesRequest>,
    ) -> Result<Response<ListRepositoriesResponse>, Status> {
        self.list_repositories_impl(request).await
    }

    async fn fork_repository(
        &self,
        request: Request<ForkRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        self.fork_repository_impl(request).await
    }

    async fn transfer_repository(
        &self,
        request: Request<TransferRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        self.transfer_repository_impl(request).await
    }

    async fn create_access_token(
        &self,
        request: Request<CreateGitTokenRequest>,
    ) -> Result<Response<CreateGitTokenResponse>, Status> {
        self.create_access_token_impl(request).await
    }

    async fn list_access_tokens(
        &self,
        request: Request<ListGitTokensRequest>,
    ) -> Result<Response<ListGitTokensResponse>, Status> {
        self.list_access_tokens_impl(request).await
    }

    async fn revoke_access_token(
        &self,
        request: Request<RevokeGitTokenRequest>,
    ) -> Result<Response<RevokeGitTokenResponse>, Status> {
        self.revoke_access_token_impl(request).await
    }

    async fn add_ssh_key(
        &self,
        request: Request<AddSshKeyRequest>,
    ) -> Result<Response<ProtoSshKey>, Status> {
        self.add_ssh_key_impl(request).await
    }

    async fn remove_ssh_key(
        &self,
        request: Request<RemoveSshKeyRequest>,
    ) -> Result<Response<RemoveSshKeyResponse>, Status> {
        self.remove_ssh_key_impl(request).await
    }

    async fn list_ssh_keys(
        &self,
        request: Request<ListSshKeysRequest>,
    ) -> Result<Response<ListSshKeysResponse>, Status> {
        self.list_ssh_keys_impl(request).await
    }

    async fn list_access_tokens_by_user(
        &self,
        request: Request<ListGitTokensByUserRequest>,
    ) -> Result<Response<ListGitTokensResponse>, Status> {
        self.list_access_tokens_by_user_impl(request).await
    }

    async fn list_ssh_keys_by_user(
        &self,
        request: Request<ListSshKeysByUserRequest>,
    ) -> Result<Response<ListSshKeysResponse>, Status> {
        self.list_ssh_keys_by_user_impl(request).await
    }

    async fn create_webhook(
        &self,
        request: Request<CreateWebhookRequest>,
    ) -> Result<Response<GitWebhook>, Status> {
        self.create_webhook_impl(request).await
    }

    async fn list_webhooks(
        &self,
        request: Request<ListWebhooksRequest>,
    ) -> Result<Response<ListWebhooksResponse>, Status> {
        self.list_webhooks_impl(request).await
    }

    async fn delete_webhook(
        &self,
        request: Request<DeleteWebhookRequest>,
    ) -> Result<Response<DeleteWebhookResponse>, Status> {
        self.delete_webhook_impl(request).await
    }

    async fn add_repository_collaborator(
        &self,
        request: Request<AddCollaboratorRequest>,
    ) -> Result<Response<CollaboratorResponse>, Status> {
        self.add_repository_collaborator_impl(request).await
    }

    async fn remove_repository_collaborator(
        &self,
        request: Request<RemoveCollaboratorRequest>,
    ) -> Result<Response<CollaboratorResponse>, Status> {
        self.remove_repository_collaborator_impl(request).await
    }

    async fn list_repository_collaborators(
        &self,
        request: Request<ListCollaboratorsRequest>,
    ) -> Result<Response<ListCollaboratorsResponse>, Status> {
        self.list_repository_collaborators_impl(request).await
    }

    async fn update_repository_visibility(
        &self,
        request: Request<UpdateVisibilityRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        self.update_repository_visibility_impl(request).await
    }

    async fn create_impersonation_token(
        &self,
        request: Request<CreateImpersonationTokenRequest>,
    ) -> Result<Response<CreateGitTokenResponse>, Status> {
        self.create_impersonation_token_impl(request).await
    }
}
