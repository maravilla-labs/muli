// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git token management RPCs.

use chrono::{Duration, Utc};
use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::git::{GitPermission, GitToken};
use muli_proto::{
    CreateGitTokenRequest, CreateGitTokenResponse, CreateImpersonationTokenRequest, GitTokenInfo,
    ListGitTokensByUserRequest, ListGitTokensRequest, ListGitTokensResponse, RevokeGitTokenRequest,
    RevokeGitTokenResponse,
};

use super::GitServiceImpl;
use crate::grpc::conversions::{core_git_permission_to_proto, proto_git_permission_to_core};
use crate::grpc::util::{
    datetime_to_proto, extract_tenant_id, generate_token, hash_token, token_prefix,
};

impl GitServiceImpl {
    pub async fn create_access_token_impl(
        &self,
        request: Request<CreateGitTokenRequest>,
    ) -> Result<Response<CreateGitTokenResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        if req.permissions.is_empty() {
            return Err(Status::invalid_argument(
                "at least one permission is required",
            ));
        }

        let core_permissions: Vec<GitPermission> = req
            .permissions
            .iter()
            .map(|&p| proto_git_permission_to_core(p))
            .collect::<Result<Vec<_>, _>>()?;

        let plaintext = generate_token();
        let token_hash = hash_token(&plaintext).await;
        let prefix = token_prefix(&plaintext);

        let expires_at = req
            .ttl_seconds
            .filter(|&ttl| ttl > 0)
            .map(|ttl| Utc::now() + Duration::seconds(ttl));

        let mut token = GitToken::new(
            req.tenant_id.clone(),
            token_hash,
            prefix,
            core_permissions,
            req.description.clone(),
            expires_at,
        );
        token.user_id = req.user_id.clone();

        let token_id = self
            .token_store
            .create_token(&token)
            .await
            .map_err(|e| Status::internal(format!("Failed to create git token: {e}")))?;

        info!(
            operation = "create_access_token",
            tenant_id = %req.tenant_id,
            token_id = %token_id,
            "audit: git access token created"
        );

        Ok(Response::new(CreateGitTokenResponse {
            token_id,
            plaintext_token: plaintext,
        }))
    }

    pub async fn list_access_tokens_impl(
        &self,
        request: Request<ListGitTokensRequest>,
    ) -> Result<Response<ListGitTokensResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let tokens = self
            .token_store
            .list_tokens(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list git tokens: {e}")))?;

        let token_infos: Vec<GitTokenInfo> = tokens
            .into_iter()
            .map(|t| GitTokenInfo {
                id: t.id,
                tenant_id: t.tenant_id,
                permissions: t
                    .permissions
                    .iter()
                    .map(core_git_permission_to_proto)
                    .collect(),
                description: t.description,
                created_at: Some(datetime_to_proto(t.created_at)),
                expires_at: t.expires_at.map(datetime_to_proto),
                revoked: t.revoked,
                user_id: t.user_id,
            })
            .collect();

        Ok(Response::new(ListGitTokensResponse {
            tokens: token_infos,
        }))
    }

    pub async fn revoke_access_token_impl(
        &self,
        request: Request<RevokeGitTokenRequest>,
    ) -> Result<Response<RevokeGitTokenResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.token_id.is_empty() {
            return Err(Status::invalid_argument("token_id is required"));
        }

        let tokens = self
            .token_store
            .list_tokens(&caller_tenant)
            .await
            .map_err(|e| Status::internal(format!("Failed to list tokens: {e}")))?;

        let owns_token = tokens.iter().any(|t| t.id == req.token_id);
        if !owns_token {
            return Err(Status::permission_denied(format!(
                "token {} does not belong to tenant {}",
                req.token_id, caller_tenant
            )));
        }

        self.token_store
            .revoke_token(&req.token_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to revoke token: {e}")))?;

        info!(
            operation = "revoke_access_token",
            tenant_id = %caller_tenant,
            token_id = %req.token_id,
            "audit: git access token revoked"
        );

        Ok(Response::new(RevokeGitTokenResponse {}))
    }

    pub async fn list_access_tokens_by_user_impl(
        &self,
        request: Request<ListGitTokensByUserRequest>,
    ) -> Result<Response<ListGitTokensResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.user_id.is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }

        let tokens = self
            .token_store
            .list_tokens_by_user(&req.tenant_id, &req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list git tokens by user: {e}")))?;

        let token_infos: Vec<GitTokenInfo> = tokens
            .into_iter()
            .map(|t| GitTokenInfo {
                id: t.id,
                tenant_id: t.tenant_id,
                permissions: t
                    .permissions
                    .iter()
                    .map(core_git_permission_to_proto)
                    .collect(),
                description: t.description,
                created_at: Some(datetime_to_proto(t.created_at)),
                expires_at: t.expires_at.map(datetime_to_proto),
                revoked: t.revoked,
                user_id: t.user_id,
            })
            .collect();

        Ok(Response::new(ListGitTokensResponse {
            tokens: token_infos,
        }))
    }

    pub async fn create_impersonation_token_impl(
        &self,
        request: Request<CreateImpersonationTokenRequest>,
    ) -> Result<Response<CreateGitTokenResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.user_id.is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        if req.permissions.is_empty() {
            return Err(Status::invalid_argument(
                "at least one permission is required",
            ));
        }

        let core_permissions: Vec<GitPermission> = req
            .permissions
            .iter()
            .map(|&p| proto_git_permission_to_core(p))
            .collect::<Result<Vec<_>, _>>()?;

        let plaintext = generate_token();
        let token_hash = hash_token(&plaintext).await;
        let prefix = token_prefix(&plaintext);

        let expires_at = req
            .ttl_seconds
            .filter(|&ttl| ttl > 0)
            .map(|ttl| Utc::now() + Duration::seconds(ttl));

        let description = if req.description.is_empty() {
            format!("impersonation token for user {}", req.user_id)
        } else {
            req.description.clone()
        };

        let mut token = GitToken::new(
            req.tenant_id.clone(),
            token_hash,
            prefix,
            core_permissions,
            description,
            expires_at,
        );
        token.user_id = Some(req.user_id.clone());

        let token_id =
            self.token_store.create_token(&token).await.map_err(|e| {
                Status::internal(format!("Failed to create impersonation token: {e}"))
            })?;

        info!(
            operation = "create_impersonation_token",
            tenant_id = %req.tenant_id,
            user_id = %req.user_id,
            token_id = %token_id,
            "audit: impersonation token created"
        );

        Ok(Response::new(CreateGitTokenResponse {
            token_id,
            plaintext_token: plaintext,
        }))
    }
}
