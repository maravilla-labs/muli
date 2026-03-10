// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Registry token management gRPC service.

use std::sync::Arc;

use chrono::{Duration, Utc};
use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::registry::model::RegistryToken;
use muli_core::traits::{RegistryTokenStore, TenantQuotaStore};

use muli_proto::registry_service_server::RegistryService;
use muli_proto::{
    CreateRegistryTokenRequest, CreateRegistryTokenResponse, GetRegistryUsageRequest,
    GetRegistryUsageResponse, GetTenantQuotaRequest, GetTenantQuotaResponse,
    ListRegistryTokensRequest, ListRegistryTokensResponse, RegistryTokenInfo,
    RevokeRegistryTokenRequest, RevokeRegistryTokenResponse, RotateRegistryTokenRequest,
    RotateRegistryTokenResponse, SetTenantQuotaRequest, SetTenantQuotaResponse,
};

use super::conversions::{core_registry_permission_to_proto, proto_registry_permission_to_core};
use super::util::{datetime_to_proto, extract_tenant_id, generate_token, hash_token, token_prefix};

pub struct RegistryServiceImpl {
    pub token_store: Arc<dyn RegistryTokenStore>,
    pub quota_store: Arc<dyn TenantQuotaStore>,
}

impl RegistryServiceImpl {
    /// Look up a token by ID and verify it belongs to the caller tenant.
    async fn find_owned_token(
        &self,
        caller_tenant: &str,
        token_id: &str,
    ) -> Result<RegistryToken, Status> {
        let token = self
            .token_store
            .get_token_by_id(token_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get token: {e}")))?;

        match token {
            Some(t) if t.tenant_id == caller_tenant => Ok(t),
            _ => Err(Status::permission_denied(format!(
                "token {token_id} does not belong to tenant {caller_tenant}"
            ))),
        }
    }
}

#[tonic::async_trait]
impl RegistryService for RegistryServiceImpl {
    async fn create_registry_token(
        &self,
        request: Request<CreateRegistryTokenRequest>,
    ) -> Result<Response<CreateRegistryTokenResponse>, Status> {
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

        let mut core_permissions = Vec::new();
        for p in &req.permissions {
            core_permissions.push(proto_registry_permission_to_core(*p)?);
        }

        let plaintext = generate_token();
        let token_hash = hash_token(&plaintext);
        let prefix = token_prefix(&plaintext);

        let expires_at = req
            .ttl_seconds
            .filter(|&ttl| ttl > 0)
            .map(|ttl| Utc::now() + Duration::seconds(ttl as i64));

        let token = RegistryToken::new(
            req.tenant_id.clone(),
            token_hash,
            prefix,
            core_permissions,
            req.description,
            expires_at,
        );

        let token_id = self
            .token_store
            .create_token(&token)
            .await
            .map_err(|e| Status::internal(format!("Failed to create token: {e}")))?;

        info!(
            operation = "create_registry_token",
            tenant_id = %req.tenant_id,
            token_id = %token_id,
            "audit: registry token created"
        );

        Ok(Response::new(CreateRegistryTokenResponse {
            token_id,
            plaintext_token: plaintext,
        }))
    }

    async fn list_registry_tokens(
        &self,
        request: Request<ListRegistryTokensRequest>,
    ) -> Result<Response<ListRegistryTokensResponse>, Status> {
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
            .map_err(|e| Status::internal(format!("Failed to list tokens: {e}")))?;

        let token_infos: Vec<RegistryTokenInfo> = tokens
            .into_iter()
            .map(|t| RegistryTokenInfo {
                id: t.id,
                permissions: t
                    .permissions
                    .iter()
                    .map(core_registry_permission_to_proto)
                    .collect(),
                description: t.description,
                created_at: Some(datetime_to_proto(t.created_at)),
                expires_at: t.expires_at.map(datetime_to_proto),
                revoked: t.revoked,
            })
            .collect();

        Ok(Response::new(ListRegistryTokensResponse {
            tokens: token_infos,
        }))
    }

    async fn revoke_registry_token(
        &self,
        request: Request<RevokeRegistryTokenRequest>,
    ) -> Result<Response<RevokeRegistryTokenResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.token_id.is_empty() {
            return Err(Status::invalid_argument("token_id is required"));
        }

        self.find_owned_token(&caller_tenant, &req.token_id).await?;

        self.token_store
            .revoke_token(&req.token_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to revoke token: {e}")))?;

        info!(
            operation = "revoke_registry_token",
            tenant_id = %caller_tenant,
            token_id = %req.token_id,
            "audit: registry token revoked"
        );

        Ok(Response::new(RevokeRegistryTokenResponse {}))
    }

    async fn rotate_registry_token(
        &self,
        request: Request<RotateRegistryTokenRequest>,
    ) -> Result<Response<RotateRegistryTokenResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.token_id.is_empty() {
            return Err(Status::invalid_argument("token_id is required"));
        }

        let old_token = self.find_owned_token(&caller_tenant, &req.token_id).await?;

        if old_token.revoked {
            return Err(Status::failed_precondition("cannot rotate a revoked token"));
        }

        let plaintext = generate_token();
        let token_hash = hash_token(&plaintext);
        let prefix = token_prefix(&plaintext);

        let new_token = RegistryToken::new(
            caller_tenant.clone(),
            token_hash,
            prefix,
            old_token.permissions.clone(),
            format!("rotated from {}", req.token_id),
            old_token.expires_at,
        );

        let new_token_id = self
            .token_store
            .create_token(&new_token)
            .await
            .map_err(|e| Status::internal(format!("Failed to create rotated token: {e}")))?;

        let grace_seconds = req.grace_period_seconds.unwrap_or(3600);
        let grace_expiry = Utc::now() + Duration::seconds(grace_seconds as i64);

        let should_set = old_token
            .expires_at
            .map(|existing| grace_expiry < existing)
            .unwrap_or(true);

        if should_set {
            self.token_store
                .set_token_expiry(&req.token_id, grace_expiry)
                .await
                .map_err(|e| Status::internal(format!("Failed to set old token expiry: {e}")))?;
        }

        info!(
            operation = "rotate_registry_token",
            tenant_id = %caller_tenant,
            old_token_id = %req.token_id,
            new_token_id = %new_token_id,
            grace_period_seconds = grace_seconds,
            "audit: registry token rotated"
        );

        Ok(Response::new(RotateRegistryTokenResponse {
            token_id: new_token_id,
            plaintext_token: plaintext,
        }))
    }

    async fn get_registry_usage(
        &self,
        request: Request<GetRegistryUsageRequest>,
    ) -> Result<Response<GetRegistryUsageResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let quota = self
            .quota_store
            .get_quota(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get quota: {e}")))?;

        let storage_bytes = quota.as_ref().map(|q| q.current_usage_bytes).unwrap_or(0);

        Ok(Response::new(GetRegistryUsageResponse {
            storage_bytes,
            blob_count: 0,
            manifest_count: 0,
            repository_count: 0,
        }))
    }

    async fn set_tenant_quota(
        &self,
        request: Request<SetTenantQuotaRequest>,
    ) -> Result<Response<SetTenantQuotaResponse>, Status> {
        // Quota management is an administrative operation.
        // Tenants must not be able to set their own quota to arbitrary values.
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        // Reject self-service quota changes — only an admin (different tenant)
        // should be able to set quotas. Since we don't have a dedicated admin role
        // yet, prevent tenants from modifying their own quota.
        if req.tenant_id == caller_tenant {
            return Err(Status::permission_denied(
                "tenants cannot set their own quota; contact an administrator",
            ));
        }

        if req.max_storage_bytes == 0 {
            return Err(Status::invalid_argument(
                "max_storage_bytes must be greater than 0",
            ));
        }

        self.quota_store
            .set_quota(&req.tenant_id, req.max_storage_bytes)
            .await
            .map_err(|e| Status::internal(format!("Failed to set quota: {e}")))?;

        info!(
            operation = "set_tenant_quota",
            tenant_id = %req.tenant_id,
            caller_tenant = %caller_tenant,
            max_storage_bytes = req.max_storage_bytes,
            "audit: tenant quota set"
        );

        Ok(Response::new(SetTenantQuotaResponse {}))
    }

    async fn get_tenant_quota(
        &self,
        request: Request<GetTenantQuotaRequest>,
    ) -> Result<Response<GetTenantQuotaResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let quota = self
            .quota_store
            .get_quota(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get quota: {e}")))?;

        match quota {
            Some(q) => Ok(Response::new(GetTenantQuotaResponse {
                max_storage_bytes: q.max_storage_bytes,
                current_usage_bytes: q.current_usage_bytes,
            })),
            None => Ok(Response::new(GetTenantQuotaResponse {
                max_storage_bytes: 0,
                current_usage_bytes: 0,
            })),
        }
    }
}
