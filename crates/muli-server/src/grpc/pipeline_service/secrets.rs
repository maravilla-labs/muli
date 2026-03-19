// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC handlers for secret management RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::pipeline::{OrgSecret, PipelineSecret};
use muli_proto::{
    DeleteOrgSecretRequest, DeleteOrgSecretResponse, DeleteSecretRequest, DeleteSecretResponse,
    ListOrgSecretsRequest, ListOrgSecretsResponse, ListSecretsRequest, ListSecretsResponse,
    SetOrgSecretRequest, SetOrgSecretResponse, SetSecretRequest, SetSecretResponse,
};

use super::PipelineServiceImpl;
use crate::grpc::util::validate_tenant;

impl PipelineServiceImpl {
    fn encrypt_value(&self, plaintext: &str) -> Result<String, Status> {
        let key = self.encryption_key.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "pipeline secret encryption key not configured (set MULI_PIPELINE_SECRET_ENCRYPTION_KEY)",
            )
        })?;
        muli_core::crypto::encrypt(plaintext, key)
            .map_err(|e| Status::internal(format!("encryption failed: {e}")))
    }

    // --- Repo-level secrets ---

    pub async fn set_secret_impl(
        &self,
        request: Request<SetSecretRequest>,
    ) -> Result<Response<SetSecretResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        if req.value.is_empty() {
            return Err(Status::invalid_argument("value is required"));
        }

        let encrypted = self.encrypt_value(&req.value)?;
        let secret = PipelineSecret::new(
            caller_tenant.clone(),
            req.repo_id.clone(),
            req.name.clone(),
            encrypted,
        );

        self.secret_store
            .set_secret(&secret)
            .await
            .map_err(|e| Status::internal(format!("failed to set secret: {e}")))?;

        info!(
            operation = "set_secret",
            tenant_id = %caller_tenant,
            repo_id = %req.repo_id,
            name = %req.name,
            "audit: repo secret set"
        );

        Ok(Response::new(SetSecretResponse {}))
    }

    pub async fn delete_secret_impl(
        &self,
        request: Request<DeleteSecretRequest>,
    ) -> Result<Response<DeleteSecretResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        self.secret_store
            .delete_secret(&caller_tenant, &req.repo_id, &req.name)
            .await
            .map_err(|e| Status::internal(format!("failed to delete secret: {e}")))?;

        info!(
            operation = "delete_secret",
            tenant_id = %caller_tenant,
            repo_id = %req.repo_id,
            name = %req.name,
            "audit: repo secret deleted"
        );

        Ok(Response::new(DeleteSecretResponse {}))
    }

    pub async fn list_secrets_impl(
        &self,
        request: Request<ListSecretsRequest>,
    ) -> Result<Response<ListSecretsResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        let names = self
            .secret_store
            .list_names(&caller_tenant, &req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("failed to list secrets: {e}")))?;

        Ok(Response::new(ListSecretsResponse { names }))
    }

    // --- Org-level secrets ---

    pub async fn set_org_secret_impl(
        &self,
        request: Request<SetOrgSecretRequest>,
    ) -> Result<Response<SetOrgSecretResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.org_id.is_empty() {
            return Err(Status::invalid_argument("org_id is required"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        if req.value.is_empty() {
            return Err(Status::invalid_argument("value is required"));
        }

        let encrypted = self.encrypt_value(&req.value)?;
        let secret = OrgSecret::new(
            caller_tenant.clone(),
            req.org_id.clone(),
            req.name.clone(),
            encrypted,
        );

        self.org_secret_store
            .set_org_secret(&secret)
            .await
            .map_err(|e| Status::internal(format!("failed to set org secret: {e}")))?;

        info!(
            operation = "set_org_secret",
            tenant_id = %caller_tenant,
            org_id = %req.org_id,
            name = %req.name,
            "audit: org secret set"
        );

        Ok(Response::new(SetOrgSecretResponse {}))
    }

    pub async fn delete_org_secret_impl(
        &self,
        request: Request<DeleteOrgSecretRequest>,
    ) -> Result<Response<DeleteOrgSecretResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.org_id.is_empty() {
            return Err(Status::invalid_argument("org_id is required"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        self.org_secret_store
            .delete_org_secret(&caller_tenant, &req.org_id, &req.name)
            .await
            .map_err(|e| Status::internal(format!("failed to delete org secret: {e}")))?;

        info!(
            operation = "delete_org_secret",
            tenant_id = %caller_tenant,
            org_id = %req.org_id,
            name = %req.name,
            "audit: org secret deleted"
        );

        Ok(Response::new(DeleteOrgSecretResponse {}))
    }

    pub async fn list_org_secrets_impl(
        &self,
        request: Request<ListOrgSecretsRequest>,
    ) -> Result<Response<ListOrgSecretsResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.org_id.is_empty() {
            return Err(Status::invalid_argument("org_id is required"));
        }

        let names = self
            .org_secret_store
            .list_org_names(&caller_tenant, &req.org_id)
            .await
            .map_err(|e| Status::internal(format!("failed to list org secrets: {e}")))?;

        Ok(Response::new(ListOrgSecretsResponse { names }))
    }
}
