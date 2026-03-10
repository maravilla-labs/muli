// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tenant management gRPC service.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::tenant::Tenant;
use muli_core::traits::TenantStore;

use muli_proto::tenant_service_server::TenantService;
use muli_proto::{
    CreateTenantRequest, DeleteTenantRequest, DeleteTenantResponse, GetTenantRequest,
    ListTenantsRequest, ListTenantsResponse, Tenant as ProtoTenant,
};

use super::util::{datetime_to_proto, extract_tenant_id};

pub struct TenantServiceImpl {
    pub tenant_store: Arc<dyn TenantStore>,
}

fn tenant_to_proto(t: &Tenant) -> ProtoTenant {
    ProtoTenant {
        id: t.id.clone(),
        name: t.name.clone(),
        description: t.description.clone(),
        created_at: Some(datetime_to_proto(t.created_at)),
    }
}

#[tonic::async_trait]
impl TenantService for TenantServiceImpl {
    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<ProtoTenant>, Status> {
        let req = request.into_inner();

        if req.id.is_empty() {
            return Err(Status::invalid_argument("id is required"));
        }

        if self
            .tenant_store
            .get_tenant(&req.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to check tenant: {e}")))?
            .is_some()
        {
            return Err(Status::already_exists(format!(
                "tenant {} already exists",
                req.id
            )));
        }

        let tenant = Tenant::new(req.id.clone(), req.name.clone(), req.description.clone());

        self.tenant_store
            .create_tenant(&tenant)
            .await
            .map_err(|e| Status::internal(format!("Failed to create tenant: {e}")))?;

        info!(
            operation = "create_tenant",
            tenant_id = %req.id,
            "audit: tenant created"
        );

        Ok(Response::new(tenant_to_proto(&tenant)))
    }

    async fn get_tenant(
        &self,
        request: Request<GetTenantRequest>,
    ) -> Result<Response<ProtoTenant>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.id.is_empty() {
            return Err(Status::invalid_argument("id is required"));
        }
        if req.id != caller_tenant {
            return Err(Status::permission_denied("can only access your own tenant"));
        }

        let tenant = self
            .tenant_store
            .get_tenant(&req.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get tenant: {e}")))?
            .ok_or_else(|| Status::not_found(format!("tenant {} not found", req.id)))?;

        Ok(Response::new(tenant_to_proto(&tenant)))
    }

    async fn list_tenants(
        &self,
        request: Request<ListTenantsRequest>,
    ) -> Result<Response<ListTenantsResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;

        // Only return the caller's own tenant
        let tenant = self
            .tenant_store
            .get_tenant(&caller_tenant)
            .await
            .map_err(|e| Status::internal(format!("Failed to get tenant: {e}")))?;

        let tenants = tenant.iter().map(tenant_to_proto).collect();

        Ok(Response::new(ListTenantsResponse { tenants }))
    }

    async fn delete_tenant(
        &self,
        request: Request<DeleteTenantRequest>,
    ) -> Result<Response<DeleteTenantResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.id.is_empty() {
            return Err(Status::invalid_argument("id is required"));
        }
        if req.id != caller_tenant {
            return Err(Status::permission_denied("can only delete your own tenant"));
        }

        self.tenant_store
            .get_tenant(&req.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up tenant: {e}")))?
            .ok_or_else(|| Status::not_found(format!("tenant {} not found", req.id)))?;

        self.tenant_store
            .delete_tenant(&req.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete tenant: {e}")))?;

        info!(
            operation = "delete_tenant",
            tenant_id = %req.id,
            "audit: tenant deleted"
        );

        Ok(Response::new(DeleteTenantResponse {}))
    }
}
