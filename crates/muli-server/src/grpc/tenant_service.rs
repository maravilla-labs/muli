// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tenant management gRPC service.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::tenant::Tenant;
use muli_core::traits::{
    JobStore, PipelineRunStore, RepositoryStore, TenantLimitsStore, TenantQuotaStore, TenantStore,
};

use muli_proto::tenant_service_server::TenantService;
use muli_proto::{
    CreateTenantRequest, DeleteTenantRequest, DeleteTenantResponse, GetTenantLimitsRequest,
    GetTenantLimitsResponse, GetTenantRequest, GetTenantUsageRequest, GetTenantUsageResponse,
    ListTenantsRequest, ListTenantsResponse, SetTenantLimitsRequest, SetTenantLimitsResponse,
    SuspendTenantRequest, SuspendTenantResponse, Tenant as ProtoTenant,
    TenantLimits as ProtoTenantLimits, UnsuspendTenantRequest, UnsuspendTenantResponse,
};

use super::conversions::{core_tier_to_proto, proto_tier_to_core};
use super::util::{datetime_to_proto, extract_tenant_id};

pub struct TenantServiceImpl {
    pub tenant_store: Arc<dyn TenantStore>,
    pub tenant_limits_store: Arc<dyn TenantLimitsStore>,
    pub tenant_quota_store: Arc<dyn TenantQuotaStore>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub job_store: Arc<dyn JobStore>,
    pub pipeline_run_store: Arc<dyn PipelineRunStore>,
}

fn tenant_to_proto(t: &Tenant) -> ProtoTenant {
    ProtoTenant {
        id: t.id.clone(),
        name: t.name.clone(),
        description: t.description.clone(),
        created_at: Some(datetime_to_proto(t.created_at)),
        limits: None,
    }
}

fn limits_to_proto(l: &muli_core::tenant::limits::TenantLimits) -> ProtoTenantLimits {
    ProtoTenantLimits {
        max_concurrent_jobs: l.max_concurrent_jobs,
        max_concurrent_pipelines: l.max_concurrent_pipelines,
        max_pipeline_runs_per_day: l.max_pipeline_runs_per_day,
        max_repos: l.max_repos,
        max_storage_bytes: l.max_storage_bytes,
        priority_tier: core_tier_to_proto(l.priority_tier),
        suspended: l.suspended,
        suspended_reason: l.suspended_reason.clone().unwrap_or_default(),
    }
}

fn proto_limits_to_core(
    tenant_id: &str,
    proto: &ProtoTenantLimits,
) -> muli_core::tenant::limits::TenantLimits {
    muli_core::tenant::limits::TenantLimits {
        tenant_id: tenant_id.to_string(),
        max_concurrent_jobs: proto.max_concurrent_jobs,
        max_concurrent_pipelines: proto.max_concurrent_pipelines,
        max_pipeline_runs_per_day: proto.max_pipeline_runs_per_day,
        max_repos: proto.max_repos,
        max_storage_bytes: proto.max_storage_bytes,
        priority_tier: proto_tier_to_core(proto.priority_tier),
        suspended: proto.suspended,
        suspended_reason: if proto.suspended_reason.is_empty() {
            None
        } else {
            Some(proto.suspended_reason.clone())
        },
        updated_at: chrono::Utc::now(),
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

        let mut proto = tenant_to_proto(&tenant);
        // Attach limits if configured
        if let Ok(Some(limits)) = self.tenant_limits_store.get_limits(&req.id).await {
            proto.limits = Some(limits_to_proto(&limits));
        }

        Ok(Response::new(proto))
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

    async fn set_tenant_limits(
        &self,
        request: Request<SetTenantLimitsRequest>,
    ) -> Result<Response<SetTenantLimitsResponse>, Status> {
        let req = request.into_inner();

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let proto_limits = req
            .limits
            .ok_or_else(|| Status::invalid_argument("limits are required"))?;

        let limits = proto_limits_to_core(&req.tenant_id, &proto_limits);

        self.tenant_limits_store
            .set_limits(&limits)
            .await
            .map_err(|e| Status::internal(format!("Failed to set tenant limits: {e}")))?;

        info!(
            operation = "set_tenant_limits",
            tenant_id = %req.tenant_id,
            "audit: tenant limits updated"
        );

        Ok(Response::new(SetTenantLimitsResponse {}))
    }

    async fn get_tenant_limits(
        &self,
        request: Request<GetTenantLimitsRequest>,
    ) -> Result<Response<GetTenantLimitsResponse>, Status> {
        let req = request.into_inner();

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let limits = self
            .tenant_limits_store
            .get_limits(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get tenant limits: {e}")))?;

        let proto_limits = match limits {
            Some(l) => limits_to_proto(&l),
            None => ProtoTenantLimits::default(),
        };

        Ok(Response::new(GetTenantLimitsResponse {
            limits: Some(proto_limits),
        }))
    }

    async fn suspend_tenant(
        &self,
        request: Request<SuspendTenantRequest>,
    ) -> Result<Response<SuspendTenantResponse>, Status> {
        let req = request.into_inner();

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let mut limits = self
            .tenant_limits_store
            .get_limits(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get tenant limits: {e}")))?
            .unwrap_or_else(|| muli_core::tenant::limits::TenantLimits::new(&req.tenant_id));

        limits.suspended = true;
        limits.suspended_reason = if req.reason.is_empty() {
            None
        } else {
            Some(req.reason.clone())
        };
        limits.updated_at = chrono::Utc::now();

        self.tenant_limits_store
            .set_limits(&limits)
            .await
            .map_err(|e| Status::internal(format!("Failed to suspend tenant: {e}")))?;

        info!(
            operation = "suspend_tenant",
            tenant_id = %req.tenant_id,
            reason = %req.reason,
            "audit: tenant suspended"
        );

        Ok(Response::new(SuspendTenantResponse {}))
    }

    async fn unsuspend_tenant(
        &self,
        request: Request<UnsuspendTenantRequest>,
    ) -> Result<Response<UnsuspendTenantResponse>, Status> {
        let req = request.into_inner();

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let mut limits = self
            .tenant_limits_store
            .get_limits(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get tenant limits: {e}")))?
            .unwrap_or_else(|| muli_core::tenant::limits::TenantLimits::new(&req.tenant_id));

        limits.suspended = false;
        limits.suspended_reason = None;
        limits.updated_at = chrono::Utc::now();

        self.tenant_limits_store
            .set_limits(&limits)
            .await
            .map_err(|e| Status::internal(format!("Failed to unsuspend tenant: {e}")))?;

        info!(
            operation = "unsuspend_tenant",
            tenant_id = %req.tenant_id,
            "audit: tenant unsuspended"
        );

        Ok(Response::new(UnsuspendTenantResponse {}))
    }

    async fn get_tenant_usage(
        &self,
        request: Request<GetTenantUsageRequest>,
    ) -> Result<Response<GetTenantUsageResponse>, Status> {
        let req = request.into_inner();

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let storage_bytes = self
            .tenant_quota_store
            .get_quota(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get quota: {e}")))?
            .map(|q| q.current_usage_bytes)
            .unwrap_or(0);

        let active_jobs = self
            .job_store
            .count_active_by_tenant(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to count active jobs: {e}")))?;

        let active_pipelines = self
            .pipeline_run_store
            .count_active(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to count active pipelines: {e}")))?;

        let pipeline_runs_today = self
            .tenant_limits_store
            .get_daily_run_count(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get daily run count: {e}")))?;

        let repo_count = self
            .repo_store
            .count_by_tenant(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to count repos: {e}")))?;

        Ok(Response::new(GetTenantUsageResponse {
            storage_bytes,
            active_jobs: active_jobs as u32,
            active_pipelines: active_pipelines as u32,
            pipeline_runs_today: pipeline_runs_today as u32,
            repo_count: repo_count as u32,
        }))
    }
}
