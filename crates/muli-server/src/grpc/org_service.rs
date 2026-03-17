// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Organization management gRPC service.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::org::{OrgMember, Organization};
use muli_core::traits::{OrgMemberStore, OrgStore};

use muli_proto::org_service_server::OrgService;
use muli_proto::{
    AddMemberRequest, CreateOrgRequest, DeleteOrgRequest, DeleteOrgResponse, GetOrgRequest,
    ListMembersRequest, ListMembersResponse, ListOrgsRequest, ListOrgsResponse,
    OrgMember as ProtoOrgMember, Organization as ProtoOrganization, RemoveMemberRequest,
    RemoveMemberResponse, UpdateMemberRoleRequest,
};

use super::conversions::{domain_role_from_proto, proto_role_from_domain};
use super::util::{datetime_to_proto, validate_tenant};

pub struct OrgServiceImpl {
    pub org_store: Arc<dyn OrgStore>,
    pub member_store: Arc<dyn OrgMemberStore>,
}

fn org_to_proto(o: &Organization) -> ProtoOrganization {
    ProtoOrganization {
        id: o.id.clone(),
        tenant_id: o.tenant_id.clone(),
        handle: o.handle.clone(),
        display_name: o.display_name.clone(),
        description: o.description.clone(),
        created_at: Some(datetime_to_proto(o.created_at)),
    }
}

fn member_to_proto(m: &OrgMember) -> ProtoOrgMember {
    ProtoOrgMember {
        id: m.id.clone(),
        org_id: m.org_id.clone(),
        user_id: m.user_id.clone(),
        role: proto_role_from_domain(m.role) as i32,
        created_at: Some(datetime_to_proto(m.created_at)),
    }
}

impl OrgServiceImpl {
    /// Load an org by id and verify it belongs to the caller's tenant.
    async fn get_verified_org(
        &self,
        org_id: &str,
        caller_tenant: &str,
    ) -> Result<Organization, Status> {
        let org = self
            .org_store
            .get_org(org_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up org: {e}")))?
            .ok_or_else(|| Status::not_found(format!("org {org_id} not found")))?;
        if org.tenant_id != caller_tenant {
            return Err(Status::not_found(format!("org {org_id} not found")));
        }
        Ok(org)
    }
}

#[tonic::async_trait]
impl OrgService for OrgServiceImpl {
    async fn create_org(
        &self,
        request: Request<CreateOrgRequest>,
    ) -> Result<Response<ProtoOrganization>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        if req.handle.is_empty() {
            return Err(Status::invalid_argument("handle is required"));
        }

        let org = Organization::new(
            req.tenant_id.clone(),
            req.handle.clone(),
            req.display_name.clone(),
            req.description.clone(),
        );

        self.org_store
            .create_org(&org)
            .await
            .map_err(|e| Status::already_exists(format!("Failed to create org: {e}")))?;

        info!(
            operation = "create_org",
            tenant_id = %req.tenant_id,
            handle = %req.handle,
            org_id = %org.id,
            "audit: org created"
        );

        Ok(Response::new(org_to_proto(&org)))
    }

    async fn get_org(
        &self,
        request: Request<GetOrgRequest>,
    ) -> Result<Response<ProtoOrganization>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.org_id.is_empty() {
            return Err(Status::invalid_argument("org_id is required"));
        }

        let org = self.get_verified_org(&req.org_id, &caller_tenant).await?;
        Ok(Response::new(org_to_proto(&org)))
    }

    async fn delete_org(
        &self,
        request: Request<DeleteOrgRequest>,
    ) -> Result<Response<DeleteOrgResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.org_id.is_empty() {
            return Err(Status::invalid_argument("org_id is required"));
        }

        self.get_verified_org(&req.org_id, &caller_tenant).await?;

        self.org_store
            .delete_org(&req.org_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete org: {e}")))?;

        info!(
            operation = "delete_org",
            tenant_id = %caller_tenant,
            org_id = %req.org_id,
            "audit: org deleted"
        );

        Ok(Response::new(DeleteOrgResponse {}))
    }

    async fn list_orgs(
        &self,
        request: Request<ListOrgsRequest>,
    ) -> Result<Response<ListOrgsResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let orgs = self
            .org_store
            .list_orgs(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list orgs: {e}")))?;

        Ok(Response::new(ListOrgsResponse {
            orgs: orgs.iter().map(org_to_proto).collect(),
        }))
    }

    async fn add_member(
        &self,
        request: Request<AddMemberRequest>,
    ) -> Result<Response<ProtoOrgMember>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.org_id.is_empty() {
            return Err(Status::invalid_argument("org_id is required"));
        }
        if req.user_id.is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }

        self.get_verified_org(&req.org_id, &caller_tenant).await?;

        let role = domain_role_from_proto(req.role)?;
        let member = OrgMember::new(req.org_id.clone(), req.user_id.clone(), role);

        self.member_store
            .add_member(&member)
            .await
            .map_err(|e| Status::already_exists(format!("Failed to add member: {e}")))?;

        info!(
            operation = "add_member",
            tenant_id = %caller_tenant,
            org_id = %req.org_id,
            user_id = %req.user_id,
            "audit: org member added"
        );

        Ok(Response::new(member_to_proto(&member)))
    }

    async fn remove_member(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<RemoveMemberResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        self.get_verified_org(&req.org_id, &caller_tenant).await?;

        self.member_store
            .remove_member(&req.org_id, &req.user_id)
            .await
            .map_err(|e| Status::not_found(format!("Failed to remove member: {e}")))?;

        info!(
            operation = "remove_member",
            tenant_id = %caller_tenant,
            org_id = %req.org_id,
            user_id = %req.user_id,
            "audit: org member removed"
        );

        Ok(Response::new(RemoveMemberResponse {}))
    }

    async fn list_members(
        &self,
        request: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        self.get_verified_org(&req.org_id, &caller_tenant).await?;

        let members = self
            .member_store
            .list_members(&req.org_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list members: {e}")))?;

        Ok(Response::new(ListMembersResponse {
            members: members.iter().map(member_to_proto).collect(),
        }))
    }

    async fn update_member_role(
        &self,
        request: Request<UpdateMemberRoleRequest>,
    ) -> Result<Response<ProtoOrgMember>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        self.get_verified_org(&req.org_id, &caller_tenant).await?;

        let role = domain_role_from_proto(req.role)?;

        self.member_store
            .update_member_role(&req.org_id, &req.user_id, role)
            .await
            .map_err(|e| Status::not_found(format!("Failed to update member role: {e}")))?;

        let member = self
            .member_store
            .get_member(&req.org_id, &req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get updated member: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "member user_id={} not found in org {}",
                    req.user_id, req.org_id
                ))
            })?;

        info!(
            operation = "update_member_role",
            tenant_id = %caller_tenant,
            org_id = %req.org_id,
            user_id = %req.user_id,
            "audit: org member role updated"
        );

        Ok(Response::new(member_to_proto(&member)))
    }
}
