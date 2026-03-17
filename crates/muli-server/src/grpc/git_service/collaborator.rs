// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository collaborator management RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::git::{GitPermission, RepositoryCollaborator};
use muli_proto::{
    AddCollaboratorRequest, CollaboratorInfo, CollaboratorResponse, ListCollaboratorsRequest,
    ListCollaboratorsResponse, RemoveCollaboratorRequest,
};

use super::GitServiceImpl;
use crate::grpc::conversions::{core_git_permission_to_proto, proto_git_permission_to_core};
use crate::grpc::util::{datetime_to_proto, validate_tenant};

impl GitServiceImpl {
    pub async fn add_repository_collaborator_impl(
        &self,
        request: Request<AddCollaboratorRequest>,
    ) -> Result<Response<CollaboratorResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let repo = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.repo)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.repo
                ))
            })?;

        if req.user_id.is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }

        let perms: Vec<GitPermission> = req
            .permissions
            .iter()
            .map(|&p| proto_git_permission_to_core(p))
            .collect::<Result<Vec<_>, _>>()?;

        if perms.is_empty() {
            return Err(Status::invalid_argument(
                "at least one permission is required",
            ));
        }

        let collaborator = RepositoryCollaborator::new(
            req.tenant_id.clone(),
            repo.id.clone(),
            req.user_id.clone(),
            perms,
        );

        self.collaborator_store
            .upsert_collaborator(&collaborator)
            .await
            .map_err(|e| Status::internal(format!("Failed to add collaborator: {e}")))?;

        info!(
            operation = "add_collaborator",
            tenant_id = %req.tenant_id,
            repo = %format!("{}/{}", req.namespace, req.repo),
            user_id = %req.user_id,
            "audit: collaborator added"
        );

        Ok(Response::new(CollaboratorResponse { success: true }))
    }

    pub async fn remove_repository_collaborator_impl(
        &self,
        request: Request<RemoveCollaboratorRequest>,
    ) -> Result<Response<CollaboratorResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let repo = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.repo)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.repo
                ))
            })?;

        self.collaborator_store
            .remove_collaborator(&repo.id, &req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to remove collaborator: {e}")))?;

        info!(
            operation = "remove_collaborator",
            tenant_id = %req.tenant_id,
            repo = %format!("{}/{}", req.namespace, req.repo),
            user_id = %req.user_id,
            "audit: collaborator removed"
        );

        Ok(Response::new(CollaboratorResponse { success: true }))
    }

    pub async fn list_repository_collaborators_impl(
        &self,
        request: Request<ListCollaboratorsRequest>,
    ) -> Result<Response<ListCollaboratorsResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let repo = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.repo)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.repo
                ))
            })?;

        let collaborators = self
            .collaborator_store
            .list_collaborators(&repo.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list collaborators: {e}")))?;

        let infos: Vec<CollaboratorInfo> = collaborators
            .iter()
            .map(|c| CollaboratorInfo {
                user_id: c.user_id.clone(),
                permissions: c
                    .permissions
                    .iter()
                    .map(core_git_permission_to_proto)
                    .collect(),
                created_at: Some(datetime_to_proto(c.created_at)),
            })
            .collect();

        Ok(Response::new(ListCollaboratorsResponse {
            collaborators: infos,
        }))
    }
}
