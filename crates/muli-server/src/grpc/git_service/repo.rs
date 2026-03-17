// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository CRUD RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_proto::{
    CreateRepositoryRequest, DeleteRepositoryRequest, DeleteRepositoryResponse,
    ForkRepositoryRequest, GetRepositoryRequest, GitRepository, ListRepositoriesRequest,
    ListRepositoriesResponse, TransferRepositoryRequest, UpdateVisibilityRequest,
};

use muli_git::api::helpers::validate_path_component;

use super::GitServiceImpl;
use super::helpers::repo_to_proto;
use crate::grpc::util::{domain_err_to_grpc, validate_tenant};

impl GitServiceImpl {
    pub async fn create_repository_impl(
        &self,
        request: Request<CreateRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        // Enforcement checks
        crate::enforcement::check_not_suspended(
            self.tenant_limits_store.as_deref(),
            &req.tenant_id,
        )
        .await?;
        crate::enforcement::check_repo_limit(
            self.tenant_limits_store.as_deref(),
            &*self.repo_store,
            &req.tenant_id,
        )
        .await?;

        if req.tenant_id.is_empty() || req.namespace.is_empty() || req.name.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, namespace, and name are required",
            ));
        }
        if !validate_path_component(&req.namespace) || !validate_path_component(&req.name) {
            return Err(Status::invalid_argument("invalid namespace or name"));
        }

        let mut repo = self
            .repo_service
            .create(
                &req.tenant_id,
                &req.namespace,
                &req.name,
                &req.description,
                req.is_private,
            )
            .await
            .map_err(domain_err_to_grpc)?;

        // Set owner_id and owner_type from request if provided
        if !req.owner_id.is_empty() {
            repo.owner_id = req.owner_id.clone();
        }
        if !req.owner_type.is_empty() {
            repo.owner_type = match req.owner_type.as_str() {
                "organization" => muli_core::git::OwnerType::Organization,
                _ => muli_core::git::OwnerType::User,
            };
        }
        // If owner was set, update the record
        if !req.owner_id.is_empty() || !req.owner_type.is_empty() {
            let _ = self.repo_store.update_repository(&repo).await;
        }

        info!(
            operation = "create_repository",
            tenant_id = %req.tenant_id,
            namespace = %req.namespace,
            name = %req.name,
            "audit: git repository created"
        );

        Ok(Response::new(repo_to_proto(&repo)))
    }

    pub async fn get_repository_impl(
        &self,
        request: Request<GetRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let repo = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.name)
            .await
            .map_err(|e| Status::internal(format!("Failed to get repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.name
                ))
            })?;

        Ok(Response::new(repo_to_proto(&repo)))
    }

    pub async fn delete_repository_impl(
        &self,
        request: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        self.repo_service
            .delete(&req.tenant_id, &req.namespace, &req.name)
            .await
            .map_err(domain_err_to_grpc)?;

        info!(
            operation = "delete_repository",
            tenant_id = %req.tenant_id,
            namespace = %req.namespace,
            name = %req.name,
            "audit: git repository deleted"
        );

        Ok(Response::new(DeleteRepositoryResponse {}))
    }

    pub async fn list_repositories_impl(
        &self,
        request: Request<ListRepositoriesRequest>,
    ) -> Result<Response<ListRepositoriesResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let repos = self
            .repo_store
            .list_repositories(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list repositories: {e}")))?;

        Ok(Response::new(ListRepositoriesResponse {
            repositories: repos.iter().map(repo_to_proto).collect(),
        }))
    }

    pub async fn fork_repository_impl(
        &self,
        request: Request<ForkRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.dest_namespace.is_empty() || req.dest_name.is_empty() {
            return Err(Status::invalid_argument(
                "dest_namespace and dest_name are required",
            ));
        }
        if !validate_path_component(&req.dest_namespace) || !validate_path_component(&req.dest_name)
        {
            return Err(Status::invalid_argument(
                "invalid dest_namespace or dest_name",
            ));
        }

        let forked = self
            .repo_service
            .fork(
                &req.tenant_id,
                &req.source_namespace,
                &req.source_name,
                &req.dest_namespace,
                &req.dest_name,
            )
            .await
            .map_err(domain_err_to_grpc)?;

        info!(
            operation = "fork_repository",
            tenant_id = %req.tenant_id,
            source = %format!("{}/{}", req.source_namespace, req.source_name),
            dest = %format!("{}/{}", req.dest_namespace, req.dest_name),
            "audit: git repository forked"
        );

        Ok(Response::new(repo_to_proto(&forked)))
    }

    pub async fn transfer_repository_impl(
        &self,
        request: Request<TransferRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.new_namespace.is_empty() {
            return Err(Status::invalid_argument("new_namespace is required"));
        }
        if !validate_path_component(&req.new_namespace) {
            return Err(Status::invalid_argument("invalid new_namespace"));
        }

        let updated = self
            .repo_service
            .transfer(
                &req.tenant_id,
                &req.namespace,
                &req.name,
                &req.new_namespace,
            )
            .await
            .map_err(domain_err_to_grpc)?;

        info!(
            operation = "transfer_repository",
            tenant_id = %req.tenant_id,
            from = %format!("{}/{}", req.namespace, req.name),
            to = %format!("{}/{}", updated.namespace, req.name),
            "audit: git repository transferred"
        );

        Ok(Response::new(repo_to_proto(&updated)))
    }

    pub async fn update_repository_visibility_impl(
        &self,
        request: Request<UpdateVisibilityRequest>,
    ) -> Result<Response<GitRepository>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        let mut repo = self
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

        repo.is_private = req.is_private;

        self.repo_store
            .update_repository(&repo)
            .await
            .map_err(|e| Status::internal(format!("Failed to update repository: {e}")))?;

        info!(
            operation = "update_visibility",
            tenant_id = %req.tenant_id,
            repo = %format!("{}/{}", req.namespace, req.repo),
            is_private = req.is_private,
            "audit: repository visibility updated"
        );

        Ok(Response::new(repo_to_proto(&repo)))
    }
}
