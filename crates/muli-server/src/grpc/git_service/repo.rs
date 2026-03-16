// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository CRUD RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::git::Repository;
use muli_proto::{
    CreateRepositoryRequest, DeleteRepositoryRequest, DeleteRepositoryResponse,
    ForkRepositoryRequest, GetRepositoryRequest, GitRepository, ListRepositoriesRequest,
    ListRepositoriesResponse, TransferRepositoryRequest, UpdateVisibilityRequest,
};

use muli_git::api::helpers::validate_path_component;

use super::GitServiceImpl;
use super::helpers::repo_to_proto;
use crate::grpc::util::extract_tenant_id;

impl GitServiceImpl {
    pub async fn create_repository_impl(
        &self,
        request: Request<CreateRepositoryRequest>,
    ) -> Result<Response<GitRepository>, Status> {
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
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        if !validate_path_component(&req.namespace) || !validate_path_component(&req.name) {
            return Err(Status::invalid_argument("invalid namespace or name"));
        }

        let mut repo = Repository::new(
            req.tenant_id.clone(),
            req.namespace.clone(),
            req.name.clone(),
            req.description.clone(),
            req.is_private,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

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

        self.git_storage
            .init_repo(&req.tenant_id, &req.namespace, &req.name)
            .await
            .map_err(|e| Status::internal(format!("Failed to initialize git repo: {e}")))?;

        self.repo_store
            .create_repository(&repo)
            .await
            .map_err(|e| Status::internal(format!("Failed to create repository: {e}")))?;

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
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

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
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let repo = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.name)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.name
                ))
            })?;

        self.repo_store
            .delete_repository(&repo.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete repository: {e}")))?;

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
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

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
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

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

        let source = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.source_namespace, &req.source_name)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up source repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "source repository {}/{} not found",
                    req.source_namespace, req.source_name
                ))
            })?;

        let mut forked = Repository::new(
            req.tenant_id.clone(),
            req.dest_namespace.clone(),
            req.dest_name.clone(),
            source.description.clone(),
            source.is_private,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        forked.fork_of = Some(source.id.clone());

        self.git_storage
            .fork_repo(
                &req.tenant_id,
                &req.source_namespace,
                &req.source_name,
                &req.tenant_id,
                &req.dest_namespace,
                &req.dest_name,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to fork git repo: {e}")))?;

        self.repo_store
            .create_repository(&forked)
            .await
            .map_err(|e| Status::internal(format!("Failed to create forked repository: {e}")))?;

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
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        if req.new_namespace.is_empty() {
            return Err(Status::invalid_argument("new_namespace is required"));
        }
        if !validate_path_component(&req.new_namespace) {
            return Err(Status::invalid_argument("invalid new_namespace"));
        }

        let repo = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.name)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.name
                ))
            })?;

        if repo.namespace == req.new_namespace {
            return Err(Status::invalid_argument(
                "new_namespace is same as current namespace",
            ));
        }

        self.git_storage
            .transfer_repo(
                &req.tenant_id,
                &req.namespace,
                &req.name,
                &req.new_namespace,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to transfer git repo: {e}")))?;

        self.repo_store
            .transfer_repository(&repo.id, &req.new_namespace)
            .await
            .map_err(|e| Status::internal(format!("Failed to update repository namespace: {e}")))?;

        let mut updated = repo;
        updated.namespace = req.new_namespace;

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
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

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
