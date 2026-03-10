// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! User management gRPC service.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::traits::UserStore;
use muli_core::user::TenantUser;

use muli_proto::user_service_server::UserService;
use muli_proto::{
    CreateUserRequest, DeleteUserRequest, DeleteUserResponse, GetUserRequest, ListUsersRequest,
    ListUsersResponse, TenantUser as ProtoTenantUser,
};

use super::util::{datetime_to_proto, extract_tenant_id};

pub struct UserServiceImpl {
    pub user_store: Arc<dyn UserStore>,
}

fn user_to_proto(u: &TenantUser) -> ProtoTenantUser {
    ProtoTenantUser {
        id: u.id.clone(),
        tenant_id: u.tenant_id.clone(),
        handle: u.handle.clone(),
        external_id: u.external_id.clone(),
        email: u.email.clone(),
        created_at: Some(datetime_to_proto(u.created_at)),
    }
}

#[tonic::async_trait]
impl UserService for UserServiceImpl {
    async fn create_user(
        &self,
        request: Request<CreateUserRequest>,
    ) -> Result<Response<ProtoTenantUser>, Status> {
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
        if req.handle.is_empty() {
            return Err(Status::invalid_argument("handle is required"));
        }
        if req.external_id.is_empty() {
            return Err(Status::invalid_argument("external_id is required"));
        }

        let user = TenantUser::new(
            req.tenant_id.clone(),
            req.handle.clone(),
            req.external_id.clone(),
            req.email.clone(),
        );

        self.user_store
            .create_user(&user)
            .await
            .map_err(|e| Status::already_exists(format!("Failed to create user: {e}")))?;

        info!(
            operation = "create_user",
            tenant_id = %req.tenant_id,
            handle = %req.handle,
            user_id = %user.id,
            "audit: tenant user created"
        );

        Ok(Response::new(user_to_proto(&user)))
    }

    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<ProtoTenantUser>, Status> {
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

        let user = self
            .user_store
            .get_user(&req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get user: {e}")))?
            .ok_or_else(|| Status::not_found(format!("user {} not found", req.user_id)))?;

        // Verify the user belongs to the caller's tenant
        if user.tenant_id != caller_tenant {
            return Err(Status::not_found(format!("user {} not found", req.user_id)));
        }

        Ok(Response::new(user_to_proto(&user)))
    }

    async fn delete_user(
        &self,
        request: Request<DeleteUserRequest>,
    ) -> Result<Response<DeleteUserResponse>, Status> {
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

        // Verify the user belongs to this tenant before deletion
        let user = self
            .user_store
            .get_user(&req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to look up user: {e}")))?
            .ok_or_else(|| Status::not_found(format!("user {} not found", req.user_id)))?;

        if user.tenant_id != caller_tenant {
            return Err(Status::not_found(format!("user {} not found", req.user_id)));
        }

        self.user_store
            .delete_user(&req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete user: {e}")))?;

        info!(
            operation = "delete_user",
            tenant_id = %caller_tenant,
            user_id = %req.user_id,
            "audit: tenant user deleted"
        );

        Ok(Response::new(DeleteUserResponse {}))
    }

    async fn list_users(
        &self,
        request: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let users = self
            .user_store
            .list_users(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list users: {e}")))?;

        Ok(Response::new(ListUsersResponse {
            users: users.iter().map(user_to_proto).collect(),
        }))
    }
}
