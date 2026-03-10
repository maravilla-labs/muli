// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH key management RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_core::git::{GitPermission, SshKey};
use muli_proto::{
    AddSshKeyRequest, ListSshKeysByUserRequest, ListSshKeysRequest, ListSshKeysResponse,
    RemoveSshKeyRequest, RemoveSshKeyResponse, SshKey as ProtoSshKey,
};

use super::GitServiceImpl;
use super::helpers::{compute_ssh_fingerprint, ssh_key_to_proto};
use crate::grpc::conversions::proto_git_permission_to_core;
use crate::grpc::util::extract_tenant_id;

impl GitServiceImpl {
    pub async fn add_ssh_key_impl(
        &self,
        request: Request<AddSshKeyRequest>,
    ) -> Result<Response<ProtoSshKey>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }
        if req.public_key.is_empty() {
            return Err(Status::invalid_argument("public_key is required"));
        }
        if req.title.is_empty() {
            return Err(Status::invalid_argument("title is required"));
        }

        let fingerprint = compute_ssh_fingerprint(&req.public_key)?;

        let permissions: Vec<GitPermission> = if req.permissions.is_empty() {
            vec![GitPermission::Pull, GitPermission::Push]
        } else {
            req.permissions
                .iter()
                .map(|&p| proto_git_permission_to_core(p))
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut key = SshKey::new(
            req.tenant_id.clone(),
            fingerprint,
            req.public_key.clone(),
            req.title.clone(),
            permissions,
        );
        key.user_id = req.user_id.clone();

        self.ssh_key_store
            .add_key(&key)
            .await
            .map_err(|e| Status::internal(format!("Failed to add SSH key: {e}")))?;

        info!(
            operation = "add_ssh_key",
            tenant_id = %req.tenant_id,
            key_id = %key.id,
            "audit: SSH key added"
        );

        Ok(Response::new(ssh_key_to_proto(&key)))
    }

    pub async fn remove_ssh_key_impl(
        &self,
        request: Request<RemoveSshKeyRequest>,
    ) -> Result<Response<RemoveSshKeyResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.key_id.is_empty() {
            return Err(Status::invalid_argument("key_id is required"));
        }

        let keys = self
            .ssh_key_store
            .list_keys(&caller_tenant)
            .await
            .map_err(|e| Status::internal(format!("Failed to list SSH keys: {e}")))?;

        let owns_key = keys.iter().any(|k| k.id == req.key_id);
        if !owns_key {
            return Err(Status::permission_denied(format!(
                "SSH key {} does not belong to tenant {}",
                req.key_id, caller_tenant
            )));
        }

        self.ssh_key_store
            .remove_key(&req.key_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to remove SSH key: {e}")))?;

        info!(
            operation = "remove_ssh_key",
            tenant_id = %caller_tenant,
            key_id = %req.key_id,
            "audit: SSH key removed"
        );

        Ok(Response::new(RemoveSshKeyResponse {}))
    }

    pub async fn list_ssh_keys_impl(
        &self,
        request: Request<ListSshKeysRequest>,
    ) -> Result<Response<ListSshKeysResponse>, Status> {
        let caller_tenant = extract_tenant_id(&request)?;
        let req = request.into_inner();

        if req.tenant_id != caller_tenant {
            return Err(Status::permission_denied(
                "tenant_id in request does not match authenticated tenant",
            ));
        }

        let keys = self
            .ssh_key_store
            .list_keys(&req.tenant_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list SSH keys: {e}")))?;

        Ok(Response::new(ListSshKeysResponse {
            keys: keys.iter().map(ssh_key_to_proto).collect(),
        }))
    }

    pub async fn list_ssh_keys_by_user_impl(
        &self,
        request: Request<ListSshKeysByUserRequest>,
    ) -> Result<Response<ListSshKeysResponse>, Status> {
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

        let keys = self
            .ssh_key_store
            .list_keys_by_user(&req.tenant_id, &req.user_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list SSH keys by user: {e}")))?;

        Ok(Response::new(ListSshKeysResponse {
            keys: keys.iter().map(ssh_key_to_proto).collect(),
        }))
    }
}
