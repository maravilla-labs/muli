// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Conversion helpers between git domain types and protobuf messages.

use base64::Engine as _;
use sha2::{Digest, Sha256};
use tonic::Status;

use muli_core::git::{Repository, SshKey, Webhook};
use muli_proto::{GitRepository, GitWebhook, SshKey as ProtoSshKey};

use crate::grpc::conversions::{core_git_permission_to_proto, core_webhook_event_to_proto};
use crate::grpc::util::datetime_to_proto;

pub fn repo_to_proto(r: &Repository) -> GitRepository {
    GitRepository {
        id: r.id.clone(),
        tenant_id: r.tenant_id.clone(),
        namespace: r.namespace.clone(),
        name: r.name.clone(),
        description: r.description.clone(),
        is_private: r.is_private,
        default_branch: r.default_branch.clone(),
        fork_of: r.fork_of.clone(),
        created_at: Some(datetime_to_proto(r.created_at)),
        updated_at: Some(datetime_to_proto(r.updated_at)),
        owner_id: r.owner_id.clone(),
        owner_type: r.owner_type_str().to_string(),
    }
}

pub fn ssh_key_to_proto(k: &SshKey) -> ProtoSshKey {
    ProtoSshKey {
        id: k.id.clone(),
        tenant_id: k.tenant_id.clone(),
        fingerprint: k.fingerprint.clone(),
        public_key: k.public_key.clone(),
        title: k.title.clone(),
        created_at: Some(datetime_to_proto(k.created_at)),
        user_id: k.user_id.clone(),
        permissions: k
            .permissions
            .iter()
            .map(core_git_permission_to_proto)
            .collect(),
    }
}

pub fn webhook_to_proto(w: &Webhook) -> GitWebhook {
    GitWebhook {
        id: w.id.clone(),
        tenant_id: w.tenant_id.clone(),
        repo_id: w.repo_id.clone(),
        url: w.url.clone(),
        events: w.events.iter().map(core_webhook_event_to_proto).collect(),
        active: w.active,
        created_at: Some(datetime_to_proto(w.created_at)),
    }
}

/// Compute the SSH fingerprint the same way russh does:
/// `"SHA256:" + base64_no_pad(SHA256(raw_wire_format_key_bytes))`.
pub fn compute_ssh_fingerprint(public_key_text: &str) -> Result<String, Status> {
    let parts: Vec<&str> = public_key_text.trim().splitn(3, ' ').collect();
    if parts.len() < 2 {
        return Err(Status::invalid_argument("invalid public key format"));
    }
    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(parts[1])
        .map_err(|e| Status::invalid_argument(format!("invalid base64 key: {e}")))?;
    let digest = Sha256::digest(&key_bytes);
    Ok(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest)
    ))
}
