// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git hosting domain models.

mod model;
mod webhook;

pub use model::{
    GitPermission, GitToken, HasPermissions, OwnerType, Repository, RepositoryCollaborator, SshKey,
};
pub use webhook::{
    Webhook, WebhookEvent, is_private_ip, validate_webhook_target, validate_webhook_url,
};
