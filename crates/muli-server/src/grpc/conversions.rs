// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Conversions between domain types and protobuf messages.

use tonic::Status;

use muli_core::git::{GitPermission, WebhookEvent};
use muli_core::job::model::PriorityTier;
use muli_core::job::state_machine::JobState;
use muli_core::org::OrgRole;
use muli_core::registry::model::RegistryPermission;

use muli_proto::{
    GitPermission as ProtoGitPermission, OrgRole as ProtoOrgRole,
    RegistryPermission as ProtoRegistryPermission, WebhookEvent as ProtoWebhookEvent,
};

// ── Job state / tier conversions ────────────────────────────────────────────

pub fn core_state_to_proto(state: JobState) -> i32 {
    match state {
        JobState::Pending => 1,
        JobState::Scheduled => 2,
        JobState::Pulling => 3,
        JobState::Running => 4,
        JobState::Succeeded => 5,
        JobState::Failed => 6,
        JobState::Cancelled => 7,
        JobState::TimedOut => 8,
    }
}

pub fn proto_state_to_core(state: i32) -> Option<JobState> {
    match state {
        1 => Some(JobState::Pending),
        2 => Some(JobState::Scheduled),
        3 => Some(JobState::Pulling),
        4 => Some(JobState::Running),
        5 => Some(JobState::Succeeded),
        6 => Some(JobState::Failed),
        7 => Some(JobState::Cancelled),
        8 => Some(JobState::TimedOut),
        _ => None,
    }
}

pub fn proto_tier_to_core(tier: i32) -> PriorityTier {
    match tier {
        1 => PriorityTier::Free,
        2 => PriorityTier::Standard,
        3 => PriorityTier::Premium,
        4 => PriorityTier::Enterprise,
        _ => PriorityTier::Standard,
    }
}

pub fn core_tier_to_proto(tier: PriorityTier) -> i32 {
    match tier {
        PriorityTier::Free => 1,
        PriorityTier::Standard => 2,
        PriorityTier::Premium => 3,
        PriorityTier::Enterprise => 4,
    }
}

// ── Git permission conversions ──────────────────────────────────────────────

pub fn proto_git_permission_to_core(p: i32) -> Result<GitPermission, Status> {
    match p {
        1 => Ok(GitPermission::Pull),
        2 => Ok(GitPermission::Push),
        3 => Ok(GitPermission::Admin),
        _ => Err(Status::invalid_argument(format!(
            "unknown permission value: {p}"
        ))),
    }
}

pub fn core_git_permission_to_proto(p: &GitPermission) -> i32 {
    match p {
        GitPermission::Pull => ProtoGitPermission::Pull as i32,
        GitPermission::Push => ProtoGitPermission::Push as i32,
        GitPermission::Admin => ProtoGitPermission::Admin as i32,
    }
}

// ── Webhook event conversions ───────────────────────────────────────────────

pub fn proto_webhook_event_to_core(e: i32) -> Result<WebhookEvent, Status> {
    match e {
        1 => Ok(WebhookEvent::Push),
        2 => Ok(WebhookEvent::Create),
        3 => Ok(WebhookEvent::Delete),
        4 => Ok(WebhookEvent::PrOpened),
        5 => Ok(WebhookEvent::PrMerged),
        6 => Ok(WebhookEvent::PrClosed),
        7 => Ok(WebhookEvent::PipelineStarted),
        8 => Ok(WebhookEvent::PipelineCompleted),
        _ => Err(Status::invalid_argument(format!(
            "unknown webhook event: {e}"
        ))),
    }
}

pub fn core_webhook_event_to_proto(e: &WebhookEvent) -> i32 {
    match e {
        WebhookEvent::Push => ProtoWebhookEvent::Push as i32,
        WebhookEvent::Create => ProtoWebhookEvent::Create as i32,
        WebhookEvent::Delete => ProtoWebhookEvent::Delete as i32,
        WebhookEvent::PrOpened => ProtoWebhookEvent::PrOpened as i32,
        WebhookEvent::PrMerged => ProtoWebhookEvent::PrMerged as i32,
        WebhookEvent::PrClosed => ProtoWebhookEvent::PrClosed as i32,
        WebhookEvent::PipelineStarted => ProtoWebhookEvent::PipelineStarted as i32,
        WebhookEvent::PipelineCompleted => ProtoWebhookEvent::PipelineCompleted as i32,
    }
}

// ── Registry permission conversions ─────────────────────────────────────────

pub fn proto_registry_permission_to_core(p: i32) -> Result<RegistryPermission, Status> {
    match p {
        1 => Ok(RegistryPermission::Pull),
        2 => Ok(RegistryPermission::Push),
        3 => Ok(RegistryPermission::Admin),
        _ => Err(Status::invalid_argument(format!(
            "invalid registry permission: {p}"
        ))),
    }
}

pub fn core_registry_permission_to_proto(p: &RegistryPermission) -> i32 {
    match p {
        RegistryPermission::Pull => ProtoRegistryPermission::Pull as i32,
        RegistryPermission::Push => ProtoRegistryPermission::Push as i32,
        RegistryPermission::Admin => ProtoRegistryPermission::Admin as i32,
    }
}

// ── Org role conversions ────────────────────────────────────────────────────

pub fn proto_role_from_domain(role: OrgRole) -> ProtoOrgRole {
    match role {
        OrgRole::Owner => ProtoOrgRole::Owner,
        OrgRole::Admin => ProtoOrgRole::Admin,
        OrgRole::Member => ProtoOrgRole::Member,
        OrgRole::Viewer => ProtoOrgRole::Viewer,
    }
}

pub fn domain_role_from_proto(role: i32) -> Result<OrgRole, Status> {
    match ProtoOrgRole::try_from(role).unwrap_or(ProtoOrgRole::Unspecified) {
        ProtoOrgRole::Owner => Ok(OrgRole::Owner),
        ProtoOrgRole::Admin => Ok(OrgRole::Admin),
        ProtoOrgRole::Member => Ok(OrgRole::Member),
        ProtoOrgRole::Viewer => Ok(OrgRole::Viewer),
        ProtoOrgRole::Unspecified => Err(Status::invalid_argument("role must be specified")),
    }
}
