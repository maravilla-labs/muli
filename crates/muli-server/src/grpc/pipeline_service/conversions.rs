// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Domain ↔ Proto conversion functions for pipeline types.

use muli_core::pipeline::{
    Artifact, CacheEntry, FailureStrategy as DomainFailureStrategy, PipelineRun as DomainRun,
    PipelineRunState as DomainRunState, PipelineTrigger, StepRun as DomainStep,
    StepRunState as DomainStepState,
};
use muli_proto::{
    FailureStrategy as ProtoFailureStrategy, PipelineArtifact, PipelineCache, PipelineRun,
    PipelineRunState, PipelineStepState, PipelineTriggerType, StepRun,
};

use crate::grpc::util::datetime_to_proto;

pub fn run_state_to_proto(s: DomainRunState) -> i32 {
    match s {
        DomainRunState::Pending => PipelineRunState::Pending as i32,
        DomainRunState::Running => PipelineRunState::Running as i32,
        DomainRunState::Succeeded => PipelineRunState::Succeeded as i32,
        DomainRunState::Failed => PipelineRunState::Failed as i32,
        DomainRunState::Cancelled => PipelineRunState::Cancelled as i32,
        DomainRunState::Degraded => PipelineRunState::Degraded as i32,
    }
}

pub fn proto_to_run_state(v: i32) -> Option<DomainRunState> {
    match PipelineRunState::try_from(v) {
        Ok(PipelineRunState::Pending) => Some(DomainRunState::Pending),
        Ok(PipelineRunState::Running) => Some(DomainRunState::Running),
        Ok(PipelineRunState::Succeeded) => Some(DomainRunState::Succeeded),
        Ok(PipelineRunState::Failed) => Some(DomainRunState::Failed),
        Ok(PipelineRunState::Cancelled) => Some(DomainRunState::Cancelled),
        Ok(PipelineRunState::Degraded) => Some(DomainRunState::Degraded),
        _ => None,
    }
}

fn step_state_to_proto(s: DomainStepState) -> i32 {
    match s {
        DomainStepState::Pending => PipelineStepState::Pending as i32,
        DomainStepState::Ready => PipelineStepState::Ready as i32,
        DomainStepState::Running => PipelineStepState::Running as i32,
        DomainStepState::Succeeded => PipelineStepState::Succeeded as i32,
        DomainStepState::Failed => PipelineStepState::Failed as i32,
        DomainStepState::Skipped => PipelineStepState::Skipped as i32,
        DomainStepState::Cancelled => PipelineStepState::Cancelled as i32,
    }
}

fn trigger_to_proto(t: &PipelineTrigger) -> i32 {
    match t {
        PipelineTrigger::Push { .. } => PipelineTriggerType::Push as i32,
        PipelineTrigger::PullRequest { .. } => PipelineTriggerType::PullRequest as i32,
        PipelineTrigger::Manual { .. } => PipelineTriggerType::Manual as i32,
        PipelineTrigger::Schedule { .. } => PipelineTriggerType::Schedule as i32,
        PipelineTrigger::Retry { .. } => PipelineTriggerType::Retry as i32,
    }
}

pub fn failure_strategy_to_proto(s: DomainFailureStrategy) -> i32 {
    match s {
        DomainFailureStrategy::Stop => ProtoFailureStrategy::Stop as i32,
        DomainFailureStrategy::Continue => ProtoFailureStrategy::Continue as i32,
        DomainFailureStrategy::Ignore => ProtoFailureStrategy::Ignore as i32,
    }
}

pub fn step_to_proto(s: &DomainStep) -> StepRun {
    StepRun {
        id: s.id.clone(),
        run_id: s.run_id.clone(),
        step_name: s.step_name.clone(),
        job_id: s.job_id.clone().unwrap_or_default(),
        state: step_state_to_proto(s.state),
        matrix_values: s
            .matrix_values
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default(),
        failure_strategy: failure_strategy_to_proto(s.failure_strategy),
        started_at: s.started_at.map(datetime_to_proto),
        finished_at: s.finished_at.map(datetime_to_proto),
    }
}

pub fn run_to_proto(r: &DomainRun, steps: &[DomainStep]) -> PipelineRun {
    PipelineRun {
        id: r.id.clone(),
        pipeline_id: r.pipeline_id.clone(),
        tenant_id: r.tenant_id.clone(),
        repo_id: r.repo_id.clone(),
        run_number: r.run_number,
        commit_sha: r.commit_sha.clone(),
        ref_name: r.ref_name.clone(),
        trigger_type: trigger_to_proto(&r.trigger),
        state: run_state_to_proto(r.state),
        steps: steps.iter().map(step_to_proto).collect(),
        created_at: Some(datetime_to_proto(r.created_at)),
        started_at: r.started_at.map(datetime_to_proto),
        finished_at: r.finished_at.map(datetime_to_proto),
    }
}

pub fn artifact_to_proto(a: &Artifact) -> PipelineArtifact {
    PipelineArtifact {
        id: a.id.clone(),
        run_id: a.run_id.clone(),
        step_name: a.step_name.clone(),
        name: a.name.clone(),
        size_bytes: a.size_bytes,
        sha256: a.sha256.clone(),
        expires_at: a.expires_at.map(datetime_to_proto),
        created_at: Some(datetime_to_proto(a.created_at)),
    }
}

pub fn cache_to_proto(c: &CacheEntry) -> PipelineCache {
    PipelineCache {
        id: c.id.clone(),
        repo_id: c.repo_id.clone(),
        cache_key: c.cache_key.clone(),
        size_bytes: c.size_bytes,
        last_used_at: Some(datetime_to_proto(c.last_used_at)),
        created_at: Some(datetime_to_proto(c.created_at)),
    }
}
