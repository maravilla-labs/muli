// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory job query helpers and filtering logic.

use muli_core::job::model::Job;
use muli_core::job::state_machine::JobState;

/// Shared filter predicate for `list_jobs` and `count_jobs`.
pub(super) fn matches_filter(
    job: &Job,
    state_filter: &Option<JobState>,
    tenant_id: Option<&str>,
) -> bool {
    if let Some(state) = state_filter
        && job.state != *state
    {
        return false;
    }
    if let Some(tid) = tenant_id
        && job.spec.tenant_id != tid
    {
        return false;
    }
    true
}
