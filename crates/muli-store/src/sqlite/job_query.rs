// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite job query helpers and filtering logic.

use muli_core::job::model::Job;

use super::util::from_json;

/// Fetch all pending jobs, ordered by priority (highest first).
pub(super) fn list_pending(c: &rusqlite::Connection) -> rusqlite::Result<Vec<Job>> {
    let mut stmt = c.prepare(
        "SELECT full_json FROM jobs WHERE state = 'Pending' ORDER BY priority_score DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        let json: String = row.get(0)?;
        from_json(&json)
    })?;
    rows.collect()
}

/// Fetch all jobs for a given tenant.
pub(super) fn list_by_tenant(
    c: &rusqlite::Connection,
    tenant_id: &str,
) -> rusqlite::Result<Vec<Job>> {
    let mut stmt = c.prepare("SELECT full_json FROM jobs WHERE tenant_id = ?1")?;
    let rows = stmt.query_map(rusqlite::params![tenant_id], |row| {
        let json: String = row.get(0)?;
        from_json(&json)
    })?;
    rows.collect()
}

/// Fetch all jobs in a given state.
pub(super) fn list_by_state(
    c: &rusqlite::Connection,
    state_str: &str,
) -> rusqlite::Result<Vec<Job>> {
    let mut stmt = c.prepare("SELECT full_json FROM jobs WHERE state = ?1")?;
    let rows = stmt.query_map(rusqlite::params![state_str], |row| {
        let json: String = row.get(0)?;
        from_json(&json)
    })?;
    rows.collect()
}

/// Paginated job listing with optional state and tenant filters.
pub(super) fn list_jobs(
    c: &rusqlite::Connection,
    state_str: &Option<String>,
    tenant_id: &Option<String>,
    limit: i64,
    offset: i64,
) -> rusqlite::Result<Vec<Job>> {
    let mut result = Vec::new();
    match (state_str, tenant_id) {
        (Some(s), Some(t)) => {
            let mut stmt = c.prepare(
                "SELECT full_json FROM jobs WHERE state = ?1 AND tenant_id = ?2 ORDER BY created_at DESC LIMIT ?3 OFFSET ?4"
            )?;
            let mut rows = stmt.query(rusqlite::params![s, t, limit, offset])?;
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json(&json)?);
            }
        }
        (Some(s), None) => {
            let mut stmt = c.prepare(
                "SELECT full_json FROM jobs WHERE state = ?1 ORDER BY created_at DESC LIMIT ?2 OFFSET ?3"
            )?;
            let mut rows = stmt.query(rusqlite::params![s, limit, offset])?;
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json(&json)?);
            }
        }
        (None, Some(t)) => {
            let mut stmt = c.prepare(
                "SELECT full_json FROM jobs WHERE tenant_id = ?1 ORDER BY created_at DESC LIMIT ?2 OFFSET ?3"
            )?;
            let mut rows = stmt.query(rusqlite::params![t, limit, offset])?;
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json(&json)?);
            }
        }
        (None, None) => {
            let mut stmt = c.prepare(
                "SELECT full_json FROM jobs ORDER BY created_at DESC LIMIT ?1 OFFSET ?2",
            )?;
            let mut rows = stmt.query(rusqlite::params![limit, offset])?;
            while let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                result.push(from_json(&json)?);
            }
        }
    }
    Ok(result)
}

/// Count jobs with optional state and tenant filters.
pub(super) fn count_jobs(
    c: &rusqlite::Connection,
    state_str: &Option<String>,
    tenant_id: &Option<String>,
) -> rusqlite::Result<u64> {
    let count: i64 = match (state_str, tenant_id) {
        (Some(s), Some(t)) => c.query_row(
            "SELECT COUNT(*) FROM jobs WHERE state = ?1 AND tenant_id = ?2",
            rusqlite::params![s, t],
            |row| row.get(0),
        )?,
        (Some(s), None) => c.query_row(
            "SELECT COUNT(*) FROM jobs WHERE state = ?1",
            rusqlite::params![s],
            |row| row.get(0),
        )?,
        (None, Some(t)) => c.query_row(
            "SELECT COUNT(*) FROM jobs WHERE tenant_id = ?1",
            rusqlite::params![t],
            |row| row.get(0),
        )?,
        (None, None) => c.query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))?,
    };
    Ok(count as u64)
}

/// Count non-terminal jobs for a tenant.
pub(super) fn count_active_by_tenant(
    c: &rusqlite::Connection,
    tenant_id: &str,
) -> rusqlite::Result<u64> {
    let count: i64 = c.query_row(
        "SELECT COUNT(*) FROM jobs WHERE tenant_id = ?1
         AND state NOT IN ('Succeeded','Failed','Cancelled','TimedOut')",
        rusqlite::params![tenant_id],
        |row| row.get(0),
    )?;
    Ok(count as u64)
}

/// Delete terminal jobs older than `threshold_ms` (epoch milliseconds).
pub(super) fn cleanup_old(c: &rusqlite::Connection, threshold_ms: i64) -> rusqlite::Result<u64> {
    let rows = c.execute(
        "DELETE FROM jobs WHERE state IN ('Succeeded','Failed','Cancelled','TimedOut')
         AND updated_at < ?1",
        rusqlite::params![threshold_ms],
    )?;
    Ok(rows as u64)
}

/// Find a job by its idempotency key within a tenant.
pub(super) fn find_by_idempotency_key(
    c: &rusqlite::Connection,
    tenant_id: &str,
    key: &str,
) -> rusqlite::Result<Option<Job>> {
    let mut stmt = c.prepare(
        "SELECT full_json FROM jobs WHERE tenant_id = ?1 AND idempotency_key = ?2 LIMIT 1",
    )?;
    let mut rows = stmt.query(rusqlite::params![tenant_id, key])?;
    if let Some(row) = rows.next()? {
        let json: String = row.get(0)?;
        Ok(Some(from_json(&json)?))
    } else {
        Ok(None)
    }
}
