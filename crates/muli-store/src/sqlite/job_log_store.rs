// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite job log store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::Result;
use muli_core::job::model::StoredLogLine;
use muli_core::traits::JobLogStore;

use super::factory::SqliteStoreFactory;
use super::util::store_err;

/// SQLite-backed persistent log store for completed jobs.
pub struct SqliteJobLogStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteJobLogStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl JobLogStore for SqliteJobLogStore {
    async fn append_logs(&self, job_id: &str, lines: Vec<StoredLogLine>) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let conn = self.factory.global_conn();
        let job_id = job_id.to_string();
        conn.call(move |c| {
            for line in &lines {
                c.execute(
                    "INSERT OR IGNORE INTO job_logs
                     (job_id, seq, stream, line, ts_ms, substep_name, event_type, exit_code)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        job_id,
                        line.sequence as i64,
                        line.stream,
                        line.message,
                        line.timestamp.timestamp_millis(),
                        line.substep_name,
                        line.event_type,
                        line.exit_code,
                    ],
                )?;
            }
            Ok(())
        })
        .await
        .map_err(store_err)
    }

    async fn get_logs(&self, job_id: &str, tail: usize) -> Result<Vec<StoredLogLine>> {
        let conn = self.factory.global_conn();
        let job_id = job_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare(
                "SELECT seq, stream, line, ts_ms, substep_name, event_type, exit_code FROM (
                   SELECT seq, stream, line, ts_ms, substep_name, event_type, exit_code FROM job_logs
                   WHERE job_id = ?1 ORDER BY seq DESC LIMIT ?2
                 ) ORDER BY seq ASC",
            )?;
            let mut rows = stmt.query(rusqlite::params![job_id, tail as i64])?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let seq: i64 = row.get(0)?;
                let stream: String = row.get(1)?;
                let message: String = row.get(2)?;
                let ts_ms: i64 = row.get(3)?;
                let substep_name: Option<String> = row.get(4)?;
                let event_type: Option<String> = row.get(5)?;
                let exit_code: Option<i32> = row.get(6)?;
                result.push(StoredLogLine {
                    sequence: seq as u64,
                    stream,
                    message,
                    timestamp: chrono::DateTime::from_timestamp_millis(ts_ms)
                        .unwrap_or_else(chrono::Utc::now),
                    substep_name,
                    event_type,
                    exit_code,
                });
            }
            Ok(result)
        })
        .await
        .map_err(store_err)
    }
}
