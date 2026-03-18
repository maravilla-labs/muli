// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed job log store.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::job::model::StoredLogLine;
use muli_core::traits::JobLogStore;

const LOG_COLLECTION: &str = "job_logs";

/// MongoDB-backed persistent log store for completed jobs.
#[derive(Debug, Clone)]
pub struct MongoJobLogStore {
    collection: Collection<mongodb::bson::Document>,
}

impl MongoJobLogStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection(LOG_COLLECTION),
        }
    }
}

#[async_trait]
impl JobLogStore for MongoJobLogStore {
    async fn append_logs(&self, job_id: &str, lines: Vec<StoredLogLine>) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let docs: Vec<mongodb::bson::Document> = lines
            .into_iter()
            .map(|l| {
                doc! {
                    "job_id": job_id,
                    "seq": l.sequence as i64,
                    "stream": l.stream,
                    "line": l.message,
                    "ts_ms": l.timestamp.timestamp_millis(),
                    "substep_name": l.substep_name,
                    "event_type": l.event_type,
                    "exit_code": l.exit_code,
                }
            })
            .collect();
        self.collection
            .insert_many(docs)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to append logs: {e}")))?;
        Ok(())
    }

    async fn get_logs(&self, job_id: &str, tail: usize) -> Result<Vec<StoredLogLine>> {
        let opts = FindOptions::builder()
            .sort(doc! { "seq": -1 })
            .limit(tail as i64)
            .build();
        let cursor = self
            .collection
            .find(doc! { "job_id": job_id })
            .with_options(opts)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get logs: {e}")))?;
        let mut docs: Vec<mongodb::bson::Document> = cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect logs: {e}")))?;
        // Reverse to ascending order
        docs.reverse();
        docs.into_iter()
            .map(|d| {
                let seq = d.get_i64("seq").unwrap_or(0) as u64;
                let stream = d.get_str("stream").unwrap_or("stdout").to_string();
                let message = d.get_str("line").unwrap_or("").to_string();
                let ts_ms = d.get_i64("ts_ms").unwrap_or(0);
                let substep_name = d.get_str("substep_name").ok().map(ToString::to_string);
                let event_type = d.get_str("event_type").ok().map(ToString::to_string);
                let exit_code = d.get_i32("exit_code").ok();
                Ok(StoredLogLine {
                    sequence: seq,
                    stream,
                    message,
                    timestamp: chrono::DateTime::from_timestamp_millis(ts_ms)
                        .unwrap_or_else(chrono::Utc::now),
                    substep_name,
                    event_type,
                    exit_code,
                })
            })
            .collect()
    }
}
