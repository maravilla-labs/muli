// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Real-time container log streaming and collection.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bollard::container::LogsOptions;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use tracing::{debug, error, warn};

use super::client::DockerClient;

const DEFAULT_RING_BUFFER_SIZE: usize = 10_000;
const BROADCAST_CHANNEL_SIZE: usize = 1_024;

/// A single log line with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogLine {
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub stream: LogStream,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Manages log collection for a single container.
pub struct LogCollector {
    buffer: Arc<RwLock<VecDeque<LogLine>>>,
    tx: broadcast::Sender<LogLine>,
    sequence: Arc<AtomicU64>,
    /// Sequence number of the next line that has not yet been flushed to durable storage.
    /// Lines with sequence < flush_offset have already been persisted via `peek_unflushed`.
    flush_offset: Arc<AtomicU64>,
    max_lines: usize,
}

impl Default for LogCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl LogCollector {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_RING_BUFFER_SIZE)
    }

    pub fn with_capacity(max_lines: usize) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        Self {
            buffer: Arc::new(RwLock::new(VecDeque::with_capacity(max_lines))),
            tx,
            sequence: Arc::new(AtomicU64::new(0)),
            flush_offset: Arc::new(AtomicU64::new(0)),
            max_lines,
        }
    }

    /// Start streaming logs from a container. Returns a JoinHandle for the background task.
    pub fn start_log_stream(
        &self,
        docker: DockerClient,
        container_id: String,
    ) -> tokio::task::JoinHandle<()> {
        let buffer = self.buffer.clone();
        let tx = self.tx.clone();
        let sequence = self.sequence.clone();
        let max_lines = self.max_lines;

        tokio::spawn(async move {
            let options = LogsOptions::<String> {
                follow: true,
                stdout: true,
                stderr: true,
                timestamps: true,
                tail: "all".to_string(),
                ..Default::default()
            };

            let mut stream = docker.inner().logs(&container_id, Some(options));

            while let Some(result) = stream.next().await {
                match result {
                    Ok(output) => {
                        let (stream_type, message) = match output {
                            bollard::container::LogOutput::StdOut { message } => {
                                (LogStream::Stdout, message)
                            }
                            bollard::container::LogOutput::StdErr { message } => {
                                (LogStream::Stderr, message)
                            }
                            _ => continue,
                        };

                        let text = String::from_utf8_lossy(&message).to_string();

                        let line = LogLine {
                            sequence: sequence.fetch_add(1, Ordering::SeqCst),
                            timestamp: chrono::Utc::now(),
                            stream: stream_type,
                            message: text,
                        };

                        // Trace container output so it appears in muli's logs
                        match line.stream {
                            LogStream::Stderr => {
                                warn!(job_id = %container_id, "[container:stderr] {}", line.message)
                            }
                            LogStream::Stdout => {
                                debug!(job_id = %container_id, "[container:stdout] {}", line.message)
                            }
                        }

                        // Push to ring buffer
                        {
                            let mut buf = buffer.write().await;
                            if buf.len() >= max_lines {
                                buf.pop_front();
                            }
                            buf.push_back(line.clone());
                        }

                        // Broadcast to live subscribers (ignore if no receivers)
                        let _ = tx.send(line);
                    }
                    Err(e) => {
                        warn!(
                            container_id = %container_id,
                            error = %e,
                            "Error reading container logs"
                        );
                        break;
                    }
                }
            }

            debug!(container_id = %container_id, "Log stream ended");
        })
    }

    /// Subscribe to live log events.
    pub fn subscribe(&self) -> broadcast::Receiver<LogLine> {
        self.tx.subscribe()
    }

    /// Return the next sequence number that would be assigned to a new line.
    pub fn next_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get the last N lines from the ring buffer.
    pub async fn get_historical(&self, tail: usize) -> Vec<LogLine> {
        let buf = self.buffer.read().await;
        let start = buf.len().saturating_sub(tail);
        buf.iter().skip(start).cloned().collect()
    }

    /// Push a log line from an external source (e.g. a remote agent).
    /// The line's sequence number is preserved as-is; no re-sequencing is applied.
    pub async fn push_line(&self, line: LogLine) {
        {
            let mut buf = self.buffer.write().await;
            if buf.len() >= self.max_lines {
                buf.pop_front();
            }
            buf.push_back(line.clone());
        }
        let _ = self.tx.send(line);
    }

    /// One-shot fetch of all container logs (non-follow). Called as a safety net
    /// after the follow stream ends.
    pub async fn fetch_remaining_logs(
        &self,
        docker: &DockerClient,
        container_id: &str,
        container_log_start_seq: u64,
    ) -> Result<usize, String> {
        debug!(container_id = %container_id, "Fetching container logs with one-shot request");

        let options = LogsOptions::<String> {
            follow: false,
            stdout: true,
            stderr: true,
            timestamps: true,
            tail: "all".to_string(),
            ..Default::default()
        };

        let mut stream = docker.inner().logs(container_id, Some(options));
        let mut fetched = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(output) => {
                    let (stream_type, message) = match output {
                        bollard::container::LogOutput::StdOut { message } => {
                            (LogStream::Stdout, message)
                        }
                        bollard::container::LogOutput::StdErr { message } => {
                            (LogStream::Stderr, message)
                        }
                        _ => continue,
                    };

                    fetched.push((stream_type, String::from_utf8_lossy(&message).to_string()));
                }
                Err(e) => {
                    error!(
                        container_id = %container_id,
                        error = %e,
                        "Error reading container logs (one-shot fallback)"
                    );
                    return Err(format!(
                        "Error reading container logs (one-shot fallback): {e}"
                    ));
                }
            }
        }

        let existing = self.snapshot_from(container_log_start_seq).await;
        let overlap = overlap_len(&existing, &fetched);
        let mut appended = 0usize;

        for (stream, message) in fetched.into_iter().skip(overlap) {
            let line = LogLine {
                sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
                timestamp: chrono::Utc::now(),
                stream,
                message,
            };

            match line.stream {
                LogStream::Stderr => {
                    warn!(job_id = %container_id, "[container:stderr] {}", line.message)
                }
                LogStream::Stdout => {
                    debug!(job_id = %container_id, "[container:stdout] {}", line.message)
                }
            }

            self.push_line(line).await;
            appended += 1;
        }

        Ok(appended)
    }

    /// Return all lines buffered since the last call to `peek_unflushed`, without clearing the
    /// ring buffer. Advances the flush offset so the same lines are not returned twice.
    /// Safe to call concurrently with log streaming — does not remove lines from the buffer.
    pub async fn peek_unflushed(&self) -> Vec<LogLine> {
        let offset = self.flush_offset.load(Ordering::SeqCst);
        let buf = self.buffer.read().await;
        let new_lines: Vec<LogLine> = buf
            .iter()
            .filter(|l| l.sequence >= offset)
            .cloned()
            .collect();
        if let Some(last) = new_lines.last() {
            self.flush_offset.store(last.sequence + 1, Ordering::SeqCst);
        }
        new_lines
    }

    /// Drain lines that have not yet been flushed via `peek_unflushed`, then clear the buffer.
    /// Used on job completion to capture any remaining lines after the last periodic flush.
    pub async fn drain(&self) -> Vec<LogLine> {
        let offset = self.flush_offset.load(Ordering::SeqCst);
        let mut buf = self.buffer.write().await;
        let remaining: Vec<LogLine> = buf
            .iter()
            .filter(|l| l.sequence >= offset)
            .cloned()
            .collect();
        buf.clear();
        remaining
    }

    async fn snapshot_from(&self, start_sequence: u64) -> Vec<(LogStream, String)> {
        let buf = self.buffer.read().await;
        buf.iter()
            .filter(|line| line.sequence >= start_sequence)
            .map(|line| (line.stream, line.message.clone()))
            .collect()
    }
}

fn overlap_len(existing: &[(LogStream, String)], fetched: &[(LogStream, String)]) -> usize {
    let max_overlap = existing.len().min(fetched.len());
    for overlap in (0..=max_overlap).rev() {
        if existing[existing.len().saturating_sub(overlap)..] == fetched[..overlap] {
            return overlap;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::{LogStream, overlap_len};

    #[test]
    fn overlap_detects_full_duplicate_fetch() {
        let lines = vec![
            (LogStream::Stdout, "one".to_string()),
            (LogStream::Stderr, "two".to_string()),
        ];
        assert_eq!(overlap_len(&lines, &lines), 2);
    }

    #[test]
    fn overlap_detects_suffix_prefix_match() {
        let existing = vec![
            (LogStream::Stdout, "one".to_string()),
            (LogStream::Stdout, "two".to_string()),
            (LogStream::Stdout, "three".to_string()),
        ];
        let fetched = vec![
            (LogStream::Stdout, "two".to_string()),
            (LogStream::Stdout, "three".to_string()),
            (LogStream::Stdout, "four".to_string()),
        ];
        assert_eq!(overlap_len(&existing, &fetched), 2);
    }

    #[test]
    fn overlap_returns_zero_without_match() {
        let existing = vec![(LogStream::Stdout, "one".to_string())];
        let fetched = vec![(LogStream::Stdout, "two".to_string())];
        assert_eq!(overlap_len(&existing, &fetched), 0);
    }
}
