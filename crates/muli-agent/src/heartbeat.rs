// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Periodic heartbeat and job assignment polling.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use muli_proto::{HeartbeatRequest, JobAssignment};

use crate::AgentClient;
use crate::capabilities::build_capabilities;
use crate::config::AgentConfig;
use crate::registration;

const MAX_CONSECUTIVE_FAILURES: u32 = 10;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Run the heartbeat loop. Sends periodic heartbeats and forwards any
/// job assignments received in the response to the worker channel.
/// On consecutive failures, applies exponential backoff. After too many
/// failures, attempts re-registration.
pub async fn heartbeat_loop(
    mut client: AgentClient,
    agent_id: String,
    config: AgentConfig,
    running_jobs: Arc<AtomicU32>,
    assignment_tx: mpsc::Sender<JobAssignment>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    let interval = Duration::from_secs(config.heartbeat_interval_secs);
    let mut ticker = tokio::time::interval(interval);
    let mut consecutive_failures: u32 = 0;
    let mut current_agent_id = agent_id;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // If in backoff due to failures, wait additional time
                if consecutive_failures > 0 {
                    let backoff = INITIAL_BACKOFF
                        .saturating_mul(1 << consecutive_failures.min(5))
                        .min(MAX_BACKOFF);
                    tokio::time::sleep(backoff).await;
                }

                let current_running = running_jobs.load(Ordering::Relaxed);
                let caps = build_capabilities(&config, current_running);

                match client
                    .heartbeat(HeartbeatRequest {
                        agent_id: current_agent_id.clone(),
                        capabilities: Some(caps),
                    })
                    .await
                {
                    Ok(response) => {
                        if consecutive_failures > 0 {
                            info!(
                                previous_failures = consecutive_failures,
                                "heartbeat recovered"
                            );
                        }
                        consecutive_failures = 0;

                        let resp = response.into_inner();
                        for assignment in resp.assignments {
                            info!(job_id = %assignment.job_id, "received job assignment");
                            if let Err(e) = assignment_tx.send(assignment).await {
                                error!(error = %e, "failed to forward job assignment");
                            }
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        warn!(
                            error = %e,
                            consecutive_failures = consecutive_failures,
                            "heartbeat failed"
                        );

                        // After too many failures, attempt re-registration
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                            warn!("too many consecutive heartbeat failures, attempting re-registration");
                            match registration::register(&config).await {
                                Ok((new_client, new_id)) => {
                                    client = new_client;
                                    current_agent_id = new_id;
                                    consecutive_failures = 0;
                                    info!("re-registration successful");
                                }
                                Err(re) => {
                                    error!(error = %re, "re-registration failed");
                                }
                            }
                        }
                    }
                }
            }
            _ = shutdown.recv() => {
                info!("heartbeat loop shutting down");
                break;
            }
        }
    }

    Ok(())
}
