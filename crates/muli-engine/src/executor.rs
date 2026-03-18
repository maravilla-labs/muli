// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! High-level job executor orchestrating the full container lifecycle.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info};

use muli_core::error::{MuliError, Result};
use muli_core::job::artifact_handler::ArtifactHandler;
use muli_core::job::model::{Job, JobResult};
use muli_core::resource::limits::parse_cpu_millicores;
use muli_core::resource::limits::parse_memory_bytes;

use crate::docker::client::DockerClient;
use crate::docker::logs::LogCollector;
use crate::docker::{checkout, container, image, network, volume};
use crate::resource_manager::ResourceManager;
use crate::timeout::enforce_timeout;

/// High-level job executor that orchestrates the full container lifecycle.
pub struct DockerExecutor {
    docker: DockerClient,
    resource_manager: Arc<ResourceManager>,
    artifact_handler: Option<Arc<dyn ArtifactHandler>>,
}

impl DockerExecutor {
    pub fn new(docker: DockerClient, resource_manager: Arc<ResourceManager>) -> Self {
        Self {
            docker,
            resource_manager,
            artifact_handler: None,
        }
    }

    /// Attach an artifact handler for upload/restore operations.
    pub fn with_artifact_handler(mut self, handler: Arc<dyn ArtifactHandler>) -> Self {
        self.artifact_handler = Some(handler);
        self
    }

    /// Execute a job end-to-end: pull image, create network, create container,
    /// start, monitor, collect logs, and clean up.
    ///
    /// The provided `log_collector` is used to stream container logs; the caller
    /// is responsible for draining and persisting it after this call returns.
    pub async fn execute_job(
        &self,
        job: &Job,
        log_collector: Arc<LogCollector>,
    ) -> Result<JobResult> {
        let job_id = &job.id;
        let spec = &job.spec;

        info!(job_id = %job_id, image = %spec.runner_image, "Starting job execution");

        // Parse resource requirements
        let cpu_millicores = parse_cpu_millicores(&spec.resources.cpu_limit)?;
        let memory_bytes = parse_memory_bytes(&spec.resources.memory_limit)?;

        // Allocate resources
        let _allocation =
            self.resource_manager
                .try_allocate(job_id, cpu_millicores, memory_bytes)?;

        // Ensure cleanup on any exit path
        let result = self.execute_inner(job, log_collector).await;

        // Release resources
        self.resource_manager.release(job_id);

        // Cleanup infrastructure (best-effort)
        self.cleanup(job_id).await;

        result
    }

    async fn execute_inner(
        &self,
        job: &Job,
        log_collector: Arc<LogCollector>,
    ) -> Result<JobResult> {
        let job_id = &job.id;
        let spec = &job.spec;
        let timeout_duration = Duration::from_secs(spec.resources.timeout_seconds);

        // 1. Ensure image is available
        image::ensure_image(
            &self.docker,
            &spec.runner_image,
            spec.registry_credentials.as_ref(),
        )
        .await?;

        // 2. Create network
        let network_id = network::create_network(&self.docker, job_id).await?;

        // 3. Create workspace directory
        let workspace_path = volume::create_temp_dir(job_id)?;
        let workspace_str = workspace_path.to_string_lossy().to_string();

        // 3a. Host-side git checkout (before artifact restore so artifacts overlay the tree)
        if let Some(checkout_spec) = &spec.checkout {
            checkout::perform_checkout(checkout_spec, &workspace_path, &log_collector).await?;
        }

        // 3b. Restore artifacts from dependency jobs
        if !spec.artifact_downloads.is_empty() {
            if let Some(handler) = &self.artifact_handler {
                let needed_jobs: Vec<&str> = spec
                    .artifact_downloads
                    .iter()
                    .map(|d| d.job_name.as_str())
                    .collect();
                // Use the run_id from the first download (they all share the same run)
                let run_id = spec
                    .artifact_downloads
                    .first()
                    .map(|d| d.run_id.as_str())
                    .unwrap_or(&spec.deployment_id);
                handler
                    .restore_artifacts(&spec.tenant_id, run_id, &needed_jobs, &workspace_path)
                    .await?;
            }
        }

        // 4. Parse docker resource limits
        let docker_limits = spec.resources.to_docker_limits()?;

        // 5. Create container
        let container_id = container::create_container(
            &self.docker,
            job_id,
            spec,
            &docker_limits,
            &workspace_str,
            Some(&network_id),
        )
        .await?;

        // 6. Start log collector using the provided (externally-owned) collector
        let container_log_start_seq = log_collector.next_sequence();
        let log_handle = log_collector.start_log_stream(self.docker.clone(), container_id.clone());

        // 7. Start container
        container::start_container(&self.docker, &container_id).await?;

        // 8. Wait for container with timeout
        let wait_result = tokio::time::timeout(
            timeout_duration,
            container::wait_container(&self.docker, &container_id),
        )
        .await;

        // Wait for log stream to finish naturally (Docker closes stream when container exits)
        match tokio::time::timeout(Duration::from_secs(5), log_handle).await {
            Ok(Ok(_)) => info!(job_id = %job_id, "Log stream ended"),
            Ok(Err(e)) => tracing::warn!(job_id = %job_id, error = %e, "Log stream task panicked"),
            Err(_) => {
                tracing::warn!(job_id = %job_id, "Log stream did not finish within 5s after container exit");
            }
        }

        match wait_result {
            Ok(wait_outcome) => {
                let inspect_snapshot = match container::inspect_container(
                    &self.docker,
                    &container_id,
                )
                .await
                {
                    Ok(snapshot) => Some(snapshot),
                    Err(e) => {
                        error!(job_id = %job_id, container_id = %container_id, error = %e, "Failed to inspect container after wait");
                        None
                    }
                };

                let should_fetch_remaining_logs = log_collector.next_sequence()
                    == container_log_start_seq
                    || wait_outcome.wait_error.is_some()
                    || wait_outcome.exit_code != Some(0)
                    || inspect_snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.exit_code)
                        .unwrap_or(0)
                        != 0;
                let log_fetch_error = if should_fetch_remaining_logs {
                    match log_collector
                        .fetch_remaining_logs(&self.docker, &container_id, container_log_start_seq)
                        .await
                    {
                        Ok(_) => None,
                        Err(e) => {
                            error!(
                                job_id = %job_id,
                                container_id = %container_id,
                                error = %e,
                                "Failed to fetch final container logs"
                            );
                            Some(e)
                        }
                    }
                } else {
                    None
                };

                if let Some(wait_error) = wait_outcome.wait_error.as_deref()
                    && let Some(snapshot) = inspect_snapshot.as_ref()
                    && snapshot.exit_code.is_some()
                {
                    error!(
                        job_id = %job_id,
                        container_id = %container_id,
                        wait_error,
                        inspected_exit_code = snapshot.exit_code,
                        inspected_status = snapshot.status.as_deref().unwrap_or("unknown"),
                        oom_killed = snapshot.oom_killed.unwrap_or(false),
                        "Docker wait returned an error but inspect reported a concrete exit code"
                    );
                }
                if let (Some(wait_exit), Some(snapshot)) =
                    (wait_outcome.exit_code, inspect_snapshot.as_ref())
                    && let Some(inspected_exit) = snapshot.exit_code
                    && wait_exit != inspected_exit
                {
                    error!(
                        job_id = %job_id,
                        container_id = %container_id,
                        wait_exit_code = wait_exit,
                        inspected_exit_code = inspected_exit,
                        inspected_status = snapshot.status.as_deref().unwrap_or("unknown"),
                        oom_killed = snapshot.oom_killed.unwrap_or(false),
                        "Docker wait/inspect exit code mismatch"
                    );
                }

                let resolved = resolve_container_outcome(
                    &wait_outcome,
                    inspect_snapshot.as_ref(),
                    log_fetch_error.as_deref(),
                )?;
                let exit_code_i32 = resolved.exit_code.unwrap_or(0);
                let message = resolved.message;

                // 9. Upload artifacts if exit 0 and paths configured
                if exit_code_i32 == 0 && !spec.artifact_upload_paths.is_empty() {
                    if let (Some(handler), Some(key)) =
                        (&self.artifact_handler, &spec.artifact_upload_key)
                    {
                        if let Err(e) = handler
                            .upload_artifacts(
                                &spec.tenant_id,
                                &spec.deployment_id,
                                key,
                                &workspace_path,
                                &spec.artifact_upload_paths,
                            )
                            .await
                        {
                            tracing::warn!(
                                job_id = %job_id,
                                error = %e,
                                "artifact upload failed (job still succeeded)"
                            );
                        }
                    }
                }

                info!(
                    job_id = %job_id,
                    exit_code = exit_code_i32,
                    "Job execution finished"
                );

                Ok(JobResult {
                    exit_code: Some(exit_code_i32),
                    message,
                    container_id: Some(container_id),
                })
            }
            Err(_) => {
                // Timeout - enforce stop then return the timeout error
                enforce_timeout(&self.docker, &container_id, timeout_duration).await?;
                unreachable!("enforce_timeout always returns Err")
            }
        }
    }

    /// Best-effort cleanup of job infrastructure.
    async fn cleanup(&self, job_id: &str) {
        let container_name = format!("muli-{job_id}");

        // Remove container
        if let Err(e) = container::remove_container(&self.docker, &container_name).await {
            tracing::debug!(job_id = %job_id, error = %e, "Cleanup: container removal failed (may already be removed)");
        }

        // Remove network
        if let Err(e) = network::remove_network(&self.docker, job_id).await {
            tracing::debug!(job_id = %job_id, error = %e, "Cleanup: network removal failed (may already be removed)");
        }

        // Remove temp directory
        if let Err(e) = volume::cleanup_temp_dir(job_id) {
            tracing::debug!(job_id = %job_id, error = %e, "Cleanup: temp dir removal failed");
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedContainerOutcome {
    exit_code: Option<i32>,
    message: String,
}

fn resolve_container_outcome(
    wait_outcome: &container::ContainerWaitOutcome,
    inspect_snapshot: Option<&container::ContainerStateSnapshot>,
    log_fetch_error: Option<&str>,
) -> Result<ResolvedContainerOutcome> {
    let inspected_exit_code = inspect_snapshot.and_then(|snapshot| snapshot.exit_code);
    let effective_exit_code = inspected_exit_code.or(wait_outcome.exit_code);

    if let Some(exit_code) = effective_exit_code {
        let exit_code_i32 = exit_code as i32;
        if exit_code_i32 == 0 {
            return Ok(ResolvedContainerOutcome {
                exit_code: Some(0),
                message: "Job completed successfully".to_string(),
            });
        }

        return Ok(ResolvedContainerOutcome {
            exit_code: Some(exit_code_i32),
            message: format_failure_message(
                Some(exit_code_i32),
                wait_outcome.wait_error.as_deref(),
                inspect_snapshot,
                log_fetch_error,
            ),
        });
    }

    Err(MuliError::Docker(format_failure_message(
        None,
        wait_outcome.wait_error.as_deref(),
        inspect_snapshot,
        log_fetch_error,
    )))
}

fn format_failure_message(
    exit_code: Option<i32>,
    wait_error: Option<&str>,
    inspect_snapshot: Option<&container::ContainerStateSnapshot>,
    log_fetch_error: Option<&str>,
) -> String {
    let mut details = Vec::new();
    if let Some(wait_error) = wait_error {
        details.push(format!("docker_wait_error={wait_error}"));
    }
    if let Some(snapshot) = inspect_snapshot {
        if let Some(status) = snapshot.status.as_deref() {
            details.push(format!("container_status={status}"));
        }
        if let Some(inspected_exit_code) = snapshot.exit_code {
            details.push(format!("container_exit_code={inspected_exit_code}"));
        }
        if let Some(oom_killed) = snapshot.oom_killed {
            details.push(format!("oom_killed={oom_killed}"));
        }
        if let Some(runtime_error) = snapshot.runtime_error.as_deref()
            && !runtime_error.is_empty()
        {
            details.push(format!("container_error={runtime_error}"));
        }
    }
    if let Some(log_fetch_error) = log_fetch_error {
        details.push(format!("log_fetch_error={log_fetch_error}"));
    }

    match exit_code {
        Some(code) if details.is_empty() => format!("Job failed with exit code {code}"),
        Some(code) => format!("Job failed with exit code {code} ({})", details.join(", ")),
        None if details.is_empty() => "Container execution failed".to_string(),
        None => format!("Container execution failed ({})", details.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::{format_failure_message, resolve_container_outcome};
    use crate::docker::container::{ContainerStateSnapshot, ContainerWaitOutcome};

    #[test]
    fn wait_error_uses_inspected_exit_code_when_available() {
        let wait_outcome = ContainerWaitOutcome {
            exit_code: None,
            wait_error: Some("docker wait error code=243: transport failure".to_string()),
        };
        let inspect_snapshot = ContainerStateSnapshot {
            status: Some("exited".to_string()),
            exit_code: Some(2),
            oom_killed: Some(false),
            runtime_error: None,
        };

        let resolved =
            resolve_container_outcome(&wait_outcome, Some(&inspect_snapshot), None).unwrap();

        assert_eq!(resolved.exit_code, Some(2));
        assert!(
            resolved
                .message
                .contains("docker_wait_error=docker wait error code=243"),
            "{}",
            resolved.message
        );
        assert!(resolved.message.contains("container_exit_code=2"));
    }

    #[test]
    fn wait_error_without_inspected_exit_code_is_docker_error() {
        let wait_outcome = ContainerWaitOutcome {
            exit_code: None,
            wait_error: Some("docker wait error code=243: transport failure".to_string()),
        };
        let inspect_snapshot = ContainerStateSnapshot {
            status: Some("dead".to_string()),
            exit_code: None,
            oom_killed: Some(false),
            runtime_error: Some("shim disconnected".to_string()),
        };

        let err = resolve_container_outcome(&wait_outcome, Some(&inspect_snapshot), None)
            .expect_err("expected docker execution error");
        let message = err.to_string();
        assert!(message.contains("docker wait error code=243"), "{message}");
        assert!(message.contains("container_status=dead"), "{message}");
        assert!(
            message.contains("container_error=shim disconnected"),
            "{message}"
        );
    }

    #[test]
    fn failure_message_includes_log_fetch_error() {
        let message = format_failure_message(
            Some(2),
            None,
            Some(&ContainerStateSnapshot {
                status: Some("exited".to_string()),
                exit_code: Some(2),
                oom_killed: Some(false),
                runtime_error: None,
            }),
            Some("docker logs failed"),
        );
        assert!(
            message.contains("log_fetch_error=docker logs failed"),
            "{message}"
        );
    }
}
