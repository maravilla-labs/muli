// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Container creation, start, wait, and removal.

use std::collections::HashMap;

use base64::Engine;
use bollard::container::{
    Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions,
    StopContainerOptions, WaitContainerOptions,
};
use bollard::models::{HostConfig, Mount, MountTypeEnum};
use futures::StreamExt;
use tracing::{debug, info};

use muli_core::error::{MuliError, Result};
use muli_core::job::model::JobSpec;
use muli_core::resource::limits::DockerResourceLimits;

use super::client::DockerClient;

pub const SUBSTEP_START_MARKER: &str = "__MULI_SUBSTEP_START__";
pub const SUBSTEP_FINISH_MARKER: &str = "__MULI_SUBSTEP_FINISH__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerWaitOutcome {
    pub exit_code: Option<i64>,
    pub wait_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStateSnapshot {
    pub status: Option<String>,
    pub exit_code: Option<i64>,
    pub oom_killed: Option<bool>,
    pub runtime_error: Option<String>,
}

/// Create a container for a job.
pub async fn create_container(
    docker: &DockerClient,
    job_id: &str,
    spec: &JobSpec,
    limits: &DockerResourceLimits,
    workspace_path: &str,
    network_id: Option<&str>,
) -> Result<String> {
    let container_name = format!("muli-{job_id}");

    let mut labels = HashMap::new();
    labels.insert("managed-by".to_string(), "muli".to_string());
    labels.insert("job-id".to_string(), job_id.to_string());
    labels.insert("tenant-id".to_string(), spec.tenant_id.clone());

    let mut env: Vec<String> = spec
        .env_vars
        .iter()
        .map(|e| format!("{}={}", e.name, e.value))
        .collect();
    // Suppress progress bars/spinners from build tools (cargo, npm, etc.)
    // when running with TTY mode enabled.
    env.push("TERM=dumb".to_string());

    let workspace_mount = Mount {
        target: Some("/workspace".to_string()),
        source: Some(workspace_path.to_string()),
        typ: Some(MountTypeEnum::BIND),
        read_only: Some(false),
        ..Default::default()
    };

    // Provide a writable /tmp via tmpfs since root filesystem is read-only
    let mut tmpfs = HashMap::new();
    tmpfs.insert("/tmp".to_string(), "size=64m".to_string());

    let host_config = HostConfig {
        nano_cpus: Some(limits.nano_cpus),
        memory: Some(limits.memory_bytes),
        mounts: Some(vec![workspace_mount]),
        network_mode: network_id.map(|n| n.to_string()),
        // Security hardening
        cap_drop: Some(vec!["ALL".to_string()]),
        security_opt: Some(vec!["no-new-privileges:true".to_string()]),
        pids_limit: Some(256),
        readonly_rootfs: Some(false),
        privileged: Some(false),
        tmpfs: Some(tmpfs),
        // Allow containers to reach the host machine's services (e.g. git HTTP on :7000).
        // On Linux, `host-gateway` resolves to the Docker bridge IP.
        // Docker Desktop (macOS/Windows) already provides host.docker.internal automatically.
        extra_hosts: Some(vec!["host.docker.internal:host-gateway".to_string()]),
        ..Default::default()
    };

    // If commands are specified (pipeline steps), override the container's CMD
    // to run them as a shell script. Structured jobs-mode substeps emit
    // internal lifecycle markers that are later converted into substep metadata.
    let cmd = if spec.commands.is_empty() && spec.substeps.is_empty() {
        None
    } else {
        let script = build_job_script(spec);
        Some(vec!["/bin/sh".to_string(), "-c".to_string(), script])
    };

    // Enable TTY only when running explicit commands, to force line-buffered output
    // for real-time log streaming. Without commands, the container runs its default
    // entrypoint which may hang if given a TTY (e.g. alpine's /bin/sh).
    let tty = cmd.is_some();

    let config = Config {
        image: Some(spec.runner_image.clone()),
        cmd,
        env: Some(env),
        labels: Some(labels),
        host_config: Some(host_config),
        working_dir: Some("/workspace".to_string()),
        tty: Some(tty),
        ..Default::default()
    };

    let options = CreateContainerOptions {
        name: container_name.as_str(),
        platform: None,
    };

    let response = docker
        .inner()
        .create_container(Some(options), config)
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to create container: {e}")))?;

    info!(
        container_id = %response.id,
        job_id = %job_id,
        image = %spec.runner_image,
        "Container created"
    );

    Ok(response.id)
}

fn build_job_script(spec: &JobSpec) -> String {
    if spec.substeps.is_empty() {
        let mut script = String::from("set -e\n");
        for command in &spec.commands {
            script.push_str(command);
            script.push('\n');
        }
        return script;
    }

    let mut script = String::new();
    for substep in &spec.substeps {
        let encoded_name = base64::engine::general_purpose::STANDARD.encode(&substep.name);
        script.push_str(&format!(
            "printf '%s\\n' '{SUBSTEP_START_MARKER}|{encoded_name}'\n"
        ));
        script.push_str("(\nset -e\n");
        for command in &substep.commands {
            script.push_str(command);
            script.push('\n');
        }
        script.push_str(")\n");
        script.push_str("status=$?\n");
        script.push_str(&format!(
            "printf '%s|%s\\n' '{SUBSTEP_FINISH_MARKER}|{encoded_name}' \"$status\"\n"
        ));
        script.push_str("if [ \"$status\" -ne 0 ]; then\n  exit \"$status\"\nfi\n");
    }
    script
}

/// Start a created container.
pub async fn start_container(docker: &DockerClient, container_id: &str) -> Result<()> {
    docker
        .inner()
        .start_container(container_id, None::<StartContainerOptions<String>>)
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to start container: {e}")))?;

    debug!(container_id = %container_id, "Container started");
    Ok(())
}

/// Stop a container with a graceful timeout.
pub async fn stop_container(
    docker: &DockerClient,
    container_id: &str,
    timeout_secs: i64,
) -> Result<()> {
    let options = StopContainerOptions { t: timeout_secs };

    docker
        .inner()
        .stop_container(container_id, Some(options))
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to stop container: {e}")))?;

    debug!(container_id = %container_id, "Container stopped");
    Ok(())
}

/// Remove a container.
pub async fn remove_container(docker: &DockerClient, container_id: &str) -> Result<()> {
    let options = RemoveContainerOptions {
        force: true,
        v: true,
        ..Default::default()
    };

    docker
        .inner()
        .remove_container(container_id, Some(options))
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to remove container: {e}")))?;

    debug!(container_id = %container_id, "Container removed");
    Ok(())
}

pub async fn inspect_container(
    docker: &DockerClient,
    container_id: &str,
) -> Result<ContainerStateSnapshot> {
    let response = docker
        .inner()
        .inspect_container(container_id, None)
        .await
        .map_err(|e| MuliError::Docker(format!("Failed to inspect container: {e}")))?;

    let state = response.state.unwrap_or_default();
    Ok(ContainerStateSnapshot {
        status: state.status.map(|status| status.to_string()),
        exit_code: state.exit_code,
        oom_killed: state.oom_killed,
        runtime_error: state.error,
    })
}

/// Wait for a container to exit and return either the exit code or the Docker wait error.
pub async fn wait_container(docker: &DockerClient, container_id: &str) -> ContainerWaitOutcome {
    let options = WaitContainerOptions {
        condition: "not-running",
    };

    let mut stream = docker.inner().wait_container(container_id, Some(options));

    if let Some(result) = stream.next().await {
        match result {
            Ok(response) => {
                let exit_code = response.status_code;
                info!(
                    container_id = %container_id,
                    exit_code = exit_code,
                    "Container exited"
                );
                return ContainerWaitOutcome {
                    exit_code: Some(exit_code),
                    wait_error: None,
                };
            }
            Err(bollard::errors::Error::DockerContainerWaitError { code, error, .. }) => {
                return ContainerWaitOutcome {
                    exit_code: None,
                    wait_error: Some(format!("docker wait error code={code}: {error}")),
                };
            }
            Err(e) => {
                return ContainerWaitOutcome {
                    exit_code: None,
                    wait_error: Some(format!("Error waiting for container: {e}")),
                };
            }
        }
    }

    ContainerWaitOutcome {
        exit_code: None,
        wait_error: Some("Container wait stream ended unexpectedly".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{SUBSTEP_FINISH_MARKER, build_job_script};
    use muli_core::job::model::{JobSpec, JobSubstepSpec, PriorityTier};
    use muli_core::resource::limits::ResourceSpec;

    #[test]
    fn build_job_script_formats_substep_finish_marker_with_exit_code_argument() {
        let spec = JobSpec {
            deployment_id: "dep-1".into(),
            project_id: "proj-1".into(),
            workspace_id: "ws-1".into(),
            tenant_id: "tenant".into(),
            env_vars: vec![],
            resources: ResourceSpec::default(),
            priority_tier: PriorityTier::Standard,
            framework: "none".into(),
            idempotency_key: None,
            registry_credentials: None,
            pipeline_step_run_id: None,
            runner_image: "alpine:latest".into(),
            commands: vec![],
            substeps: vec![JobSubstepSpec {
                name: "Build".into(),
                commands: vec!["echo hi".into()],
            }],
            checkout: None,
            artifact_downloads: vec![],
            artifact_upload_paths: vec![],
            artifact_upload_key: None,
        };

        let script = build_job_script(&spec);
        assert!(script.contains("printf '%s|%s\\n'"));
        assert!(script.contains(SUBSTEP_FINISH_MARKER));
        assert!(!script.contains("|%s' \"$status\""));
    }
}
