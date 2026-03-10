# muli-engine

Docker-based job execution engine for the Muli system.

## Overview

This crate handles the full lifecycle of running a containerized job: image pulling, network creation, volume setup, container execution with resource limits, log collection, timeout enforcement, and cleanup.

## Key Components

### DockerExecutor

The primary entry point. Orchestrates job execution through these steps:

1. Pull the Docker image (with retry and 10-minute timeout)
2. Create an isolated network for the job
3. Create a workspace volume
4. Create a hardened container with CPU/memory limits, environment variables, and command
5. Start log collection in the background
6. Start the container
7. Wait for completion with timeout enforcement
8. Clean up infrastructure (container, network, volume)

### Container Security

Containers are created with the following hardening by default:

- **Capabilities**: All dropped (`--cap-drop=ALL`)
- **Privileges**: `--security-opt=no-new-privileges`, `privileged=false`
- **Filesystem**: Read-only root filesystem with writable `/workspace` mount and `/tmp` tmpfs (64MB)
- **PID limit**: 256 (prevents fork bombs)
- **Network**: Isolated bridge network per job
- **Resources**: CPU and memory limits enforced via Docker cgroups

### ResourceManager

Tracks total CPU and memory allocation across all running jobs. Prevents overallocation by issuing RAII permits that automatically release resources when a job finishes.

### Docker Modules

- `docker/client` — Bollard Docker client wrapper
- `docker/image` — Image pulling and verification
- `docker/container` — Container creation, start, and wait
- `docker/network` — Per-job network isolation
- `docker/volume` — Workspace directory management
- `docker/logs` — Real-time log collection and streaming
- `docker/cleanup` — Garbage collection of old containers and networks

## Usage

```toml
[dependencies]
muli-engine = { path = "../muli-engine" }
```

Used by both `muli-server` (local execution) and `muli-agent` (remote execution). Requires a running Docker daemon.

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
