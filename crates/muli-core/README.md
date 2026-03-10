# muli-core

Foundational domain models, traits, and error types for the Muli job execution system.

## Overview

This crate defines the shared vocabulary used across all other Muli crates: job specifications, the job state machine, agent models, resource specifications, and the storage trait interfaces.

## Key Types

### Job Model

- **`Job`** — The central domain entity. Contains a `JobSpec`, current `JobState`, priority score, optional agent assignment, timestamps, and execution result.
- **`JobSpec`** — Describes what to run: Docker image, command, environment variables, resource requirements, timeout, and tenant ID.
- **`JobState`** — Eight-state machine: `Pending → Scheduled → Pulling → Running → Succeeded | Failed | Cancelled | TimedOut`. State transitions are validated to prevent illegal moves.
- **`PriorityTier`** — Four tiers (`Free`, `Standard`, `Premium`, `Enterprise`) with increasing weights used in priority scoring.

### Resource Model

- **`ResourceSpec`** — Kubernetes-compatible resource requests and limits. Parses CPU strings like `"500m"` or `"2"` and memory strings like `"512Mi"` or `"1Gi"`. Uses checked arithmetic to prevent integer overflow (max 1M millicores, max 1TB memory).

### Agent Model

- **`AgentInfo`** — Tracks a registered agent's ID, capabilities, health status, and last heartbeat time.
- **`AgentStatus`** — `Healthy`, `Unhealthy`, or `Dead`.
- **`AgentCapabilities`** — What an agent can offer: max concurrent jobs, CPU/memory capacity, labels.

### Validation

- **`validation`** module — Reusable validation functions for all gRPC inputs: identifiers (tenant/project/workspace IDs), Docker image references, environment variable names, agent names, labels (key=value format), resource spec strings, and general string length limits.

### Error Handling

- **`MuliError`** — Categorized into retryable errors (Docker, ImagePullFailed, Timeout, InsufficientResources, Storage, Grpc) and permanent errors (JobNotFound, InvalidStateTransition, Cancelled, ContainerNotFound).

### Registry Model

- **`RegistryToken`** — Scoped authentication token for registry access. Contains tenant ID, SHA-256 token hash, permissions (Pull/Push/Admin), optional expiration, and revocation status.
- **`RegistryPermission`** — Three levels: `Pull` (read), `Push` (write), `Admin` (delete).
- **`TenantQuota`** — Per-tenant storage quota with current usage tracking and helper methods (`would_exceed`, `remaining_bytes`).

### Traits

- **`JobStore`** — Async trait for job CRUD: create, get, update state, list by state/tenant, cleanup.
- **`AgentRegistry`** — Async trait for agent lifecycle: register, heartbeat, get, list, remove.
- **`RegistryTokenStore`** — Async trait for token management: create, lookup by hash, list by tenant, revoke, set expiry, delete expired.
- **`TenantQuotaStore`** — Async trait for quota management: get, set, update usage.

## Priority Scoring

Jobs are scored with a formula that prevents starvation:

```
score = tier_weight * (10 + minutes_in_queue) / 10
```

Lower-priority jobs gradually increase in score the longer they wait.

## Usage

This crate is a dependency of every other Muli crate. It is not intended to be used standalone.

```toml
[dependencies]
muli-core = { path = "../muli-core" }
```

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
