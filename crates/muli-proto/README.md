# muli-proto

Protobuf definitions and generated gRPC code for the Muli job execution system.

## Overview

This crate compiles the `.proto` files in [`proto/muli/v1/`](../../proto/muli/v1/) into Rust types and gRPC service stubs using `tonic-build` and `prost-build`. Other crates depend on `muli-proto` for all gRPC client and server types.

## gRPC Services

### JobService
- `SubmitJob` — Submit a containerized job with image, command, env vars, and resource limits
- `GetJobStatus` / `GetDetailedJobStatus` — Query current job state
- `CancelJob` / `DeleteJob` — Cancel a running job or remove a completed one
- `ListJobs` — List jobs with optional state/tenant filtering
- `WatchJobStatus` — Server-streaming real-time status updates

### AgentService
- `RegisterAgent` / `DeregisterAgent` — Agent registration lifecycle
- `Heartbeat` — Periodic agent check-in; returns `JobAssignment` list
- `ReportJobResult` — Agent reports final execution result
- `StreamJobLogs` — Client-streaming log upload from agent to server

### LogService
- `StreamLogs` — Server-streaming live log tailing for a job
- `GetLogs` — Fetch stored log entries

### HealthService
- `Check` / `Watch` — Standard gRPC health checking protocol

### RegistryService
- `CreateRegistryToken` — Generate a scoped token for registry access
- `ListRegistryTokens` — List token metadata for a tenant
- `RevokeRegistryToken` — Revoke a token
- `RotateRegistryToken` — Create replacement token with grace period on old
- `GetRegistryUsage` — Current storage usage for a tenant
- `SetTenantQuota` / `GetTenantQuota` — Configure per-tenant storage limits

## Key Message Types

- `JobState`, `PriorityTier` — Enums matching `muli-core` domain types
- `EnvVar`, `ResourceSpec`, `RegistryCredentials` — Job specification components
- `AgentCapabilities`, `JobAssignment` — Agent communication types
- `LogEntry`, `LogStream` — Log streaming types
- `RegistryPermission`, `RegistryTokenInfo` — Registry authentication types
- `TenantQuota` related request/response types — Registry quota management

## Code Generation

Proto compilation runs automatically via `build.rs` during `cargo build`. To regenerate after editing `.proto` files:

```bash
cargo build -p muli-proto
```

Requires `protoc` to be installed.

## Usage

```toml
[dependencies]
muli-proto = { path = "../muli-proto" }
```

Access generated types:

```rust
use muli_proto::muli::v1::*;
```

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
