# muli-server

The central control plane binary for the Muli job execution system.

## Overview

`muli-server` is the main server binary. It hosts gRPC services for job management, agent coordination, log streaming, and health checks. It also runs a Prometheus metrics endpoint and optionally an embedded OCI registry.

## Startup Sequence

1. Load configuration from `MULI_*` environment variables
2. Connect to Docker daemon
3. Initialize storage (MongoDB or in-memory) and create database indexes
4. Create the resource manager and Docker executor
5. Start the scheduler background loop (with cancellation token for graceful shutdown)
6. Start the cleanup service
7. Optionally start the embedded registry
8. Start gRPC server (with optional TLS and authentication interceptor)
9. Start metrics HTTP server

## Security

- **Authentication**: When `MULI_API_KEY` is set, an interceptor validates `Authorization: Bearer <key>` on all gRPC requests. Disabled when unset.
- **TLS**: When `MULI_TLS_CERT_PATH` and `MULI_TLS_KEY_PATH` are set, the gRPC server uses TLS. Falls back to plaintext otherwise.
- **Tenant Authorization**: All job operations verify the requesting tenant owns the resource. Agents can only report results for jobs assigned to them.
- **Input Validation**: All gRPC request fields are validated (IDs, image references, env vars, resource specs, labels).
- **Audit Logging**: All mutating gRPC operations emit structured log events with operation, tenant, and resource IDs.
- **Rate Limiting**: gRPC server enforces per-connection concurrency limits and request timeouts. Watch streams have a maximum duration (1 hour).

## gRPC Service Implementations

- **`JobServiceImpl`** — Handles job submission, status queries, cancellation, deletion, listing, and streaming status updates. All operations are tenant-scoped.
- **`AgentServiceImpl`** — Manages agent registration, heartbeats (with job assignment dispatch), result reporting, and log streaming. Validates agent identity on result reporting.
- **`LogServiceImpl`** — Serves stored and live-streamed job logs. Log line count is configurable via `MULI_MAX_LOG_LINES`.
- **`HealthServiceImpl`** — Standard gRPC health checking.
- **`RegistryServiceImpl`** — Manages per-tenant registry tokens (create, list, revoke, rotate) and storage quotas. Token hashes are stored via SHA-256; plaintext is returned only on creation. Token rotation creates a new token and sets a configurable grace period on the old one. Expired tokens are cleaned up automatically every hour.

## Graceful Shutdown

On SIGTERM/SIGINT, the server:
1. Signals all services via a cancellation token
2. Stops accepting new gRPC connections
3. Waits for the scheduler to drain (up to `MULI_SHUTDOWN_TIMEOUT_SECONDS`, default 30s)
4. Shuts down HTTP servers gracefully
5. Exits

## Running

```bash
# With defaults from .env
cargo run --bin muli-server

# Or the release binary
./target/release/muli-server
```

Default ports:
- gRPC: `50051`
- Metrics: `9090`
- Registry: `5000` (if enabled)

See the [root README](../../README.md) for the full configuration reference.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
