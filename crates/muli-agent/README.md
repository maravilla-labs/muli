# muli-agent

Distributed worker agent binary for the Muli job execution system.

## Overview

`muli-agent` is a standalone binary that connects to a `muli-server` instance, registers itself as an available worker, and executes containerized jobs on its local Docker daemon.

## Agent Lifecycle

1. **Register** — Connects to the server and advertises its capabilities (CPU, memory, concurrency, labels)
2. **Heartbeat** — Sends periodic heartbeats; the server responds with job assignments
3. **Execute** — Runs assigned jobs using `muli-engine`'s `DockerExecutor`
4. **Stream logs** — Sends real-time container logs back to the server
5. **Report** — Reports job results (success/failure) to the server
6. **Deregister** — Gracefully deregisters on shutdown (Ctrl+C)

## CLI Arguments

All configuration is via command-line flags (powered by `clap`). Flags marked with `env` can also be set via environment variables.

| Flag | Default | Env Var | Description |
|------|---------|---------|-------------|
| `--name` | `agent-1` | | Unique agent identifier |
| `--server-url` | `http://127.0.0.1:50051` | | gRPC server endpoint |
| `--heartbeat-interval-secs` | `10` | | Seconds between heartbeats |
| `--max-concurrent-jobs` | `4` | | Max parallel job executions |
| `--total-cpu-millicores` | `4000` | | CPU capacity to advertise |
| `--total-memory-bytes` | `8589934592` | | Memory capacity to advertise (8 GiB) |
| `--labels` | _(empty)_ | | Comma-separated labels (e.g. `gpu=true,region=us-west`) |
| `--shutdown-timeout-secs` | `60` | | Seconds to wait for running jobs on shutdown |
| `--api-key` | _(unset)_ | `MULI_API_KEY` | Bearer token for server authentication |
| `--tls-ca-cert` | _(unset)_ | `MULI_TLS_CA_CERT_PATH` | Custom CA certificate (PEM) for TLS verification |

## Running

```bash
# Development (no auth)
cargo run --bin muli-agent -- --name my-agent --server-url http://localhost:50051

# Production with auth and TLS
./target/release/muli-agent \
  --name worker-1 \
  --server-url https://muli-server:50051 \
  --api-key <your-key> \
  --tls-ca-cert /path/to/ca.pem \
  --max-concurrent-jobs 8 \
  --total-cpu-millicores 8000 \
  --total-memory-bytes 17179869184 \
  --labels "region=us-west,pool=gpu"
```

## Resilience

- **Connection retry**: On startup, the agent retries connecting to the server with exponential backoff (1s to 30s, up to 10 attempts).
- **Heartbeat retry**: On heartbeat failures, applies exponential backoff. After 10 consecutive failures, attempts re-registration with the server.
- **Graceful shutdown**: On Ctrl+C/SIGTERM, stops accepting new jobs, waits for running jobs to complete (up to `--shutdown-timeout-secs`), then deregisters from the server.

## Modules

- `registration` — Register/deregister with server (with connection retry)
- `capabilities` — Build `AgentCapabilities` from config and current state
- `heartbeat` — Periodic heartbeat loop with retry and re-registration
- `worker` — Docker job execution via `DockerExecutor`, log streaming, result reporting
- `auth` — Client-side authentication interceptor for gRPC requests

## Requirements

- Running Docker daemon on the agent host
- Network connectivity to the `muli-server` gRPC endpoint

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
