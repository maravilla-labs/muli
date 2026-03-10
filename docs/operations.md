# Muli Operations Runbook

This document covers day-to-day operations for a production muli deployment.

---

## Table of Contents

1. [Config File](#1-config-file)
2. [Environment Variables](#2-environment-variables)
3. [Startup and Shutdown](#3-startup-and-shutdown)
4. [Backup and Restore](#4-backup-and-restore)
5. [Disaster Recovery](#5-disaster-recovery)
6. [Monitoring and Alerting](#6-monitoring-and-alerting)
7. [Token Rotation](#7-token-rotation)
8. [Auth Hardening Checklist](#8-auth-hardening-checklist)

---

## 1. Config File

muli supports TOML config files in addition to environment variables.  Configuration sources are merged in the following priority order (highest wins):

```
compiled-in defaults
  < /etc/muli/config.toml          (system install)
  < ~/.config/muli/config.toml     (XDG user install)
  < ./muli.toml                    (local / dev — current working directory)
  < path in MULI_CONFIG env var    (explicit override)
  < MULI_* environment variables   (always win)
```

Config-file keys match the `MULI_*` env var names minus the prefix, lowercased:

```toml
# muli.toml
grpc_port = 50051
api_key = "YOUR_API_KEY_HERE"
require_auth = true
data_dir = "/var/lib/muli"
git_enabled = true
registry_enabled = true
```

All env vars continue to work identically and always take highest priority.

---

## 2. Environment Variables

### Config file path

| Variable | Description | Default |
|---|---|---|
| `MULI_CONFIG` | Path to an additional TOML config file (highest file priority) | *(none)* |

### Required in production

| Variable | Description | Default |
|---|---|---|
| `MULI_API_KEY` | Bearer token required for all gRPC requests | *(none — dev mode)* |
| `MULI_REQUIRE_AUTH` | Refuse to start if `MULI_API_KEY` is unset | `false` |
| `MULI_DATA_DIR` | Root directory for all persistent data (SQLite, registry, git) | `./data` |

### Networking

| Variable | Description | Default |
|---|---|---|
| `MULI_GRPC_PORT` | gRPC server port | `50051` |
| `MULI_METRICS_PORT` | Prometheus metrics HTTP port | `9090` |
| `MULI_TLS_CERT_PATH` | PEM certificate for gRPC TLS | *(none — plaintext)* |
| `MULI_TLS_KEY_PATH` | PEM private key for gRPC TLS | *(none — plaintext)* |

### Job scheduling

| Variable | Description | Default |
|---|---|---|
| `MULI_MAX_CONCURRENT_JOBS` | Max jobs running simultaneously | `10` |
| `MULI_MAX_JOBS_PER_TENANT` | Per-tenant concurrency cap | `3` |
| `MULI_TOTAL_CPU_MILLICORES` | CPU budget for resource tracking | `8000` |
| `MULI_TOTAL_MEMORY_BYTES` | Memory budget for resource tracking | `17179869184` (16 GiB) |
| `MULI_MAX_LOG_LINES` | Max log lines stored/returned per job | `10000` |

### Cleanup

| Variable | Description | Default |
|---|---|---|
| `MULI_CLEANUP_INTERVAL_SECONDS` | How often the cleanup task runs | `300` (5 min) |
| `MULI_CLEANUP_MAX_AGE_SECONDS` | Age threshold for deleting terminal-state jobs | `3600` (1 hour) |

### Storage backends

| Variable | Description | Default |
|---|---|---|
| `MULI_MONGODB_URL` | MongoDB connection string; enables MongoDB backend when set | *(SQLite used)* |
| `MULI_MONGODB_DATABASE` | MongoDB database name | `muli` |

### Shutdown

| Variable | Description | Default |
|---|---|---|
| `MULI_SHUTDOWN_TIMEOUT_SECONDS` | Grace period for in-flight requests on SIGTERM | `30` |

### Registry (optional)

| Variable | Description | Default |
|---|---|---|
| `MULI_REGISTRY_ENABLED` | Enable the OCI registry | `false` |
| `MULI_REGISTRY_PORT` | Registry HTTP(S) port | `5000` |
| `MULI_REGISTRY_DOMAIN` | Externally reachable hostname | `localhost` |
| `MULI_REGISTRY_ROOT` | Data directory for blobs/manifests (optional override) | `{MULI_DATA_DIR}/registry` |
| `MULI_REGISTRY_MAX_SIZE_GB` | Total registry storage cap | `50` |
| `MULI_REGISTRY_MAX_BLOB_SIZE_MB` | Per-blob upload cap | `5120` (5 GiB) |
| `MULI_REGISTRY_TLS_CERT_PATH` | PEM cert for registry TLS | *(none)* |
| `MULI_REGISTRY_TLS_KEY_PATH` | PEM key for registry TLS | *(none)* |

### Git hosting (optional)

| Variable | Description | Default |
|---|---|---|
| `MULI_GIT_ENABLED` | Enable the git hosting service | `false` |
| `MULI_GIT_PORT` | Git HTTP port | `7000` |
| `MULI_GIT_DOMAIN` | Externally reachable hostname | `localhost` |
| `MULI_GIT_ROOT` | Root directory for bare git repositories (optional override) | `{MULI_DATA_DIR}/git` |
| `MULI_GIT_SSH_ENABLED` | Enable SSH transport | `false` |
| `MULI_GIT_SSH_PORT` | SSH listen port | `2222` |
| `MULI_GIT_SSH_HOST_KEY_PATH` | Path to ED25519 host key | `{git_root}/ssh_host_ed25519_key` |

---

## 3. Startup and Shutdown

### Migration note (data directory defaults)

Prior to this change, `MULI_REGISTRY_ROOT` defaulted to `/var/lib/muli/registry` and `MULI_GIT_ROOT` defaulted to `/var/lib/muli/git`. They now default to `{MULI_DATA_DIR}/registry` and `{MULI_DATA_DIR}/git` respectively.

**Existing deployments** that never set these variables must either:
- Set `MULI_DATA_DIR=/var/lib/muli` to preserve the old paths, **or**
- Explicitly set `MULI_REGISTRY_ROOT=/var/lib/muli/registry` and `MULI_GIT_ROOT=/var/lib/muli/git`.

Deployments that already set these variables explicitly are unaffected.

### Startup sequence

1. The server loads configuration from TOML config files and environment variables (see [Config File](#1-config-file)).
2. **Auth fail-fast**: if `MULI_REQUIRE_AUTH=true` and `MULI_API_KEY` is unset, the process exits immediately with an error — it will not bind any port.
3. Storage backends are initialised (SQLite DDL migration runs automatically).
4. **Job recovery**: jobs left in `Scheduled`, `Pulling`, or `Running` state from the previous run are reset to `Pending` and re-enqueued. This is idempotent and safe to run on every startup.
5. Background tasks start: scheduler dispatch loop, cleanup task, registry/git listeners (if enabled).
6. gRPC server begins accepting connections.

### Graceful shutdown (SIGTERM)

Send `SIGTERM` to the process. The server:
- Stops accepting new gRPC connections.
- Waits up to `MULI_SHUTDOWN_TIMEOUT_SECONDS` (default 30 s) for in-flight requests to complete.
- Forcefully exits after the timeout if requests are still running.

Jobs already dispatched to agents continue running on the agent side; their results will be reported when the agent reconnects after the server restarts.

```bash
# Graceful stop
kill -TERM $(pgrep muli-server)

# Force stop (use only when graceful stop hangs)
kill -KILL $(pgrep muli-server)
```

---

## 4. Backup and Restore

### SQLite backend (default)

SQLite files are stored under `MULI_DATA_DIR` (default `./data`):

```
data/
  _global.db          # tenants, users, orgs
  <tenant_id>.db      # per-tenant: jobs, agents, logs, registry tokens
```

**Backup:**

```bash
DATA_DIR=${MULI_DATA_DIR:-./data}

# Atomic online backup — safe while the server is running
for db in "$DATA_DIR"/*.db; do
    sqlite3 "$db" ".backup ${db}.bak"
done

# Or, if the server is stopped, a simple copy works:
cp -r "$DATA_DIR" "$DATA_DIR.$(date +%Y%m%d_%H%M%S)"
```

**Recommended schedule:**
- Jobs database (`<tenant>.db`): every 30 minutes — job records are small and frequently mutated.
- Global database (`_global.db`): hourly.
- Registry / git data directories: daily (these are large blobs, use incremental backups).

**Restore:**

```bash
# Stop the server first
kill -TERM $(pgrep muli-server)

# Replace the file(s)
cp "$DATA_DIR/_global.db.bak" "$DATA_DIR/_global.db"
cp "$DATA_DIR/<tenant_id>.db.bak" "$DATA_DIR/<tenant_id>.db"

# Verify integrity before restarting
sqlite3 "$DATA_DIR/_global.db" "PRAGMA integrity_check;"

# Restart
./muli-server
```

### MongoDB backend

**Backup:**

```bash
mongodump \
    --uri "${MULI_MONGODB_URL}" \
    --db "${MULI_MONGODB_DATABASE:-muli}" \
    --out "/var/backups/muli/$(date +%Y%m%d_%H%M%S)"
```

**Restore:**

```bash
mongorestore \
    --uri "${MULI_MONGODB_URL}" \
    --db "${MULI_MONGODB_DATABASE:-muli}" \
    /var/backups/muli/<timestamp>/muli
```

**Recommended schedule:** Hourly `mongodump` via cron, with 7-day retention.

---

## 5. Disaster Recovery

### Server loss checklist

1. **Provision a new host** with the same CPU/memory profile.
2. **Restore data** using the latest backup (see Section 3).
3. **Verify SQLite integrity** (if applicable):
   ```bash
   for db in data/*.db; do
       result=$(sqlite3 "$db" "PRAGMA integrity_check;")
       echo "$db: $result"
   done
   ```
   All databases must return `ok`. If any return errors, restore from an earlier backup.
4. **Set all environment variables** (especially `MULI_API_KEY`, `MULI_REQUIRE_AUTH`, `MULI_DATA_DIR`).
5. **Start the server.** The startup recovery sequence (Section 2) will automatically re-enqueue any jobs that were interrupted.
6. **Verify agents reconnect** by checking `agents_connected` in the metrics endpoint within a few minutes of restart.
7. **Smoke test**: submit a trivial job and confirm it reaches `Succeeded` state.

### Partial data loss (single tenant DB)

If only one tenant's database is corrupted, restore only that tenant's file:
```bash
cp data/<tenant_id>.db.bak data/<tenant_id>.db
sqlite3 data/<tenant_id>.db "PRAGMA integrity_check;"
```
The server does not need to be restarted — the next operation for that tenant opens a fresh connection.

---

## 6. Monitoring and Alerting

### Prometheus metrics endpoint

```
GET http://<host>:<MULI_METRICS_PORT>/metrics
```

Default: `http://localhost:9090/metrics`

### Key metrics

| Metric | Type | Alert when |
|---|---|---|
| `jobs_submitted_total{tenant, tier}` | Counter | Sudden drop may indicate client issues |
| `jobs_completed_total{tenant, tier, state}` | Counter | `state="failed"` rate > 10% warrants investigation |
| `jobs_running{tenant}` | Gauge | Sustained at `MULI_MAX_CONCURRENT_JOBS` for > 5 min = queue backup |
| `job_duration_seconds{tenant, tier}` | Histogram | p99 > 2× baseline = performance regression or resource contention |
| `agents_connected` | Gauge | **Alert immediately if == 0** — no agents means no jobs can run |

### Recommended Prometheus alert rules (examples)

```yaml
groups:
  - name: muli
    rules:
      - alert: NoAgentsConnected
        expr: agents_connected == 0
        for: 2m
        annotations:
          summary: "No muli agents connected — jobs will not run"

      - alert: HighJobFailureRate
        expr: |
          rate(jobs_completed_total{state="failed"}[5m])
          / rate(jobs_completed_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "Job failure rate exceeds 10%"

      - alert: QueueBacklog
        expr: jobs_running >= scalar(MULI_MAX_CONCURRENT_JOBS * 0.95)
        for: 5m
        annotations:
          summary: "Job queue near capacity"
```

### Disk usage

Monitor disk usage on the data directory and git/registry roots. Neither the job database nor log storage has a hard size cap enforced at the OS level — rely on `MULI_CLEANUP_MAX_AGE_SECONDS` for job/log rotation and `MULI_REGISTRY_MAX_SIZE_GB` for registry.

```bash
du -sh ${MULI_DATA_DIR:-./data}
du -sh ${MULI_REGISTRY_ROOT:-/var/lib/muli/registry}
du -sh ${MULI_GIT_ROOT:-/var/lib/muli/git}
```

---

## 7. Token Rotation

### gRPC API key

The gRPC API key is a plain string configured via `MULI_API_KEY`. To rotate it:

1. Generate a new key:
   ```bash
   openssl rand -hex 32
   ```
2. Update `MULI_API_KEY` in your environment/secrets manager with the new value.
3. Restart the server (the key is read only at startup).
4. Update all clients (agents, CI pipelines) with the new key.
5. Verify the old key no longer works by making a gRPC call with it — expect `UNAUTHENTICATED`.

**Note:** There is no built-in key versioning or dual-key overlap window. Schedule rotation during a low-traffic window or coordinate a brief maintenance window with all clients.

### Registry and git tokens

Registry and git HTTP tokens are stored per-tenant as Argon2id password hashes (with prefix lookup metadata). To rotate a token:

1. Issue a new token via the API (`POST /api/v1/tokens` for the relevant service).
2. Update clients that use the old token.
3. Revoke the old token via the API (`DELETE /api/v1/tokens/{token_id}`).

The `registry_tokens` table enforces expiry at read time — expired tokens are rejected automatically.

### Webhook egress controls

Webhook delivery uses SSRF safeguards by default:
- only `http` / `https` URLs are accepted
- localhost and private/link-local targets are rejected
- hostnames are DNS-resolved and blocked if they map to private IP ranges
- redirects are not followed

For local development only, `MULI_GIT_ALLOW_LOCALHOST_WEBHOOKS=true` relaxes create-time restrictions.

---

## 8. Auth Hardening Checklist

Use this checklist before going to production.

- [ ] **Set `MULI_API_KEY`** to a cryptographically random value (minimum 32 bytes / 64 hex chars):
  ```bash
  openssl rand -hex 32
  ```

- [ ] **Set `MULI_REQUIRE_AUTH=true`** — causes the server to refuse to start if `MULI_API_KEY` is unset, preventing accidental open deployments.

- [ ] **Enable TLS for gRPC** by setting `MULI_TLS_CERT_PATH` and `MULI_TLS_KEY_PATH`. Without TLS, API keys and tenant metadata are transmitted in plaintext.

- [ ] **Enable TLS for the registry** (if enabled) by setting `MULI_REGISTRY_TLS_CERT_PATH` and `MULI_REGISTRY_TLS_KEY_PATH`.

- [ ] **Use TLS for git HTTP traffic** (via reverse proxy or local TLS termination) and avoid exposing plaintext git/registry endpoints directly to the public internet.

- [ ] **Disable `default_tenant` in git config** — the `TenantConfig` default-tenant fallback is intended for localhost development only. Ensure `GIT_DEFAULT_TENANT` is not set in production.

- [ ] **Restrict network access** — the gRPC port (`MULI_GRPC_PORT`) should not be exposed to the public internet. Place it behind a private network or VPN.

- [ ] **Rotate tokens before go-live** — any tokens created during development or testing should be revoked and replaced with fresh production tokens.

- [ ] **Monitor auth failures** — the gRPC interceptor emits `WARN`-level structured log events for every authentication failure. Wire these to your alerting system to detect credential-stuffing or misconfigured clients:
  ```
  level=WARN msg="gRPC auth failure: invalid API key"
  level=WARN msg="gRPC auth failure: missing authorization header"
  ```

- [ ] **Review agent API keys** — each agent must be configured with the server's `MULI_API_KEY`. Agents with stale keys will be unable to connect after a key rotation.

- [ ] **Protect webhook secrets at rest** — webhook signing secrets are persisted in plaintext today; enforce disk/database encryption and least-privilege DB access.
