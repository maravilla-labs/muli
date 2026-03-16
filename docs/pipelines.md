# CI/CD Pipelines

Muli pipelines provide GitHub Actions-style CI/CD directly from your git repositories.
Push a `.maravilla/pipeline.yml` file to your repo and Muli automatically parses it,
builds a dependency graph, and executes each step as an isolated Docker container.

---

## How It Works

1. You push code containing `.maravilla/pipeline.yml` to a Muli-hosted repo
2. The receive-pack hook reads the YAML from the commit via git2
3. Muli parses, validates, and matches the trigger config against the push event
4. A **PipelineRun** is created with a sequential run number
5. Steps are expanded (matrix) and organized into a DAG (topological levels)
6. Each level executes in parallel — every step becomes a **Job** submitted to the scheduler
7. The scheduler dispatches Jobs to the Docker executor
8. Each container runs `/bin/sh -c` with `set -e` and the step commands
9. Logs are captured in real-time and persisted
10. On completion, the pipeline state is computed: **Succeeded**, **Failed**, **Degraded**, or **Cancelled**

---

## Quick Example

```yaml
# .maravilla/pipeline.yml
name: ci

on:
  push:
    branches: [main]

steps:
  - name: test
    image: node:22-alpine
    commands:
      - npm ci
      - npm test

  - name: build
    image: node:22-alpine
    needs: [test]
    commands:
      - npm run build
```

Push this file and Muli runs `test` first, then `build` after it succeeds.

---

## Full YAML Reference

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Pipeline name (displayed in UI and logs) |
| `on` | object | no | Trigger configuration (when to run) |
| `env` | map | no | Environment variables injected into every step |
| `services` | map | no | Sidecar containers (databases, caches) |
| `secrets` | list | no | Secret names resolved from the pipeline secret store |
| `steps` | list | yes | Ordered list of step definitions |

### Triggers (`on`)

```yaml
on:
  push:
    branches: [main, develop, "release/*"]
    paths: ["src/**", "Cargo.toml"]

  pull_request:
    branches: [main]
    events: [opened, synchronize]

  manual: true

  schedule:
    - cron: "0 2 * * *"
    - cron: "0 14 * * 1-5"
```

| Trigger | Fields | Description |
|---------|--------|-------------|
| `push` | `branches`, `paths` | Fires on git push. Branches support glob patterns (`*`, `**`). Paths filter by changed files. Empty lists match all. |
| `pull_request` | `branches`, `events` | Fires on PR events against target branches. Events: `opened`, `synchronize`, `closed`. |
| `manual` | boolean | When `true`, the pipeline can be triggered via the API or UI. |
| `schedule` | list of `{cron}` | Cron-based scheduling (UTC). Standard 5-field cron syntax. |

**Glob patterns:**
- `main` — exact match
- `release/*` — matches `release/1.0` but not `release/1.0/hotfix`
- `src/**` — matches `src/lib.rs`, `src/deep/nested/file.rs`
- `*` — matches everything

### Steps

```yaml
steps:
  - name: test
    image: rust:1.82
    commands:
      - cargo test --workspace
    needs: [install]
    env:
      DATABASE_URL: postgres://localhost:5432/test
    cache:
      key: "cargo-{{ hash('Cargo.lock') }}"
      restore_keys: [cargo-]
      paths: [/usr/local/cargo/registry, target]
    artifacts:
      upload:
        name: test-results
        paths: [target/test-results/]
      download: [build-output]
    resources:
      cpu: "2000m"
      memory: "4Gi"
    matrix:
      rust_version: ["1.80", "1.82"]
    if: "branch == 'main' && event == 'push'"
    failure_strategy: stop
    timeout: 1800
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique step name within the pipeline |
| `image` | string | yes | Docker image to run (e.g., `node:22-alpine`, `rust:1.82`) |
| `commands` | list | no | Shell commands executed in order with `set -e` (stop on first failure) |
| `needs` | list | no | Step names this step depends on (DAG edges) |
| `env` | map | no | Step-specific environment variables (override pipeline-level env) |
| `cache` | object | no | Dependency caching configuration |
| `artifacts` | object | no | Artifact upload/download configuration |
| `resources` | object | no | CPU and memory limits for the container |
| `matrix` | map | no | Matrix expansion — runs the step for every combination |
| `if` | string | no | Condition expression — skip step if false |
| `failure_strategy` | string | no | What to do when step fails: `stop` (default), `continue`, `ignore` |
| `timeout` | integer | no | Maximum execution time in seconds (default: 1800) |

### Cache

```yaml
cache:
  key: "npm-{{ hash('package-lock.json') }}"
  restore_keys: [npm-]
  paths: [node_modules]
```

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Exact cache key. Supports `{{ hash('filename') }}` templates that SHA-256 hash the file contents. |
| `restore_keys` | list | Prefix fallback keys. If exact key misses, tries these prefixes in order. |
| `paths` | list | Directories/files to cache. |

Cache is stored per-repository with zstd compression. A per-tenant size limit (default 5 GB)
enforces LRU eviction when exceeded.

### Artifacts

```yaml
artifacts:
  upload:
    name: dist
    paths: [dist/, coverage/]
  download: [other-step-artifact]
```

| Field | Type | Description |
|-------|------|-------------|
| `upload.name` | string | Artifact name (must be unique within the run) |
| `upload.paths` | list | Files/directories to upload after step succeeds |
| `download` | list | Artifact names from earlier steps to download before this step runs |

Artifacts are retained for 30 days by default (configurable via `MULI_PIPELINE_ARTIFACT_RETENTION_DAYS`).

### Matrix

```yaml
matrix:
  node_version: ["18", "20", "22"]
  os: [alpine, bookworm]
```

This expands the step into 6 parallel copies (3 x 2). Each copy gets the matrix values
as uppercase environment variables (`NODE_VERSION=18`, `OS=alpine`).

**Limits:** Maximum 25 combinations per step. Maximum 100 steps per pipeline.

### Conditions (`if`)

```yaml
if: "branch == 'main' && event == 'push'"
```

Simple expressions supporting:
- `branch == 'value'` / `branch != 'value'`
- `event == 'push'` / `event == 'pull_request'` / `event == 'manual'`
- `tag == 'v1.0'`
- `&&` for AND (up to 10 conditions)

When a condition evaluates to false, the step is **skipped** (not failed).
Unknown expressions default to true (step runs).

### Failure Strategy

| Value | Behavior |
|-------|----------|
| `stop` | (default) Pipeline fails immediately. Downstream steps are cancelled. |
| `continue` | Pipeline continues. Final state is **Degraded** if any step failed. |
| `ignore` | Failure is ignored entirely. Pipeline can still be **Succeeded**. |

### Resources

```yaml
resources:
  cpu: "2000m"     # 2 CPU cores (millicores)
  memory: "4Gi"    # 4 GB RAM
```

Defaults: 1000m CPU, 512Mi memory. These are Docker container limits.

### Services (Sidecars)

```yaml
services:
  postgres:
    image: postgres:16
    env:
      POSTGRES_PASSWORD: test
      POSTGRES_DB: mydb
  redis:
    image: redis:7-alpine
```

Service containers run alongside your step containers on the same Docker network.
Access them via their name as hostname (e.g., `postgres:5432`).

---

## Built-in Environment Variables

Every step automatically receives these environment variables:

| Variable | Example | Description |
|----------|---------|-------------|
| `PIPELINE_RUN_ID` | `a1b2c3d4-...` | Unique ID of this pipeline run |
| `PIPELINE_REF` | `refs/heads/main` | Full git ref that triggered the run |
| `PIPELINE_SHA` | `abc123def456` | Commit SHA being built |
| `PIPELINE_BRANCH` | `main` | Branch name (extracted from ref) |
| `PIPELINE_EVENT` | `push` | Trigger type: `push`, `pull_request`, `manual`, `schedule`, `retry` |
| `PIPELINE_CLONE_URL` | `http://...` | Git clone URL (when auto-checkout is enabled) |
| `PIPELINE_STEP_NAME` | `build` | Name of the current step |

Additionally, pipeline-level `env`, step-level `env`, and vault secrets (as `WUNDER_*`) are injected.

**Priority order** (highest wins): Built-in vars > Run env_vars (vault) > Step env > Pipeline env

---

## Auto-Checkout

When a clone URL is configured, Muli automatically prepends a git clone command before your step commands:

```bash
git clone "$PIPELINE_CLONE_URL" /workspace && cd /workspace && git checkout "$PIPELINE_SHA"
```

Your commands then run in `/workspace` with the repo checked out at the exact commit.

---

## DAG Execution

Steps are organized into **levels** based on `needs` dependencies:

```
Level 0:  [install]           ← no dependencies, runs first
Level 1:  [lint, test]        ← both need install, run in parallel
Level 2:  [build]             ← needs lint + test, runs after both complete
Level 3:  [deploy]            ← needs build
```

Within a level, all steps are submitted to the scheduler simultaneously.
The scheduler dispatches them based on priority and available resources.

If a step fails with `failure_strategy: stop`, all steps in subsequent levels are **cancelled**.
With `continue`, execution proceeds and the pipeline ends in **Degraded** state.

---

## Pipeline States

### Run States

| State | Description |
|-------|-------------|
| **Pending** | Run created, not yet started |
| **Running** | At least one step is executing |
| **Succeeded** | All steps completed successfully |
| **Failed** | A required step failed (stop strategy) |
| **Degraded** | Some steps failed but execution continued (continue strategy) |
| **Cancelled** | Cancelled by user or system |

### Step States

| State | Description |
|-------|-------------|
| **Pending** | Waiting for dependencies |
| **Ready** | Dependencies met, queued for execution |
| **Running** | Docker container is running |
| **Succeeded** | Exit code 0 |
| **Failed** | Non-zero exit code or execution error |
| **Skipped** | `if` condition evaluated to false |
| **Cancelled** | Cancelled due to upstream failure or user action |

---

## Security

- **Tenant isolation:** All pipeline data is scoped by tenant_id. Cross-tenant access is prevented at the store level.
- **Path traversal protection:** Artifact names and cache keys are validated — `..`, `/`, `\`, null bytes rejected.
- **Container hardening:** Steps run with `cap_drop: ALL`, `no-new-privileges`, `readonly_rootfs`, `pids_limit: 256`.
- **Secret safety:** Pipeline secrets are AES-256-GCM encrypted at rest. They never appear in API responses (only names are listed). Log output is not filtered — avoid `echo $SECRET` in commands.
- **YAML limits:** Max 1 MB YAML size, max 100 steps, max 25 matrix combinations, max 512-char conditions.
- **Rate limiting:** Per-repo cooldown of 5 seconds between pipeline triggers.
- **Network isolation:** Each step runs in an isolated Docker network.

---

## Configuration

Server-level configuration via environment variables or `muli.toml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MULI_PIPELINE_ENABLED` | `false` | Enable pipeline feature (requires `MULI_GIT_ENABLED=true`) |
| `MULI_PIPELINE_ARTIFACT_RETENTION_DAYS` | `30` | Days before artifacts are auto-deleted |
| `MULI_PIPELINE_CACHE_MAX_GB` | `5.0` | Per-tenant cache size limit in GB |
| `MULI_PIPELINE_MAX_MATRIX_SIZE` | `25` | Maximum matrix combinations per step |
| `MULI_PIPELINE_SECRET_ENCRYPTION_KEY` | — | Base64-encoded AES-256 key for pipeline secrets |

---

## REST API

Pipeline endpoints are available under the git service:

```
GET    /api/v1/repos/{ns}/{repo}/pipelines/runs                    List runs
GET    /api/v1/repos/{ns}/{repo}/pipelines/runs/{number}           Get run details
POST   /api/v1/repos/{ns}/{repo}/pipelines/runs/{number}/cancel    Cancel a running pipeline
POST   /api/v1/repos/{ns}/{repo}/pipelines/runs/{number}/retry     Retry a failed pipeline
GET    /api/v1/repos/{ns}/{repo}/pipelines/runs/{number}/steps/{step}/logs  Step logs
GET    /api/v1/repos/{ns}/{repo}/pipelines/runs/{number}/artifacts  List artifacts
POST   /api/v1/repos/{ns}/{repo}/pipelines/trigger                 Manual trigger
POST   /api/v1/repos/{ns}/{repo}/pipelines/secrets                 Set secret
GET    /api/v1/repos/{ns}/{repo}/pipelines/secrets                 List secret names
DELETE /api/v1/repos/{ns}/{repo}/pipelines/secrets/{name}          Delete secret
```

## gRPC API

The `PipelineService` provides 13 RPCs for programmatic access:

```protobuf
service PipelineService {
  rpc TriggerPipeline(...)       returns (PipelineRunResponse);
  rpc GetPipelineRun(...)        returns (PipelineRunResponse);
  rpc ListPipelineRuns(...)      returns (ListPipelineRunsResponse);
  rpc CancelPipeline(...)        returns (CancelPipelineResponse);
  rpc RetryPipeline(...)         returns (PipelineRunResponse);
  rpc GetStepLogs(...)           returns (GetStepLogsResponse);
  rpc StreamStepLogs(...)        returns (stream LogLine);
  rpc ListArtifacts(...)         returns (ListArtifactsResponse);
  rpc DownloadArtifact(...)      returns (stream ArtifactChunk);
  rpc ListCaches(...)            returns (ListCachesResponse);
  rpc DeleteCache(...)           returns (DeleteCacheResponse);
  rpc WatchPipelineRun(...)      returns (stream PipelineRunEvent);
  rpc GetPipelineConfig(...)     returns (GetPipelineConfigResponse);
}
```

---

## Real-World Examples

### Rust Project

```yaml
name: rust-ci
on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: "-D warnings"

steps:
  - name: check
    image: rust:1.82
    commands:
      - cargo check --workspace
    cache:
      key: "cargo-{{ hash('Cargo.lock') }}"
      paths: [/usr/local/cargo/registry, target]

  - name: test
    image: rust:1.82
    needs: [check]
    commands:
      - cargo test --workspace
    cache:
      key: "cargo-{{ hash('Cargo.lock') }}"
      paths: [target]

  - name: clippy
    image: rust:1.82
    needs: [check]
    commands:
      - rustup component add clippy
      - cargo clippy --workspace -- -D warnings

  - name: build-release
    image: rust:1.82
    needs: [test, clippy]
    commands:
      - cargo build --release
    artifacts:
      upload:
        name: binary
        paths: [target/release/myapp]
    if: "branch == 'main' && event == 'push'"
```

### Node.js Full-Stack App

```yaml
name: fullstack-ci
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
    events: [opened, synchronize]

services:
  postgres:
    image: postgres:16
    env:
      POSTGRES_PASSWORD: test
      POSTGRES_DB: app_test
  redis:
    image: redis:7-alpine

secrets: [DATABASE_URL, REDIS_URL, DEPLOY_TOKEN]

steps:
  - name: install
    image: node:22-alpine
    commands:
      - npm ci
    cache:
      key: "npm-{{ hash('package-lock.json') }}"
      paths: [node_modules]

  - name: lint
    image: node:22-alpine
    needs: [install]
    commands:
      - npx eslint src/ --max-warnings 0
      - npx prettier --check src/
    failure_strategy: continue

  - name: typecheck
    image: node:22-alpine
    needs: [install]
    commands:
      - npx tsc --noEmit

  - name: test-unit
    image: node:22-alpine
    needs: [install]
    commands:
      - npm run test:unit -- --coverage
    artifacts:
      upload:
        name: coverage
        paths: [coverage/]

  - name: test-integration
    image: node:22-alpine
    needs: [install]
    commands:
      - npm run test:integration
    env:
      DATABASE_URL: postgres://postgres:test@postgres:5432/app_test
      REDIS_URL: redis://redis:6379
    timeout: 600

  - name: build
    image: node:22-alpine
    needs: [lint, typecheck, test-unit, test-integration]
    commands:
      - npm run build
    artifacts:
      upload:
        name: dist
        paths: [dist/]
    resources:
      cpu: "2000m"
      memory: "2Gi"

  - name: deploy
    image: alpine:latest
    needs: [build]
    commands:
      - apk add --no-cache curl
      - curl -sSf -X POST "https://deploy.example.com/api/deploy"
        -H "Authorization: Bearer $DEPLOY_TOKEN"
        -d '{"version":"$PIPELINE_SHA","env":"production"}'
    artifacts:
      download: [dist]
    if: "branch == 'main' && event == 'push'"

  - name: notify
    image: alpine:latest
    needs: [deploy]
    commands:
      - apk add --no-cache curl
      - curl -sSf -X POST "https://hooks.slack.com/services/..."
        -d '{"text":"Deployed $PIPELINE_SHA to production"}'
    failure_strategy: ignore
    if: "branch == 'main' && event == 'push'"
```

### Multi-Platform Build (Matrix)

```yaml
name: cross-compile
on:
  push:
    branches: [main]

steps:
  - name: build
    image: rust:1.82
    commands:
      - rustup target add $TARGET
      - cargo build --release --target $TARGET
    matrix:
      target:
        - x86_64-unknown-linux-gnu
        - aarch64-unknown-linux-gnu
        - x86_64-apple-darwin
    artifacts:
      upload:
        name: binary
        paths: [target/$TARGET/release/myapp]
    resources:
      cpu: "2000m"
      memory: "4Gi"
```

This runs 3 builds in parallel — one per target architecture.
