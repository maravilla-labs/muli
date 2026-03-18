# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.4] - 2026-03-18

### Added

- **Structured job substeps in pipeline runs** — `jobs.<name>.commands` are now surfaced as a synthetic `Preparation` substep, named `jobs.<name>.steps[]` are preserved as first-class substeps, and `StepRun` now returns live substep progress with timestamps, exit codes, and per-substep log sequence ranges.
- **Structured pipeline step log events** — pipeline log APIs now include substep-aware metadata (`substep_name`, `event_type`, `exit_code`) for both historical log fetches and live streaming, enabling clients to render collapsible step explorers without guessing from raw output.
- **Substep runtime regression coverage** — Muli’s log streaming tests now verify hidden substep lifecycle markers are consumed into structured events, and step-log streaming still replays backlog plus live lines without duplicates.

### Changed

- **Jobs-mode execution model** — the Docker shell wrapper now emits internal substep lifecycle markers around structured job substeps while keeping those control markers out of user-visible logs.

## [0.4.3] - 2026-03-18

### Added

- **Exact fast-failing Node pipeline e2e coverage** — Muli's server e2e suite now exercises the real private-repo checkout path with a `node:22-alpine` pipeline using `jobs.<name>.commands` plus named `steps`, and verifies that a fast failing build preserves the actual container output and exit code.
- **Localhost host-checkout regression coverage** — Muli's server e2e suite now covers `jobs:` pipelines running with `MULI_GIT_BASE_URL=http://127.0.0.1:7000` and verifies that host-side checkout keeps the configured local git URL instead of rewriting it to `host.docker.internal`.

### Fixed

- **Bogus Docker wait exit codes** — job execution no longer treats Docker wait API error codes as authoritative container exit codes. Muli now inspects the container state after wait and uses the inspected exit code when available, avoiding misleading failures such as `exit code 243`.
- **Missing final container logs on failed jobs** — failed jobs now always perform a best-effort final container log fetch before cleanup, even when a follow stream already started, so fast failures keep their real runtime output instead of only checkout logs.
- **Docker runtime diagnostics in step failures** — when container execution fails before a trustworthy exit code is available, step failure messages now include structured Docker wait/inspect diagnostics instead of collapsing into an incorrect numeric exit code.
- **Workspace write permissions for checked-out jobs** — host bind-mounted `/workspace` directories are now normalized before container start so capability-dropped containers can create `node_modules`, build outputs, and other writable job artifacts without relaxing container hardening.
- **Host checkout clone URL rewriting** — `jobs:` pipelines now keep the configured `MULI_GIT_BASE_URL` for host-side checkout, while the `host.docker.internal` rewrite remains limited to the legacy in-container clone path used by `steps:` mode.

## [0.4.2] - 2026-03-18

### Fixed

- **Missing fast-failure container logs after checkout** — pipeline jobs that failed immediately after host-side checkout could previously persist only the checkout output, hiding the actual container error. Muli now detects the absence of container log lines after the container log stream starts and performs a one-shot recovery fetch so the real runtime failure is preserved.
- **Repository pipeline overview ordering** — pipeline runs listed at repository scope are now ordered by real run creation time rather than per-pipeline `run_number`, so multi-pipeline repositories show the newest run first in clients.
- **Checkout log noise** — host-side `git checkout <sha>` now suppresses detached HEAD advice output to keep pipeline logs focused on actionable information.

## [0.4.1] - 2026-03-18

### Added

- **Repo-scoped CI checkout tokens for private repositories** — push-triggered, manual, and retry pipeline runs now generate short-lived pull tokens scoped to the target repository. Private repository checkout no longer depends on a user-bound token identity.
- **Named job steps inside `jobs:` pipelines** — `jobs.<name>.steps[]` now groups commands under explicit step names while keeping `jobs.<name>.commands` as the prep phase. This gives clients and logs a clearer execution structure without reviving the legacy top-level unnamed step format.
- **Multi-file pipeline discovery** — Muli now discovers pipelines from both `.maravilla/pipeline.yml` and `.maravilla/pipeline/*.yml|yaml`, allowing more than one pipeline definition per repository commit.
- **Pipeline lifecycle webhooks** — repositories can now receive `pipeline.started` and `pipeline.completed` webhook events in addition to the existing git events.
- **End-to-end checkout coverage for private repositories** — new Muli-side e2e tests cover repo-scoped private clone auth, token denial for the wrong repo or wrong permission, checkout success for generated CI tokens, checkout failure surfacing, and multi-pipeline discovery.

### Fixed

- **Private-repo pipeline checkout auth** — temporary CI tokens used for checkout are now accepted for the matching private repository without requiring an owner/collaborator `user_id`, while remaining denied for other repositories and write operations.
- **Structured pipeline failure surfacing** — `StepRun` now carries `error_message` so checkout/setup failures are preserved even when no useful container logs exist.
- **Stable pipeline identity across multi-file configs** — push-triggered runs now keep consistent pipeline IDs keyed by repository and pipeline name, even when multiple pipeline files exist under `.maravilla/pipeline/`.
- **Proto rebuild drift** — pipeline and webhook proto updates now reliably trigger code generation again in downstream consumers.

### Changed

- **Pipeline config loading** — `GetPipelineConfig` now reads from the same multi-file pipeline discovery path used by trigger execution, so config inspection matches what actually runs.
- **Checkout flow diagnostics** — checkout failures now fail the run with explicit structured reasons instead of collapsing into an unhelpful generic clone failure.

## [0.4.0] - 2026-03-18

### Added

- **`jobs:` pipeline format** — new top-level `jobs:` map replaces `steps:` as the recommended way to declare CI/CD pipelines. Each job is a named DAG node with its own image, commands, `needs:` dependencies, resources, artifacts, and optional `if:` condition. Pipeline-level `image:` is inherited by all jobs that do not declare their own. Fully backward-compatible: existing `steps:` pipelines continue to work without changes.
- **Host-side git checkout** — the engine now clones and checks out the repository directly on the host (not inside the user container) before the job container starts. Clone URL is kept separate from all log output and error messages to prevent accidental auth-token leakage. `checkout: submodules: true` populates submodules via `git submodule update --init --recursive --depth 1`. Applies to both `jobs:` mode and legacy `steps:` mode when a clone URL is provided.
- **Job-to-job artifact handover** — jobs can declare `artifacts: paths: [dist/, node_modules/]` to tar and upload outputs at completion. All jobs listed in `needs:` whose dependency exported artifacts automatically have those artifacts downloaded and extracted into their workspace before commands run — no explicit download declaration needed.
- **`ArtifactHandler` trait** (`muli-core::job::artifact_handler`) — decouples the engine from pipeline-specific tar/storage logic. `DockerExecutor` accepts an optional `Arc<dyn ArtifactHandler>` via `with_artifact_handler()` builder method.
- **`CheckoutSpec` and `ArtifactDownload` models** — new fields on `JobSpec`: `checkout: Option<CheckoutSpec>`, `artifact_downloads: Vec<ArtifactDownload>`, `artifact_upload_paths: Vec<String>`, `artifact_upload_key: Option<String>`.
- **`checkout:` pipeline config block** — `checkout: submodules: true/false` (default false) in pipeline YAML controls submodule initialization during host-side checkout.
- **`DownloadArtifact` gRPC streaming** — `PipelineServiceImpl.download_artifact_impl` now streams artifact bytes in 64 KB chunks via the existing `ArtifactChunk` message. Requires `ArtifactStorage` injected into the gRPC service (wired through `start_grpc.rs` and `startup.rs`).
- **Sequence-numbered checkout logs** — each stdout/stderr line emitted by git commands during checkout carries a monotonically increasing sequence number via a shared `AtomicU64`, ensuring correct ordering in the log stream.
- **32 new tests** — 7 DAG executor e2e tests covering `jobs:` format execution, DAG ordering with `needs:`, artifact upload/download path population in `JobSpec`, pipeline/job-level image inheritance, `PIPELINE_JOB_NAME` env var, checkout spec propagation, and failure propagation; 6 `ArtifactManager` unit tests (file roundtrip, directory roundtrip, empty paths noop, missing artifact skip, multi-job restore, partial-missing path skip); 3 host-checkout tests using a local bare git repo via `file://` URL (clone+checkout, log forwarding, invalid-URL error without leaking the URL); 16 existing tests expanded via `CapturingJobSubmitter` for JobSpec field assertions.

### Fixed

- **HTTP push pipeline runs had empty `commit_sha`** — `receive_pack` in the HTTP smart protocol path was parsing the pushed SHA from raw pkt-line bytes using hard-coded offsets, which silently returned no ref updates on any deviation in capabilities order or push-cert format. The handler now snapshots refs with `git for-each-ref` before and after the push and diffs them — the same approach the SSH path has always used — guaranteeing the correct 40-char SHA is recorded on every push-triggered run.

### Changed

- **DAG executor refactored** — `executor.rs` reduced from ~784 lines to ~490 lines by extracting shared helpers (`execute_dag_levels`, `submit_level`, `wait_level`, `cancel_level_runs`, `evaluate_conditions`, `build_run_maps`) used by both `execute_jobs` and `execute_steps` paths. Matrix-expanded run names are now resolved to their original definition name via pre-computed `(orig, sr)` pairs, fixing a bug where matrix jobs were left in `Pending` after the refactor.
- **`muli-engine` checkout** moved to `docker/checkout.rs` with URL-safe error formatting — clone errors never include the clone URL string; only non-URL git commands (fetch, checkout, submodule) include their args in error messages.

### Security

- Clone URLs (which may contain short-lived auth tokens) are passed to `git clone` as a dedicated argument and are never interpolated into error messages, log lines, or format strings.
- Artifact tar extraction enforces `set_preserve_permissions(false)` (no setuid/setgid bit restoration) and relies on tar 0.4.16+ path traversal rejection for `..` and absolute paths.

## [0.3.3] - 2026-03-17

### Added

- **Periodic log flushing** — `LogCollector` gains `peek_unflushed()` for incremental drain without clearing the ring buffer. `execute_job` spawns a background task that flushes buffered lines to durable storage every 2 s, making step logs visible in the UI while jobs are running rather than only on completion.
- **CI token for `RetryPipeline`** — `RetryPipeline` gRPC now generates a short-lived pull-only CI token and injects `PIPELINE_CLONE_URL` into the retry run, consistent with push-triggered runs. `PipelineServiceImpl` gains `token_store` and `git_base_url` fields wired through `start_grpc.rs` and `startup.rs`.

### Fixed

- **CI containers cannot reach host git service** — Docker containers now receive `extra_hosts: host.docker.internal:host-gateway` so they can resolve the host machine's services on Linux. The injected CI clone URL rewrites `localhost`/`127.0.0.1` in the host segment to `host.docker.internal`.
- **`effective_git_base_url()` included `git_domain` in the fallback** — the default is now `http://localhost:{git_port}` instead of `http://{git_domain}:{git_port}`. Same-machine runners work out of the box; set `MULI_GIT_BASE_URL=https://git.example.com` when behind a reverse proxy.
- **`GetPipelineConfig` now requires `commit_sha`** — returns `INVALID_ARGUMENT` if empty, removing the ambiguous default-branch fallback added in 0.3.2.
- **CLI `gitHost`/`registryHost` defaults** — changed from `'local.localhost'` (does not resolve via DNS) to `'localhost'`.
- **CLI git URL always used `http://` with an explicit port** — new `buildGitUrl()` helper emits `https://` without port for port 443, `http://` without port for port 80, and `http://host:port` otherwise.

## [0.3.2] - 2026-03-17

### Added

- **`GetPipelineConfig` RPC** — reads `.maravilla/pipeline.yml` from a specific commit SHA (or falls back to the default branch HEAD) in the git repository and returns the raw YAML content, enabling clients to preview or validate pipeline configuration without triggering a run.

## [0.3.1] - 2026-03-17

### Added

- **`run_id` UUID lookup for `GetPipelineRun`** — clients can now pass either `run_id` (UUID) or `run_number` (integer) to fetch a pipeline run; UUID lookup is preferred for O(1) access and avoids leaking sequential run numbers.

### Security

- **`repo_id` verification on pipeline RPCs** — `CancelPipeline`, `RetryPipeline`, and `GetStepLogs` now verify the run's `repo_id` matches the caller's `repo_id`, preventing cross-project access within a shared tenant.

## [0.3.0] - 2026-03-17

### Added

- **Per-tenant resource limits** — new `TenantLimits` model with configurable caps for concurrent jobs, concurrent pipelines, daily pipeline runs, repository count, and storage bytes. Zero values mean unlimited / use server default, so standalone deployments are unaffected.
- **Priority tier system** — `PriorityTier` enum (Free/Standard/Premium/Enterprise) with weighted scheduling scores. Server-side `resolve_effective_tier` always uses the tenant's stored tier, ignoring client hints. Scoring formula `tier_weight * (10 + queue_minutes) / 10` prevents starvation for lower-tier jobs.
- **Tenant enforcement** — centralized enforcement helpers (`check_not_suspended`, `check_job_limit`, `check_pipeline_limits`, `check_repo_limit`, `resolve_effective_tier`) applied at job submit, pipeline trigger, and repo create. All checks are fail-open: store errors allow the operation rather than blocking all tenants.
- **Tenant suspension** — `SuspendTenant`/`UnsuspendTenant` gRPC RPCs to block all operations for a tenant with an optional reason string.
- **`GetTenantUsage` RPC** — returns current storage bytes, active jobs, active pipelines, daily pipeline runs, and repo count for a tenant.
- **`SetTenantLimits`/`GetTenantLimits` RPCs** — configure and read per-tenant limits including priority tier, concurrency caps, and suspension state.
- **`TenantLimitsStore` trait** with 5 methods (`get_limits`, `set_limits`, `is_suspended`, `increment_daily_run_count`, `get_daily_run_count`) and both SQLite and in-memory implementations.
- **Stale job watchdog** — background task that detects jobs stuck in Running/Scheduled for longer than a configurable grace period (default 5 min) and transitions them to Failed. Configured via `MULI_WATCHDOG_INTERVAL_SECS` and `MULI_WATCHDOG_GRACE_PERIOD_SECS`.
- **Job retry with exponential backoff** — transient failures automatically retry up to `MULI_RETRY_MAX_RETRIES` (default 2) with configurable base delay and max delay cap.
- **Shared auth module** — `muli-core::auth` consolidates Bearer, Basic, and raw token extraction with case-insensitive scheme matching per RFC 7235, eliminating duplicate auth code across muli-git and muli-registry. 13 unit tests.
- **Repository domain service** — `muli-core::service::repository` encapsulates create, delete, fork, and transfer operations with transactional rollback safety (filesystem ↔ database consistency).
- **Background cleanup tasks** — expired artifact cleanup, pipeline run retention (default 90 days), and hourly tenant quota reconciliation via full filesystem scan.
- **Global git tokens table** — `global_git_tokens` in `_global.db` enables O(1) cross-tenant token lookups for HTTP auth without scanning all tenant databases. Backfill migration runs automatically on startup.

### Changed

- **SQLite connection pooling** — global database now uses a round-robin pool of 4 connections instead of a single connection, reducing contention under concurrent gRPC load.
- **LRU tenant connection eviction** — tenant database connections are now capped at 256 with LRU eviction, preventing unbounded memory growth in large multi-tenant deployments.
- **SQLite performance PRAGMAs** — all connections (global and tenant) now apply `synchronous=NORMAL`, `cache_size=-8000` (8 MB), `mmap_size=268435456` (256 MB), and `temp_store=MEMORY`.
- **SSH server refactored into submodules** — monolithic `ssh/server.rs` split into `ssh/session.rs` (handler), `ssh/process.rs` (subprocess spawning), and `ssh/ref_tracking.rs` (pre/post-push ref diffing).
- **Pipeline service refactored** — monolithic `pipeline_service.rs` (799 lines, deleted) split into `pipeline_service/` directory with `mod.rs`, `runs.rs`, `artifacts.rs`, `logs.rs`, `conversions.rs`, and `helpers.rs`.
- **`tenant.proto` expanded** — added `TenantLimits` message, `SetTenantLimits`/`GetTenantLimits`/`SuspendTenant`/`UnsuspendTenant`/`GetTenantUsage` RPCs, and optional `limits` field on `Tenant`.
- **Execution loop** now accepts `Scheduler` and `RetryPolicy` for automatic retry on transient failures.
- **Server config** — 5 new configuration fields: `retry_max_retries`, `retry_base_delay_secs`, `retry_max_delay_secs`, `watchdog_interval_secs`, `watchdog_grace_period_secs`, `pipeline_run_retention_days`.

### Security

- Tenant suspension blocks all operations (job submit, pipeline trigger, repo create) with `PERMISSION_DENIED` status.
- Priority tier is server-authoritative — client-provided tier hints are only used in standalone mode when no tenant limits are configured.
- Tenant limits enforcement is additive: standalone deployments with no limits store configured experience zero overhead.

## [0.2.4] - 2026-03-16

### Fixed

- **Pipeline store queries scan all tenant databases, causing 500 errors** — all pipeline store trait methods (`PipelineStore`, `PipelineRunStore`, `StepRunStore`, `ArtifactStore`, `CacheStore`, `PipelineSecretStore`) now accept `tenant_id` as a parameter and query only the caller's tenant database instead of scanning every `.db` file on disk via `all_tenant_ids()`. This eliminates the `attempt to write a readonly database` error when stale or read-only database files exist.
- **`list_by_repo` pagination applied in-memory instead of SQL** — `SqlitePipelineRunStore::list_by_repo` now uses SQL `LIMIT`/`OFFSET` instead of fetching all rows across all tenants and slicing in Rust.
- **`ArtifactStore::delete_expired` crashes on inaccessible tenant DB** — cross-tenant expired artifact cleanup now skips tenant databases that fail to open instead of aborting the entire operation.

### Security

- Pipeline store queries are now fully tenant-isolated at the storage layer. Previously, query methods scanned all tenant databases, which was both a performance issue and a potential information leak vector.

## [0.2.3] - 2026-03-16

### Fixed

- **SSH clone/push broken after auth changes** — removed `UserStore` workaround from SSH server that resolved `external_id` for ACL checks. The proper fix is on the flightdeck side: all API calls now use muli-internal UUIDs instead of mixing StaticLab ObjectIds with muli UUIDs, so the per-repo ACL comparisons match correctly.

### Removed

- `user_store` field from `SshServer` and `SshSessionHandler` — no longer needed since flightdeck now sends muli UUIDs for all collaborator and owner operations.

## [0.2.2] - 2026-03-16

### Added

- **`MuliError::Conflict` variant** — new permanent error type for business-rule conflicts (e.g. duplicate unique keys), distinct from retryable `Storage` errors.

### Fixed

- **Duplicate SSH key returns 500 instead of conflict error** — adding an SSH key with a fingerprint already registered by another user now returns gRPC `AlreadyExists` status with a descriptive message, instead of leaking a raw SQLite UNIQUE constraint error as an opaque `Internal` status.

## [0.2.1] - 2026-03-16

### Added

- **`OrgRole::Viewer`** — new organization role granting read-only (pull) access to org-owned repositories. Added to proto, domain model, and gRPC conversions.
- **Org-aware repository ACL** — `check_repo_access()` now accepts an optional `OrgMember` and grants access based on org role: Owner/Admin → pull, push, admin; Member → pull, push; Viewer → pull only. Org membership is ignored for user-owned repos.
- **`check_repo_access_with_org_lookup()`** convenience wrapper that resolves org membership from stores before checking access, shared by both SSH and HTTP auth paths to avoid code duplication.
- **`owner_id` and `owner_type` on `CreateRepositoryRequest`** proto — repositories can now be created with an explicit owner identity and type (user or organization).
- **Org stores wired into HTTP `GitAuth`** — `with_org_stores()` builder method passes `OrgStore` and `OrgMemberStore` into the HTTP auth middleware for org-aware ACL checks.
- 5 new unit tests for org role ACL permissions covering Owner, Admin, Member, Viewer, and user-owned repo isolation.

### Fixed

- **Org members denied SSH/HTTP push to org-owned repos** — the ACL check now resolves org membership for org-owned repositories, granting access based on the member's org role instead of requiring an explicit collaborator record.
- **Repositories created via gRPC had empty `owner_id`** — `create_repository_impl` now populates `owner_id` and `owner_type` from the request fields.
- Pre-existing clippy warnings across `muli-pipeline`, `muli-server` tests, and `start_grpc.rs` (unused imports, collapsible `if` statements, `format!` lint, too-many-arguments).

### Security

- Org membership check is additive: direct collaborator records still work independently of org membership, so users added as collaborators without being org members retain access.

## [0.2.0] - 2026-03-16

### Added

- **CI/CD Pipeline Orchestrator** — new `muli-pipeline` crate providing a GitHub Actions-style workflow engine triggered by `.maravilla/pipeline.yml` files in git repositories.
- **YAML DSL**: declarative pipeline configuration with triggers (`push`, `pull_request`, `manual`, `schedule`), multi-step DAG dependencies (`needs`), matrix expansion, conditional execution (`if`), failure strategies (`stop`/`continue`/`ignore`), caching with `{{ hash('file') }}` templates, artifact upload/download, resource limits, service sidecars, and configurable timeouts.
- **DAG executor**: processes pipeline steps level-by-level using topological sort, submits each step as a Job to the existing scheduler, polls for completion, and computes final run state (Succeeded/Failed/Degraded/Cancelled).
- **Docker execution**: pipeline steps run as isolated Docker containers via `/bin/sh -c` with `set -e`. Built-in env vars (`PIPELINE_RUN_ID`, `PIPELINE_SHA`, `PIPELINE_BRANCH`, `PIPELINE_EVENT`, `PIPELINE_STEP_NAME`) injected into every step. Auto-checkout prepends `git clone` when a clone URL is configured.
- **`commands` field on `JobSpec`**: when set, the container's CMD is overridden with the provided shell commands instead of using the image default entrypoint. Enables pipeline steps to run arbitrary commands in any Docker image.
- **Pipeline domain models**: `Pipeline`, `PipelineRun` (with `env_vars` map for vault/secret injection), `StepRun` (with `tenant_id` for isolation), `Artifact`, `CacheEntry`, `PipelineSecret` in `muli-core`.
- **6 store traits + SQLite implementations**: `PipelineStore`, `PipelineRunStore`, `StepRunStore`, `ArtifactStore`, `CacheStore`, `PipelineSecretStore` with 6 DDL tables and indexes.
- **Pipeline trigger hook**: fires on `receive_pack` (push) and PR events in `muli-git`. Reads `.maravilla/pipeline.yml` from the commit via `git2`, parses and validates the YAML, matches triggers against the event, creates run/step records, and spawns DAG execution.
- **`PipelineService` gRPC** with 13 RPCs: `TriggerPipeline` (with generic `env_vars` for caller-provided secrets), `GetPipelineRun`, `ListPipelineRuns`, `CancelPipeline` (cascades to all non-terminal steps), `RetryPipeline` (re-creates from original YAML), `GetStepLogs` (fetches from `JobLogStore` via step's `job_id`), `ListArtifacts`, `ListCaches`, `DeleteCache`, plus streaming RPCs for logs, artifacts, and run events.
- **`WebhookEvent::PipelineCompleted`** for deploy integration callbacks.
- **Artifact filesystem storage** with SHA-256 integrity and path traversal protection.
- **Cache filesystem storage** with zstd compression, LRU eviction, and per-tenant size limits.
- **Pipeline REST API**: 10 endpoints under `/api/v1/repos/{ns}/{repo}/pipelines/` for runs, secrets, artifacts, and manual triggers.
- **Server configuration**: `MULI_PIPELINE_ENABLED`, `MULI_PIPELINE_ARTIFACT_RETENTION_DAYS`, `MULI_PIPELINE_CACHE_MAX_GB`, `MULI_PIPELINE_MAX_MATRIX_SIZE`, `MULI_PIPELINE_SECRET_ENCRYPTION_KEY`.
- **Pipeline documentation**: full YAML reference, DAG execution model, security considerations, configuration, API reference, and real-world examples (Rust, Node.js, multi-platform matrix) in `docs/pipelines.md`.
- **300+ tests** including 21 YAML parser/validation tests, 25 store CRUD tests, 7 store integration tests, 8 pipeline integration tests, 7 DAG executor tests with mock submitters, 3 Docker pipeline tests (real containers), and 1 realistic npm CI pipeline test running `npm install → lint → test → build` across 4 steps in `node:22-alpine` containers with real log capture.

### Changed

- **`startup.rs` refactored**: extracted gRPC service construction into `start_grpc.rs` (208 lines) reducing `startup.rs` from 630 to 448 lines.
- **`GitState` and `GitRouterConfig`** now include `pipeline_trigger: Option<Arc<dyn PipelineTriggerHook>>` for pipeline event delivery.

### Security

- Pipeline secrets encrypted at rest with AES-256-GCM; encrypted values are never injected into step environments (skipped with warning until decryption is wired).
- YAML bomb protection: 1 MB size limit, max 100 steps, max 25 matrix combinations, max 512-char conditions with max 10 AND parts.
- `tenant_id` added to `StepRun` model to prevent cross-tenant data access in step store operations.
- Pagination capped at 100 results for `ListPipelineRuns`.
- Per-repo rate limiting (5-second cooldown) on pipeline triggers to prevent push-spam DoS.
- N+1 query eliminated: `ListPipelineRuns` no longer fetches per-run steps.
- Container hardening: `cap_drop: ALL`, `no-new-privileges`, `readonly_rootfs`, `pids_limit: 256`, isolated Docker network per step.

## [0.1.14] - 2026-03-15

### Added

- **Git blame endpoint**: `GET /api/v1/repos/{namespace}/{repo}/blame/{*path}?ref=<branch>` returns per-hunk blame information including commit SHA, author name/email, timestamp, and commit message summary. Uses `git2::Repository::blame_file()` with `newest_commit` option for ref-scoped blame.

## [0.1.13] - 2026-03-15

### Added

- **Git LFS 2.0 Batch API** with full HTTP protocol support: batch negotiate, object upload (PUT), download (GET), and verify endpoints.
- LFS storage abstraction trait (`LfsStorage`) with pluggable backends for runtime dispatch.
- **Filesystem LFS backend**: content-addressable per-tenant storage with two-level prefix directories, streaming uploads with incremental SHA-256 verification, atomic temp-file rename, and concurrent upload deduplication.
- **S3 LFS backend** (behind `lfs-s3` Cargo feature flag): supports any S3-compatible service (AWS S3, MinIO, Cloudflare R2) with presigned URL generation for direct client-to-S3 transfers.
- SSH `git-lfs-authenticate` command handling: returns LFS endpoint URL over SSH so `git lfs` clients can discover the HTTP transfer endpoint when using SSH remotes.
- `MULI_LFS_MAX_OBJECT_SIZE_MB` configuration (default 5 GB) for controlling maximum LFS object size.
- 9 LFS unit tests (types serde, filesystem storage CRUD, digest verification, dedup, size limits).
- 11 LFS end-to-end tests covering batch upload/download flows, auth enforcement (unauthenticated, wrong token), invalid oid rejection, missing object handling, verify size mismatch, digest mismatch, dedup skip, multi-object batch, and tenant isolation.

## [0.1.12] - 2026-03-15

### Fixed

- Shallow clone (`git clone --depth=1`) now works correctly. The `Git-Protocol` header from clients requesting protocol v2 is forwarded as `HTTP_GIT_PROTOCOL` to `git http-backend`, and `info/refs` handlers now pass actual request headers instead of empty ones.
- New bare repositories now default HEAD to `refs/heads/main` via `git init --bare -b main`, ensuring the symref is properly advertised in ref discovery when users push to `main`.
- SSH accept loop no longer terminates on transient errors (EMFILE, ECONNRESET). The listener now logs the error, sleeps briefly, and continues instead of breaking out of the loop.

### Added

- SSH connection semaphore limiting concurrent sessions to 128, preventing resource exhaustion under load. Connections beyond the limit are dropped with a warning log.
- `SERVER_PROTOCOL=HTTP/1.1` environment variable set for `git http-backend` CGI subprocess.
- End-to-end test for shallow clone with protocol v2 (`--depth=1`), verifying single-commit history and `.git/shallow` marker.
- End-to-end test verifying HEAD symref points to `refs/heads/main` after repository creation, both on disk and via `git ls-remote --symref`.
- End-to-end test for SSH concurrency: 5 parallel clone operations followed by a post-concurrency clone to verify server stability.

## [0.1.11] - 2026-03-15

### Security

- SSH per-repo ACL enforcement: SSH push and pull operations now verify the user is the repository owner or a collaborator with the required permission. Previously, any user with a valid SSH key could push to any repository.
- Org-not-found during SSH cross-tenant access now correctly rejects the request instead of silently skipping the membership check.
- HTTP push to public repositories now requires the user to be an owner or collaborator, matching the SSH path behavior.

### Added

- Shared `check_repo_access()` function in `muli-core` used by both HTTP and SSH auth paths, eliminating duplicated ACL logic and preventing future drift.
- `RepoAccessVerdict` enum for clear, testable access control decisions.
- `CollaboratorStore` wiring for SSH server, enabling per-repo collaborator checks.
- `MemoryCollaboratorStore` in-memory implementation for testing.
- Comprehensive unit tests for `check_repo_access` covering all ACL branches (15 tests).
- End-to-end SSH security tests: private repo clone/push denied for non-collaborators, push denied for pull-only collaborators, public repo push denied for non-collaborators (5 tests).
- End-to-end HTTP ACL tests: anonymous public read, push denied for non-collaborators, private repo access control, owner-based access (5 tests).

### Fixed

- gRPC agent log streaming tests now include tenant metadata, fixing "missing x-tenant-id" failures introduced by tenant enforcement.
- gRPC test harness `run_job` helper now persists logs and removes the log collector after job completion, matching production behavior and fixing `is_complete` flag assertions.
- Docker log streaming test assertions relaxed to not require non-empty log output from containers that produce no stdout.

## [0.1.10] - 2026-03-15

### Fixed

- `init_repo` now tolerates an existing git directory on disk, allowing re-link after unlink without errors. Previously, unlinking a project removed only the DB record while the bare repo directory remained, causing subsequent link attempts to fail with "repository already exists".

## [0.1.9] - 2026-03-14

### Added

- SSH authentication now requires `user_id` on keys; keys without a user are rejected.
- SSH server resolves tenant by namespace with fallback to `default_tenant_id`, supporting both single-tenant and subdomain multi-tenant deployments.
- Org membership verification for cross-tenant SSH access: users pushing to a repo in a different tenant must be a member of the target org.
- `find_by_fingerprint_in_tenant` method on `SshKeyStore` trait for tenant-scoped SSH key lookups.
- Global `ssh_key_fingerprints` index table in SQLite `_global.db` for O(1) fingerprint lookups instead of scanning all tenant databases.
- Automatic backfill migration populates the global fingerprint index when tenant databases are opened.

### Changed

- Webhook delivery now respects `allow_localhost_webhooks` flag, skipping SSRF validation in development/test environments.

## [0.1.8] - 2026-03-14

### Changed

- Token hashing (Argon2id) moved to `spawn_blocking` to avoid blocking async workers during CPU-intensive computation.
- Job watch streams now track consecutive errors and terminate cleanly after 3 failures instead of retrying indefinitely.

### Fixed

- Log streaming tests now send proper tenant metadata headers via `with_tenant` wrapper.
- Log streaming tests verify sequence number ordering and job ID correctness.

## [0.1.7] - 2026-03-09

### Added

- Centralized webhook URL/target SSRF validation shared across REST and gRPC paths.
- Webhook delivery hardening: redirect disabled and pre-delivery target checks.
- CLI TLS support for `https://` gRPC endpoints with optional custom CA path.
- CLI packaging update to include proto files in npm package.
- Tag-driven release workflow that publishes multi-platform Rust binaries, checksums, and npm CLI.
- CLI `server` lifecycle commands: install/start/stop/status/update with latest-version awareness.
- CI workflows for Rust and CLI checks.
- Security model documentation and ops hardening updates.
- OSS project templates: issue templates and pull request template.

### Changed

- README rewritten to be quickstart-first and documentation-index oriented.
- CONTRIBUTING guide expanded with setup/checklists and validation guidance.
- Release automation split: `v*` tags now publish Rust binaries/GitHub release only; npm CLI publish moved to dedicated `npm-cli-v*` workflow.
- Server and agent startup now install a Rustls process-level crypto provider to avoid runtime TLS panics.
- Embedded agent now auto-selects `https://` + local CA when gRPC TLS is enabled, fixing secure-local registration failures.

### Security

- gRPC webhook creation now enforces SSRF-safe URL validation by default.
- Documented current webhook secret-at-rest behavior and operational mitigations.
