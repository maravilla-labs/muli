# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
