# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
