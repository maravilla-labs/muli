# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
