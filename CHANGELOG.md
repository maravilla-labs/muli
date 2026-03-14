# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
