# Muli Security Model

This document describes the current security posture and trust boundaries for Muli deployments.

## Trust Boundaries

- **Control plane (gRPC):** job submission, token management, tenant APIs.
- **Data plane (agents + Docker):** job execution and log streaming.
- **Git/Registry HTTP services:** tenant-scoped artifact and source access.
- **Persistence layer:** SQLite/MongoDB state, git repos, registry blobs.

## Authentication and Authorization

- gRPC uses a shared Bearer API key when `MULI_API_KEY` is set.
- `MULI_REQUIRE_AUTH=true` enforces fail-fast startup if the API key is missing.
- Tenant scoping is enforced through `x-tenant-id` metadata and per-resource ownership checks.
- Git/registry tokens are scoped per tenant and permission-checked (`pull` / `push` / `admin`).

## Transport Security

- gRPC TLS is optional but strongly recommended in production (`MULI_TLS_CERT_PATH`, `MULI_TLS_KEY_PATH`).
- Registry TLS is optional but strongly recommended in production (`MULI_REGISTRY_TLS_CERT_PATH`, `MULI_REGISTRY_TLS_KEY_PATH`).
- Git HTTP should be TLS-protected (either directly or via reverse proxy).

## Webhook Security

- Webhook URLs are validated on create and before delivery:
  - only `http`/`https`
  - rejects localhost and private/link-local IPs
  - DNS resolution checks block domains that resolve to private targets
- Redirects are disabled for webhook delivery requests.
- Webhook payloads are signed with `X-Hub-Signature-256` (HMAC-SHA256).
- **Current limitation:** webhook secrets are stored plaintext at rest to support signing.

## Token and Secret Storage

- Registry/git tokens are stored as Argon2id hashes with a short lookup prefix.
- Webhook secrets are currently plaintext in storage.
- Recommended mitigations:
  - encrypt disks and database volumes
  - restrict DB access with least privilege
  - isolate service accounts and backups

## Threat Assumptions

- Host and container runtime are trusted and patched.
- Deployment perimeter (load balancer / reverse proxy / firewall) enforces expected network segmentation.
- Operators protect environment variables and config files containing secrets.

## Non-Goals (Current)

- Per-user gRPC identity model (today: shared API key).
- Built-in KMS integration for encrypted webhook secret storage.
- mTLS between all internal components by default.
