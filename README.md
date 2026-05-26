# Muli

Muli is a self-hostable developer infrastructure platform written in Rust.

It combines:
- containerized job execution
- CI/CD pipelines (YAML DSL, DAG, matrix, artifacts, caching)
- multi-tenant package registries (OCI/Docker, npm, Cargo, Maven)
- multi-tenant git hosting (HTTP + SSH)
- repository releases (tagged snapshots with downloadable assets)
- user/org/tenant APIs

## Status

Muli is currently in early-stage development (`0.1.x`). Expect fast iteration and API changes.

## Quickstart (Local)

For the full end-to-end walkthrough (registry + git + npm), use [docs/quickstart.md](docs/quickstart.md).

### 1. Prerequisites

- Docker daemon running
- Node.js 18+ (for the `muli` CLI)

### 2. Install CLI

```bash
npm install -g @maravilla-labs/muli
```

From source (before npm publish):

```bash
cd packages/cli
npm install
npm run build
npm install -g .
```

### 3. Install and start local server

```bash
# Install latest release binary for your platform
muli server install

# Start server in foreground (first run triggers guided setup)
muli server start

# Optional: run in background
muli server start --detach

# Show running state + latest-version awareness
muli server status
```

If you use a private CA for TLS gRPC:

```bash
muli config set tlsCaCertPath /path/to/ca.pem
```

### 4. Connect and run a test job

```bash
muli auth login http://localhost:50051
muli auth whoami
muli job run --image alpine -- sh -c "echo 'hello from muli'"
```

To check/update server binary:

```bash
muli server update --check
muli server update
```

Setup utilities:

```bash
muli setup doctor
muli setup rerun
```

### Release Channels

- `vX.Y.Z` tags publish Rust binaries (`muli-server`, `muli-agent`) and GitHub Release assets.
- `npm-cli-vX.Y.Z` tags publish the npm CLI package `@maravilla-labs/muli`.

## Production Hardening

Before internet-facing deployment:
- set `MULI_API_KEY` and `MULI_REQUIRE_AUTH=true`
- enable TLS for gRPC and registry, and TLS for git HTTP (directly or via reverse proxy)
- keep gRPC on private network boundaries
- rotate any development tokens before go-live

See [docs/operations.md](docs/operations.md) and [docs/security-model.md](docs/security-model.md) for the full runbook and threat model.

## Architecture (High Level)

```
Client (gRPC / HTTP / SSH)
    |
    v
muli-server
  |- muli-queue      (scheduler + concurrency control)
  |- muli-agent      (remote or embedded workers)
  |- muli-engine     (Docker executor)
  |- muli-store      (SQLite / MongoDB / memory backends)
  |- muli-registry   (OCI + npm + Cargo + Maven)
  \- muli-git        (git HTTP + SSH + REST)
```

## Workspace Layout

| Crate | Purpose |
|---|---|
| `muli-server` | Main server binary and gRPC services |
| `muli-agent` | Agent binary for remote workers |
| `muli-git` | Embedded git hosting service |
| `muli-registry` | Embedded package registries |
| `muli-core` | Shared domain models, validation, traits |
| `muli-store` | Storage implementations |

## Documentation

- [Documentation Index](docs/index.md)
- [Quickstart](docs/quickstart.md)
- [Pipelines](docs/pipelines.md)
- [Releases](docs/releases.md)
- [Operations Runbook](docs/operations.md)
- [Security Model](docs/security-model.md)
- [Release Policy](docs/release-policy.md)
- [Vision](VISION.md)
- [Changelog](CHANGELOG.md)

## Contributing

- [Contributing Guide](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Support](SUPPORT.md)

## License

Dual-licensed under:
- [MIT](LICENSE-MIT)
- [Apache-2.0](LICENSE-APACHE)
