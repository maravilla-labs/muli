# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build                                              # Build all crates
cargo test --workspace                                   # Run all tests
cargo test -p muli-git                                   # Test a single crate
cargo test -p muli-git test_name                         # Run a single test
cargo fmt --all                                          # Format
cargo clippy --workspace --all-targets -- -D warnings    # Lint (CI treats warnings as errors)
```

CLI (packages/cli):
```bash
cd packages/cli && npm ci && npm run build && npm test
```

Toolchain: Rust 1.88 (`rust-toolchain.toml`). Protobuf compiler (`protoc`) needed only when editing `.proto` files. Docker daemon required for integration/e2e tests.

## Architecture

Muli is a multi-tenant DevOps platform: git hosting, package registry, CI/CD pipelines, and job execution — all in one Rust workspace.

### Crate dependency flow

```
proto/muli/v1/           (11 .proto files — gRPC service + message definitions)
    ↓
muli-proto               (protobuf codegen via tonic-build)
    ↓
muli-core                (domain models + store traits in src/traits/)
    ↓
muli-store               (memory / SQLite / MongoDB implementations)
    ↓
muli-engine              (Docker executor via Bollard)
muli-queue               (job scheduler)
muli-registry            (OCI, npm, Cargo, Maven registries)
muli-git                 (HTTP smart protocol, SSH, REST API, LFS)
muli-pipeline            (CI/CD DAG orchestrator, YAML in .maravilla/pipeline.yml)
    ↓
muli-server              (main binary — gRPC + embedded HTTP services)
muli-agent               (remote worker binary)
```

### Other packages

- **`packages/cli/`** — TypeScript/Node.js CLI client (`npm ci && npm run build && npm test`). Published to npm via `npm-cli-v*` tags.

### Documentation

- **`docs/`** — user-facing docs: `quickstart.md`, `operations.md` (production runbook), `pipelines.md` (CI/CD guide), `security-model.md`, `feature-comparison.md`.
- **`.env.example`** — all environment variables with descriptions.
- **`CONTRIBUTING.md`** — dev setup and PR guidelines.

### Key design patterns

- **Trait-based storage**: Store traits live in `muli-core/src/traits/`. Three backends implement them: memory (tests), SQLite (default), MongoDB (optional via `MULI_MONGODB_URL`).
- **StoreFactory pattern**: Each backend exposes a factory (`SqliteStoreFactory`, `MemoryStoreFactory`, etc.) that produces all store implementations for a given tenant.
- **Multi-tenant isolation**: `tenant_id` threaded through all models. SQLite uses `DashMap<tenant_id, Arc<Connection>>`. Filesystem paths: `{root}/{tenant_id}/...`.
- **Token auth**: SHA-256 hashed, constant-time comparison via `subtle` crate. Never store plaintext.
- **Proto enums**: prost strips the enum name prefix — `ORG_ROLE_OWNER` in proto becomes `OrgRole::Owner` in Rust.

### Ports (defaults)

| Service  | Port  | Env var              |
|----------|-------|----------------------|
| gRPC     | 50051 | `MULI_GRPC_PORT`     |
| Registry | 5000  | `MULI_REGISTRY_PORT` |
| Git HTTP | 7000  | `MULI_GIT_PORT`      |
| Git SSH  | 2222  | `MULI_GIT_SSH_PORT`  |

### Storage layout

- SQLite DBs: `{MULI_DATA_DIR}/` (default `./data`)
- Registry blobs: `{MULI_DATA_DIR}/registry/{tenant_id}/...`
- Git bare repos: `{MULI_DATA_DIR}/git/{tenant_id}/{namespace}/{repo}.git/`

## Known pitfalls

- **git http-backend** requires `REMOTE_USER` env var for push or it returns 403.
- **git http-backend stdin**: Must always close stdin (drop the pipe) even for GET requests, or the process blocks.
- **axum/matchit routing**: matchit 0.8 doesn't support static text after params (`/{repo}.git/...` is invalid). Capture the full segment and strip `.git` suffix in the handler.
- **`#[tokio::test]`** is single-threaded by default. Use `tokio::process::Command` (not `std::process::Command`) in async tests to avoid blocking the runtime.
- **tokio-rusqlite**: `conn.call()` closures must return `tokio_rusqlite::Result<T>`. Avoid chaining `query_map()?.collect()` in match arms (E0597); use `while let Some(row) = rows.next()?` instead.
- **rusqlite version**: `tokio-rusqlite 0.5` requires `rusqlite ^0.30` — they must stay in sync.

## Coding conventions

- Avoid `unwrap()`/`expect()` outside tests.
- Keep source files under ~300-500 lines; split into submodules.
- `#[async_trait]` on all store trait definitions and implementations.
