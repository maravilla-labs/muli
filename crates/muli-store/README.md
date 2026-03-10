# muli-store

Persistent storage backends for the Muli job execution system.

## Overview

This crate provides implementations of the `JobStore`, `AgentRegistry`, `RegistryTokenStore`, and `TenantQuotaStore` traits defined in `muli-core`. Two backends are included:

- **MongoDB** — Production backend with indexed queries and atomic state transitions.
- **In-memory** — Lightweight backend using `HashMap` behind `Arc<Mutex<>>`, suitable for development and testing.

## Backends

### MongoDB (`mongodb` module)

- `MongoJobStore` — Stores jobs in a MongoDB collection with indexes for state, tenant, and timestamp queries. Uses atomic updates for state transitions.
- `MongoAgentStore` — Stores agent registrations with heartbeat tracking.
- `MongoRegistryTokenStore` — Stores registry tokens with indexes on `token_hash` (unique), `tenant_id`, and `expires_at`.
- `MongoTenantQuotaStore` — Stores per-tenant storage quotas with upsert semantics.
- `setup_indexes()` — Creates the necessary MongoDB indexes on startup.

### In-Memory (`memory` module)

- `MemoryJobStore` — Thread-safe in-memory job storage.
- `MemoryAgentStore` — Thread-safe in-memory agent registry.
- `MemoryRegistryTokenStore` — Thread-safe in-memory registry token storage using `DashMap`.
- `MemoryTenantQuotaStore` — Thread-safe in-memory tenant quota storage using `DashMap`.

Both backends implement the same trait interface, making them interchangeable.

## Usage

```toml
[dependencies]
muli-store = { path = "../muli-store" }
```

The server selects the backend based on configuration. The in-memory backend requires no external dependencies; the MongoDB backend requires a running MongoDB instance.

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
