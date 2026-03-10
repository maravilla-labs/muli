# muli-test

Test utilities, fixtures, and integration tests for the Muli system.

## Overview

This crate provides shared test helpers used across the Muli workspace. It contains test data generators, Docker test setup/teardown utilities, and gRPC test helpers.

## Contents

- **Fixtures** — Factory functions for creating test `Job`, `JobSpec`, `AgentInfo`, and configuration objects with sensible defaults.
- **Docker helpers** — Utilities for setting up and tearing down Docker environments during integration tests.
- **gRPC helpers** — Test gRPC clients and mock server setup for service-level testing.

## Running Tests

From the workspace root:

```bash
# Run all unit tests
cargo test

# Run integration tests (requires Docker + MongoDB)
cargo test -- --ignored
```

## Usage

```toml
[dev-dependencies]
muli-test = { path = "../muli-test" }
```

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
