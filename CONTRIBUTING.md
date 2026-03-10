# Contributing to Muli

Thanks for contributing to Muli.

## Ways to Contribute

- report bugs
- propose features
- improve docs
- submit code changes
- improve tests and CI

## Development Setup

### Prerequisites

- Rust 1.88+
- Docker daemon running
- `protoc` (only when editing `.proto` files)
- Node.js 18+ (for `packages/cli` work)

### Clone and build

```bash
git clone https://github.com/maravilla-labs/muli
cd muli
cargo build
```

### Run checks locally

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CLI-specific checks:

```bash
cd packages/cli
npm ci
npm run build
npm pack --dry-run
```

## Branches and Pull Requests

1. Fork the repo and create a focused branch.
2. Keep PRs scoped to one logical change.
3. Include tests for behavioral changes.
4. Update docs when user-facing behavior changes.
5. Ensure CI passes.

### PR checklist

- [ ] Code builds cleanly (`cargo build`)
- [ ] Formatting/linting clean (`cargo fmt`, `cargo clippy`)
- [ ] Relevant tests added/updated
- [ ] Docs updated (`README` / `docs/*` when needed)
- [ ] Breaking changes clearly called out

## Coding Guidelines

- Prefer explicit, readable code over clever code.
- Avoid `unwrap()`/`expect()` in non-test paths unless failure is unrecoverable by design.
- Keep dependencies minimal.
- Follow existing module patterns and naming.

## Testing Notes

Some integration/e2e tests require network bind permissions and Docker. If they fail in constrained environments, include that context in your PR notes.

## Reporting Issues

Use GitHub Issues and include:

- environment (OS, Rust version, Docker version)
- steps to reproduce
- expected behavior
- actual behavior
- relevant logs/errors

For security issues, do **not** file a public issue. Follow [SECURITY.md](SECURITY.md).

## License

By contributing, you agree contributions are dual-licensed under:

- [MIT](LICENSE-MIT)
- [Apache-2.0](LICENSE-APACHE)
