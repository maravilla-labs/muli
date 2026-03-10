# Release Policy

## Versioning

Muli uses Semantic Versioning (`MAJOR.MINOR.PATCH`).

- `MAJOR`: incompatible API or behavior changes
- `MINOR`: backward-compatible features
- `PATCH`: backward-compatible bug/security fixes

Current maturity is early-stage (`0.x`), so minor releases may still contain breaking changes. Breaking changes must be documented in release notes.

## Branching and Tags

- `main` is the active development branch.
- Binary releases are created by pushing a tag in the form `vX.Y.Z`.
- CLI npm releases are created by pushing a tag in the form `npm-cli-vX.Y.Z` (or by manual workflow dispatch).
- Release automation is implemented in:
  - `.github/workflows/release.yml` (Rust binaries + GitHub release)
  - `.github/workflows/npm-cli-publish.yml` (npm CLI package)

## Automated Release Flow

On `v*` tags, GitHub Actions will:

1. Build Rust binaries:
   - `muli-server`
   - `muli-agent`
2. Publish GitHub Release assets for supported targets:
   - macOS (`darwin-x86_64`, `darwin-aarch64`)
   - Linux (`linux-x86_64`)
   - Windows (`windows-x86_64`)
3. Generate and publish `checksums-<version>.txt`.

On `npm-cli-v*` tags (or manual dispatch), GitHub Actions will:

1. Build and test CLI from `packages/cli`.
2. Publish npm package `@maravilla-labs/muli`.
3. Use repository secret `NPM_TOKEN` (npm token with access to the `@maravilla-labs` scope).

## Release Artifact Contract

Asset names are stable and consumed by the CLI downloader:

- `muli-server-<version>-<target><ext>`
- `muli-agent-<version>-<target><ext>`
- `checksums-<version>.txt`

Examples:

- `muli-server-0.1.0-linux-x86_64`
- `muli-server-0.1.0-darwin-aarch64`
- `muli-server-0.1.0-windows-x86_64.exe`

The CLI `muli server install/update` resolves the correct asset by OS/arch and verifies SHA-256 from `checksums-<version>.txt`.

## Maintainer Checklist (Before Binary Tag `vX.Y.Z`)

1. Ensure CI is green.
2. Run local validations:
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo test --workspace`
   - `npm --prefix packages/cli run build`
   - `npm --prefix packages/cli test`
   - `npm --prefix packages/cli pack --dry-run`
3. Update `CHANGELOG.md`:
   - move relevant entries from `Unreleased` to new version section
4. Create and push tag: `vX.Y.Z`.

## Maintainer Checklist (Before CLI npm Tag `npm-cli-vX.Y.Z`)

1. Ensure CLI validations are green:
   - `npm --prefix packages/cli run build`
   - `npm --prefix packages/cli test`
   - `npm --prefix packages/cli pack --dry-run`
2. Ensure `packages/cli/package.json` uses the intended release version.
3. Ensure repository secret `NPM_TOKEN` is configured.
4. Create and push tag: `npm-cli-vX.Y.Z`.

## Security Releases

For critical security issues:

- prioritize patch release turnaround
- coordinate disclosure per [SECURITY.md](../SECURITY.md)
- clearly mark security fixes in release notes and changelog
