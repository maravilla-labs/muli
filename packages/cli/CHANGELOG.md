# Changelog

All notable changes to the `@maravilla-labs/muli` CLI are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-03-10

### Changed

- `muli server start` now runs in the foreground by default.
- `muli server start --detach` remains available for background execution.

### Fixed

- Detached start now validates that `muli-server` is still running shortly after launch.
- If detached startup fails, CLI now returns a clear error instead of false success.
- Startup failure output now includes recent server log context to speed up debugging.

