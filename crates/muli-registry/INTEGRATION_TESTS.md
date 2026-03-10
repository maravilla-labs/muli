# Integration Tests

The registry has 9 integration tests covering OCI/Docker, npm, and Cargo protocols. Tests 1-6 use in-process HTTP via `tower::oneshot()` and need no external tools. Tests 7-9 use real CLI binaries against a TCP server.

## Running Tests

```bash
# All tests (unit + integration)
cargo test -p muli-registry

# Integration tests only
cargo test -p muli-registry --test integration

# Single test with output
cargo test -p muli-registry --test integration -- test_docker_cli --nocapture
```

## Test Matrix

| # | Test | Requires | Auto-skips |
|---|------|----------|------------|
| 1 | `test_oci_push_pull` | Nothing | - |
| 2 | `test_npm_publish_install` | Nothing | - |
| 3 | `test_npm_scoped_publish` | Nothing | - |
| 4 | `test_cargo_publish_download` | Nothing | - |
| 5 | `test_cargo_duplicate_publish` | Nothing | - |
| 6 | `test_unauthenticated_rejected` | Nothing | - |
| 7 | `test_npm_cli_publish_install` | `npm` CLI | Yes, if npm missing |
| 8 | `test_cargo_cli_publish_fetch` | `cargo` CLI | Yes, if cargo missing |
| 9 | `test_docker_cli_push_pull_tag` | Docker daemon | Yes, if docker missing or not configured |

Tests 7-9 skip gracefully when their CLI tool is not installed. No test failure occurs.

## Setup for CLI Tests

### npm (test 7)

Install Node.js/npm. No extra configuration needed.

```bash
brew install node   # macOS
```

### Cargo (test 8)

Install Rust. No extra configuration needed (if you can run `cargo test`, you already have it).

### Docker (test 9)

Docker requires platform-specific setup because of how the daemon accesses the test server.

#### Linux (native Docker)

No extra configuration needed. The Docker daemon runs natively and can reach `127.0.0.1` directly.

```bash
# Install Docker Engine
# https://docs.docker.com/engine/install/

# Verify
docker info
```

#### macOS / Windows (Docker Desktop)

Docker Desktop runs the daemon inside a Linux VM. The VM cannot reach the host's `127.0.0.1`, so the test uses `host.docker.internal` instead. Docker must be configured to allow plain HTTP (insecure registry) for that hostname.

**1. Install Docker Desktop**

```bash
brew install --cask docker   # macOS
```

Then open Docker Desktop and let it finish starting.

**2. Configure insecure registries**

Add the Docker Desktop VM gateway subnet to `~/.docker/daemon.json`:

```json
{
  "insecure-registries": ["192.168.65.0/24"]
}
```

If the file already exists, merge the `insecure-registries` array with any existing entries.

The CIDR `192.168.65.0/24` covers the IP that `host.docker.internal` resolves to inside the Docker Desktop VM (typically `192.168.65.254`). Using a CIDR rather than a hostname allows any dynamic port to work.

**3. Restart Docker Desktop**

```bash
docker desktop stop && docker desktop start
```

**4. Verify**

```bash
docker info | grep -A5 "Insecure Registries"
```

Expected output should include `192.168.65.0/24`:

```
Insecure Registries:
  192.168.65.0/24
  127.0.0.0/8
  ...
```

**5. Run the test**

```bash
cargo test -p muli-registry --test integration -- test_docker_cli --nocapture
```

If the insecure-registries CIDR is not configured, the test prints a `SKIP` message and passes without running.

## What the Docker Test Verifies

The test exercises a full Docker CLI roundtrip against our OCI registry:

1. `docker build` -- builds a minimal `FROM scratch` image
2. `docker push :v1.0` -- pushes to our registry (Basic auth)
3. `docker rmi` -- removes local copy
4. `docker pull :v1.0` -- pulls back from registry
5. `docker inspect` -- verifies pulled image exists
6. `docker tag :v2.0` -- re-tags locally
7. `docker push :v2.0` -- pushes second tag
8. HTTP verify `/v2/{name}/tags/list` -- both `v1.0` and `v2.0` present
9. HTTP verify `/v2/_catalog` -- repository listed
10. `docker rmi` -- cleanup (best-effort)

## Troubleshooting

### Docker test skips with "docker daemon not running"

Start Docker Desktop or the Docker daemon:

```bash
open /Applications/Docker.app          # macOS
sudo systemctl start docker            # Linux
```

### Docker push fails with "server gave HTTP response to HTTPS client"

The insecure-registries CIDR is missing or doesn't match. Verify with:

```bash
docker info | grep -A5 "Insecure"
```

Add `192.168.65.0/24` to `~/.docker/daemon.json` and restart Docker.

### Docker push fails with "connection refused"

On Docker Desktop, this means the daemon (in the VM) cannot reach the host. Ensure Docker Desktop is up to date and the VM networking is working:

```bash
docker run --rm alpine sh -c "wget -q -O- http://host.docker.internal:80/ 2>&1 || echo ok"
```

If `host.docker.internal` doesn't resolve, restart Docker Desktop.
