# Quickstart: Install from Source & End-to-End Walkthrough

This guide walks you from a fresh clone to running jobs, pushing Docker images to the embedded registry, publishing npm packages, and pushing to git — all locally without any DNS infrastructure.

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| [Rust toolchain](https://rustup.rs/) | Build the server |
| Docker (daemon running) | Job execution and registry push/pull |
| `git` binary | Git hosting feature |
| Node.js >= 18 | `muli` CLI |
| `npm` | Install the CLI and npm registry section (optional) |

---

## 1. Install from source

```bash
git clone https://github.com/maravilla-labs/muli
cd muli

# Install to ~/.cargo/bin/
cargo install --path crates/muli-server
```

Alternatively, build a release binary without global install:

```bash
cargo build --release
# Binary at: target/release/muli-server
```

---

## 2. Localhost setup

The registry and git services use subdomain routing: `{tenant}.{base_domain}:{port}`. Throughout this guide the tenant name is **`local`** and the base domain is **`localhost`**, so all service URLs use `local.localhost`.

**macOS 13+**: `*.localhost` resolves to `127.0.0.1` automatically — no action needed.

**Linux** (and macOS 12 and earlier): add one line to `/etc/hosts`:

```
127.0.0.1  local.localhost
```

Verify resolution:

```bash
ping -c1 local.localhost
```

---

## 3. Start the server

`muli server start` defaults to a local full-stack profile, runs first-run setup when needed, and stays in the foreground unless `--detach` is passed.

```bash
muli server start

# Optional: run in background
muli server start --detach
```

You can run prerequisite diagnostics or rerun setup at any time:

```bash
muli setup doctor
muli setup rerun
```

If you start the server binary directly, `--embedded-agent` starts a built-in agent in the same process — no second terminal needed.

```bash
muli-server --registry full --git --embedded-agent --default-tenant local
```

> **macOS — port 5000 conflict:** macOS 12+ runs AirPlay Receiver on port 5000 by default.
> If startup fails with `Address already in use`, either:
> - Disable it: **System Settings → General → AirDrop & Handoff → AirPlay Receiver → Off**
> - Or use a different port: `muli-server --registry full --registry-port 5001 --git --embedded-agent`
>   (replace `5000` with `5001` in all registry commands below)

> **macOS — port 7000 conflict:** macOS 13+ (Ventura and later) binds port 7000 for
> AirPlay / Control Center. If startup fails with `Address already in use` on port 7000:
> - Use a different port: `muli-server --registry full --git --git-port 7001 --embedded-agent`
>   (replace `7000` with `7001` in all git commands below)

Expected startup output:

```
INFO muli_server: SQLite store initialized  data_dir=~/.local/share/muli
INFO muli_server: Default tenant configured  default_tenant=local
INFO muli_server: HTTP metrics server listening  addr=0.0.0.0:9090
INFO muli_server: Registry listening on port 5000  addr=0.0.0.0:5000
INFO muli_server: Git service listening on port 7000  addr=0.0.0.0:7000
INFO muli_server: Embedded agent enabled — spawning in-process agent
INFO muli_server: gRPC server listening  addr=0.0.0.0:50051
INFO muli_agent::registration: registered with server  agent_id=...
INFO muli_server: embedded agent registered  agent_id=...
```

Service ports:

| Service | Port | Protocol |
|---------|------|---------|
| gRPC (jobs, tokens, git mgmt) | `50051` | gRPC (plaintext) |
| Metrics | `9090` | HTTP |
| Docker registry + npm | `5000` (or `--registry-port`) | HTTP |
| Git HTTP | `7000` (or `--git-port`) | HTTP |

Data is persisted to `~/.local/share/muli/` on Linux or `~/Library/Application Support/muli/` on macOS (SQLite + bare git repos). Override with `--data-dir <PATH>` or `MULI_DATA_DIR`.

---

## 4. Install the CLI

In a separate terminal, build and install the `muli` CLI:

```bash
cd packages/cli
npm install
npm run build
npm install -g .
```

Connect the CLI to your running server (no API key required in dev mode):

```bash
muli auth login http://localhost:50051
# → Connected to http://localhost:50051, tenant: local
```

You can confirm the connection at any time with:

```bash
muli auth whoami
```

---

## 5. Docker registry

### Mark the registry as insecure

The registry runs over plain HTTP. Docker refuses to push to an HTTP registry unless it is listed as insecure.

**Docker Desktop (macOS/Windows):** Settings → Docker Engine → add to `daemon.json`:

```json
{
  "insecure-registries": ["local.localhost:5000"]
}
```

Apply & Restart.

**Linux (standard Docker):** add to `/etc/docker/daemon.json`:

```json
{
  "insecure-registries": ["local.localhost:5000"]
}
```

Then `sudo systemctl restart docker`.

### Create a token and log in

```bash
# Create a registry token
muli registry token create --description quickstart

# Log in to the local registry (runs docker login automatically)
muli registry docker-login
```

### Push and pull

```bash
# Tag an image
docker pull hello-world
docker tag hello-world local.localhost:5000/hello-world:latest

# Push
docker push local.localhost:5000/hello-world:latest

# Pull back
docker rmi local.localhost:5000/hello-world:latest
docker pull local.localhost:5000/hello-world:latest
```

---

## 6. npm registry

```bash
# Create a registry token
TOKEN_OUTPUT=$(muli registry token create --description npm)
# Copy the token value from the output above

# Write a project .npmrc pointing at the local registry
cat > .npmrc <<EOF
registry=http://local.localhost:5000/-/npm/
//local.localhost:5000/-/npm/:_authToken=<paste-token-here>
EOF

# Create and publish a minimal package
mkdir -p /tmp/my-pkg && cd /tmp/my-pkg
npm init -y
npm publish --registry http://local.localhost:5000/-/npm/

# Install it back
cd /tmp
npm install my-pkg --registry http://local.localhost:5000/-/npm/
```

---

## 7. Git hosting

### Create a git access token

```bash
muli git token create --description quickstart
```

Copy the token value printed in the output — you'll use it as the git password.

### Create a repository

```bash
# Create the repository (--init also clones it locally)
muli repo create hello --namespace demo --init
cd hello
```

Or without `--init`:

```bash
muli repo create hello --namespace demo
git clone http://user:<token>@local.localhost:7000/demo/hello
cd hello
```

### Commit and push

```bash
echo "# hello" > README.md
git add README.md
git commit -m "initial commit"
git push origin main
```

---

## 8. Submit a job

```bash
# Run a job — opens a live TUI that streams logs in real-time
muli job run --image alpine -- sh -c "echo 'hello from muli'"
```

The terminal UI shows state transitions (PENDING → PULLING → RUNNING → SUCCEEDED) and streams log output as the container runs. The command exits with code 0 on success.

### Manage jobs

```bash
# List recent jobs
muli job list

# Check job status
muli job status <job-id>

# Stream logs from a completed job
muli job logs <job-id>

# Follow logs in real-time (same TUI as job run)
muli job logs <job-id> --follow

# Cancel a running job
muli job cancel <job-id>
```

---

## 9. Verification checklist

1. `which muli-server` finds the binary in `~/.cargo/bin/`
2. Server starts with all three feature logs: registry port 5000, git port 7000, gRPC port 50051
3. Server logs show `embedded agent registered`
4. `muli auth login http://localhost:50051` prints "Connected"
5. `muli registry docker-login` succeeds
6. `docker push local.localhost:5000/hello-world:latest` succeeds
7. `docker pull local.localhost:5000/hello-world:latest` succeeds after `docker rmi`
8. `npm publish` succeeds; `npm install` finds the package
9. `muli repo create hello --namespace demo` prints the repo URL
10. `git push origin main` succeeds; a fresh `git clone` retrieves the commit
11. `muli job run --image alpine -- echo hello` shows RUNNING → SUCCEEDED in the TUI

---

## 10. Scaling out with standalone agents

The embedded agent is convenient for local development but runs inside the server process and shares its resources. For production or multi-machine setups you can run `muli-agent` as a separate process — or run several of them — each connecting to the same server.

Install the agent binary:

```bash
cargo install --path crates/muli-agent
```

Start the server **without** the embedded agent flag:

```bash
muli-server --registry full --git --default-tenant local
```

Then start one or more agents in separate terminals (or on separate machines):

```bash
# Agent on the same machine
muli-agent --name agent-1

# Agent on a different machine pointing at the server
muli-agent --name agent-2 --server-url http://192.168.1.10:50051
```

Each agent registers independently, sends heartbeats, and picks up jobs from the queue. You can run as many as you need — the scheduler distributes work across all registered agents.

---

## 11. What's next

- **Production deployment**: Set `MULI_API_KEY`, `MULI_REQUIRE_AUTH=true`, and configure TLS via `MULI_TLS_CERT_PATH` / `MULI_TLS_KEY_PATH` (registry TLS: `MULI_REGISTRY_TLS_CERT_PATH` / `MULI_REGISTRY_TLS_KEY_PATH`).
- **Multi-tenant**: Point wildcard DNS `*.registry.yourdomain.com` and `*.git.yourdomain.com` at your server and set `MULI_REGISTRY_DOMAIN` / `MULI_GIT_DOMAIN` accordingly.
- **Operations**: See `docs/operations.md` for a production runbook.
