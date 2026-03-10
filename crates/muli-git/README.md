# muli-git

Multi-tenant git hosting embedded in the Muli server. Provides git clone and push over both HTTP Smart Protocol and SSH, a REST API for repository management, and webhook delivery on push.

## Features

- **HTTP Smart Protocol** — `git clone` / `git push` over plain HTTP via `git http-backend`
- **SSH** — `git clone` / `git push` over SSH with Ed25519 public key authentication
- **Multi-tenant** — each tenant's repositories are isolated under `{root}/{tenant_id}/{namespace}/{repo}.git/`
- **Token auth** — Bearer and Basic auth for HTTP; SHA-256-hashed tokens stored per tenant
- **REST API** — create/delete repos, list refs, commits, blob contents, forks, tags, webhooks
- **Webhooks** — HTTP POST delivery on `push` events, HMAC-SHA256 signed payloads
- **Bare repo storage** — standard git bare repositories; compatible with any git client

## Quick Start

### Enable the git service

```bash
MULI_GIT_ENABLED=true \
MULI_GIT_PORT=7000 \
MULI_GIT_ROOT=/var/lib/muli/git \
MULI_GIT_DOMAIN=git.example.com \
cargo run --release --bin muli-server
```

To also enable SSH:

```bash
MULI_GIT_SSH_ENABLED=true \
MULI_GIT_SSH_PORT=2222 \
cargo run --release --bin muli-server
```

### Create a repository

```bash
curl -X POST http://git.example.com:7000/api/v1/repos \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"namespace": "acme", "name": "my-repo", "description": "My first repo", "is_private": false}'
```

### Clone and push over HTTP

```bash
git clone http://user:<token>@git.example.com:7000/acme/my-repo
# or
git clone http://git.example.com:7000/acme/my-repo  # if auth is disabled
```

### Clone and push over SSH

```bash
# Register your public key via the GitService gRPC API first (see below)
git clone ssh://git@git.example.com:2222/acme/my-repo
```

---

## Configuration

All configuration is through environment variables on the `muli-server` binary:

| Variable | Default | Description |
|----------|---------|-------------|
| `MULI_GIT_ENABLED` | `false` | Enable the HTTP git service |
| `MULI_GIT_PORT` | `7000` | HTTP port |
| `MULI_GIT_DOMAIN` | `localhost` | Base domain for subdomain tenant routing |
| `MULI_GIT_ROOT` | `/var/lib/muli/git` | Root directory for bare repository storage |
| `MULI_GIT_SSH_ENABLED` | `false` | Enable the SSH git service |
| `MULI_GIT_SSH_PORT` | `2222` | SSH port |
| `MULI_GIT_SSH_HOST_KEY_PATH` | `{git_root}/ssh_host_ed25519_key` | Path to the Ed25519 SSH host key. Generated automatically on first run if absent. |

### Storage layout

```
{MULI_GIT_ROOT}/
└── {tenant_id}/
    └── {namespace}/
        └── {repo}.git/     ← bare git repository
```

### Tenant routing

The service supports two tenant routing modes:

**Subdomain** (production): `tenant-a.git.example.com:7000`

Requires wildcard DNS (`*.git.example.com`) pointing to the server.

**Default tenant** (development/single-tenant): all requests to `localhost` fall back to a configured default tenant, no subdomain needed.

```bash
MULI_GIT_DOMAIN=localhost  # enables default-tenant mode
```

---

## Authentication

### HTTP token auth

Tokens are managed via the `GitService` gRPC API and stored as SHA-256 hashes. Each token has a permission level: **pull** or **push**.

The HTTP server accepts both Basic and Bearer auth:

```bash
# Bearer token
curl -H "Authorization: Bearer <token>" http://git.example.com:7000/api/v1/repos

# Basic auth (username is ignored; use anything)
git clone http://user:<token>@git.example.com:7000/acme/my-repo
```

To create a token (via gRPC):

```bash
grpcurl -plaintext \
  -d '{"tenant_id":"acme","permissions":["GIT_PERMISSION_PULL","GIT_PERMISSION_PUSH"],"description":"dev token"}' \
  localhost:50051 muli.v1.GitService/CreateGitToken
```

### SSH public key auth

SSH keys are registered per tenant. The server accepts Ed25519 keys and matches incoming connections by their SHA-256 fingerprint (`SHA256:xxx` format, as produced by `ssh-keygen -l -E sha256`).

Register a key via gRPC:

```bash
grpcurl -plaintext \
  -d '{
    "tenant_id": "acme",
    "title": "my laptop",
    "public_key": "ssh-ed25519 AAAA... user@host"
  }' \
  localhost:50051 muli.v1.GitService/AddSshKey
```

Then clone with your private key:

```bash
git clone ssh://git@git.example.com:2222/acme/my-repo
# Or to specify a key explicitly:
GIT_SSH_COMMAND="ssh -i ~/.ssh/id_ed25519 -o StrictHostKeyChecking=no" \
  git clone ssh://git@git.example.com:2222/acme/my-repo
```

#### Host key

On first start the server generates an Ed25519 host key at `{MULI_GIT_SSH_HOST_KEY_PATH}` (default `{MULI_GIT_ROOT}/ssh_host_ed25519_key`) using `ssh-keygen`. If `ssh-keygen` is unavailable the key is generated in memory (and a new one will be generated on each restart — clients will see a host key change warning).

To pre-generate and pin the host key:

```bash
ssh-keygen -t ed25519 -N "" -f /var/lib/muli/git/ssh_host_ed25519_key
```

---

## REST API

All endpoints require `Authorization: Bearer <token>` (or Basic auth) unless authentication is disabled.

### Repositories

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/repos` | Create a repository |
| `GET` | `/api/v1/repos` | List repositories for the tenant |
| `DELETE` | `/api/v1/repos/{namespace}/{repo}` | Delete a repository |
| `POST` | `/api/v1/repos/{namespace}/{repo}/forks` | Fork a repository |

**Create repository:**

```bash
curl -X POST http://git.example.com:7000/api/v1/repos \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "acme",
    "name": "my-repo",
    "description": "Optional description",
    "is_private": false
  }'
```

Response `201`:
```json
{
  "id": "...",
  "tenant_id": "acme",
  "namespace": "acme",
  "name": "my-repo",
  "description": "Optional description",
  "is_private": false,
  "default_branch": "main",
  "fork_of": null,
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z"
}
```

**Fork a repository** (copies the bare repo on disk):

```bash
curl -X POST http://git.example.com:7000/api/v1/repos/acme/upstream/forks \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"namespace": "acme", "name": "my-fork"}'
```

### References

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/repos/{namespace}/{repo}/refs` | List all refs (branches and tags) |

```bash
curl http://git.example.com:7000/api/v1/repos/acme/my-repo/refs \
  -H "Authorization: Bearer <token>"
```

Response:
```json
[
  {"name": "main", "sha": "abc123...", "type": "branch"},
  {"name": "v1.0", "sha": "def456...", "type": "tag"}
]
```

### Commits

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/repos/{namespace}/{repo}/commits` | List commits on default branch |

Optional query parameters: `?branch=feature&limit=50`

```bash
curl "http://git.example.com:7000/api/v1/repos/acme/my-repo/commits?limit=10" \
  -H "Authorization: Bearer <token>"
```

### File contents

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/repos/{namespace}/{repo}/contents/{path}` | Get raw blob at path |

Optional query parameter: `?ref=main` (defaults to HEAD)

```bash
curl "http://git.example.com:7000/api/v1/repos/acme/my-repo/contents/src/main.rs" \
  -H "Authorization: Bearer <token>"
```

### Tags

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/repos/{namespace}/{repo}/tags` | Create a lightweight or annotated tag |
| `DELETE` | `/api/v1/repos/{namespace}/{repo}/tags/{tag}` | Delete a tag |

**Create a lightweight tag:**

```bash
curl -X POST http://git.example.com:7000/api/v1/repos/acme/my-repo/tags \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "v1.0", "target": "abc123..."}'
```

**Create an annotated tag** (include `"message"`):

```bash
curl -X POST http://git.example.com:7000/api/v1/repos/acme/my-repo/tags \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "v1.0", "target": "abc123...", "message": "Release v1.0"}'
```

Response `201`:
```json
{"name": "v1.0", "target": "abc123..."}
```

**Delete a tag:**

```bash
curl -X DELETE http://git.example.com:7000/api/v1/repos/acme/my-repo/tags/v1.0 \
  -H "Authorization: Bearer <token>"
```

Response: `204 No Content`

Tags can also be created and deleted via normal git push:

```bash
git tag v1.0 && git push origin v1.0
git push origin :refs/tags/v1.0   # delete
```

### Webhooks

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/repos/{namespace}/{repo}/hooks` | Register a webhook |
| `GET` | `/api/v1/repos/{namespace}/{repo}/hooks` | List webhooks |
| `DELETE` | `/api/v1/repos/{namespace}/{repo}/hooks/{hook_id}` | Delete a webhook |

**Register a webhook:**

```bash
curl -X POST http://git.example.com:7000/api/v1/repos/acme/my-repo/hooks \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://ci.example.com/webhook",
    "secret": "my-hmac-secret",
    "events": ["push"]
  }'
```

Webhook deliveries are HTTP `POST` requests with:
- `Content-Type: application/json`
- `X-Muli-Event: push`
- `X-Muli-Signature: sha256=<hmac-hex>` — HMAC-SHA256 of the body using the stored secret

### Health check

```
GET /-/health   →  200 "ok"   (no auth required)
```

---

## Git Smart HTTP Protocol

The server proxies git protocol traffic through `git http-backend`. Clone and push URLs follow the pattern:

```
http(s)://{tenant}.{domain}:{port}/{namespace}/{repo}
```

Or with the `.git` suffix (also accepted):

```
http(s)://{tenant}.{domain}:{port}/{namespace}/{repo}.git
```

In single-tenant / localhost mode:

```
http://localhost:7000/{namespace}/{repo}
```

---

## SSH Protocol

Clone and push URLs:

```
ssh://git@{domain}:{ssh_port}/{namespace}/{repo}
ssh://git@{domain}:{ssh_port}/{namespace}/{repo}.git
```

The username (`git`) is ignored; tenant resolution is done via the SSH public key fingerprint lookup.

The server spawns `git-upload-pack` or `git-receive-pack` as a subprocess and bridges stdio over the SSH channel, so all standard git operations (clone, fetch, push, force-push, tag push) work without any special configuration.

---

## Development

### Run tests

```bash
cargo test -p muli-git
```

The test suite includes 27 unit tests and 19 end-to-end tests. E2e tests start an in-process HTTP server (and optionally an SSH server) against a temporary filesystem, then drive real `git` CLI commands.

Requirements for the full e2e suite:
- `git` binary on `$PATH`
- `ssh-keygen` binary on `$PATH` (for the SSH test)

### Local development with curl

Start the server with all defaults:

```bash
MULI_GIT_ENABLED=true MULI_GIT_DOMAIN=localhost cargo run --bin muli-server
```

Create a repo and push to it (auth disabled in localhost mode):

```bash
curl -X POST http://localhost:7000/api/v1/repos \
  -H "Content-Type: application/json" \
  -d '{"namespace": "dev", "name": "test", "description": "", "is_private": false}'

mkdir test && cd test && git init
git remote add origin http://localhost:7000/dev/test
echo "hello" > README.md && git add . && git commit -m "init"
git push -u origin main
```
