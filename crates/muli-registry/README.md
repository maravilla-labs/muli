# muli-registry

Multi-tenant OCI Distribution v2 compliant registry for the Muli system. Supports Docker images, npm packages, and Cargo crates through a single server.

## Overview

When `MULI_REGISTRY_ENABLED=true`, the server starts a registry on its own port (default `5000`). The OCI/Docker protocol is always available. npm and Cargo protocols are opt-in via their own flags:

| What | Flag | Gives you |
|------|------|-----------|
| Docker push/pull | `MULI_REGISTRY_ENABLED=true` | OCI Distribution v2 API |
| npm publish/install | `MULI_NPM_ENABLED=true` | npm registry API (`/-/npm/`) |
| Cargo publish/download | `MULI_CARGO_ENABLED=true` | Cargo sparse index + API (`/index/`, `/api/v1/crates/`) |
| Luat packages | `MULI_LUAT_ENABLED=true` | Luat registry API (`/index/@scope/name`, `/api/v1/packages/`, `/api/v1/me`) |

Each tenant accesses their registry via subdomain routing (`{tenant}.registry.example.com`), with separate storage, authentication, quotas, and metrics. A **default tenant** mode lets you skip subdomain setup entirely for single-tenant or local development.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MULI_REGISTRY_ENABLED` | `false` | Enable the registry |
| `MULI_REGISTRY_PORT` | `5000` | Listen port |
| `MULI_REGISTRY_DOMAIN` | `localhost` | Base domain for tenant subdomains |
| `MULI_REGISTRY_ROOT` | `/var/lib/muli/registry` | Storage path |
| `MULI_REGISTRY_MAX_SIZE_GB` | `50` | Max total storage |
| `MULI_REGISTRY_MAX_BLOB_SIZE_MB` | `5120` | Max single blob size |
| `MULI_NPM_ENABLED` | `false` | Enable npm protocol |
| `MULI_CARGO_ENABLED` | `false` | Enable Cargo protocol |
| `MULI_LUAT_ENABLED` | `false` | Enable the Luat package protocol |
| `MULI_REGISTRY_TLS_CERT_PATH` | — | TLS certificate (wildcard for subdomains) |
| `MULI_REGISTRY_TLS_KEY_PATH` | — | TLS private key |

## Tenant Modes

**Multi-tenant** — requests are routed by subdomain. `acme.registry.example.com` maps to tenant `acme`. Requires wildcard DNS + wildcard TLS.

**Single-tenant / local dev** — when using the library, set a default tenant so any request without a subdomain is accepted:

```rust
let tenant_config = TenantConfig::new("localhost")
    .with_default_tenant("myorg");
```

Via `muli-server`, set `MULI_REGISTRY_DOMAIN=localhost` and access with `myorg.localhost:5000` (most systems resolve `*.localhost` to `127.0.0.1`).

## Authentication

Tokens are created via gRPC (`RegistryService.CreateRegistryToken`) with permissions: **pull**, **push**, or **admin**.

```bash
grpcurl -plaintext \
  -d '{"tenant_id":"myorg","permissions":["REGISTRY_PERMISSION_PULL","REGISTRY_PERMISSION_PUSH"],"description":"dev token"}' \
  localhost:50051 muli.v1.RegistryService/CreateRegistryToken
```

Save the returned `plaintext_token`. The registry accepts it as Basic, Bearer, or raw `Authorization` header — each client sends its preferred format automatically.

## Quick Start

Examples below use `myorg.localhost:5000` (single-tenant local dev). Replace with your actual host in production.

### Docker

```bash
docker login myorg.localhost:5000 -u user -p <token>
docker tag myapp:latest myorg.localhost:5000/myapp:v1.0
docker push myorg.localhost:5000/myapp:v1.0
docker pull myorg.localhost:5000/myapp:v1.0
```

> **HTTP registries:** Docker auto-allows HTTP for `127.0.0.1`. For other hostnames, add them to `insecure-registries` in `~/.docker/daemon.json` (see [INTEGRATION_TESTS.md](INTEGRATION_TESTS.md#docker-test-9)).

### npm

`.npmrc` (project or user level):

```ini
registry=http://myorg.localhost:5000/-/npm/
//myorg.localhost:5000/-/npm/:_authToken=<token>
```

```bash
npm publish
npm install my-package
```

### Cargo

`.cargo/config.toml`:

```toml
[registries.muli]
index = "sparse+http://myorg.localhost:5000/index/"
token = "<token>"
```

```bash
cargo publish --registry muli
cargo add my-crate --registry muli
```

## API Endpoints

### OCI (Docker)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v2/` | Version check (unauthenticated) |
| `GET` | `/v2/_catalog` | List repositories |
| `GET` | `/v2/{name}/tags/list` | List tags |
| `GET/HEAD/PUT/DELETE` | `/v2/{name}/manifests/{ref}` | Manifest CRUD |
| `GET/HEAD/DELETE` | `/v2/{name}/blobs/{digest}` | Blob read/delete |
| `POST` | `/v2/{name}/blobs/uploads/` | Start upload or mount blob |
| `PATCH/PUT` | `/v2/{name}/blobs/uploads/{id}` | Chunked upload |

### npm (when `MULI_NPM_ENABLED=true`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/-/ping` | Health check |
| `GET` | `/-/whoami` | Authenticated tenant |
| `PUT` | `/-/npm/{package}` | Publish |
| `GET` | `/-/npm/{package}` | Packument |
| `GET` | `/-/npm/{package}/-/{tarball}` | Download tarball |
| `GET` | `/-/v1/search?text=query` | Search |

### Cargo (when `MULI_CARGO_ENABLED=true`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/index/config.json` | Sparse index config |
| `GET` | `/index/{prefix}/{crate}` | Index entry |
| `PUT` | `/api/v1/crates/new` | Publish |
| `GET` | `/api/v1/crates/{name}/{version}/download` | Download .crate |
| `DELETE/PUT` | `/api/v1/crates/{name}/{version}/yank` | Yank / unyank |

### Luat (when `MULI_LUAT_ENABLED=true`)

The server side of the Luat registry protocol (`luat/docs/packages.md`), at
the root of the tenant's host. It shares its host with cargo without
overlapping it: Luat paths always carry `@scope`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/index/@{scope}/{name}` | NDJSON index, oldest first; `ETag` / `If-None-Match` |
| `GET` | `/api/v1/packages?q=&page=&per_page=` | Search (no `q`: most recently updated first) |
| `GET` | `/api/v1/packages/@{scope}/{name}` | Package page data (latest version) |
| `GET` | `/api/v1/packages/@{scope}/{name}/{version}` | Page data for one version |
| `GET` | `/api/v1/packages/@{scope}/{name}/{version}/download` | Tarball (`application/gzip`) |
| `PUT` | `/api/v1/packages/@{scope}/{name}/{version}` | Publish (tarball body) |
| `DELETE` / `PUT` | `/api/v1/packages/@{scope}/{name}/{version}/yank` / `unyank` | Flip the yanked flag |
| `GET` | `/api/v1/me` | The token's user and publishable scopes |

**Reads** follow the tenant's registry visibility: on a `public` tenant they
are anonymous, and the read endpoints answer CORS (`Access-Control-Allow-Origin: *`,
preflight included) so a browser site on another origin can call them.
Publish, yank and `me` never get CORS headers.

**Writes** need a registry token with `push` that is **bound to a tenant
user** (`user_id` on `CreateRegistryToken`). The user may publish to a scope
that is their own handle, or the handle of an org of the tenant in which they
are owner, admin or member (viewers may not). Errors are `{"error": "..."}`:
401 no/bad token, 403 scope not allowed (or token not bound to a user),
409 version exists, 413 over 10 MiB compressed / 50 MiB or 5000 files
unpacked, 400 for any other invalid package. Nothing is ever deleted.

**Public registry** (`luat.registry.maravilla.cloud`): it is the tenant `luat`
under `MULI_REGISTRY_DOMAIN=registry.maravilla.cloud`. Set it up once
(gRPC, `x-tenant-id: luat` metadata, plus `authorization: Bearer $MULI_API_KEY`
when an API key is configured):

```bash
G="grpcurl -plaintext -H x-tenant-id:luat localhost:50051"
$G -d '{"id":"luat","name":"Luat packages"}' muli.v1.TenantService/CreateTenant
$G -d '{"tenant_id":"luat","visibility":"public"}' muli.v1.RegistryService/SetRegistryVisibility
$G -d '{"tenant_id":"luat","handle":"senol","external_id":"senol","email":"..."}' muli.v1.UserService/CreateUser   # → user id
$G -d '{"tenant_id":"luat","handle":"maravilla","display_name":"Maravilla"}' muli.v1.OrgService/CreateOrg     # → org id
$G -d '{"tenant_id":"luat","org_id":"<org id>","user_id":"<user id>","role":"ORG_ROLE_OWNER"}' muli.v1.OrgService/AddMember
$G -d '{"tenant_id":"luat","user_id":"<user id>","permissions":["REGISTRY_PERMISSION_PULL","REGISTRY_PERMISSION_PUSH"],"description":"luat publish"}' \
   muli.v1.RegistryService/CreateRegistryToken                                                                     # → plaintext_token
```

Package files live under `{tenant-id}/luat/packages/{scope}/{name}/`
(`index`, `summary.json`, `versions/{version}.tgz`, `versions/{version}.json`)
and count towards the tenant's storage quota.

## Library Usage

Embed in any axum application:

```rust
let storage = FilesystemStorage::new("/var/lib/registry").await?;
let auth = RegistryAuth::new(token_store);
let tenant_config = TenantConfig::new("localhost")
    .with_default_tenant("myorg");

let router = registry_router(
    storage.into(),
    Some(auth),
    tenant_config,
    None,
    RegistryConfig { npm_enabled: true, cargo_enabled: true, ..Default::default() },
);

let listener = tokio::net::TcpListener::bind("0.0.0.0:5000").await?;
axum::serve(listener, router).await?;
```

## Storage Layout

```
{MULI_REGISTRY_ROOT}/{tenant-id}/
  blobs/sha256/{hash}
  manifests/{repository}/{tag-or-digest}
  uploads/{upload-uuid}
```

## Testing

```bash
cargo test -p muli-registry                    # all tests
cargo test -p muli-registry --test integration # integration only
```

CLI tests (npm, cargo, docker) auto-skip when their tool is missing. See [INTEGRATION_TESTS.md](INTEGRATION_TESTS.md) for Docker Desktop setup and details.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
