# Releases

A **release** is a named, tag-anchored snapshot of a repository — a git tag plus
human-facing metadata (title, notes/changelog) and optional **downloadable
binary assets** (compiled artifacts, archives, checksums). Releases are the
distribution surface for tagged versions of a repo, analogous to GitHub/Gitea
releases.

## Model

| Field | Description |
|-------|-------------|
| `tag` | Git tag the release is anchored to (e.g. `v1.0.0`). Unique per repo. |
| `target_commitish` | Branch or commit the tag points at (informational). |
| `name` | Display title. Defaults to the tag when omitted. |
| `body` | Release notes / changelog (markdown). |
| `draft` | Unpublished. Drafts are excluded from public listings and never fire release triggers. |
| `prerelease` | Marks a non-production release (e.g. `v1.0.0-rc1`). |
| `published_at` | Stamped when a release transitions from draft to published. |
| `assets[]` | Downloadable binaries attached to the release. |

Each **asset** records `name`, `size`, `sha256`, `content_type`, and a storage
key; the bytes live in the asset object store (see [Storage](#storage)).

## Lifecycle

1. Push a tag (e.g. `git push origin v1.0.0`).
2. Create a release referencing that tag — as a **draft** while you prepare
   notes/assets, or published immediately.
3. Attach assets (upload one or more binaries).
4. **Publish** the release (`draft = false`). This stamps `published_at` and
   makes the release visible in published listings.

Updating a release can change its name, notes, `prerelease` flag, and draft
state. Deleting a release removes its asset bytes and records.

## Assets

Assets are uploaded and downloaded per release:

- **Upload** stores the bytes and returns the computed `size` and `sha256`.
- **Download** streams the bytes back with the recorded `content_type`.
- **List/Delete** operate on a release's assets.

Asset names are validated to prevent path traversal (`..`, `/`, `\`, null bytes
are rejected).

## Visibility & access control

Releases have **no independent access control** — they inherit the
repository's existing gate. By default repositories are authenticated-only
(`anonymous_pull` defaults to `false`; see the
[Security Model](security-model.md)), so release metadata and asset downloads
require authentication and are scoped to the owning tenant. There is **no new
public surface**: a release of a private repo stays private.

> A public, unauthenticated asset-download path for repositories that opt into
> anonymous pull (mirroring anonymous git read) is planned; until then, asset
> downloads follow the authenticated, tenant-scoped path.

## Storage

Asset bytes are kept in a dedicated object store, keyed by
`tenant_id/release_id/asset_id`. The default backend is the local filesystem
(under the server data directory), using the same upload → `(size, sha256)` /
download / delete shape as pipeline artifacts, so an S3/object-store backend is
a drop-in addition later.

## API

Releases are exposed by the `ReleaseService` gRPC API:

```
service ReleaseService {
  rpc CreateRelease(...)        returns (Release);
  rpc GetRelease(...)           returns (Release);
  rpc GetReleaseByTag(...)      returns (Release);
  rpc ListReleases(...)         returns (ListReleasesResponse);   // published_only filter
  rpc UpdateRelease(...)        returns (Release);
  rpc DeleteRelease(...)        returns (DeleteReleaseResponse);

  rpc UploadReleaseAsset(...)   returns (ReleaseAsset);
  rpc DownloadReleaseAsset(...) returns (DownloadReleaseAssetResponse);
  rpc ListReleaseAssets(...)    returns (ListReleaseAssetsResponse);
  rpc DeleteReleaseAsset(...)   returns (DeleteReleaseAssetResponse);
}
```

Every RPC is tenant-scoped: the caller's tenant (from the `x-tenant-id`
metadata) must match the request, and the release must belong to the caller's
tenant + repo.

## Relationship to pipelines

Releases pair naturally with [pipelines](pipelines.md): a tagged release can
build and publish artifacts, then attach them as release assets. First-class
**`on: release`** and **`on: push: tags:`** pipeline triggers — so cutting a
release runs a release pipeline automatically — are planned (today a
push-triggered pipeline can branch on the tag via an `if:` expression such as
`if: tag == 'v1.0.0'`).

## Status

The release **engine** (model, storage, and the `ReleaseService` gRPC API
including asset upload/download) is implemented. Tag/release pipeline triggers
and the public anonymous-pull asset-download path are planned follow-ups.
