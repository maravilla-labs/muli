# Plan: pipeline-driven releases (tag triggers + declarative `release:`)

## Context

Today a Muli pipeline cannot cut a repository release. The release engine is
complete — `ReleaseService` (gRPC) + `ReleaseStore` + `ReleaseAssetStorage` all
exist, and any control-plane / UI in front of Muli can drive them over gRPC — but
nothing connects a pipeline run to it. Three gaps:

1. **No tag trigger.** `TriggerDef` (`crates/muli-pipeline/src/yaml/schema.rs:45`)
   has only `push`/`pull_request`/`manual`/`schedule`, and `PipelineEvent`
   (`crates/muli-pipeline/src/trigger/matcher.rs:10`) has no `Tag` variant. A
   `git push --tags` reaches `on_push` with `ref_name = refs/tags/v1`, but
   `pipeline_trigger.rs:710` only strips `refs/heads/`, so a tag is mis-handled as
   a branch literally named `refs/tags/v1`.
2. **The documented `if: tag == …` workaround is a dead letter.**
   `make_expr_ctx` hardcodes `tag: None` (`crates/muli-pipeline/src/dag/executor.rs:806`),
   so every tag condition is false even though the evaluator fully supports it
   (`yaml/expression.rs:43`).
3. **No way to create a release from a run.** `create_tag` is parsed and then
   ignored (`crates/muli-server/src/grpc/release_service.rs:94`).

The goal is a GitHub/GitLab-grade experience: push a `v*` tag → a pipeline runs →
it records a release (tag + notes + an archive asset) **with no release credential
inside the sandboxed job container**, because the pipeline engine and the release
store are the same process.

### Decisions (locked)

- **Sequence:** refactor first (behaviour-preserving), then features.
- **Tag syntax:** GitHub-style `on: { push: { tags: ["v*"] } }`.
- **Notes source:** configurable — `notes.from: changelog | git_log | inline`.
- **Assets:** a release attaches the job's artifact **as a single archive asset**.
  Per-file distribution is **not** a release concern.

### Explicit non-goals

- **Package publishing is a separate job, not part of a release.** Muli embeds a
  multi-format registry (`crates/muli-registry` — npm `PUT /-/npm/…`, plus Cargo,
  Maven, OCI), tenant-scoped by token, and publishing to any external registry
  already works from a normal job by writing the client's own auth config.
  Distributing individual package files is the registry's job; a release just
  records the tag + notes + a downloadable archive. This is why the "attach
  individual files" option was dropped — it duplicates the registry. A dedicated
  helper to make in-registry publishing frictionless is designed separately in
  **Part E**.
- No new credential is injected into job containers *for releases*. The release is
  created server-side, after the run, by code that already holds the stores.

## Part A — Refactor `pipeline_trigger.rs` into a module (no behaviour change)

`crates/muli-server/src/pipeline_trigger.rs` is 969 lines and `trigger_pipeline`
alone is 526 (lines 175–701): one async fn with 11 inline numbered phases. It is
the wrong place to add a 12th. Split it into `pipeline_trigger/` — each numbered
block becomes a named, testable function; `trigger_pipeline` becomes ~15 lines of
orchestration.

| New file | Absorbs (current lines) |
|---|---|
| `mod.rs` | `PipelineTriggerImpl` struct + `new`, the `PipelineTriggerHook` impl (`on_push`/`on_pr_event`), and the thin `trigger_pipeline` orchestrator |
| `admission.rs` | rate-limit + tenant daily/concurrent enforcement (phases 0–0b, 184–235) |
| `discovery.rs` | repo lookup, bare-path resolve, read pipeline configs from commit, commit metadata (phases 1–3, 236–292) |
| `git_meta.rs` | the already-pure free fns at the tail: `collect_tree_paths`, `diff_paths_between`, `resolve_push_changed_paths`, `resolve_pr_changed_paths`, `resolve_commit_info`, `resolve_branch_head`, `is_zero_sha`, `ci_clone_url_target` (837–969) |
| `plan.rs` | per-config: size limit, `parse_pipeline`, `matches_trigger`, upsert Pipeline, build `PipelineRun` (phases 4–8, 293–407) |
| `expand.rs` | jobs/steps + matrix expansion into StepRuns (phase 9, 472–547) |
| `run.rs` | mint CI token → `DagExecutor::execute` → revoke token → query artifacts (phases 10–11, 548–645) |
| `webhook.rs` | `deliver_pipeline_webhook` + the success/failure payload construction (113–134, 648–690) |
| `release.rs` | **new** — the declarative `release:` executor (Part D) |

Guardrail: this PR changes no behaviour. The existing suites must stay green
unchanged — `crates/muli-server/tests/{e2e_tests,pipeline_realistic_test}.rs` and
the trigger/matcher unit tests. Land it on its own before any feature work.

## Part B — Tag trigger (`on.push.tags`)

- `schema.rs`: add `tags: Vec<String>` to `PushTrigger` (sits beside `branches`,
  `paths`; both may coexist as the chosen syntax shows).
- `matcher.rs`: add `PipelineEvent::Tag { tag, changed_paths }` and a match arm —
  fires when `push.tags` is non-empty and a glob matches the tag (reuse
  `matches_glob`). Keep branch-push behaviour intact: a plain branch push must not
  match a tags-only trigger, and vice-versa.
- `pipeline_trigger/mod.rs::on_push`: branch on the raw ref. `refs/tags/*` →
  emit `PipelineEvent::Tag` + a `PipelineTrigger` that carries the full ref;
  `refs/heads/*` → today's `Push`. The raw ref is already in scope (it's the
  `ref_name` param), so this is additive.
- `trigger_event_str` (executor) gains a `Tag` → `"tag"` mapping so `if: event == 'tag'` works.

## Part C — Populate the `tag` expression context

One line, no schema change. `PipelineRun.ref_name` already holds `refs/tags/v1`:

```rust
// crates/muli-pipeline/src/dag/executor.rs — make_expr_ctx
ExpressionContext {
    branch: run.ref_name.strip_prefix("refs/heads/").unwrap_or("").to_string(),
    event: trigger_event_str(&run.trigger),
    tag: run.ref_name.strip_prefix("refs/tags/").map(str::to_string),
}
```

`if: tag == 'v1.0.0'` and `tag != ''` start working. Add a `make_expr_ctx` unit
test; `expression.rs:151` already asserts the evaluator half.

## Part D — Declarative `release:` job keyword

### Schema (`schema.rs`)

```rust
pub struct JobDef { /* … */ pub release: Option<ReleaseDef> }

pub struct ReleaseDef {
    pub tag: Option<String>,          // interpolated ($PIPELINE_TAG default when a tag trigger)
    pub name: Option<String>,
    pub notes: Option<NotesDef>,
    pub draft: Option<bool>,
    pub prerelease: Option<bool>,
    pub create_tag: Option<bool>,     // create the git tag if the run wasn't a tag push
    #[serde(default)] pub assets: Vec<String>,  // globs, matched inside the job's artifact archive
}

#[serde(tag = "from", rename_all = "snake_case")]
pub enum NotesDef {
    Changelog { file: String },   // read a path the job produced/committed
    GitLog,                       // server computes commits since the previous tag
    Inline { text: String },      // literal, with $PIPELINE_* interpolation
}
```

### Execution (`pipeline_trigger/release.rs`), called from `run.rs` on `Ok(Succeeded)`

Runs in `muli-server`, which can hold the stores. Steps:

1. Resolve `tag` (explicit or the run's `refs/tags/*`). If absent and
   `create_tag` is false → skip with a logged warning.
2. If `create_tag` and the tag doesn't exist → create it via `muli-git` at
   `run.commit_sha` (this is the real implementation of the currently-ignored
   flag). Idempotent: a pre-existing tag is fine.
3. Build notes:
   - `Changelog{file}` → read from the run's artifact archive (or the checked-out
     tree) at `file`.
   - `Inline{text}` → interpolate `$PIPELINE_*`.
   - `GitLog` → server-side `git log <prev_tag>..<sha>`; the job's checkout is
     `--depth 1` with no tags, so this must be computed server-side, not in the job.
4. `store.create_release(&Release::new(NewRelease{ tag, name, body: notes, draft,
   prerelease, … }))` (`crates/muli-core/src/release/mod.rs:57`). Reuse
   `get_release_by_tag` first for idempotency (re-runs must not duplicate).
5. Assets: download the job's artifact archive
   (`ArtifactStorage::download(tenant, run_id, job_name)`), and attach it as a
   **single** release asset via `asset_storage.upload(...)` + `store.add_asset(...)`
   (`release_service.rs:224` is the reference sequence). `assets:` globs name which
   job's archive(s) to attach; per-file explosion is intentionally out of scope
   (registry's job).
6. Fold a `"release": { id, tag, asset_ids }` object into the
   `pipeline.completed` webhook payload (`webhook.rs`, currently
   `pipeline_trigger.rs:648`).

### Wiring

`PipelineTriggerImpl` gains `release_store: Arc<dyn ReleaseStore>`,
`release_asset_storage: Arc<ReleaseAssetStorage>`, and `artifact_storage:
Arc<ArtifactStorage>` (it currently holds only the artifact *metadata* store).
Construct them at `crates/muli-server/src/startup.rs:175` (storage created near
`:49`), mirroring how `start_grpc.rs:150` already builds the release service.

### `create_tag` in muli-git

Replace the ignored flag with a real tag write at the target commit, exposed as a
`muli-git` helper the release step calls. Covered by a store/e2e test.

## File-by-file summary

- `crates/muli-pipeline/src/yaml/schema.rs` — `PushTrigger.tags`, `JobDef.release`, `ReleaseDef`, `NotesDef`.
- `crates/muli-pipeline/src/trigger/matcher.rs` — `PipelineEvent::Tag` + match arm.
- `crates/muli-pipeline/src/dag/executor.rs` — `make_expr_ctx` tag population; `trigger_event_str` Tag arm.
- `crates/muli-server/src/pipeline_trigger.rs` → `pipeline_trigger/` module (Part A), with new `release.rs`.
- `crates/muli-server/src/startup.rs` — inject release + artifact byte stores.
- `crates/muli-git` — real `create_tag` helper.
- Docs: update `docs/releases.md` (remove the "planned / `if: tag ==` interim" note — both now real) and `docs/pipelines.md` (document `on.push.tags` + `release:`).

## Verification

1. **Unit:** `matcher.rs` tag-trigger cases (tag matches glob; branch push doesn't
   fire a tags trigger; tags + branches coexist). `make_expr_ctx` populates `tag`.
   `NotesDef` deserialization for all three `from:` values.
2. **Store:** extend `crates/muli-store/.../release_store.rs:341`
   (`test_release_crud_and_assets`) with a create-from-run path; assert re-run
   idempotency via `get_release_by_tag`.
3. **e2e (the real proof):** in `crates/muli-server/tests/`, push a `v*` tag to a
   test repo with a `release:` job → assert (a) the run fired on the tag, (b) a
   release exists at that tag with the expected notes, (c) one archive asset is
   attached, (d) a second identical push does not duplicate the release.
4. **Behaviour-preservation for Part A:** the full existing server + pipeline
   suites pass unchanged after the module split, before any feature code.
5. **Manual:** `git push origin v0.1.0` on a repo whose pipeline has a tag trigger
   + `release:`; confirm the release is queryable via `GetReleaseByTag` (and shows
   in whatever UI fronts the release store) — a pipeline-created release is
   indistinguishable from a manually-created one, so it appears with no extra
   wiring.

## Part E — Ambient registry credentials for publish jobs (follow-up feature)

**Goal.** Let a pipeline job publish to the tenant/handle's own embedded registry
with no manual token setup — the GitHub-`GITHUB_TOKEN` / GitLab-`CI_JOB_TOKEN`
model. A job just runs `npm publish` / `cargo publish` / `docker push` against a
pre-known URL with a pre-injected credential.

This is a **separate feature from releases** (Parts A–D create no in-container
credential; this one deliberately does). It is scoped out of the core run so the
credential-in-container posture gets an explicit decision. Ships after A–D.

**Why one token is enough.** The registry is host-based —
`https://{handle}.{base_domain}/...` — and a single global auth middleware backs
**every** format (npm `/-/npm`, Cargo `/api/v1/crates` + sparse index, Maven
`/-/maven`, OCI `/v2`). Tenant is taken from the `Host` subdomain; writes require
the `Push` permission. So one `Push`-scoped `RegistryToken` for the run's handle
authenticates publishes to all four formats. (`crates/muli-registry/src/tenant.rs`,
`.../auth.rs:56-63`, `crates/muli-core/src/registry/model.rs:12-18`.)

**What already exists (reuse, don't rebuild).** `RegistryToken::new(permissions,
expires_at)`, the `create_registry_token` gRPC that mints one, rotation/revocation,
and expiry GC (`crates/muli-server/src/grpc/registry_service.rs:58`,
`crates/muli-server/src/cleanup.rs:19`). And the CI **git** token already models the
whole per-run lifecycle to copy: mint → carry → inject → revoke
(`pipeline_trigger.rs:546-618`, `dag/executor.rs:698-760`).

**The gap (medium).** `PipelineTriggerImpl` holds only a `GitTokenStore`, not a
`RegistryTokenStore`; nothing connects a run to the registry today.

**Increment E1 — ambient token (the 80%).**
1. Thread `RegistryTokenStore` into `PipelineTriggerImpl` (struct `pipeline_trigger.rs:42`,
   ctor wiring `startup.rs:178`); it already exists in `stores.rs:37`.
2. Mint a `Push`-only, short-TTL `RegistryToken` for the run's tenant beside the git
   token; revoke it on completion (mirror `pipeline_trigger.rs:546-618`).
3. Carry it into `PipelineRun`; inject `MULI_REGISTRY_TOKEN` +
   `MULI_REGISTRY_URL` (=`https://{handle}.{base_domain}`) in `build_env_vars`
   (`executor.rs:698`), exactly like `PIPELINE_CLONE_URL`.

With E1, a job publishes with a two-line config it writes itself, e.g.
`npm config set //${…}/:_authToken $MULI_REGISTRY_TOKEN`.

**Increment E2 — per-format convenience (the bespoke 20%, incremental).** A `publish:`
job keyword (no such keyword exists today) or a setup step that auto-writes the
client config per format: `.npmrc` (scope→registry + authToken), Cargo
`CARGO_REGISTRIES_*` + sparse index, `docker login`, Maven `settings.xml`. Same
token, different files; ship npm first.

**Decision needed before E1.** (a) Inject the token into **every** job (GitHub
default) or only opt-in jobs? (b) The `Push` scope is per-handle, not per-package —
a leaked token can publish any package in the handle. Acceptable (GitHub-token
equivalent), mitigated by short TTL + post-run revoke + single-tenant server-side
enforcement, but should be acknowledged.

## Sequencing

1. **Refactor** (`pipeline_trigger/` split), suites green, zero behaviour change. — *core run*
2. **Tag trigger + expression fix** (Parts B, C): pipelines fire on `v*`; `if: tag` works. — *core run*
3. **`release:` keyword** (Part D): in-process release creation, `create_tag`, configurable notes, assets, webhook field, docs. — *core run*
4. **Ambient registry credentials** (Part E1, then E2): the publish-to-your-handle helper. — *next run, after the E1 decision above*

This run executes 1–3 (credential-free, fully mapped) as sequential commits on
`main`. Item 4 follows once the E1 injection policy is chosen.
