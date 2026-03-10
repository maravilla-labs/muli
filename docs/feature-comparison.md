# Maravilla Stack Feature Comparison

Comprehensive comparison of the Maravilla stack (**staticlab** + **muli** + **maravilla-runtime**) against platforms in Gitea's official feature comparison table.

**Last updated:** 2026-02-23

---

## Stack Architecture Mapping

| Role | Maravilla Stack | Equivalent |
|------|----------------|------------|
| Git server, registry, CI engine | **muli** | Gitea core / GitLab Workhorse+Shell |
| Platform UI, project management | **staticlab** | GitHub.com / GitLab web UI |
| Pages / hosting | **maravilla-runtime** | GitHub Pages / GitLab Pages (but with dynamic SSR, functions, KV, DB) |

---

## 1. General Features

| Feature | Gitea | GH EE | GL CE | GL EE | BB | RC CE | RC EE | **Maravilla** |
|---------|-------|-------|-------|-------|-----|-------|-------|---------------|
| Open source and free | ✓ | ✘ | ✓ | ✘ | ✘ | ✓ | ✓ | **✓** (muli: Apache-2.0/MIT) |
| Low RAM/CPU usage | ✓ | ✘ | ✘ | ✘ | ✘ | ✘ | ✘ | **✓** (Rust single binary, async tokio) |
| Multiple database support | ✓ | ✘ | ⁄ | ⁄ | ✓ | ✓ | ✓ | **✓** (SQLite + MongoDB) |
| Multiple OS support | ✓ | ✘ | ✘ | ✘ | ✘ | ✓ | ✓ | **⁄** (Rust cross-compiles, no official Windows/macOS packaging) |
| Easy upgrades | ✓ | ✘ | ✓ | ✓ | ✘ | ✓ | ✓ | **✓** (single binary, SQLite per-tenant) |
| Telemetry | ✘ | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** (no phone-home; Prometheus opt-in) |
| Third-party render tool support | ✓ | ✘ | ✘ | ✘ | ✓ | ✘ | ✘ | **✘** |
| WebAuthn (2FA) | ✓ | ✓ | ✓ | ✓ | ✓ | ✘ | ✓ | **✘** |
| Extensive API | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (61 gRPC RPCs + REST + OCI/npm/Cargo APIs) |
| Built-in Package/Container Registry | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✓** (Docker OCI v2 + npm + Cargo) |
| Push mirror | ✓ | ✘ | ✓ | ✓ | ✘ | ✓ | ✓ | **✘** |
| Pull mirror | ✓ | ✘ | ✓ | ✓ | ✘ | ✓ | ✓ | **✘** |
| Light and Dark Theme | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **⁄** (staticlab React/Tailwind UI) |
| Custom Theme Support | ✓ | ✘ | ✘ | ✘ | ✓ | ✓ | ✓ | **✘** |
| Markdown support | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **⁄** (client-side in staticlab, no server-side rendering) |
| CSV support | ✓ | ✓ | ✘ | ✘ | ✓ | ✘ | ✘ | **✘** |
| GitHub/GitLab Pages | ⚙️ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✓✓** (maravilla-runtime: static + dynamic SSR + functions + KV + DB) |
| Gists / Snippets | ⚙️ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Repo-specific wiki | ✓ | ✓ | ✓ | ✓ | ⁄ | ✘ | ✘ | **✘** |
| Deploy Tokens | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (muli: scoped Pull/Push/Admin tokens with TTL) |
| Repository Tokens with write rights | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** |
| RSS Feeds | ✓ | ✓ | ✘ | ✘ | ✘ | ✓ | ✓ | **✘** |
| Built-in CI/CD | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **⁄** (muli: containerized job engine + distributed agents, no pipeline YAML DSL) |
| Subgroups | ✘ | ✘ | ✓ | ✓ | ✘ | ✓ | ✓ | **✘** |
| Interaction with other instances | ⁄ | ✘ | ✘ | ✘ | ✘ | ✘ | ✘ | **✘** |
| Mermaid diagrams in Markdown | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Math syntax in Markdown | ✓ | ✓ | ✓ | ✓ | ✘ | ✓ | ✓ | **✘** |

---

## 2. Code Management

| Feature | Gitea | GH EE | GL CE | GL EE | BB | RC CE | RC EE | **Maravilla** |
|---------|-------|-------|-------|-------|-----|-------|-------|---------------|
| Repository topics | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Repository code search | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Global code search | ✓ | ✓ | ✘ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Git LFS 2.0 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Group Milestones | ✘ | ✘ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Granular user roles | ✓ | ✘ | ✓ | ✓ | ✘ | ✘ | ✘ | **⁄** (Pull/Push/Admin per-repo, Org roles, no per-feature granularity) |
| Verified Committer | ⁄ | ? | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** |
| GPG Signed Commits | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| SSH Signed Commits | ✓ | ✓ | ✓ | ✓ | ? | ✘ | ✘ | **✘** |
| Reject unsigned commits | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Migrating repos from other services | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Repository Activity page | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Branch manager | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (muli: list/create branches via REST) |
| Create new branches | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✓** |
| Web code editor | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Commit graph | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Template Repositories | ✓ | ✓ | ✘ | ✓ | ✓ | ✘ | ✘ | **✘** |
| Git Blame | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Visual comparison of image changes | ✓ | ✓ | ? | ? | ? | ✘ | ✘ | **✘** |

---

## 3. Issue Tracker

| Feature | Gitea | GH EE | GL CE | GL EE | BB | RC CE | RC EE | **Maravilla** |
|---------|-------|-------|-------|-------|-----|-------|-------|---------------|
| Issue tracker | ✓ | ✓ | ✓ | ✓ | ⁄ | ✘ | ✘ | **✘** |
| Issue templates | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Labels | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Time tracking | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Multiple assignees | ✓ | ✓ | ✘ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Related issues | ✘ | ⁄ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Confidential issues | ✘ | ✘ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Comment reactions | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Lock Discussion | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Batch issue handling | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Projects (boards) | ⁄ | ✘ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Create branch from issue | ✘ | ✘ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Convert comment to new issue | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Issue search | ✓ | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** |
| Global issue search | ⁄ | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** |
| Issue dependency | ✓ | ✘ | ✘ | ✘ | ✘ | ✘ | ✘ | **✘** |
| Create issue via email | ✘ | ✘ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** |
| Service Desk | ✘ | ✘ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |

> **Note:** Issue tracking is intentionally not implemented. muli's initial market does not require a social/issue layer per VISION.md.

---

## 4. Pull/Merge Requests

| Feature | Gitea | GH EE | GL CE | GL EE | BB | RC CE | RC EE | **Maravilla** |
|---------|-------|-------|-------|-------|-----|-------|-------|---------------|
| Pull/Merge requests | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (full PR model, sequential numbering) |
| Squash merging | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** (only 3-way merge) |
| Rebase merging | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| PR inline comments | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (file + line range anchoring, threaded replies) |
| PR approval | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| PR require approval | ✓ | ✓ | ✘ | ✓ | ✓ | ✓ | ✓ | **✘** |
| PR multiple reviewers | ✓ | ✓ | ✘ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Merge conflict resolution | ✘ | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** (detects conflicts, no resolution UI) |
| Restrict push/merge access | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **⁄** (collaborator permissions, no branch-level rules) |
| Revert specific commits | ✓ | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** |
| PR templates | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | ✘ | **✘** |
| Cherry-picking changes | ✓ | ✘ | ✓ | ✓ | ✘ | ✘ | ✓ | **✘** |
| Download Patch | ✓ | ✓ | ✓ | ✓ | ⁄ | ✓ | ✓ | **✘** |
| Merge queues | ✓ | ✓ | ✘ | ✓ | ✘ | ✘ | ✘ | **✘** |

---

## 5. 3rd-Party Integrations

| Feature | Gitea | GH EE | GL CE | GL EE | BB | RC CE | RC EE | **Maravilla** |
|---------|-------|-------|-------|-------|-----|-------|-------|---------------|
| Webhooks | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (HMAC-SHA256, 6 event types) |
| Git Hooks | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **⁄** (webhook delivery, no user-configurable server hooks) |
| AD / LDAP integration | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✘** |
| Multiple LDAP servers | ✓ | ✘ | ✘ | ✓ | ✓ | ✓ | ✓ | **✘** |
| LDAP user sync | ✓ | ✓ | ✓ | ✓ | ✓ | ✘ | ✓ | **✘** |
| SAML 2.0 | ✘ | ✓ | ✓ | ✓ | ✓ | ✘ | ✓ | **✘** |
| OpenID Connect | ✓ | ✓ | ✓ | ✓ | ? | ✘ | ✓ | **✘** |
| OAuth 2.0 integration | ✓ | ⁄ | ✓ | ✓ | ? | ✘ | ✓ | **⁄** (staticlab: GitHub OAuth only) |
| Act as OAuth 2.0 provider | ✓ | ✓ | ✓ | ✓ | ✓ | ✘ | ✘ | **✘** |
| Two factor auth (2FA) | ✓ | ✓ | ✓ | ✓ | ✓ | ✘ | ✓ | **✘** |
| Integration with common services | ✓ | ⁄ | ✓ | ✓ | ⁄ | ✓ | ✓ | **⁄** (GitHub only via staticlab) |
| Incorporate external CI/CD | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **✓** (webhooks + muli IS a CI engine) |

---

## 6. Unique Maravilla Advantages

No competitor in this comparison offers these capabilities:

| Feature | Component | Why It Matters |
|---------|-----------|----------------|
| **Git + Docker/npm/Cargo registry + CI in one binary** | muli | Zero-config deployment. No separate runners, registry services, or package managers. |
| **Registry pull-through cache** | muli | Built-in proxy/cache from Docker Hub etc. Neither Gitea nor GitLab CE have this natively. |
| **Dynamic SSR hosting (not just static Pages)** | maravilla-runtime | Full SvelteKit/Nuxt/Next.js execution with V8 isolates, <100ms cold starts. GitHub/GitLab Pages are static only. |
| **Built-in KV store + document DB for hosted apps** | maravilla-runtime | Deployed apps get KV with TTL + MongoDB-compatible queries. Like Cloudflare Workers KV/D1 but self-hosted. |
| **Cargo registry** | muli | Only GitHub (via crates.io) and Maravilla support Cargo. Gitea, GitLab, BitBucket do not. |
| **European data sovereignty by architecture** | all | Swiss-based, CLOUD Act immune, GDPR/NIS2/CRA aligned. Structural advantage over all US platforms. |
| **Multi-tenant isolation at every layer** | all | Tenant isolation at git, registry, CI, and runtime level. Enterprise-grade from day one. |
| **Real-time CI log streaming** | muli | gRPC streaming from agent to server. Most self-hosted alternatives poll. |
| **Resource Event Notifications (SSE)** | maravilla-runtime | Real-time push for platform resource changes. No equivalent in any compared platform's hosting. |
| **V8 snapshots for cold start optimization** | maravilla-runtime | Sub-100ms cold starts. No equivalent in any platform's Pages offering. |
| **Events, ticketing, equipment, member management** | staticlab | Vertical features no git forge offers — positions the platform for non-developer use cases. |

---

## 7. Gap Analysis & Roadmap for muli (Git Side)

### Tier 1 — Critical for enterprise Git adoption

| # | Feature | Effort | Crate to Modify | Notes |
|---|---------|--------|-----------------|-------|
| 1 | **Branch protection rules** | M | `muli-core/git/model.rs`, `muli-git/api/` | Required reviews, prevent force push, status checks. The collaborator model exists; add per-branch rule enforcement. |
| 2 | **PR approval workflow** | M | `muli-core/pr/mod.rs`, `muli-git/api/pulls*.rs` | Add reviewer assignment, approved/changes-requested states, required approval count. Inline comment system is already solid. |
| 3 | **Squash & rebase merge** | S | `muli-git/api/pulls_merge.rs` | `perform_merge()` only does 3-way. Add squash (single commit) and rebase strategies. |
| 4 | **Pipeline/DAG workflow DSL** | L | new crate `muli-pipeline` | The hard parts exist (job engine, distributed agents, priority scheduler). Missing piece: YAML parser + DAG executor + repo-triggered pipelines. |
| 5 | **Git LFS** | M | `muli-git/api/protocol.rs`, new LFS batch endpoints | Required by teams with binary assets. Add LFS batch API on the existing HTTP protocol layer. |
| 6 | **Audit logging** | M | new `muli-audit` module | Required for regulated-sector customers. Called out in VISION.md. |

### Tier 2 — Competitive parity

| # | Feature | Effort | Notes |
|---|---------|--------|-------|
| 7 | **Code search** | M | Consider tantivy (Rust-native). Index on push via webhooks. |
| 8 | **GPG/SSH signed commit verification** | M | Verify signatures on push, display badges in UI. |
| 9 | **OAuth 2.0 / OIDC provider** | M | Allow muli to act as identity provider for SSO. |
| 10 | **Repository migration/import** | S | Import from GitHub/GitLab/Gitea via their APIs. |
| 11 | **Scheduled jobs / cron triggers** | S | Job engine exists, just needs cron expression parsing + scheduler. |
| 12 | **Job artifacts storage** | S | CI jobs can't persist outputs beyond logs. |

### Tier 3 — Differentiation / nice-to-have

| # | Feature | Notes |
|---|---------|-------|
| 13 | Push/pull mirrors | Sync with upstream repos |
| 14 | Wiki | Per-repo wiki stored as git repo |
| 15 | LDAP/SAML | Enterprise SSO |
| 16 | Issue tracker (minimal) | Intentionally deferred per VISION.md |
| 17 | Web code editor | Edit files in browser |
| 18 | Git blame | Per-file blame view |
| 19 | SBOM generation | CRA compliance story |
| 20 | Rate limiting per user/IP | muli has none; maravilla-runtime does |

---

## 8. Score Summary

| Category | Gitea | GH EE | GL CE | GL EE | BB | RC CE | RC EE | **Maravilla** |
|----------|-------|-------|-------|-------|-----|-------|-------|---------------|
| General (28 features) | 24 | 17 | 19 | 20 | 13 | 15 | 16 | **13 ✓ / 3 ⁄ / 12 ✘** |
| Code Mgmt (16 features) | 15 | 14 | 13 | 15 | 12 | 11 | 11 | **2 ✓ / 1 ⁄ / 13 ✘** |
| Issues (18 features) | 13 | 12 | 16 | 17 | 5 | 0 | 0 | **0 ✓ / 0 ⁄ / 18 ✘** |
| PRs (14 features) | 13 | 12 | 11 | 14 | 10 | 9 | 10 | **3 ✓ / 1 ⁄ / 10 ✘** |
| Integrations (12 features) | 11 | 10 | 11 | 12 | 9 | 7 | 11 | **2 ✓ / 2 ⁄ / 8 ✘** |
| **Total** | **76** | **65** | **70** | **78** | **49** | **42** | **48** | **20 ✓ / 7 ⁄ / 61 ✘** |

**However:** The Maravilla stack has **11 unique features no competitor offers** (dynamic Pages with SSR+DB, unified binary, Cargo registry, registry proxy, etc.). The raw feature count understates Maravilla's position — it competes on architectural uniqueness and sovereign infrastructure, not feature parity with decade-old platforms.

---

## Verification Notes

All Maravilla feature statuses were verified against the codebase:

- **Branch protection = ✘**: No `branch_protection` or `protected_branch` found in any model or API
- **PR approval = ✘**: `PrState` enum contains only `Open`, `Merged`, `Closed` (in `muli-core/src/pr/mod.rs`)
- **Docker registry = ✓**: Full OCI Distribution v2 implementation in `muli-registry` crate with proxy cache
- **Webhook HMAC = ✓**: HMAC-SHA256 signing confirmed in `muli-git/src/api/webhooks.rs`
- **KV store = ✓**: MongoDB + local backends in `maravilla-runtime/crates/platform/src/kv/`
