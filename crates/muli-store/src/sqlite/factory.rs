// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite connection pool factory with per-tenant databases.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio_rusqlite::Connection;

use muli_core::error::{MuliError, Result};

use super::tenant_store::SqliteTenantStore;

/// Raw row from the `git_tokens` table: (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json).
type GitTokenRow = (
    String,
    String,
    Option<String>,
    String,
    String,
    Option<i64>,
    i64,
    String,
);

/// Number of connections in the global DB pool.
const GLOBAL_POOL_SIZE: usize = 4;

/// Maximum number of tenant connections to cache (LRU eviction beyond this).
const MAX_TENANT_CONNS: usize = 256;

/// How long the cached tenant ID list is considered fresh.
const TENANT_ID_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// Performance PRAGMAs applied to every connection (global and tenant).
/// Safe with WAL mode enabled.
const PERF_PRAGMAS: &str = "
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -8000;
PRAGMA mmap_size = 268435456;
PRAGMA temp_store = MEMORY;
";

// --- Global DB DDL ---

const GLOBAL_DDL: &str = "
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT,
  spec TEXT NOT NULL,
  state TEXT NOT NULL,
  priority_score REAL NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER,
  error TEXT,
  full_json TEXT NOT NULL,
  idempotency_key TEXT
);
CREATE INDEX IF NOT EXISTS jobs_state_priority ON jobs(state, priority_score DESC);
CREATE INDEX IF NOT EXISTS jobs_tenant ON jobs(tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_name ON jobs(name) WHERE name IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency ON jobs(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_logs (
  job_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  stream TEXT NOT NULL,
  line TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  substep_name TEXT,
  event_type TEXT,
  exit_code INTEGER,
  PRIMARY KEY (job_id, seq)
);
CREATE INDEX IF NOT EXISTS job_logs_job ON job_logs(job_id);

CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY,
  full_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registry_tokens (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  token_hash TEXT NOT NULL,
  token_prefix TEXT NOT NULL,
  expires_at INTEGER,
  revoked INTEGER NOT NULL DEFAULT 0,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS registry_tokens_tenant ON registry_tokens(tenant_id);
CREATE INDEX IF NOT EXISTS registry_tokens_expires ON registry_tokens(expires_at);
CREATE INDEX IF NOT EXISTS registry_tokens_prefix ON registry_tokens(token_prefix);

CREATE TABLE IF NOT EXISTS tenant_quotas (
  tenant_id TEXT PRIMARY KEY,
  max_storage_bytes INTEGER NOT NULL,
  current_usage_bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tenants (
  id        TEXT PRIMARY KEY,
  name      TEXT NOT NULL,
  full_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ssh_key_fingerprints (
  ssh_key_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  PRIMARY KEY (ssh_key_id)
);
CREATE INDEX IF NOT EXISTS ssh_key_fp_by_fingerprint ON ssh_key_fingerprints(fingerprint);
CREATE INDEX IF NOT EXISTS ssh_key_fp_by_tenant ON ssh_key_fingerprints(tenant_id);

CREATE TABLE IF NOT EXISTS global_git_tokens (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT,
  token_hash TEXT NOT NULL,
  token_prefix TEXT NOT NULL,
  expires_at INTEGER,
  revoked INTEGER NOT NULL DEFAULT 0,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS global_git_tokens_tenant ON global_git_tokens(tenant_id);
CREATE INDEX IF NOT EXISTS global_git_tokens_prefix ON global_git_tokens(token_prefix);
CREATE INDEX IF NOT EXISTS global_git_tokens_expires ON global_git_tokens(expires_at);

CREATE TABLE IF NOT EXISTS tenant_limits (
  tenant_id TEXT PRIMARY KEY,
  full_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tenant_daily_run_counts (
  tenant_id TEXT NOT NULL,
  run_date TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, run_date)
);
";

// --- Per-tenant DB DDL ---

const TENANT_DDL: &str = "
CREATE TABLE IF NOT EXISTS repositories (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  fork_of TEXT,
  full_json TEXT NOT NULL,
  UNIQUE(namespace, name)
);
CREATE INDEX IF NOT EXISTS repos_fork ON repositories(fork_of);

CREATE TABLE IF NOT EXISTS git_tokens (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT,
  token_hash TEXT NOT NULL,
  token_prefix TEXT NOT NULL,
  expires_at INTEGER,
  revoked INTEGER NOT NULL DEFAULT 0,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS git_tokens_user ON git_tokens(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS git_tokens_expires ON git_tokens(expires_at);
CREATE INDEX IF NOT EXISTS git_tokens_prefix ON git_tokens(token_prefix);

CREATE TABLE IF NOT EXISTS ssh_keys (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT,
  fingerprint TEXT UNIQUE NOT NULL,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ssh_keys_user ON ssh_keys(tenant_id, user_id);

CREATE TABLE IF NOT EXISTS webhooks (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  repo_id TEXT NOT NULL,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS webhooks_repo ON webhooks(tenant_id, repo_id);

CREATE TABLE IF NOT EXISTS repo_collaborators (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  repo_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  full_json TEXT NOT NULL,
  UNIQUE(repo_id, user_id)
);
CREATE INDEX IF NOT EXISTS collabs_repo ON repo_collaborators(repo_id);
CREATE INDEX IF NOT EXISTS collabs_user ON repo_collaborators(tenant_id, user_id);

CREATE TABLE IF NOT EXISTS tenant_users (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  external_id TEXT,
  full_json TEXT NOT NULL,
  UNIQUE(handle),
  UNIQUE(external_id)
);

CREATE TABLE IF NOT EXISTS orgs (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  handle TEXT UNIQUE NOT NULL,
  full_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS org_members (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  full_json TEXT NOT NULL,
  UNIQUE(org_id, user_id)
);
CREATE INDEX IF NOT EXISTS org_members_org ON org_members(org_id);

CREATE TABLE IF NOT EXISTS pull_requests (
  id TEXT PRIMARY KEY,
  number INTEGER NOT NULL,
  repo_id TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'Open',
  full_json TEXT NOT NULL,
  UNIQUE(repo_id, number)
);
CREATE INDEX IF NOT EXISTS prs_repo_state ON pull_requests(repo_id, state);

CREATE TABLE IF NOT EXISTS pr_comments (
  id TEXT PRIMARY KEY,
  pr_id TEXT NOT NULL,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS pr_comments_pr ON pr_comments(pr_id);

CREATE TABLE IF NOT EXISTS tree_commit_cache (
  repo_id      TEXT NOT NULL,
  commit_sha   TEXT NOT NULL,
  dir_path     TEXT NOT NULL DEFAULT '',
  entries_json TEXT NOT NULL,
  created_at   INTEGER NOT NULL DEFAULT (unixepoch()),
  PRIMARY KEY (repo_id, commit_sha, dir_path)
);
CREATE INDEX IF NOT EXISTS idx_tcc_repo ON tree_commit_cache(repo_id);

CREATE TABLE IF NOT EXISTS pipelines (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  repo_id TEXT NOT NULL,
  name TEXT NOT NULL,
  full_json TEXT NOT NULL,
  UNIQUE(repo_id, name)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id TEXT PRIMARY KEY,
  pipeline_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  repo_id TEXT NOT NULL,
  run_number INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'Pending',
  full_json TEXT NOT NULL,
  UNIQUE(pipeline_id, run_number)
);
CREATE INDEX IF NOT EXISTS pipeline_runs_repo ON pipeline_runs(repo_id, state);
CREATE INDEX IF NOT EXISTS pipeline_runs_pipeline ON pipeline_runs(pipeline_id);

CREATE TABLE IF NOT EXISTS step_runs (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  job_id TEXT,
  state TEXT NOT NULL DEFAULT 'Pending',
  full_json TEXT NOT NULL,
  UNIQUE(run_id, step_name)
);
CREATE INDEX IF NOT EXISTS step_runs_run ON step_runs(run_id);

CREATE TABLE IF NOT EXISTS pipeline_artifacts (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  name TEXT NOT NULL,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  expires_at INTEGER,
  full_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS artifacts_run ON pipeline_artifacts(run_id);

CREATE TABLE IF NOT EXISTS pipeline_cache (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  repo_id TEXT NOT NULL,
  cache_key TEXT NOT NULL,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  last_used_at INTEGER NOT NULL,
  full_json TEXT NOT NULL,
  UNIQUE(repo_id, cache_key)
);
CREATE INDEX IF NOT EXISTS cache_repo ON pipeline_cache(repo_id);

CREATE TABLE IF NOT EXISTS pipeline_secrets (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  repo_id TEXT NOT NULL,
  name TEXT NOT NULL,
  full_json TEXT NOT NULL,
  UNIQUE(repo_id, name)
);
CREATE INDEX IF NOT EXISTS secrets_repo ON pipeline_secrets(repo_id);
";

/// Central SQLite connection factory. Holds one connection per tenant DB
/// and a pool of connections for the shared `_global.db`.
pub struct SqliteStoreFactory {
    data_dir: PathBuf,
    tenant_conns: DashMap<String, Arc<Connection>>,
    /// Tracks last-access time for LRU eviction of tenant connections.
    tenant_access: DashMap<String, std::time::Instant>,
    /// Round-robin pool of global DB connections.
    global_conns: Vec<Arc<Connection>>,
    global_conn_idx: AtomicUsize,
    /// Cached list of tenant IDs with TTL to avoid repeated disk scans.
    cached_tenant_ids: RwLock<(Vec<String>, std::time::Instant)>,
}

impl SqliteStoreFactory {
    /// Create a new factory, initialising `_global.db` immediately.
    pub async fn new(data_dir: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir)
            .await
            .map_err(|e| MuliError::Storage(format!("create data dir: {e}")))?;

        let global_path = data_dir.join("_global.db");

        // Open a pool of connections to the global DB.
        let mut global_conns = Vec::with_capacity(GLOBAL_POOL_SIZE);
        for i in 0..GLOBAL_POOL_SIZE {
            let conn = Connection::open(&global_path)
                .await
                .map_err(|e| MuliError::Storage(format!("open global db (conn {i}): {e}")))?;

            let is_first = i == 0;
            conn.call(move |c| {
                c.execute_batch("PRAGMA journal_mode=WAL;")?;
                c.execute_batch("PRAGMA busy_timeout = 5000;")?;
                c.execute_batch("PRAGMA foreign_keys = ON;")?;
                c.execute_batch(PERF_PRAGMAS)?;
                if is_first {
                    // Safe migrations: add columns to existing DBs (ignored if already present).
                    // Must run BEFORE DDL so that CREATE INDEX on new columns doesn't fail.
                    let _ = c.execute("ALTER TABLE jobs ADD COLUMN idempotency_key TEXT", []);
                    let _ = c.execute(
                        "ALTER TABLE registry_tokens ADD COLUMN token_prefix TEXT NOT NULL DEFAULT ''",
                        [],
                    );
                    let _ = c.execute("ALTER TABLE job_logs ADD COLUMN substep_name TEXT", []);
                    let _ = c.execute("ALTER TABLE job_logs ADD COLUMN event_type TEXT", []);
                    let _ = c.execute("ALTER TABLE job_logs ADD COLUMN exit_code INTEGER", []);
                    c.execute_batch(GLOBAL_DDL)?;
                }
                Ok(())
            })
            .await
            .map_err(|e| MuliError::Storage(format!("init global db (conn {i}): {e}")))?;

            global_conns.push(Arc::new(conn));
        }

        let factory = Arc::new(Self {
            data_dir,
            tenant_conns: DashMap::new(),
            tenant_access: DashMap::new(),
            global_conns,
            global_conn_idx: AtomicUsize::new(0),
            cached_tenant_ids: RwLock::new((Vec::new(), std::time::Instant::now())),
        });

        // Backfill git tokens from per-tenant DBs into global_git_tokens.
        factory.backfill_global_git_tokens().await?;

        Ok(factory)
    }

    /// Validate that a tenant_id is safe for use as a filename component.
    fn validate_tenant_id(tenant_id: &str) -> Result<()> {
        if tenant_id.is_empty()
            || tenant_id == "."
            || tenant_id == ".."
            || tenant_id.contains('/')
            || tenant_id.contains('\\')
            || tenant_id.contains('\0')
            || tenant_id.contains("..")
        {
            return Err(MuliError::Storage(format!(
                "invalid tenant_id: {tenant_id}"
            )));
        }
        Ok(())
    }

    /// Get (or lazily open) the per-tenant connection.
    pub async fn tenant_conn(&self, tenant_id: &str) -> Result<Arc<Connection>> {
        Self::validate_tenant_id(tenant_id)?;

        if let Some(c) = self.tenant_conns.get(tenant_id) {
            self.tenant_access
                .insert(tenant_id.to_string(), std::time::Instant::now());
            return Ok(c.value().clone());
        }

        // Evict LRU entries if at capacity.
        self.maybe_evict_tenant_conns();

        let path = self.data_dir.join(format!("{tenant_id}.db"));
        let conn = Connection::open(path)
            .await
            .map_err(|e| MuliError::Storage(format!("open tenant db {tenant_id}: {e}")))?;

        conn.call(|c| {
            c.execute_batch("PRAGMA journal_mode=WAL;")?;
            c.execute_batch("PRAGMA busy_timeout = 5000;")?;
            c.execute_batch("PRAGMA foreign_keys = ON;")?;
            c.execute_batch(PERF_PRAGMAS)?;
            // Safe migration for older tenant DBs that predate token_prefix.
            // Must run before TENANT_DDL because that DDL creates an index on token_prefix.
            let _ = c.execute(
                "ALTER TABLE git_tokens ADD COLUMN token_prefix TEXT NOT NULL DEFAULT ''",
                [],
            );
            c.execute_batch(TENANT_DDL)?;
            Ok(())
        })
        .await
        .map_err(|e| MuliError::Storage(format!("init tenant db {tenant_id}: {e}")))?;

        // Backfill SSH key fingerprints into global index (one-time per tenant).
        let global = self.global_conn();
        let tid = tenant_id.to_string();
        let tenant_keys: Vec<(String, String)> = conn
            .call(move |c| {
                let mut stmt = c.prepare("SELECT id, fingerprint FROM ssh_keys")?;
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
            .map_err(|e| MuliError::Storage(format!("read ssh keys for backfill: {e}")))?;

        if !tenant_keys.is_empty() {
            let tid2 = tid.clone();
            global
                .call(move |c| {
                    let count: i64 = c.query_row(
                        "SELECT COUNT(*) FROM ssh_key_fingerprints WHERE tenant_id = ?1",
                        rusqlite::params![tid2],
                        |row| row.get(0),
                    )?;
                    if count == 0 {
                        for (key_id, fingerprint) in &tenant_keys {
                            c.execute(
                                "INSERT OR IGNORE INTO ssh_key_fingerprints (fingerprint, tenant_id, ssh_key_id) VALUES (?1, ?2, ?3)",
                                rusqlite::params![fingerprint, tid2, key_id],
                            )?;
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(|e| MuliError::Storage(format!("backfill ssh fingerprints: {e}")))?;
        }

        // Backfill git tokens from this tenant into global_git_tokens.
        let global2 = self.global_conn();
        let tid3 = tenant_id.to_string();
        let tenant_tokens: Vec<GitTokenRow> = conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json FROM git_tokens"
                )?;
                let mut result = Vec::new();
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    result.push((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ));
                }
                Ok(result)
            })
            .await
            .map_err(|e| MuliError::Storage(format!("read git tokens for backfill: {e}")))?;

        if !tenant_tokens.is_empty() {
            global2
                .call(move |c| {
                    let count: i64 = c.query_row(
                        "SELECT COUNT(*) FROM global_git_tokens WHERE tenant_id = ?1",
                        rusqlite::params![tid3],
                        |row| row.get(0),
                    )?;
                    if count == 0 {
                        for (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json) in &tenant_tokens {
                            c.execute(
                                "INSERT OR IGNORE INTO global_git_tokens (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                                rusqlite::params![id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json],
                            )?;
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(|e| MuliError::Storage(format!("backfill git tokens: {e}")))?;
        }

        let conn = Arc::new(conn);
        let now = std::time::Instant::now();
        // Another racing caller may have inserted first; that's fine.
        self.tenant_conns
            .entry(tenant_id.to_string())
            .or_insert_with(|| conn.clone());
        self.tenant_access.insert(tenant_id.to_string(), now);

        // Eagerly add to the cached tenant ID list.
        {
            let mut cache = self.cached_tenant_ids.write().await;
            if !cache.0.contains(&tenant_id.to_string()) {
                cache.0.push(tenant_id.to_string());
            }
        }

        // Return whatever ended up in the map.
        Ok(self.tenant_conns.get(tenant_id).unwrap().value().clone())
    }

    /// Return a global connection from the round-robin pool.
    pub fn global_conn(&self) -> Arc<Connection> {
        let i = self.global_conn_idx.fetch_add(1, Ordering::Relaxed) % self.global_conns.len();
        self.global_conns[i].clone()
    }

    /// Return the IDs of all currently-open tenant connections.
    ///
    /// Used by store implementations that need to search across all tenants
    /// when the tenant is not known (e.g. `get_token_by_prefix`).
    pub fn open_tenant_ids(&self) -> Vec<String> {
        self.tenant_conns.iter().map(|e| e.key().clone()).collect()
    }

    /// Create a TenantStore backed by the global DB.
    pub fn create_tenant_store(self: &Arc<Self>) -> SqliteTenantStore {
        SqliteTenantStore::new(self.clone())
    }

    /// Return the IDs of all tenant DBs on disk (opened or not).
    ///
    /// Uses a 30-second TTL cache to avoid repeated filesystem scans.
    pub async fn all_tenant_ids(&self) -> Result<Vec<String>> {
        // Fast path: return cached value if fresh.
        {
            let cache = self.cached_tenant_ids.read().await;
            if cache.1.elapsed() < TENANT_ID_CACHE_TTL {
                return Ok(cache.0.clone());
            }
        }

        // Slow path: rescan disk and update cache.
        let ids = self.scan_tenant_ids_from_disk().await?;
        {
            let mut cache = self.cached_tenant_ids.write().await;
            // Double-check: another task may have refreshed while we waited for the write lock.
            if cache.1.elapsed() >= TENANT_ID_CACHE_TTL {
                *cache = (ids.clone(), std::time::Instant::now());
            }
            Ok(cache.0.clone())
        }
    }

    /// Scan the data directory for tenant DB files.
    async fn scan_tenant_ids_from_disk(&self) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.data_dir)
            .await
            .map_err(|e| MuliError::Storage(format!("read data dir: {e}")))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| MuliError::Storage(format!("read dir entry: {e}")))?
        {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(tenant_id) = name_str.strip_suffix(".db")
                && tenant_id != "_global"
            {
                ids.push(tenant_id.to_string());
            }
        }
        Ok(ids)
    }

    /// Backfill git tokens from all existing per-tenant DBs into `global_git_tokens`.
    /// Idempotent via `INSERT OR IGNORE`. Runs once at startup.
    async fn backfill_global_git_tokens(self: &Arc<Self>) -> Result<()> {
        let tenant_ids = self.scan_tenant_ids_from_disk().await?;
        for tenant_id in tenant_ids {
            let path = self.data_dir.join(format!("{tenant_id}.db"));
            if !path.exists() {
                continue;
            }
            // Open a temporary connection for backfill (don't pollute the cache).
            let conn = Connection::open(&path)
                .await
                .map_err(|e| MuliError::Storage(format!("open tenant db for backfill: {e}")))?;

            let tokens: Vec<GitTokenRow> = conn
                .call(move |c| {
                    // Check if git_tokens table exists.
                    let exists: bool = c.query_row(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='git_tokens'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )? > 0;
                    if !exists {
                        return Ok(Vec::new());
                    }
                    let mut stmt = c.prepare(
                        "SELECT id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json FROM git_tokens"
                    )?;
                    let mut result = Vec::new();
                    let mut rows = stmt.query([])?;
                    while let Some(row) = rows.next()? {
                        result.push((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                        ));
                    }
                    Ok(result)
                })
                .await
                .map_err(|e| MuliError::Storage(format!("read git tokens for startup backfill: {e}")))?;

            if !tokens.is_empty() {
                let global = self.global_conn();
                global
                    .call(move |c| {
                        for (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json) in &tokens {
                            c.execute(
                                "INSERT OR IGNORE INTO global_git_tokens (id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                                rusqlite::params![id, tenant_id, user_id, token_hash, token_prefix, expires_at, revoked, full_json],
                            )?;
                        }
                        Ok(())
                    })
                    .await
                    .map_err(|e| MuliError::Storage(format!("backfill git tokens at startup: {e}")))?;
            }
        }
        Ok(())
    }

    /// Evict the least-recently-used tenant connections if the cache exceeds `MAX_TENANT_CONNS`.
    fn maybe_evict_tenant_conns(&self) {
        if self.tenant_conns.len() < MAX_TENANT_CONNS {
            return;
        }

        // Find the oldest entries to evict (remove ~25% of capacity).
        let evict_count = MAX_TENANT_CONNS / 4;
        let mut entries: Vec<(String, std::time::Instant)> = self
            .tenant_access
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();
        entries.sort_by_key(|(_, ts)| *ts);

        for (key, _) in entries.into_iter().take(evict_count) {
            self.tenant_conns.remove(&key);
            self.tenant_access.remove(&key);
        }
    }
}
