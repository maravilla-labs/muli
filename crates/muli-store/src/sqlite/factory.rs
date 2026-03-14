// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite connection pool factory with per-tenant databases.

use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use tokio_rusqlite::Connection;

use muli_core::error::{MuliError, Result};

use super::tenant_store::SqliteTenantStore;

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
";

/// Central SQLite connection factory. Holds one connection per tenant DB
/// and one for the shared `_global.db`.
pub struct SqliteStoreFactory {
    data_dir: PathBuf,
    tenant_conns: DashMap<String, Arc<Connection>>,
    global_conn: Arc<Connection>,
}

impl SqliteStoreFactory {
    /// Create a new factory, initialising `_global.db` immediately.
    pub async fn new(data_dir: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir)
            .await
            .map_err(|e| MuliError::Storage(format!("create data dir: {e}")))?;

        let global_path = data_dir.join("_global.db");
        let global_conn = Connection::open(global_path)
            .await
            .map_err(|e| MuliError::Storage(format!("open global db: {e}")))?;

        global_conn
            .call(|c| {
                c.execute_batch("PRAGMA journal_mode=WAL;")?;
                c.execute_batch("PRAGMA busy_timeout = 5000;")?;
                c.execute_batch("PRAGMA foreign_keys = ON;")?;
                // Safe migrations: add columns to existing DBs (ignored if already present).
                // Must run BEFORE DDL so that CREATE INDEX on new columns doesn't fail.
                let _ = c.execute("ALTER TABLE jobs ADD COLUMN idempotency_key TEXT", []);
                let _ = c.execute(
                    "ALTER TABLE registry_tokens ADD COLUMN token_prefix TEXT NOT NULL DEFAULT ''",
                    [],
                );
                c.execute_batch(GLOBAL_DDL)?;
                Ok(())
            })
            .await
            .map_err(|e| MuliError::Storage(format!("init global db: {e}")))?;

        Ok(Arc::new(Self {
            data_dir,
            tenant_conns: DashMap::new(),
            global_conn: Arc::new(global_conn),
        }))
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
            return Ok(c.value().clone());
        }

        let path = self.data_dir.join(format!("{tenant_id}.db"));
        let conn = Connection::open(path)
            .await
            .map_err(|e| MuliError::Storage(format!("open tenant db {tenant_id}: {e}")))?;

        conn.call(|c| {
            c.execute_batch("PRAGMA journal_mode=WAL;")?;
            c.execute_batch("PRAGMA busy_timeout = 5000;")?;
            c.execute_batch("PRAGMA foreign_keys = ON;")?;
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
        let global = self.global_conn.clone();
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

        let conn = Arc::new(conn);
        // Another racing caller may have inserted first; that's fine.
        self.tenant_conns
            .entry(tenant_id.to_string())
            .or_insert_with(|| conn.clone());

        // Return whatever ended up in the map.
        Ok(self.tenant_conns.get(tenant_id).unwrap().value().clone())
    }

    /// Return the global connection (jobs, agents, registry tokens, quotas).
    pub fn global_conn(&self) -> Arc<Connection> {
        self.global_conn.clone()
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
    /// Use this for cross-tenant searches (e.g. `get_token_by_prefix`) instead of
    /// `open_tenant_ids()`, which only returns already-cached connections.
    pub async fn all_tenant_ids(&self) -> Result<Vec<String>> {
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
}
