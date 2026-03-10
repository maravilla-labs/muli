// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite agent store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use muli_core::agent::model::{AgentCapabilities, AgentInfo, AgentStatus};
use muli_core::error::{MuliError, Result};
use muli_core::traits::AgentRegistry;

use super::factory::SqliteStoreFactory;
use super::util::{from_json as agent_from_json, store_err};

pub struct SqliteAgentStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteAgentStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl AgentRegistry for SqliteAgentStore {
    async fn register(&self, agent: &AgentInfo) -> Result<String> {
        let conn = self.factory.global_conn();
        let agent = agent.clone();
        let id = agent.id.clone();
        conn.call(move |c| {
            let json = serde_json::to_string(&agent)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            c.execute(
                "INSERT INTO agents (id, full_json) VALUES (?1, ?2)",
                rusqlite::params![agent.id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn heartbeat(&self, agent_id: &str, caps: &AgentCapabilities) -> Result<()> {
        let conn = self.factory.global_conn();
        let agent_id = agent_id.to_string();
        let agent_id_owned = agent_id.clone();
        let caps = caps.clone();
        let now = Utc::now();

        let rows = conn
            .call(move |c| {
                // Read existing, update, write back
                let existing: Option<String> = {
                    let mut stmt = c.prepare("SELECT full_json FROM agents WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![agent_id])?;
                    rows.next()?
                        .map(|row| row.get::<_, String>(0))
                        .transpose()?
                };
                let Some(json) = existing else {
                    return Ok(0usize);
                };
                let mut agent: AgentInfo = serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                agent.last_heartbeat = now;
                agent.capabilities = caps;
                agent.status = AgentStatus::Healthy;
                let new_json = serde_json::to_string(&agent)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let rows = c.execute(
                    "UPDATE agents SET full_json = ?1 WHERE id = ?2",
                    rusqlite::params![new_json, agent.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::AgentNotFound(agent_id_owned));
        }
        Ok(())
    }

    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentInfo>> {
        let conn = self.factory.global_conn();
        let agent_id = agent_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM agents WHERE id = ?1")?;
            let mut rows = stmt.query(rusqlite::params![agent_id])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(agent_from_json(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn get_healthy_agents(&self) -> Result<Vec<AgentInfo>> {
        let conn = self.factory.global_conn();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM agents")?;
            let all = stmt
                .query_map([], |row| {
                    let json: String = row.get(0)?;
                    agent_from_json(&json)
                })?
                .collect::<rusqlite::Result<Vec<AgentInfo>>>()?;
            Ok(all
                .into_iter()
                .filter(|a| a.status == AgentStatus::Healthy)
                .collect())
        })
        .await
        .map_err(store_err)
    }

    async fn get_all_agents(&self) -> Result<Vec<AgentInfo>> {
        let conn = self.factory.global_conn();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM agents")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                agent_from_json(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }

    async fn mark_dead(&self, agent_id: &str) -> Result<()> {
        let conn = self.factory.global_conn();
        let agent_id = agent_id.to_string();
        let agent_id_owned = agent_id.clone();
        let rows = conn
            .call(move |c| {
                let existing: Option<String> = {
                    let mut stmt = c.prepare("SELECT full_json FROM agents WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![agent_id])?;
                    rows.next()?
                        .map(|row| row.get::<_, String>(0))
                        .transpose()?
                };
                let Some(json) = existing else {
                    return Ok(0usize);
                };
                let mut agent: AgentInfo = serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                agent.status = AgentStatus::Dead;
                let new_json = serde_json::to_string(&agent)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let rows = c.execute(
                    "UPDATE agents SET full_json = ?1 WHERE id = ?2",
                    rusqlite::params![new_json, agent.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::AgentNotFound(agent_id_owned));
        }
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> Result<()> {
        let conn = self.factory.global_conn();
        let agent_id = agent_id.to_string();
        let agent_id_owned = agent_id.clone();
        let rows = conn
            .call(move |c| {
                let rows = c.execute(
                    "DELETE FROM agents WHERE id = ?1",
                    rusqlite::params![agent_id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;

        if rows == 0 {
            return Err(MuliError::AgentNotFound(agent_id_owned));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::agent::model::AgentCapabilities;

    async fn make_store() -> (SqliteAgentStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (SqliteAgentStore::new(factory), dir)
    }

    fn make_agent(id: &str) -> AgentInfo {
        AgentInfo {
            id: id.to_string(),
            name: format!("agent-{id}"),
            hostname: "localhost".to_string(),
            capabilities: AgentCapabilities {
                total_cpu_millicores: 4000,
                total_memory_bytes: 8_000_000_000,
                available_cpu_millicores: 2000,
                available_memory_bytes: 4_000_000_000,
                max_concurrent_jobs: 4,
                running_jobs: 0,
                pre_pulled_images: vec![],
            },
            labels: vec!["default".to_string()],
            status: AgentStatus::Healthy,
            last_heartbeat: Utc::now(),
            registered_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let (store, _dir) = make_store().await;
        let agent = make_agent("a1");
        store.register(&agent).await.unwrap();
        let fetched = store.get_agent("a1").await.unwrap().unwrap();
        assert_eq!(fetched.id, "a1");
    }

    #[tokio::test]
    async fn test_mark_dead() {
        let (store, _dir) = make_store().await;
        store.register(&make_agent("a1")).await.unwrap();
        store.mark_dead("a1").await.unwrap();
        let fetched = store.get_agent("a1").await.unwrap().unwrap();
        assert_eq!(fetched.status, AgentStatus::Dead);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let (store, _dir) = make_store().await;
        store.register(&make_agent("a1")).await.unwrap();
        store.mark_dead("a1").await.unwrap();
        let caps = make_agent("a1").capabilities;
        store.heartbeat("a1", &caps).await.unwrap();
        let fetched = store.get_agent("a1").await.unwrap().unwrap();
        assert_eq!(fetched.status, AgentStatus::Healthy);
    }

    #[tokio::test]
    async fn test_deregister() {
        let (store, _dir) = make_store().await;
        store.register(&make_agent("a1")).await.unwrap();
        store.deregister("a1").await.unwrap();
        assert!(store.get_agent("a1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_healthy() {
        let (store, _dir) = make_store().await;
        store.register(&make_agent("a1")).await.unwrap();
        store.register(&make_agent("a2")).await.unwrap();
        store.mark_dead("a2").await.unwrap();
        let healthy = store.get_healthy_agents().await.unwrap();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, "a1");
    }
}
