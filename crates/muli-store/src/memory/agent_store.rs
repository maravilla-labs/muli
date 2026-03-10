// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-memory agent registration and status store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use muli_core::agent::model::{AgentCapabilities, AgentInfo, AgentStatus};
use muli_core::error::{MuliError, Result};
use muli_core::traits::AgentRegistry;

/// In-memory implementation of AgentRegistry for testing.
#[derive(Debug, Clone)]
pub struct MemoryAgentStore {
    agents: Arc<DashMap<String, AgentInfo>>,
}

impl MemoryAgentStore {
    pub fn new() -> Self {
        Self {
            agents: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryAgentStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AgentRegistry for MemoryAgentStore {
    async fn register(&self, agent: &AgentInfo) -> Result<String> {
        if self.agents.contains_key(&agent.id) {
            return Err(MuliError::Storage(format!(
                "Agent with id {} already exists",
                agent.id
            )));
        }
        let id = agent.id.clone();
        self.agents.insert(id.clone(), agent.clone());
        Ok(id)
    }

    async fn heartbeat(&self, agent_id: &str, caps: &AgentCapabilities) -> Result<()> {
        let mut entry = self
            .agents
            .get_mut(agent_id)
            .ok_or_else(|| MuliError::AgentNotFound(agent_id.to_string()))?;

        let agent = entry.value_mut();
        agent.last_heartbeat = Utc::now();
        agent.capabilities = caps.clone();
        agent.status = AgentStatus::Healthy;
        Ok(())
    }

    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentInfo>> {
        Ok(self.agents.get(agent_id).map(|entry| entry.value().clone()))
    }

    async fn get_healthy_agents(&self) -> Result<Vec<AgentInfo>> {
        Ok(self
            .agents
            .iter()
            .filter(|entry| entry.value().status == AgentStatus::Healthy)
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get_all_agents(&self) -> Result<Vec<AgentInfo>> {
        Ok(self
            .agents
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn mark_dead(&self, agent_id: &str) -> Result<()> {
        let mut entry = self
            .agents
            .get_mut(agent_id)
            .ok_or_else(|| MuliError::AgentNotFound(agent_id.to_string()))?;
        entry.value_mut().status = AgentStatus::Dead;
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> Result<()> {
        self.agents
            .remove(agent_id)
            .ok_or_else(|| MuliError::AgentNotFound(agent_id.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let store = MemoryAgentStore::new();
        let agent = make_agent("a1");
        let id = store.register(&agent).await.unwrap();
        assert_eq!(id, "a1");

        let fetched = store.get_agent("a1").await.unwrap().unwrap();
        assert_eq!(fetched.name, "agent-a1");
    }

    #[tokio::test]
    async fn test_register_duplicate_fails() {
        let store = MemoryAgentStore::new();
        let agent = make_agent("a1");
        store.register(&agent).await.unwrap();
        assert!(store.register(&agent).await.is_err());
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let store = MemoryAgentStore::new();
        let agent = make_agent("a1");
        store.register(&agent).await.unwrap();

        let new_caps = AgentCapabilities {
            total_cpu_millicores: 4000,
            total_memory_bytes: 8_000_000_000,
            available_cpu_millicores: 1000,
            available_memory_bytes: 2_000_000_000,
            max_concurrent_jobs: 4,
            running_jobs: 2,
            pre_pulled_images: vec!["node:18".to_string()],
        };
        store.heartbeat("a1", &new_caps).await.unwrap();

        let fetched = store.get_agent("a1").await.unwrap().unwrap();
        assert_eq!(fetched.capabilities.running_jobs, 2);
        assert_eq!(fetched.status, AgentStatus::Healthy);
    }

    #[tokio::test]
    async fn test_heartbeat_nonexistent_fails() {
        let store = MemoryAgentStore::new();
        let caps = AgentCapabilities {
            total_cpu_millicores: 4000,
            total_memory_bytes: 8_000_000_000,
            available_cpu_millicores: 2000,
            available_memory_bytes: 4_000_000_000,
            max_concurrent_jobs: 4,
            running_jobs: 0,
            pre_pulled_images: vec![],
        };
        assert!(store.heartbeat("nonexistent", &caps).await.is_err());
    }

    #[tokio::test]
    async fn test_get_healthy_agents() {
        let store = MemoryAgentStore::new();
        let a1 = make_agent("a1");
        let a2 = make_agent("a2");
        store.register(&a1).await.unwrap();
        store.register(&a2).await.unwrap();
        store.mark_dead("a2").await.unwrap();

        let healthy = store.get_healthy_agents().await.unwrap();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, "a1");
    }

    #[tokio::test]
    async fn test_get_all_agents() {
        let store = MemoryAgentStore::new();
        store.register(&make_agent("a1")).await.unwrap();
        store.register(&make_agent("a2")).await.unwrap();

        let all = store.get_all_agents().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_mark_dead() {
        let store = MemoryAgentStore::new();
        store.register(&make_agent("a1")).await.unwrap();
        store.mark_dead("a1").await.unwrap();

        let fetched = store.get_agent("a1").await.unwrap().unwrap();
        assert_eq!(fetched.status, AgentStatus::Dead);
    }

    #[tokio::test]
    async fn test_deregister() {
        let store = MemoryAgentStore::new();
        store.register(&make_agent("a1")).await.unwrap();
        store.deregister("a1").await.unwrap();

        let fetched = store.get_agent("a1").await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_deregister_nonexistent_fails() {
        let store = MemoryAgentStore::new();
        assert!(store.deregister("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_get_nonexistent_agent() {
        let store = MemoryAgentStore::new();
        let result = store.get_agent("nonexistent").await.unwrap();
        assert!(result.is_none());
    }
}
