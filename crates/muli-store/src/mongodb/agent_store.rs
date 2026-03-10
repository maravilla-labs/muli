// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed agent store.

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::agent::model::{AgentCapabilities, AgentInfo};
use muli_core::error::{MuliError, Result};
use muli_core::traits::AgentRegistry;

use super::util::chrono_to_bson;

const COLLECTION_NAME: &str = "agents";

/// MongoDB implementation of AgentRegistry.
#[derive(Debug, Clone)]
pub struct MongoAgentStore {
    collection: Collection<AgentInfo>,
}

impl MongoAgentStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection::<AgentInfo>(COLLECTION_NAME),
        }
    }
}

#[async_trait]
impl AgentRegistry for MongoAgentStore {
    async fn register(&self, agent: &AgentInfo) -> Result<String> {
        self.collection
            .insert_one(agent)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to register agent: {e}")))?;
        Ok(agent.id.clone())
    }

    async fn heartbeat(&self, agent_id: &str, caps: &AgentCapabilities) -> Result<()> {
        let now = Utc::now();
        let caps_bson =
            mongodb::bson::to_bson(caps).map_err(|e| MuliError::Internal(e.to_string()))?;

        let result = self
            .collection
            .update_one(
                doc! { "id": agent_id },
                doc! {
                    "$set": {
                        "last_heartbeat": chrono_to_bson(now),
                        "capabilities": caps_bson,
                        "status": "Healthy",
                    }
                },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update heartbeat: {e}")))?;

        if result.matched_count == 0 {
            return Err(MuliError::AgentNotFound(agent_id.to_string()));
        }
        Ok(())
    }

    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentInfo>> {
        self.collection
            .find_one(doc! { "id": agent_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get agent: {e}")))
    }

    async fn get_healthy_agents(&self) -> Result<Vec<AgentInfo>> {
        let cursor = self
            .collection
            .find(doc! { "status": "Healthy" })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get healthy agents: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect agents: {e}")))
    }

    async fn get_all_agents(&self) -> Result<Vec<AgentInfo>> {
        let cursor = self
            .collection
            .find(doc! {})
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get all agents: {e}")))?;

        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect agents: {e}")))
    }

    async fn mark_dead(&self, agent_id: &str) -> Result<()> {
        let result = self
            .collection
            .update_one(
                doc! { "id": agent_id },
                doc! { "$set": { "status": "Dead" } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to mark dead: {e}")))?;

        if result.matched_count == 0 {
            return Err(MuliError::AgentNotFound(agent_id.to_string()));
        }
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> Result<()> {
        let result = self
            .collection
            .delete_one(doc! { "id": agent_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to deregister: {e}")))?;

        if result.deleted_count == 0 {
            return Err(MuliError::AgentNotFound(agent_id.to_string()));
        }
        Ok(())
    }
}
