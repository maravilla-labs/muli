// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Agent data model: status, capabilities, and resource capacity.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Information about a registered agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub id: String,
    pub name: String,
    pub hostname: String,
    pub capabilities: AgentCapabilities,
    pub labels: Vec<String>,
    pub status: AgentStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
}

/// What resources an agent has available.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCapabilities {
    pub total_cpu_millicores: u64,
    pub total_memory_bytes: u64,
    pub available_cpu_millicores: u64,
    pub available_memory_bytes: u64,
    pub max_concurrent_jobs: u32,
    pub running_jobs: u32,
    pub pre_pulled_images: Vec<String>,
}

/// Agent health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    Healthy,
    Unhealthy,
    Dead,
}

impl AgentStatus {
    /// Check health based on heartbeat interval.
    /// unhealthy_threshold: 3x interval, dead_threshold: 5x interval
    pub fn from_heartbeat_age(last_heartbeat: DateTime<Utc>, heartbeat_interval_secs: u64) -> Self {
        let age_secs = (Utc::now() - last_heartbeat).num_seconds().max(0) as u64;
        let dead_threshold = heartbeat_interval_secs * 5;
        let unhealthy_threshold = heartbeat_interval_secs * 3;

        if age_secs >= dead_threshold {
            AgentStatus::Dead
        } else if age_secs >= unhealthy_threshold {
            AgentStatus::Unhealthy
        } else {
            AgentStatus::Healthy
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_agent_status_healthy() {
        let recent = Utc::now() - Duration::seconds(5);
        assert_eq!(
            AgentStatus::from_heartbeat_age(recent, 10),
            AgentStatus::Healthy
        );
    }

    #[test]
    fn test_agent_status_unhealthy() {
        let old = Utc::now() - Duration::seconds(35);
        assert_eq!(
            AgentStatus::from_heartbeat_age(old, 10),
            AgentStatus::Unhealthy
        );
    }

    #[test]
    fn test_agent_status_dead() {
        let very_old = Utc::now() - Duration::seconds(55);
        assert_eq!(
            AgentStatus::from_heartbeat_age(very_old, 10),
            AgentStatus::Dead
        );
    }
}
