// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Agent capability advertisement (labels, resources).

use muli_proto::AgentCapabilities;

use crate::config::AgentConfig;

/// Build the agent capabilities proto message from config and current state.
pub fn build_capabilities(config: &AgentConfig, running_jobs: u32) -> AgentCapabilities {
    let max_concurrent = (config.max_concurrent_jobs as u64).max(1);
    let used_cpu = running_jobs as u64 * (config.total_cpu_millicores / max_concurrent);
    let used_memory = running_jobs as u64 * (config.total_memory_bytes / max_concurrent);

    AgentCapabilities {
        total_cpu_millicores: config.total_cpu_millicores,
        total_memory_bytes: config.total_memory_bytes,
        available_cpu_millicores: config.total_cpu_millicores.saturating_sub(used_cpu),
        available_memory_bytes: config.total_memory_bytes.saturating_sub(used_memory),
        max_concurrent_jobs: config.max_concurrent_jobs,
        running_jobs,
        pre_pulled_images: Vec::new(),
    }
}
