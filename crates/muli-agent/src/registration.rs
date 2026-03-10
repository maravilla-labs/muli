// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Agent registration with the server.

use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{info, warn};

use muli_proto::agent_service_client::AgentServiceClient;
use muli_proto::{DeregisterAgentRequest, RegisterAgentRequest};

use crate::AgentClient;
use crate::auth::ClientAuthInterceptor;
use crate::capabilities::build_capabilities;
use crate::config::AgentConfig;

const MAX_CONNECT_RETRIES: u32 = 10;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Register this agent with the muli-server. Retries connection with exponential backoff.
pub async fn register(config: &AgentConfig) -> Result<(AgentClient, String)> {
    let mut endpoint = tonic::transport::Channel::from_shared(config.server_url.clone())
        .context("invalid server URL")?;

    // Configure TLS when the server URL uses https://
    if config.server_url.starts_with("https://") {
        let mut tls_config = tonic::transport::ClientTlsConfig::new();
        if let Some(ref ca_cert_path) = config.tls_ca_cert {
            let ca_cert = std::fs::read_to_string(ca_cert_path)
                .context("failed to read TLS CA certificate")?;
            let ca = tonic::transport::Certificate::from_pem(ca_cert);
            tls_config = tls_config.ca_certificate(ca);
        }
        endpoint = endpoint
            .tls_config(tls_config)
            .context("invalid TLS configuration")?;
        info!("TLS enabled for server connection");
    }

    // Retry connection with exponential backoff
    let mut attempt = 0u32;
    let channel = loop {
        attempt += 1;
        match endpoint.connect().await {
            Ok(ch) => break ch,
            Err(e) => {
                if attempt >= MAX_CONNECT_RETRIES {
                    return Err(anyhow::anyhow!(
                        "failed to connect to muli-server after {MAX_CONNECT_RETRIES} attempts: {e}"
                    ));
                }
                let backoff = INITIAL_BACKOFF
                    .saturating_mul(1 << attempt.min(5))
                    .min(MAX_BACKOFF);
                warn!(
                    attempt = attempt,
                    max_retries = MAX_CONNECT_RETRIES,
                    backoff_secs = backoff.as_secs(),
                    error = %e,
                    "failed to connect to server, retrying"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    };

    let auth = ClientAuthInterceptor::new(config.api_key.clone());
    let mut client = AgentServiceClient::with_interceptor(channel, auth);

    let hostname = hostname();

    let response = client
        .register_agent(RegisterAgentRequest {
            name: config.name.clone(),
            hostname,
            capabilities: Some(build_capabilities(config, 0)),
            labels: config.labels.clone(),
        })
        .await
        .context("failed to register agent")?
        .into_inner();

    info!(
        agent_id = %response.agent_id,
        heartbeat_interval = response.heartbeat_interval_seconds,
        "registered with server"
    );

    Ok((client, response.agent_id))
}

/// Deregister this agent from the server (graceful shutdown).
pub async fn deregister(client: &mut AgentClient, agent_id: &str, reason: &str) -> Result<()> {
    client
        .deregister_agent(DeregisterAgentRequest {
            agent_id: agent_id.to_string(),
            reason: reason.to_string(),
        })
        .await
        .context("failed to deregister agent")?;

    info!(agent_id = %agent_id, reason = %reason, "deregistered from server");
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}
