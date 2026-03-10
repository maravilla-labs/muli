// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC test client helpers.

use muli_proto::*;

/// Create a gRPC channel to a test server on localhost.
pub async fn test_channel(port: u16) -> tonic::transport::Channel {
    let endpoint = format!("http://127.0.0.1:{port}");
    tonic::transport::Channel::from_shared(endpoint)
        .expect("Invalid endpoint URL")
        .connect()
        .await
        .expect("Failed to connect to test gRPC server")
}

/// Create a JobService client connected to a test server.
pub async fn job_client(
    port: u16,
) -> job_service_client::JobServiceClient<tonic::transport::Channel> {
    let channel = test_channel(port).await;
    job_service_client::JobServiceClient::new(channel)
}

/// Create a LogService client connected to a test server.
pub async fn log_client(
    port: u16,
) -> log_service_client::LogServiceClient<tonic::transport::Channel> {
    let channel = test_channel(port).await;
    log_service_client::LogServiceClient::new(channel)
}

/// Create an AgentService client connected to a test server.
pub async fn agent_client(
    port: u16,
) -> agent_service_client::AgentServiceClient<tonic::transport::Channel> {
    let channel = test_channel(port).await;
    agent_service_client::AgentServiceClient::new(channel)
}

/// Build a SubmitJobRequest from test fixture defaults.
pub fn test_submit_request() -> SubmitJobRequest {
    SubmitJobRequest {
        deployment_id: "test-deployment".to_string(),
        project_id: "test-project".to_string(),
        workspace_id: "test-workspace".to_string(),
        runner_image: "alpine:latest".to_string(),
        env_vars: vec![],
        resources: Some(ResourceSpec {
            cpu_request: "250m".to_string(),
            cpu_limit: "500m".to_string(),
            memory_request: "128Mi".to_string(),
            memory_limit: "256Mi".to_string(),
            timeout_seconds: 60,
        }),
        priority_tier: PriorityTier::Standard.into(),
        framework: "static".to_string(),
        tenant_id: "test-tenant".to_string(),
        idempotency_key: String::new(),
        registry_credentials: None,
    }
}

/// Build a SubmitJobRequest with a specific image and command args.
pub fn submit_request_with_command(image: &str, cmd: Vec<String>) -> SubmitJobRequest {
    let mut req = test_submit_request();
    req.runner_image = image.to_string();
    // Pass command as env vars since proto SubmitJobRequest
    // doesn't have a dedicated command field.
    req.env_vars = cmd
        .into_iter()
        .enumerate()
        .map(|(i, c)| EnvVar {
            name: format!("CMD_ARG_{i}"),
            value: c,
        })
        .collect();
    req
}
