// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Remote agent binary: connects to the server, executes jobs via Docker.

pub mod auth;
pub mod capabilities;
pub mod config;
pub mod heartbeat;
pub mod registration;
pub mod worker;

/// Type alias for the agent's gRPC client with auth interceptor applied.
pub type AgentClient = muli_proto::agent_service_client::AgentServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        auth::ClientAuthInterceptor,
    >,
>;
