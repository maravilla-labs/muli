// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC service implementations.

pub mod agent_service;
pub mod auth;
pub mod conversions;
pub mod git_service;
pub mod health_service;
pub mod job_query;
pub mod job_service;
pub mod log_service;
pub mod org_service;
pub mod pipeline_service;
pub mod registry_service;
pub mod tenant_service;
pub mod user_service;
pub mod util;

pub use agent_service::AgentServiceImpl;
pub use auth::AuthInterceptor;
pub use git_service::GitServiceImpl;
pub use health_service::HealthServiceImpl;
pub use job_service::JobServiceImpl;
pub use log_service::LogServiceImpl;
pub use org_service::OrgServiceImpl;
pub use pipeline_service::PipelineServiceImpl;
pub use registry_service::RegistryServiceImpl;
pub use tenant_service::TenantServiceImpl;
pub use user_service::UserServiceImpl;
