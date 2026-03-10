// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests: Auth Interceptor (no Docker required).

mod common;

use muli_test::grpc_helpers::{job_client, test_submit_request};

use common::{TestGrpcServer, with_tenant, with_tenant_and_auth};

#[tokio::test]
async fn test_auth_rejects_missing_key() {
    let server = TestGrpcServer::start_with_options(Some("test-secret".to_string())).await;
    let mut client = job_client(server.port).await;

    let result = client
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn test_auth_accepts_valid_key() {
    let api_key = "test-secret-key";
    let server = TestGrpcServer::start_with_options(Some(api_key.to_string())).await;
    let mut client = job_client(server.port).await;

    let resp = client
        .submit_job(with_tenant_and_auth(
            test_submit_request(),
            "test-tenant",
            api_key,
        ))
        .await;
    assert!(resp.is_ok());
}
