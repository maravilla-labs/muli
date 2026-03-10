// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests: Agent Dispatch via Heartbeat (no Docker required).

mod common;

use muli_core::job::state_machine::JobState;
use muli_proto::{HeartbeatRequest, RegisterAgentRequest};
use tonic::Request;

use muli_test::grpc_helpers::{agent_client, job_client, test_submit_request};

use common::{TestGrpcServer, test_capabilities, with_tenant};

#[tokio::test]
async fn test_heartbeat_returns_assignment() {
    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    let resp = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let reg = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "dispatch-agent".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_id = reg.into_inner().agent_id;

    let hb = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id,
            capabilities: test_capabilities(),
        }))
        .await
        .unwrap();
    let inner = hb.into_inner();
    assert!(inner.acknowledged);
    assert_eq!(inner.assignments.len(), 1);
    assert_eq!(inner.assignments[0].job_id, job_id);
    assert_eq!(inner.assignments[0].runner_image, "alpine:latest");
}

#[tokio::test]
async fn test_two_agents_get_different_jobs() {
    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    let resp1 = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id_1 = resp1.into_inner().job_id;

    let resp2 = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id_2 = resp2.into_inner().job_id;

    let mut caps_one = test_capabilities().unwrap();
    caps_one.max_concurrent_jobs = 1;
    caps_one.running_jobs = 0;

    let reg1 = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "agent-1".to_string(),
            hostname: "localhost".to_string(),
            capabilities: Some(caps_one.clone()),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_id_1 = reg1.into_inner().agent_id;

    let reg2 = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "agent-2".to_string(),
            hostname: "localhost".to_string(),
            capabilities: Some(caps_one.clone()),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_id_2 = reg2.into_inner().agent_id;

    let hb1 = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: agent_id_1,
            capabilities: Some(caps_one.clone()),
        }))
        .await
        .unwrap();
    let assignments1 = hb1.into_inner().assignments;
    assert_eq!(assignments1.len(), 1);

    let hb2 = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: agent_id_2,
            capabilities: Some(caps_one),
        }))
        .await
        .unwrap();
    let assignments2 = hb2.into_inner().assignments;
    assert_eq!(assignments2.len(), 1);

    assert_ne!(assignments1[0].job_id, assignments2[0].job_id);

    let assigned_ids: Vec<String> = vec![
        assignments1[0].job_id.clone(),
        assignments2[0].job_id.clone(),
    ];
    assert!(assigned_ids.contains(&job_id_1));
    assert!(assigned_ids.contains(&job_id_2));
}

#[tokio::test]
async fn test_agent_at_capacity_gets_no_assignment() {
    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();

    let reg = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "full-agent".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_id = reg.into_inner().agent_id;

    let mut caps = test_capabilities().unwrap();
    caps.max_concurrent_jobs = 2;
    caps.running_jobs = 2;

    let hb = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id,
            capabilities: Some(caps),
        }))
        .await
        .unwrap();
    assert!(hb.into_inner().assignments.is_empty());
}

#[tokio::test]
async fn test_job_state_updated_to_scheduled() {
    let server = TestGrpcServer::start().await;
    let mut job_cl = job_client(server.port).await;
    let mut agent_cl = agent_client(server.port).await;

    let resp = job_cl
        .submit_job(with_tenant(test_submit_request(), "test-tenant"))
        .await
        .unwrap();
    let job_id = resp.into_inner().job_id;

    let status = job_cl
        .get_job_status(with_tenant(
            muli_proto::GetJobStatusRequest {
                job_id: job_id.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(status.into_inner().state, 1);

    let reg = agent_cl
        .register_agent(Request::new(RegisterAgentRequest {
            name: "sched-agent".to_string(),
            hostname: "localhost".to_string(),
            capabilities: test_capabilities(),
            labels: vec![],
        }))
        .await
        .unwrap();
    let agent_id = reg.into_inner().agent_id;

    let hb = agent_cl
        .heartbeat(Request::new(HeartbeatRequest {
            agent_id: agent_id.clone(),
            capabilities: test_capabilities(),
        }))
        .await
        .unwrap();
    assert_eq!(hb.into_inner().assignments.len(), 1);

    let status = job_cl
        .get_job_status(with_tenant(
            muli_proto::GetJobStatusRequest {
                job_id: job_id.clone(),
            },
            "test-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(status.into_inner().state, 2); // Scheduled

    let job = server.job_store.get_job(&job_id).await.unwrap().unwrap();
    assert_eq!(job.assigned_agent, Some(agent_id));
    assert_eq!(job.state, JobState::Scheduled);
}
