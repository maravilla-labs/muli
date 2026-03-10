// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Prometheus metrics endpoint.

use std::sync::LazyLock;

use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};

// ── Job execution metrics ─────────────────────────────────────────────────────

pub static JOBS_SUBMITTED_TOTAL: LazyLock<prometheus::IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "muli_jobs_submitted_total",
        "Total number of jobs submitted",
        &["tenant", "tier"]
    )
    .expect("Failed to register muli_jobs_submitted_total")
});

pub static JOBS_COMPLETED_TOTAL: LazyLock<prometheus::IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "muli_jobs_completed_total",
        "Total number of completed jobs by state",
        &["tenant", "tier", "state"]
    )
    .expect("Failed to register muli_jobs_completed_total")
});

pub static JOBS_RUNNING: LazyLock<prometheus::IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!("muli_jobs_running", "Number of currently running jobs")
        .expect("Failed to register muli_jobs_running")
});

pub static JOB_DURATION_SECONDS: LazyLock<prometheus::HistogramVec> = LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        "muli_job_duration_seconds",
        "Job execution duration in seconds",
        &["tenant", "tier"],
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0]
    )
    .expect("Failed to register muli_job_duration_seconds")
});

pub static AGENTS_CONNECTED: LazyLock<prometheus::IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!("muli_agents_connected", "Number of connected agents")
        .expect("Failed to register muli_agents_connected")
});

/// Build the HTTP router for metrics and health endpoints.
pub fn metrics_router() -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
}

async fn metrics_handler() -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    match encoder.encode_to_string(&metric_families) {
        Ok(body) => (
            StatusCode::OK,
            [("Content-Type", "text/plain; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to encode metrics: {e}"),
        )
            .into_response(),
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
