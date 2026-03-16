// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod artifacts;
pub mod runs;
pub mod secrets;

use axum::routing::{delete, get, post};
use axum::Router;

/// Build the pipeline REST API router.
/// Routes are mounted under /api/v1/repos/{namespace}/{repo}/pipelines/
pub fn pipeline_api_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs",
            get(runs::list_runs),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs/{run_number}",
            get(runs::get_run),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs/{run_number}/cancel",
            post(runs::cancel_run),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs/{run_number}/retry",
            post(runs::retry_run),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs/{run_number}/steps/{step}/logs",
            get(runs::get_step_logs),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs/{run_number}/artifacts",
            get(artifacts::list_artifacts),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/runs/{run_number}/artifacts/{name}",
            get(artifacts::download_artifact),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/trigger",
            post(runs::trigger_run),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/secrets",
            post(secrets::set_secret).get(secrets::list_secrets),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pipelines/secrets/{name}",
            delete(secrets::delete_secret),
        )
}
