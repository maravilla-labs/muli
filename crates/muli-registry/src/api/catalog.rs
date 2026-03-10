// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI catalog listing endpoint.

use axum::{
    Extension, Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::storage::{FilesystemStorage, RegistryStorage};
use crate::tenant::TenantContext;

use super::oci_error;

#[derive(Serialize)]
struct Catalog {
    repositories: Vec<String>,
}

/// GET /v2/_catalog
pub async fn catalog(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
) -> Response {
    match storage.list_repositories(&tenant.tenant_id).await {
        Ok(repositories) => Json(Catalog { repositories }).into_response(),
        Err(_) => oci_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "UNKNOWN",
            "failed to list repositories",
        ),
    }
}
