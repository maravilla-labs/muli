// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! OCI tag listing endpoint.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::storage::{FilesystemStorage, RegistryStorage};
use crate::tenant::TenantContext;

use super::{oci_error, validate_name};

#[derive(Serialize)]
struct TagList {
    name: String,
    tags: Vec<String>,
}

/// GET /v2/{name}/tags/list
pub async fn list_tags(
    State(storage): State<std::sync::Arc<FilesystemStorage>>,
    Extension(tenant): Extension<TenantContext>,
    Path(name): Path<String>,
) -> Response {
    if let Err(e) = validate_name(&name) {
        return e;
    }
    match storage.list_tags(&tenant.tenant_id, &name).await {
        Ok(tags) => Json(TagList { name, tags }).into_response(),
        Err(_) => oci_error(
            StatusCode::NOT_FOUND,
            "NAME_UNKNOWN",
            "repository not found",
        ),
    }
}
