// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! LFS HTTP API route handlers.
//!
//! Objects are stored once per tenant (content-addressed), but they BELONG to
//! repositories: every handler resolves the repository named in the path and
//! answers only for oids the LFS object index holds for it. An oid stored for
//! another repository gets exactly the answer of an oid stored nowhere, so
//! the API is no existence oracle across repositories.

mod batch;
mod transfer;

use std::sync::Arc;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use muli_core::traits::LfsObjectStore;

pub use batch::batch;
pub use transfer::{download, upload, verify};

use crate::api::GitState;
use crate::api::helpers::strip_git_suffix;
use crate::lfs::storage::LfsStorage;
use crate::lfs::types::APPLICATION_VND_GIT_LFS_JSON;

/// The message of every "this repository has no such object" answer, whether
/// the oid is stored for another repository or nowhere.
pub(crate) const OBJECT_NOT_FOUND: &str = "object not found";

/// What an LFS request works on: the storage, the index and the repository.
pub(crate) struct LfsRepo<'a> {
    pub storage: &'a Arc<dyn LfsStorage>,
    pub objects: &'a Arc<dyn LfsObjectStore>,
    pub tenant_id: String,
    pub repo_id: String,
}

impl LfsRepo<'_> {
    /// Whether `oid` belongs to this repository. A failed lookup is `Err`:
    /// callers answer 500, never "present".
    pub async fn owns(&self, oid: &str) -> Result<bool, muli_core::error::MuliError> {
        self.objects
            .has_lfs_object(&self.tenant_id, &self.repo_id, oid)
            .await
    }
}

/// Resolve the LFS backend and the repository named in the path, or the
/// response to send instead (LFS disabled, unknown repository, store error).
pub(crate) async fn resolve<'a>(
    state: &'a GitState,
    tenant_id: &str,
    namespace: &str,
    raw_repo: &str,
) -> Result<LfsRepo<'a>, Response> {
    let (Some(storage), Some(objects)) = (&state.lfs_storage, &state.lfs_objects) else {
        return Err(lfs_error(StatusCode::NOT_FOUND, "LFS is not enabled"));
    };
    let name = strip_git_suffix(raw_repo);
    let repo = match state
        .repo_store
        .get_repository_by_name(tenant_id, namespace, name)
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return Err(lfs_error(StatusCode::NOT_FOUND, "repository not found")),
        Err(e) => {
            tracing::error!(error = %e, "LFS repository lookup failed");
            return Err(lfs_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "repository lookup failed",
            ));
        }
    };
    Ok(LfsRepo {
        storage,
        objects,
        tenant_id: tenant_id.to_string(),
        repo_id: repo.id,
    })
}

pub(crate) fn lfs_error(status: StatusCode, msg: &str) -> Response {
    let body = serde_json::json!({
        "message": msg,
        "request_id": "",
    });
    (
        status,
        [(
            axum::http::header::CONTENT_TYPE,
            APPLICATION_VND_GIT_LFS_JSON,
        )],
        Json(body),
    )
        .into_response()
}
