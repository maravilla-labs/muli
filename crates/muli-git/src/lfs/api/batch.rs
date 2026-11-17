// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! LFS Batch API handler (POST /{ns}/{repo}/info/lfs/objects/batch).

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use super::{LfsRepo, resolve};
use crate::api::GitState;
use crate::lfs::storage::LfsStorageError;
use crate::lfs::types::{
    self, APPLICATION_VND_GIT_LFS_JSON, Action, Actions, BatchRequest, BatchResponse, ObjectError,
    ObjectResponse, Operation,
};
use crate::repo_target::TargetRepo;
use crate::tenant::TenantContext;

// `authenticated: true` tells git-lfs the action hrefs need no credentials, so it
// sends the transfer without the Authorization it used for the batch request.
// Only a presigned URL satisfies that; every href on this host goes through the
// same auth as the batch call, so those responses leave `authenticated` unset.

/// Default presigned URL TTL in seconds.
const PRESIGN_TTL_SECS: u64 = 3600;

/// The per-object error of a download for an oid this repository does not
/// own. Identical whether the oid is stored for another repository or nowhere.
const NOT_FOUND_MESSAGE: &str = "Not found";

/// POST `/{namespace}/{repo}/info/lfs/objects/batch`
pub async fn batch(
    State(state): State<Arc<GitState>>,
    Extension(_tenant): Extension<TenantContext>,
    TargetRepo(target): TargetRepo,
    headers: HeaderMap,
    Json(req): Json<BatchRequest>,
) -> Response {
    let repo = match resolve(&state, &target) {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    // Hrefs name the resolved repository, never echo the request path.
    let base_url = build_base_url(
        state.public_url.as_deref(),
        &headers,
        &target.namespace,
        &target.name,
    );

    let mut objects = Vec::with_capacity(req.objects.len());

    for obj in &req.objects {
        if !types::validate_oid(&obj.oid) {
            objects.push(ObjectResponse {
                oid: obj.oid.clone(),
                size: obj.size,
                authenticated: None,
                actions: None,
                error: Some(ObjectError {
                    code: 422,
                    message: "invalid oid".to_string(),
                }),
            });
            continue;
        }

        let resp = match req.operation {
            Operation::Download => build_download_response(&repo, obj, &base_url).await,
            Operation::Upload => build_upload_response(&repo, obj, &base_url).await,
        };
        objects.push(resp);
    }

    let body = BatchResponse {
        transfer: "basic".to_string(),
        objects,
        hash_algo: Some("sha256".to_string()),
    };

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            APPLICATION_VND_GIT_LFS_JSON,
        )],
        Json(body),
    )
        .into_response()
}

fn not_found_object(obj: &types::ObjectSpec) -> ObjectResponse {
    ObjectResponse {
        oid: obj.oid.clone(),
        size: obj.size,
        authenticated: None,
        actions: None,
        error: Some(ObjectError {
            code: 404,
            message: NOT_FOUND_MESSAGE.to_string(),
        }),
    }
}

fn index_error_object(obj: &types::ObjectSpec, e: &muli_core::error::MuliError) -> ObjectResponse {
    tracing::error!(error = %e, oid = %obj.oid, "LFS object index read failed");
    ObjectResponse {
        oid: obj.oid.clone(),
        size: obj.size,
        authenticated: None,
        actions: None,
        error: Some(ObjectError {
            code: 500,
            message: "object index unavailable".to_string(),
        }),
    }
}

async fn build_download_response(
    repo: &LfsRepo<'_>,
    obj: &types::ObjectSpec,
    base_url: &str,
) -> ObjectResponse {
    // Only an object of THIS repository is offered, whatever storage holds.
    match repo.owns(&obj.oid).await {
        Ok(true) => {}
        Ok(false) => return not_found_object(obj),
        Err(e) => return index_error_object(obj, &e),
    }

    let size = match repo.storage.object_size(&repo.tenant_id, &obj.oid).await {
        Ok(Some(size)) => size,
        Ok(None) => return not_found_object(obj),
        Err(e) => return error_object(&obj.oid, obj.size, &e),
    };

    // Presigned URL (S3 backend), issued only after the ownership check.
    if let Ok(Some(url)) = repo
        .storage
        .presigned_download_url(&repo.tenant_id, &obj.oid, PRESIGN_TTL_SECS)
        .await
    {
        return ObjectResponse {
            oid: obj.oid.clone(),
            size,
            // The presigned URL authenticates itself; nothing else in this response does.
            authenticated: Some(true),
            actions: Some(Actions {
                download: Some(Action {
                    href: url,
                    header: None,
                    expires_in: Some(PRESIGN_TTL_SECS),
                }),
                upload: None,
                verify: None,
            }),
            error: None,
        };
    }

    ObjectResponse {
        oid: obj.oid.clone(),
        size,
        authenticated: None,
        actions: Some(Actions {
            download: Some(Action {
                href: format!("{base_url}/{}", obj.oid),
                header: None,
                expires_in: None,
            }),
            upload: None,
            verify: None,
        }),
        error: None,
    }
}

async fn build_upload_response(
    repo: &LfsRepo<'_>,
    obj: &types::ObjectSpec,
    base_url: &str,
) -> ObjectResponse {
    // Nothing to send only when THIS repository already owns the object and
    // the bytes are there. An oid stored for another repository is answered
    // like an unknown one: the client uploads it, the server hashes it (and
    // keeps one copy), and the upload binds it to this repository.
    let owned = match repo.owns(&obj.oid).await {
        Ok(owned) => owned,
        Err(e) => return index_error_object(obj, &e),
    };
    if owned {
        match repo.storage.has_object(&repo.tenant_id, &obj.oid).await {
            Ok(true) => {
                return ObjectResponse {
                    oid: obj.oid.clone(),
                    size: obj.size,
                    authenticated: None,
                    actions: None,
                    error: None,
                };
            }
            Ok(false) => {}
            Err(e) => return error_object(&obj.oid, obj.size, &e),
        }
    }

    ObjectResponse {
        oid: obj.oid.clone(),
        size: obj.size,
        authenticated: None,
        actions: Some(Actions {
            upload: Some(Action {
                href: format!("{base_url}/{}", obj.oid),
                header: None,
                expires_in: None,
            }),
            download: None,
            verify: Some(Action {
                href: format!("{base_url}/verify"),
                header: None,
                expires_in: None,
            }),
        }),
        error: None,
    }
}

/// Build the base URL for LFS object transfer endpoints.
///
/// Uses the configured public URL when set. Otherwise it is derived from the
/// request, which is only right when the Host header is the public host (not
/// when a gateway rewrites it to an internal tenant subdomain).
fn build_base_url(
    public_url: Option<&str>,
    headers: &HeaderMap,
    namespace: &str,
    name: &str,
) -> String {
    if let Some(public_url) = public_url {
        return format!(
            "{}/objects",
            crate::lfs::lfs_endpoint(public_url, namespace, name)
        );
    }
    let scheme = if headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s == "https")
    {
        "https"
    } else {
        "http"
    };
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");

    format!("{scheme}://{host}/{namespace}/{name}.git/info/lfs/objects")
}

fn error_object(oid: &str, size: u64, e: &LfsStorageError) -> ObjectResponse {
    ObjectResponse {
        oid: oid.to_string(),
        size,
        authenticated: None,
        actions: None,
        error: Some(ObjectError {
            code: 500,
            message: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("host", "local.git.example.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers
    }

    #[test]
    fn base_url_prefers_public_url_over_host_header() {
        assert_eq!(
            build_base_url(
                Some("https://git.example.com"),
                &gateway_headers(),
                "acme",
                "site"
            ),
            "https://git.example.com/acme/site.git/info/lfs/objects"
        );
    }

    #[test]
    fn base_url_falls_back_to_host_header() {
        assert_eq!(
            build_base_url(None, &gateway_headers(), "acme", "site"),
            "https://local.git.example.com/acme/site.git/info/lfs/objects"
        );
    }
}
