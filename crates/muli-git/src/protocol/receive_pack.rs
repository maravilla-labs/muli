// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Receive-pack handler for git push with webhook triggering.

use std::path::PathBuf;

use axum::{
    body::Body,
    http::{HeaderMap, Response},
};

use crate::protocol::upload_pack::run_git_http_backend_authenticated;

/// Handle `GET /{ns}/{repo}.git/info/refs?service=git-receive-pack`
///
/// `remote_user` should be the authenticated tenant/user identifier so that
/// git http-backend enables the receive-pack service (push operations).
pub async fn info_refs_receive(repo_path: PathBuf, headers: &HeaderMap, remote_user: &str) -> Response<Body> {
    run_git_http_backend_authenticated(
        &repo_path,
        "GET",
        "/info/refs",
        "service=git-receive-pack",
        headers,
        vec![],
        remote_user,
    )
    .await
}

/// Handle `POST /{ns}/{repo}.git/git-receive-pack`
pub async fn post_receive_pack(
    repo_path: PathBuf,
    headers: &HeaderMap,
    body: Vec<u8>,
    remote_user: &str,
) -> Response<Body> {
    run_git_http_backend_authenticated(
        &repo_path,
        "POST",
        "/git-receive-pack",
        "",
        headers,
        body,
        remote_user,
    )
    .await
}
