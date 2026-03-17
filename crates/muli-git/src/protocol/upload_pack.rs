// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Upload-pack handler for git clone/fetch.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use axum::{
    body::Body,
    http::{HeaderMap, Response, StatusCode},
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Maximum time to wait for git http-backend to complete (5 minutes).
const GIT_HTTP_BACKEND_TIMEOUT: Duration = Duration::from_secs(300);

/// Handle `GET /{ns}/{repo}.git/info/refs?service=git-upload-pack`
pub async fn info_refs_upload(
    repo_path: std::path::PathBuf,
    headers: &HeaderMap,
) -> Response<Body> {
    run_git_http_backend(
        &repo_path,
        "GET",
        "/info/refs",
        "service=git-upload-pack",
        headers,
        vec![],
    )
    .await
}

/// Handle `POST /{ns}/{repo}.git/git-upload-pack`
pub async fn post_upload_pack(
    repo_path: std::path::PathBuf,
    headers: &HeaderMap,
    body: Vec<u8>,
) -> Response<Body> {
    run_git_http_backend(&repo_path, "POST", "/git-upload-pack", "", headers, body).await
}

/// Run `git http-backend` as a CGI subprocess and return its response.
///
/// `remote_user` should be set to a non-empty string for authenticated requests
/// (e.g. the tenant ID or username). git http-backend requires `REMOTE_USER` to
/// be set to enable the receive-pack service (push operations).
pub async fn run_git_http_backend(
    repo_path: &Path,
    method: &str,
    path_info_suffix: &str,
    query_string: &str,
    request_headers: &HeaderMap,
    body: Vec<u8>,
) -> Response<Body> {
    run_git_http_backend_inner(
        repo_path,
        method,
        path_info_suffix,
        query_string,
        request_headers,
        body,
        None,
    )
    .await
}

/// Same as `run_git_http_backend` but passes the given `remote_user` as the
/// `REMOTE_USER` CGI environment variable, enabling receive-pack (push).
pub async fn run_git_http_backend_authenticated(
    repo_path: &Path,
    method: &str,
    path_info_suffix: &str,
    query_string: &str,
    request_headers: &HeaderMap,
    body: Vec<u8>,
    remote_user: &str,
) -> Response<Body> {
    run_git_http_backend_inner(
        repo_path,
        method,
        path_info_suffix,
        query_string,
        request_headers,
        body,
        Some(remote_user),
    )
    .await
}

async fn run_git_http_backend_inner(
    repo_path: &Path,
    method: &str,
    path_info_suffix: &str,
    query_string: &str,
    request_headers: &HeaderMap,
    body: Vec<u8>,
    remote_user: Option<&str>,
) -> Response<Body> {
    let project_root = match repo_path.parent() {
        Some(p) => p.to_path_buf(),
        None => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "invalid repo path");
        }
    };

    let repo_dir_name = match repo_path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_string(),
        None => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid repo directory name",
            );
        }
    };

    // Build PATH_INFO: /{repo_dir_name}{path_info_suffix}
    let path_info = format!("/{repo_dir_name}{path_info_suffix}");

    let content_type = request_headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let content_length = body.len().to_string();

    let mut cmd = Command::new("git");
    cmd.arg("http-backend");
    cmd.env("GIT_PROJECT_ROOT", &project_root);
    cmd.env("GIT_HTTP_EXPORT_ALL", "1");
    cmd.env("PATH_INFO", &path_info);
    cmd.env("REQUEST_METHOD", method);
    cmd.env("QUERY_STRING", query_string);
    cmd.env("CONTENT_TYPE", &content_type);
    if !body.is_empty() {
        cmd.env("CONTENT_LENGTH", &content_length);
    }
    // REMOTE_USER must be set for git http-backend to enable the receive-pack
    // (push) service. Without it, git http-backend returns 403 for push operations.
    if let Some(user) = remote_user {
        cmd.env("REMOTE_USER", user);
    }
    // Forward Git-Protocol header for protocol v2 support (shallow clone, etc.).
    // Modern git clients (2.26+) send `Git-Protocol: version=2` which must be
    // passed as HTTP_GIT_PROTOCOL to the CGI subprocess.
    if let Some(proto) = request_headers
        .get("Git-Protocol")
        .and_then(|v| v.to_str().ok())
    {
        cmd.env("HTTP_GIT_PROTOCOL", proto);
    }
    cmd.env("SERVER_PROTOCOL", "HTTP/1.1");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to spawn git http-backend");
            if e.kind() == std::io::ErrorKind::NotFound {
                return error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "git binary not found; please install git on the server",
                );
            }
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to spawn git http-backend: {e}"),
            );
        }
    };

    // Write request body to stdin and always close it (signals EOF to git http-backend).
    // For GET requests the body is empty but stdin must still be closed or the child
    // process will block waiting for input.
    if let Some(mut stdin) = child.stdin.take() {
        if !body.is_empty()
            && let Err(e) = stdin.write_all(&body).await
        {
            tracing::error!(error = %e, "failed to write to git http-backend stdin");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("stdin error: {e}"),
            );
        }
        // Drop stdin to signal EOF — always required even for empty bodies.
        drop(stdin);
    }

    let output =
        match tokio::time::timeout(GIT_HTTP_BACKEND_TIMEOUT, child.wait_with_output()).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                tracing::error!(error = %e, "failed to wait for git http-backend");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("git error: {e}"),
                );
            }
            Err(_) => {
                tracing::error!(
                    "git http-backend timed out after {:?}",
                    GIT_HTTP_BACKEND_TIMEOUT
                );
                // child is consumed by wait_with_output; the OS will reap the process
                return error_response(StatusCode::GATEWAY_TIMEOUT, "git http-backend timed out");
            }
        };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::error!("git http-backend stderr: {}", stderr);
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "git backend error");
    }

    parse_cgi_response(output.stdout)
}

/// Parse a CGI response (headers + body separated by blank line) into an HTTP response.
fn parse_cgi_response(data: Vec<u8>) -> Response<Body> {
    // Find the header/body separator: \r\n\r\n or \n\n
    let separator = b"\r\n\r\n";
    let alt_separator = b"\n\n";

    let (header_bytes, body_bytes) = if let Some(pos) = find_subsequence(&data, separator) {
        (&data[..pos], &data[pos + 4..])
    } else if let Some(pos) = find_subsequence(&data, alt_separator) {
        (&data[..pos], &data[pos + 2..])
    } else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid CGI response: no header/body separator",
        );
    };

    let header_str = match std::str::from_utf8(header_bytes) {
        Ok(s) => s,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid CGI response: non-UTF8 headers",
            );
        }
    };

    let mut builder = Response::builder().status(StatusCode::OK);

    for line in header_str.lines() {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            let value = value.trim();
            if name.eq_ignore_ascii_case("Status") {
                // CGI Status header: "200 OK", "404 Not Found", etc.
                if let Some(code_str) = value.split_whitespace().next()
                    && let Ok(code) = code_str.parse::<u16>()
                    && let Ok(status) = StatusCode::from_u16(code)
                {
                    builder = builder.status(status);
                }
            } else {
                builder = builder.header(name, value);
            }
        }
    }

    builder
        .body(Body::from(body_bytes.to_vec()))
        .unwrap_or_else(|_| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to build response",
            )
        })
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

pub(crate) fn error_response(status: StatusCode, message: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Body::from(message.to_string()))
        .unwrap()
}
