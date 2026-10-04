// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CORS for the anonymous Luat read endpoints.
//!
//! The package browser is a static site on another origin that calls the
//! index, download, package/version info and search endpoints from the
//! browser. Those get `Access-Control-Allow-Origin: *` (no credentials) and a
//! preflight answer. Publish, yank, unyank and `me` get no CORS headers: a
//! preflight for them only ever allows GET/HEAD, so a browser can never send
//! a cross-origin write.

use axum::{
    extract::Request,
    http::{HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Paths whose GET/HEAD are public reads.
pub fn is_read_path(path: &str) -> bool {
    path.starts_with("/index/@")
        || path == "/api/v1/packages"
        || path.starts_with("/api/v1/packages/@")
}

fn add_read_headers(resp: &mut Response) {
    let h = resp.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("ETag"),
    );
}

/// Outermost middleware (runs before tenant and auth, so preflights need
/// neither a tenant nor a token).
pub async fn cors_middleware(request: Request, next: Next) -> Response {
    if !is_read_path(request.uri().path()) {
        return next.run(request).await;
    }
    match *request.method() {
        Method::OPTIONS => {
            let mut resp = StatusCode::NO_CONTENT.into_response();
            add_read_headers(&mut resp);
            let h = resp.headers_mut();
            h.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, HEAD, OPTIONS"),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("Accept, If-None-Match"),
            );
            h.insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static("86400"),
            );
            resp
        }
        Method::GET | Method::HEAD => {
            let mut resp = next.run(request).await;
            add_read_headers(&mut resp);
            resp
        }
        _ => next.run(request).await,
    }
}
