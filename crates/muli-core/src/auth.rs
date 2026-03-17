// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared HTTP authentication token extraction.
//!
//! These functions are used by both muli-git and muli-registry to extract
//! tokens from the `Authorization` header in Bearer, Basic, or raw formats.
//! Scheme matching is case-insensitive per RFC 7235.

use http::HeaderMap;

/// Extract the bearer token from the Authorization header.
/// Per RFC 7235, the auth-scheme is case-insensitive.
pub fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            if v.len() > 7 && v[..7].eq_ignore_ascii_case("Bearer ") {
                Some(&v[7..])
            } else {
                None
            }
        })
        .filter(|t| !t.is_empty())
}

/// Extract a token from Basic auth.
/// Format: `Authorization: Basic base64(username:token)` — the password portion is the token.
/// Per RFC 7235, the auth-scheme is case-insensitive.
pub fn extract_basic_auth_token(headers: &HeaderMap) -> Option<String> {
    use base64::Engine;

    let header = headers.get("authorization").and_then(|v| v.to_str().ok())?;
    let value = if header.len() > 6 && header[..6].eq_ignore_ascii_case("Basic ") {
        &header[6..]
    } else {
        return None;
    };

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    let decoded_str = String::from_utf8(decoded).ok()?;
    let (_username, token) = decoded_str.split_once(':')?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

/// Extract a raw token (no scheme prefix).
/// Some clients (e.g. Cargo) send `Authorization: <token>` without Bearer/Basic prefix.
/// Per RFC 7235, the auth-scheme is case-insensitive.
pub fn extract_raw_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get("authorization").and_then(|v| v.to_str().ok())?;
    if value.len() >= 7 && value[..7].eq_ignore_ascii_case("bearer ") {
        return None;
    }
    if value.len() >= 6 && value[..6].eq_ignore_ascii_case("basic ") {
        return None;
    }
    Some(value.trim())
}

/// Extract a token from any supported auth scheme (Bearer, Basic, or raw).
pub fn extract_any_token(headers: &HeaderMap) -> Option<String> {
    if let Some(token) = extract_bearer_token(headers) {
        return Some(token.to_string());
    }
    if let Some(token) = extract_basic_auth_token(headers) {
        return Some(token);
    }
    if let Some(token) = extract_raw_token(headers) {
        return Some(token.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer my-secret-token".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), Some("my-secret-token"));
    }

    #[test]
    fn bearer_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "bearer my-token".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), Some("my-token"));
    }

    #[test]
    fn bearer_missing_header() {
        let headers = HeaderMap::new();
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn bearer_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn bearer_empty_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer ".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn basic_valid() {
        use base64::Engine;
        let mut headers = HeaderMap::new();
        let encoded = base64::engine::general_purpose::STANDARD.encode("user:mytoken");
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert_eq!(
            extract_basic_auth_token(&headers),
            Some("mytoken".to_string())
        );
    }

    #[test]
    fn basic_case_insensitive() {
        use base64::Engine;
        let mut headers = HeaderMap::new();
        let encoded = base64::engine::general_purpose::STANDARD.encode("user:mytoken");
        headers.insert("authorization", format!("basic {encoded}").parse().unwrap());
        assert_eq!(
            extract_basic_auth_token(&headers),
            Some("mytoken".to_string())
        );
    }

    #[test]
    fn basic_empty_password() {
        use base64::Engine;
        let mut headers = HeaderMap::new();
        let encoded = base64::engine::general_purpose::STANDARD.encode("user:");
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert_eq!(extract_basic_auth_token(&headers), None);
    }

    #[test]
    fn raw_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "some-raw-token".parse().unwrap());
        assert_eq!(extract_raw_token(&headers), Some("some-raw-token"));
    }

    #[test]
    fn raw_rejects_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        assert_eq!(extract_raw_token(&headers), None);
    }

    #[test]
    fn raw_rejects_basic() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        assert_eq!(extract_raw_token(&headers), None);
    }

    #[test]
    fn any_token_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer my-token".parse().unwrap());
        assert_eq!(extract_any_token(&headers), Some("my-token".to_string()));
    }

    #[test]
    fn any_token_basic() {
        use base64::Engine;
        let mut headers = HeaderMap::new();
        let encoded = base64::engine::general_purpose::STANDARD.encode("user:mytoken");
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert_eq!(extract_any_token(&headers), Some("mytoken".to_string()));
    }

    #[test]
    fn any_token_raw() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "raw-token-value".parse().unwrap());
        assert_eq!(
            extract_any_token(&headers),
            Some("raw-token-value".to_string())
        );
    }

    #[test]
    fn any_token_missing() {
        let headers = HeaderMap::new();
        assert_eq!(extract_any_token(&headers), None);
    }
}
