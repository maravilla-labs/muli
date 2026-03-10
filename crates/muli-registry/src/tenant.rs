// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tenant isolation and per-tenant storage routing.

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Configuration for tenant extraction from subdomains.
#[derive(Clone, Debug)]
pub struct TenantConfig {
    /// Base domain for tenant subdomains (e.g., "registry.example.com").
    /// Tenant slug is extracted from `{slug}.{base_domain}`.
    pub base_domain: String,
    /// Fallback tenant when no subdomain is present (e.g., localhost connections).
    pub default_tenant: Option<String>,
}

impl TenantConfig {
    pub fn new(base_domain: impl Into<String>) -> Self {
        Self {
            base_domain: base_domain.into(),
            default_tenant: None,
        }
    }

    pub fn with_default_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.default_tenant = Some(tenant.into());
        self
    }
}

/// Extracted tenant context, injected into request extensions by the tenant middleware.
#[derive(Clone, Debug)]
pub struct TenantContext {
    pub tenant_id: String,
}

/// Validate a tenant slug: alphanumeric + hyphens, max 63 chars, no leading/trailing hyphens.
pub fn validate_tenant_slug(slug: &str) -> bool {
    if slug.is_empty() || slug.len() > 63 {
        return false;
    }
    if slug.starts_with('-') || slug.ends_with('-') {
        return false;
    }
    slug.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// Extract tenant slug from a Host header value by stripping the base domain suffix.
/// Returns `None` if the host doesn't match `{slug}.{base_domain}`.
pub fn extract_tenant_slug<'a>(host: &'a str, base_domain: &str) -> Option<&'a str> {
    // Strip port if present
    let host_no_port = host.split(':').next().unwrap_or(host);

    // Build expected suffix
    let suffix = format!(".{base_domain}");
    if let Some(slug) = host_no_port.strip_suffix(&suffix) {
        // Reject nested subdomains (slug must not contain dots)
        if !slug.is_empty() && !slug.contains('.') {
            return Some(slug);
        }
    }
    None
}

/// Paths that are allowed without tenant context (health-check / ping endpoints).
fn is_tenant_optional_path(path: &str) -> bool {
    path == "/v2/" || path == "/-/ping"
}

/// Middleware that extracts the tenant from the Host header subdomain.
///
/// Requires `TenantConfig` in request extensions (added via `Extension` layer).
/// Injects `TenantContext` into request extensions for downstream handlers.
/// Health-check endpoints are allowed without a tenant.
pub async fn tenant_middleware(request: Request, next: Next) -> Response {
    // Allow health-check endpoints without tenant context
    if is_tenant_optional_path(request.uri().path()) {
        return next.run(request).await;
    }

    let config = match request.extensions().get::<TenantConfig>() {
        Some(c) => c.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR).into_response(),
    };

    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let slug = match extract_tenant_slug(&host, &config.base_domain) {
        Some(s) => s.to_string(),
        None => match &config.default_tenant {
            Some(t) => t.clone(),
            None => {
                return crate::api::oci_error(
                    StatusCode::BAD_REQUEST,
                    "TENANT_INVALID",
                    "missing or invalid tenant subdomain",
                );
            }
        },
    };

    if !validate_tenant_slug(&slug) {
        return crate::api::oci_error(
            StatusCode::BAD_REQUEST,
            "TENANT_INVALID",
            "invalid tenant slug",
        );
    }

    let mut request = request;
    request
        .extensions_mut()
        .insert(TenantContext { tenant_id: slug });

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- extract_tenant_slug ---

    #[test]
    fn extract_valid_tenant_slugs() {
        assert_eq!(
            extract_tenant_slug("tenant-123.registry.example.com", "registry.example.com"),
            Some("tenant-123")
        );
        assert_eq!(
            extract_tenant_slug("myorg.registry.example.com", "registry.example.com"),
            Some("myorg")
        );
        assert_eq!(
            extract_tenant_slug("abc.registry.example.com:5000", "registry.example.com"),
            Some("abc")
        );
        assert_eq!(extract_tenant_slug("a.localhost", "localhost"), Some("a"));
    }

    #[test]
    fn extract_no_subdomain() {
        assert_eq!(
            extract_tenant_slug("registry.example.com", "registry.example.com"),
            None
        );
    }

    #[test]
    fn extract_wrong_domain() {
        assert_eq!(
            extract_tenant_slug("tenant.other.com", "registry.example.com"),
            None
        );
    }

    #[test]
    fn extract_nested_subdomain_rejected() {
        assert_eq!(
            extract_tenant_slug("a.b.registry.example.com", "registry.example.com"),
            None
        );
    }

    #[test]
    fn extract_empty_host() {
        assert_eq!(extract_tenant_slug("", "registry.example.com"), None);
    }

    // --- validate_tenant_slug ---

    #[test]
    fn validate_valid_slugs() {
        assert!(validate_tenant_slug("myorg"));
        assert!(validate_tenant_slug("tenant-123"));
        assert!(validate_tenant_slug("a"));
        assert!(validate_tenant_slug("a-b-c"));
        assert!(validate_tenant_slug("abc123"));
        assert!(validate_tenant_slug("ABC")); // uppercase allowed
        assert!(validate_tenant_slug("My-Org"));
        assert!(validate_tenant_slug(&"a".repeat(63)));
    }

    #[test]
    fn validate_empty_slug() {
        assert!(!validate_tenant_slug(""));
    }

    #[test]
    fn validate_leading_hyphen() {
        assert!(!validate_tenant_slug("-leading"));
    }

    #[test]
    fn validate_trailing_hyphen() {
        assert!(!validate_tenant_slug("trailing-"));
    }

    #[test]
    fn validate_both_hyphens() {
        assert!(!validate_tenant_slug("-both-"));
    }

    #[test]
    fn validate_too_long() {
        assert!(!validate_tenant_slug(&"a".repeat(64)));
    }

    #[test]
    fn validate_invalid_chars() {
        assert!(!validate_tenant_slug("has space"));
        assert!(!validate_tenant_slug("has.dot"));
        assert!(!validate_tenant_slug("has_underscore"));
        assert!(!validate_tenant_slug("has/slash"));
    }
}
