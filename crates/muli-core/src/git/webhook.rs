// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Webhook event types and delivery models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// A webhook configuration for a repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    pub tenant_id: String,
    pub repo_id: String,
    pub url: String,
    /// HMAC secret for payload signing.
    ///
    /// Note: this is currently persisted as plaintext in backing stores.
    /// See project security documentation for operational guidance.
    pub secret: String,
    pub events: Vec<WebhookEvent>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

impl Webhook {
    pub fn new(
        tenant_id: String,
        repo_id: String,
        url: String,
        secret: String,
        events: Vec<WebhookEvent>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id,
            repo_id,
            url,
            secret,
            events,
            active: true,
            created_at: Utc::now(),
        }
    }
}

/// Events that can trigger a webhook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    Push,
    Create,
    Delete,
    PrOpened,
    PrMerged,
    PrClosed,
}

/// Maximum allowed webhook URL length.
pub const MAX_WEBHOOK_URL_LEN: usize = 2048;

/// Validate webhook URL format:
/// - must be HTTP/HTTPS
/// - must include a host
/// - blocks localhost and private/link-local/loopback IP literals
/// - max length: 2048
pub fn validate_webhook_url(url: &str) -> Result<(), &'static str> {
    if url.len() > MAX_WEBHOOK_URL_LEN {
        return Err("URL exceeds maximum length of 2048 characters");
    }

    let parsed = url::Url::parse(url).map_err(|_| "invalid URL")?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => return Err("only http and https schemes are allowed"),
    }

    let host = parsed.host().ok_or("URL must have a host")?;
    match host {
        url::Host::Domain(domain) => {
            if domain == "localhost"
                || domain == "localhost.localdomain"
                || domain.ends_with(".localhost")
            {
                return Err("localhost URLs are not allowed");
            }
        }
        url::Host::Ipv4(v4) => {
            if is_private_ip(&IpAddr::V4(v4)) {
                return Err("private IP addresses are not allowed");
            }
        }
        url::Host::Ipv6(v6) => {
            if is_private_ip(&IpAddr::V6(v6)) {
                return Err("private IP addresses are not allowed");
            }
        }
    }

    Ok(())
}

/// Resolve webhook target host and reject private/link-local/loopback targets.
///
/// This defends against DNS entries that point to internal networks.
pub async fn validate_webhook_target(url: &str) -> Result<(), String> {
    validate_webhook_url(url).map_err(std::string::ToString::to_string)?;

    let parsed = url::Url::parse(url).map_err(|_| "invalid URL".to_string())?;
    let host = parsed
        .host()
        .ok_or_else(|| "URL must have a host".to_string())?;

    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "URL must include a known port".to_string())?;

    match host {
        url::Host::Ipv4(v4) => {
            if is_private_ip(&IpAddr::V4(v4)) {
                return Err("private IP addresses are not allowed".to_string());
            }
        }
        url::Host::Ipv6(v6) => {
            if is_private_ip(&IpAddr::V6(v6)) {
                return Err("private IP addresses are not allowed".to_string());
            }
        }
        url::Host::Domain(domain) => {
            let addrs = tokio::net::lookup_host((domain, port))
                .await
                .map_err(|_| "failed to resolve webhook host".to_string())?;

            let mut resolved_any = false;
            for addr in addrs {
                resolved_any = true;
                if is_private_ip(&addr.ip()) {
                    return Err("webhook host resolved to a private IP address".to_string());
                }
            }

            if !resolved_any {
                return Err("failed to resolve webhook host".to_string());
            }
        }
    }

    Ok(())
}

/// Return true when an IP is local, private, unspecified, or link-local.
pub fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.octets()[0] == 10
                || (v4.octets()[0] == 172 && (v4.octets()[1] & 0xf0) == 16)
                || (v4.octets()[0] == 192 && v4.octets()[1] == 168)
                || v4.is_unspecified()
                || v4.is_link_local()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| is_private_ip(&IpAddr::V4(v4)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_webhook_target, validate_webhook_url};

    #[test]
    fn webhook_url_valid() {
        assert!(validate_webhook_url("https://example.com/hooks").is_ok());
        assert!(validate_webhook_url("http://example.com/hooks").is_ok());
    }

    #[test]
    fn webhook_url_rejects_non_http() {
        assert!(validate_webhook_url("ftp://example.com").is_err());
        assert!(validate_webhook_url("file:///etc/passwd").is_err());
    }

    #[test]
    fn webhook_url_rejects_localhost() {
        assert!(validate_webhook_url("http://localhost/hook").is_err());
        assert!(validate_webhook_url("http://localhost:8080/hook").is_err());
    }

    #[test]
    fn webhook_url_rejects_private_ips() {
        assert!(validate_webhook_url("http://127.0.0.1/hook").is_err());
        assert!(validate_webhook_url("http://10.0.0.1/hook").is_err());
        assert!(validate_webhook_url("http://172.16.0.1/hook").is_err());
        assert!(validate_webhook_url("http://192.168.1.1/hook").is_err());
        assert!(validate_webhook_url("http://[::1]/hook").is_err());
    }

    #[tokio::test]
    async fn webhook_target_rejects_private_ip() {
        assert!(
            validate_webhook_target("http://127.0.0.1/hook")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn webhook_target_accepts_public_ip() {
        assert!(validate_webhook_target("http://8.8.8.8/hook").await.is_ok());
    }
}
