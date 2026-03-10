// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server-side gRPC authentication interceptor.

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;

/// Server-side gRPC authentication interceptor.
///
/// When an API key is configured (`MULI_API_KEY`), every request must include
/// an `authorization` metadata header with value `Bearer <key>`.
/// If no API key is configured, all requests are allowed (dev mode).
///
/// When `default_tenant_id` is set and the incoming request has no `x-tenant-id`
/// header, the interceptor injects the default tenant ID automatically.
#[derive(Clone)]
pub struct AuthInterceptor {
    api_key: Option<String>,
    default_tenant_id: Option<String>,
}

/// Compare two strings in constant time by hashing both with SHA-256 and
/// comparing the fixed-length digests. This prevents timing side-channel attacks
/// that could leak information about the expected key length or content.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let ha = Sha256::digest(a.as_bytes());
    let hb = Sha256::digest(b.as_bytes());
    ha.ct_eq(&hb).into()
}

impl AuthInterceptor {
    pub fn new(api_key: Option<String>, default_tenant_id: Option<String>) -> Self {
        Self {
            api_key,
            default_tenant_id,
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        // Authentication check
        if let Some(ref expected_key) = self.api_key {
            let token = request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));

            match token {
                Some(t) if constant_time_eq(t, expected_key) => {}
                Some(_) => {
                    tracing::warn!("gRPC auth failure: invalid API key");
                    return Err(tonic::Status::unauthenticated("invalid API key"));
                }
                None => {
                    tracing::warn!("gRPC auth failure: missing authorization header");
                    return Err(tonic::Status::unauthenticated(
                        "missing authorization header",
                    ));
                }
            }
        }

        // Inject default tenant-id when header is absent
        if let Some(ref dt) = self.default_tenant_id
            && request.metadata().get("x-tenant-id").is_none()
            && let Ok(val) = MetadataValue::try_from(dt.as_str())
        {
            request.metadata_mut().insert("x-tenant-id", val);
        }

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(auth_header: Option<&str>) -> tonic::Request<()> {
        let mut req = tonic::Request::new(());
        if let Some(val) = auth_header {
            req.metadata_mut()
                .insert("authorization", MetadataValue::try_from(val).unwrap());
        }
        req
    }

    #[test]
    fn no_api_key_allows_all() {
        let mut interceptor = AuthInterceptor::new(None, None);
        assert!(interceptor.call(make_request(None)).is_ok());
        assert!(
            interceptor
                .call(make_request(Some("Bearer anything")))
                .is_ok()
        );
    }

    #[test]
    fn valid_bearer_token() {
        let mut interceptor = AuthInterceptor::new(Some("secret-key".to_string()), None);
        assert!(
            interceptor
                .call(make_request(Some("Bearer secret-key")))
                .is_ok()
        );
    }

    #[test]
    fn missing_header_rejected() {
        let mut interceptor = AuthInterceptor::new(Some("secret-key".to_string()), None);
        let result = interceptor.call(make_request(None));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn wrong_key_rejected() {
        let mut interceptor = AuthInterceptor::new(Some("secret-key".to_string()), None);
        let result = interceptor.call(make_request(Some("Bearer wrong-key")));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_bearer_prefix_rejected() {
        let mut interceptor = AuthInterceptor::new(Some("secret-key".to_string()), None);
        let result = interceptor.call(make_request(Some("secret-key")));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn default_tenant_injected_when_missing() {
        let mut interceptor = AuthInterceptor::new(None, Some("local".to_string()));
        let result = interceptor.call(make_request(None)).unwrap();
        let val = result
            .metadata()
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok());
        assert_eq!(val, Some("local"));
    }

    #[test]
    fn default_tenant_not_overridden_when_present() {
        let mut interceptor = AuthInterceptor::new(None, Some("local".to_string()));
        let mut req = make_request(None);
        req.metadata_mut()
            .insert("x-tenant-id", MetadataValue::try_from("acme").unwrap());
        let result = interceptor.call(req).unwrap();
        let val = result
            .metadata()
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok());
        assert_eq!(val, Some("acme"));
    }
}
