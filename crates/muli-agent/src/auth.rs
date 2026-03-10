// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC client authentication interceptor.

use tonic::service::Interceptor;

/// Client-side gRPC interceptor that injects `Authorization: Bearer <key>`
/// into every outgoing request when an API key is configured.
#[derive(Clone)]
pub struct ClientAuthInterceptor {
    api_key: Option<String>,
}

impl ClientAuthInterceptor {
    pub fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }
}

impl Interceptor for ClientAuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref key) = self.api_key {
            let value = format!("Bearer {key}")
                .parse()
                .map_err(|_| tonic::Status::internal("invalid API key format"))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}
