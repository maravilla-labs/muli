// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared token validation utilities used by registry and git auth.

use chrono::{DateTime, Utc};

/// Check if a token is currently valid (not revoked, not expired).
pub fn is_token_valid(revoked: bool, expires_at: Option<DateTime<Utc>>) -> bool {
    if revoked {
        return false;
    }
    if let Some(expires_at) = expires_at {
        return Utc::now() < expires_at;
    }
    true
}

/// Check if a list of permissions contains the required permission.
pub fn has_permission<P: PartialEq>(permissions: &[P], required: &P) -> bool {
    permissions.contains(required)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn valid_token() {
        assert!(is_token_valid(false, None));
    }

    #[test]
    fn revoked_token() {
        assert!(!is_token_valid(true, None));
    }

    #[test]
    fn expired_token() {
        assert!(!is_token_valid(
            false,
            Some(Utc::now() - Duration::hours(1))
        ));
    }

    #[test]
    fn not_yet_expired_token() {
        assert!(is_token_valid(false, Some(Utc::now() + Duration::hours(1))));
    }

    #[test]
    fn permission_found() {
        assert!(has_permission(&[1, 2, 3], &2));
    }

    #[test]
    fn permission_missing() {
        assert!(!has_permission(&[1, 2, 3], &4));
    }
}
