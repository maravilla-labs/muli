// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared MongoDB utility functions.

use chrono::Utc;
use mongodb::bson::DateTime as BsonDateTime;

/// Convert a `chrono::DateTime<Utc>` to a BSON `DateTime`.
pub fn chrono_to_bson(dt: chrono::DateTime<Utc>) -> BsonDateTime {
    BsonDateTime::from_millis(dt.timestamp_millis())
}
