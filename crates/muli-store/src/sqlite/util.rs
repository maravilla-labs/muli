// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite utility functions and type conversions.

use chrono::{DateTime, Utc};

use muli_core::error::MuliError;

/// Convert a `tokio_rusqlite::Error` into a `MuliError::Storage`.
pub fn store_err(e: tokio_rusqlite::Error) -> MuliError {
    MuliError::Storage(e.to_string())
}

/// Deserialize a JSON string stored in a SQLite `TEXT` column.
pub fn from_json<T: serde::de::DeserializeOwned>(s: &str) -> rusqlite::Result<T> {
    serde_json::from_str(s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}

/// Serialize a value to a JSON string for SQLite `TEXT` storage.
pub fn to_json<T: serde::Serialize>(v: &T) -> rusqlite::Result<String> {
    serde_json::to_string(v).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
}

/// Convert a `chrono::DateTime<Utc>` to milliseconds since epoch for SQLite `INTEGER` storage.
pub fn dt_to_ms(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}
