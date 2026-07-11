// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed per-tenant registry visibility store.

use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::options::UpdateOptions;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::registry::model::RegistryVisibilityLevel;
use muli_core::traits::RegistryVisibilityStore;

const COLLECTION_NAME: &str = "registry_visibility";

/// MongoDB implementation of `RegistryVisibilityStore`. Stores `visibility` as its
/// stable string form so an unknown/corrupt value fails closed to `Private`.
#[derive(Debug, Clone)]
pub struct MongoRegistryVisibilityStore {
    collection: Collection<mongodb::bson::Document>,
}

impl MongoRegistryVisibilityStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection::<mongodb::bson::Document>(COLLECTION_NAME),
        }
    }
}

#[async_trait]
impl RegistryVisibilityStore for MongoRegistryVisibilityStore {
    async fn get_visibility(&self, tenant_id: &str) -> Result<Option<RegistryVisibilityLevel>> {
        let doc = self
            .collection
            .find_one(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get visibility: {e}")))?;
        Ok(doc
            .and_then(|d| d.get_str("visibility").ok().map(|s| s.to_string()))
            .map(|s| RegistryVisibilityLevel::parse_lenient(&s)))
    }

    async fn set_visibility(
        &self,
        tenant_id: &str,
        visibility: RegistryVisibilityLevel,
    ) -> Result<()> {
        let opts = UpdateOptions::builder().upsert(true).build();
        self.collection
            .update_one(
                doc! { "tenant_id": tenant_id },
                doc! {
                    "$set": { "visibility": visibility.as_str() },
                    "$setOnInsert": { "tenant_id": tenant_id },
                },
            )
            .with_options(opts)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to set visibility: {e}")))?;
        Ok(())
    }
}
