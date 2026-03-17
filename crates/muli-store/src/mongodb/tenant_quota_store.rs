// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed tenant quota store.

use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::options::UpdateOptions;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::registry::model::TenantQuota;
use muli_core::traits::TenantQuotaStore;

const COLLECTION_NAME: &str = "tenant_quotas";

/// MongoDB implementation of TenantQuotaStore.
#[derive(Debug, Clone)]
pub struct MongoTenantQuotaStore {
    collection: Collection<TenantQuota>,
}

impl MongoTenantQuotaStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection::<TenantQuota>(COLLECTION_NAME),
        }
    }
}

#[async_trait]
impl TenantQuotaStore for MongoTenantQuotaStore {
    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get quota: {e}")))
    }

    async fn set_quota(&self, tenant_id: &str, max_storage_bytes: u64) -> Result<()> {
        let opts = UpdateOptions::builder().upsert(true).build();
        self.collection
            .update_one(
                doc! { "tenant_id": tenant_id },
                doc! {
                    "$set": { "max_storage_bytes": max_storage_bytes as i64 },
                    "$setOnInsert": {
                        "tenant_id": tenant_id,
                        "current_usage_bytes": 0_i64,
                    }
                },
            )
            .with_options(opts)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to set quota: {e}")))?;
        Ok(())
    }

    async fn update_usage(&self, tenant_id: &str, current_usage_bytes: u64) -> Result<()> {
        self.collection
            .update_one(
                doc! { "tenant_id": tenant_id },
                doc! {
                    "$set": { "current_usage_bytes": current_usage_bytes as i64 },
                    "$setOnInsert": { "tenant_id": tenant_id, "max_storage_bytes": 0_i64 }
                },
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update usage: {e}")))?;
        Ok(())
    }

    async fn adjust_usage(&self, tenant_id: &str, delta: i64) -> Result<()> {
        // Aggregation pipeline update: atomically increment with floor at 0.
        // $ifNull handles upsert case where fields don't exist yet.
        self.collection
            .update_one(
                doc! { "tenant_id": tenant_id },
                vec![doc! {
                    "$set": {
                        "tenant_id": tenant_id,
                        "max_storage_bytes": { "$ifNull": ["$max_storage_bytes", 0_i64] },
                        "current_usage_bytes": {
                            "$max": [0_i64, { "$add": [{ "$ifNull": ["$current_usage_bytes", 0_i64] }, delta] }]
                        }
                    }
                }],
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to adjust usage: {e}")))?;
        Ok(())
    }

    async fn try_reserve(&self, tenant_id: &str, additional_bytes: u64) -> Result<bool> {
        let additional = additional_bytes as i64;
        // Conditional update: increment only if quota allows (max_storage_bytes=0 means unlimited).
        let result = self
            .collection
            .update_one(
                doc! {
                    "tenant_id": tenant_id,
                    "$or": [
                        { "max_storage_bytes": 0_i64 },
                        { "$expr": { "$lte": [
                            { "$add": ["$current_usage_bytes", additional] },
                            "$max_storage_bytes"
                        ] } }
                    ]
                },
                doc! { "$inc": { "current_usage_bytes": additional } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to try_reserve: {e}")))?;

        if result.modified_count > 0 {
            return Ok(true);
        }

        // No match: either the row doesn't exist (no quota → allow) or quota exceeded.
        let exists = self
            .collection
            .find_one(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to check quota existence: {e}")))?;

        Ok(exists.is_none()) // No quota configured → unlimited
    }
}
