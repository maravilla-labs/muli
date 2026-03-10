// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MongoDB-backed organization store.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};

use muli_core::error::{MuliError, Result};
use muli_core::org::{OrgMember, OrgRole, Organization};
use muli_core::traits::{OrgMemberStore, OrgStore};

const ORGS_COLLECTION: &str = "orgs";
const ORG_MEMBERS_COLLECTION: &str = "org_members";

#[derive(Clone)]
pub struct MongoOrgStore {
    collection: Collection<Organization>,
    db: Database,
}

impl MongoOrgStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection(ORGS_COLLECTION),
            db: db.clone(),
        }
    }
}

#[async_trait]
impl OrgStore for MongoOrgStore {
    async fn create_org(&self, org: &Organization) -> Result<String> {
        self.collection
            .insert_one(org)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to create org: {e}")))?;
        Ok(org.id.clone())
    }

    async fn get_org(&self, org_id: &str) -> Result<Option<Organization>> {
        self.collection
            .find_one(doc! { "id": org_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get org: {e}")))
    }

    async fn get_org_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<Organization>> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id, "handle": handle })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get org by handle: {e}")))
    }

    async fn delete_org(&self, org_id: &str) -> Result<()> {
        // Cascade: remove all org members before deleting the org
        self.db
            .collection::<OrgMember>(ORG_MEMBERS_COLLECTION)
            .delete_many(doc! { "org_id": org_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete org members: {e}")))?;
        let result = self
            .collection
            .delete_one(doc! { "id": org_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to delete org: {e}")))?;
        if result.deleted_count == 0 {
            return Err(MuliError::Storage(format!("org {org_id} not found")));
        }
        Ok(())
    }

    async fn list_orgs(&self, tenant_id: &str) -> Result<Vec<Organization>> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list orgs: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect orgs: {e}")))
    }
}

#[derive(Clone)]
pub struct MongoOrgMemberStore {
    collection: Collection<OrgMember>,
}

impl MongoOrgMemberStore {
    pub fn new(db: &Database) -> Self {
        Self {
            collection: db.collection(ORG_MEMBERS_COLLECTION),
        }
    }
}

#[async_trait]
impl OrgMemberStore for MongoOrgMemberStore {
    async fn add_member(&self, member: &OrgMember) -> Result<String> {
        self.collection
            .insert_one(member)
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to add org member: {e}")))?;
        Ok(member.id.clone())
    }

    async fn remove_member(&self, org_id: &str, user_id: &str) -> Result<()> {
        let result = self
            .collection
            .delete_one(doc! { "org_id": org_id, "user_id": user_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to remove org member: {e}")))?;
        if result.deleted_count == 0 {
            return Err(MuliError::Storage(format!(
                "member user_id={user_id} not found in org {org_id}"
            )));
        }
        Ok(())
    }

    async fn list_members(&self, org_id: &str) -> Result<Vec<OrgMember>> {
        let cursor = self
            .collection
            .find(doc! { "org_id": org_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to list org members: {e}")))?;
        cursor
            .try_collect()
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to collect org members: {e}")))
    }

    async fn get_member(&self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>> {
        self.collection
            .find_one(doc! { "org_id": org_id, "user_id": user_id })
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to get org member: {e}")))
    }

    async fn update_member_role(&self, org_id: &str, user_id: &str, role: OrgRole) -> Result<()> {
        use mongodb::bson::to_bson;
        let role_bson = to_bson(&role)
            .map_err(|e| MuliError::Storage(format!("Failed to serialize role: {e}")))?;
        let result = self
            .collection
            .update_one(
                doc! { "org_id": org_id, "user_id": user_id },
                doc! { "$set": { "role": role_bson } },
            )
            .await
            .map_err(|e| MuliError::Storage(format!("Failed to update member role: {e}")))?;
        if result.matched_count == 0 {
            return Err(MuliError::Storage(format!(
                "member user_id={user_id} not found in org {org_id}"
            )));
        }
        Ok(())
    }
}
