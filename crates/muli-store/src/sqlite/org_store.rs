// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite organization store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::org::Organization;
use muli_core::traits::OrgStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

// ─── OrgStore ─────────────────────────────────────────────────────────────

pub struct SqliteOrgStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteOrgStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl OrgStore for SqliteOrgStore {
    async fn create_org(&self, org: &Organization) -> Result<String> {
        let conn = self.factory.tenant_conn(&org.tenant_id).await?;
        let org = org.clone();
        let id = org.id.clone();
        conn.call(move |c| {
            let json = to_json(&org)?;
            c.execute(
                "INSERT INTO orgs (id, tenant_id, handle, full_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![org.id, org.tenant_id, org.handle, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_org(&self, org_id: &str) -> Result<Option<Organization>> {
        let oid = org_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let o = oid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare("SELECT full_json FROM orgs WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![o])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Organization>(&json)?))
                    } else {
                        Ok(None)
                    }
                })
                .await
                .map_err(store_err)?;
            if result.is_some() {
                return Ok(result);
            }
        }
        Ok(None)
    }

    async fn get_org_by_handle(
        &self,
        tenant_id: &str,
        handle: &str,
    ) -> Result<Option<Organization>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let handle = handle.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM orgs WHERE handle = ?1")?;
            let mut rows = stmt.query(rusqlite::params![handle])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<Organization>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn delete_org(&self, org_id: &str) -> Result<()> {
        let oid = org_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let o = oid.clone();
            let rows = conn
                .call(move |c| {
                    // Cascade: remove all org members before deleting the org
                    c.execute(
                        "DELETE FROM org_members WHERE org_id = ?1",
                        rusqlite::params![o],
                    )?;
                    let rows = c.execute("DELETE FROM orgs WHERE id = ?1", rusqlite::params![o])?;
                    Ok(rows)
                })
                .await
                .map_err(store_err)?;
            if rows > 0 {
                return Ok(());
            }
        }
        Err(MuliError::Storage(format!("org {org_id} not found")))
    }

    async fn list_orgs(&self, tenant_id: &str) -> Result<Vec<Organization>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM orgs")?;
            let rows = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                from_json::<Organization>(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::super::org_member_store::SqliteOrgMemberStore;
    use super::*;
    use muli_core::org::{OrgMember, OrgRole};
    use muli_core::traits::OrgMemberStore;

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    #[tokio::test]
    async fn test_org_store_crud() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteOrgStore::new(factory);
        let org = Organization::new(
            "t1".into(),
            "acme".into(),
            "Acme Corp".into(),
            "desc".into(),
        );
        let id = store.create_org(&org).await.unwrap();
        let by_handle = store
            .get_org_by_handle("t1", "acme")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_handle.id, id);
        let by_id = store.get_org(&id).await.unwrap().unwrap();
        assert_eq!(by_id.handle, "acme");
        let list = store.list_orgs("t1").await.unwrap();
        assert_eq!(list.len(), 1);
        store.delete_org(&id).await.unwrap();
        assert!(store.get_org(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_handle() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteOrgStore::new(factory);
        let o1 = Organization::new("t1".into(), "acme".into(), "Acme".into(), "".into());
        let o2 = Organization::new("t1".into(), "acme".into(), "Acme2".into(), "".into());
        store.create_org(&o1).await.unwrap();
        assert!(store.create_org(&o2).await.is_err());
    }

    #[tokio::test]
    async fn test_org_member_crud() {
        let (factory, _dir) = make_factory().await;
        let org_store = SqliteOrgStore::new(factory.clone());
        let member_store = SqliteOrgMemberStore::new(factory);

        let org = Organization::new("t1".into(), "myorg".into(), "My Org".into(), "".into());
        let org_id = org_store.create_org(&org).await.unwrap();

        let member = OrgMember::new(org_id.clone(), "user-1".into(), OrgRole::Member);
        let _ = member_store.add_member(&member).await.unwrap();

        let list = member_store.list_members(&org_id).await.unwrap();
        assert_eq!(list.len(), 1);

        let fetched = member_store
            .get_member(&org_id, "user-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.role, OrgRole::Member);

        member_store
            .update_member_role(&org_id, "user-1", OrgRole::Admin)
            .await
            .unwrap();
        let updated = member_store
            .get_member(&org_id, "user-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.role, OrgRole::Admin);

        member_store.remove_member(&org_id, "user-1").await.unwrap();
        assert!(
            member_store
                .get_member(&org_id, "user-1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_duplicate_member_rejected() {
        let (factory, _dir) = make_factory().await;
        let org_store = SqliteOrgStore::new(factory.clone());
        let member_store = SqliteOrgMemberStore::new(factory);

        let org = Organization::new("t1".into(), "org2".into(), "Org 2".into(), "".into());
        let org_id = org_store.create_org(&org).await.unwrap();

        let m1 = OrgMember::new(org_id.clone(), "u1".into(), OrgRole::Member);
        let m2 = OrgMember::new(org_id.clone(), "u1".into(), OrgRole::Admin);
        member_store.add_member(&m1).await.unwrap();
        assert!(member_store.add_member(&m2).await.is_err());
    }
}
