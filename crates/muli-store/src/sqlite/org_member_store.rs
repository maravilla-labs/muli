// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite organization member store.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::org::{OrgMember, OrgRole};
use muli_core::traits::OrgMemberStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteOrgMemberStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteOrgMemberStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }

    /// Find which tenant DB contains the given org_id.
    async fn find_tenant_for_org(
        &self,
        org_id: &str,
    ) -> Result<Option<Arc<tokio_rusqlite::Connection>>> {
        let oid = org_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let o = oid.clone();
            let exists: bool = conn
                .call(move |c| {
                    let count: i64 = c.query_row(
                        "SELECT COUNT(*) FROM orgs WHERE id = ?1",
                        rusqlite::params![o],
                        |row| row.get(0),
                    )?;
                    Ok(count > 0)
                })
                .await
                .map_err(store_err)?;
            if exists {
                return Ok(Some(conn));
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl OrgMemberStore for SqliteOrgMemberStore {
    async fn add_member(&self, member: &OrgMember) -> Result<String> {
        let conn = match self.find_tenant_for_org(&member.org_id).await? {
            Some(c) => c,
            None => {
                return Err(MuliError::Storage(format!(
                    "org {} not found",
                    member.org_id
                )));
            }
        };
        let member = member.clone();
        let id = member.id.clone();
        conn.call(move |c| {
            let json = to_json(&member)?;
            c.execute(
                "INSERT INTO org_members (id, org_id, user_id, full_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![member.id, member.org_id, member.user_id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn remove_member(&self, org_id: &str, user_id: &str) -> Result<()> {
        let conn = match self.find_tenant_for_org(org_id).await? {
            Some(c) => c,
            None => return Err(MuliError::Storage(format!("org {org_id} not found"))),
        };
        let oid = org_id.to_string();
        let uid = user_id.to_string();
        let rows = conn
            .call(move |c| {
                let rows = c.execute(
                    "DELETE FROM org_members WHERE org_id = ?1 AND user_id = ?2",
                    rusqlite::params![oid, uid],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "member user_id={user_id} not found in org {org_id}"
            )));
        }
        Ok(())
    }

    async fn list_members(&self, org_id: &str) -> Result<Vec<OrgMember>> {
        let conn = match self.find_tenant_for_org(org_id).await? {
            Some(c) => c,
            None => return Ok(vec![]),
        };
        let oid = org_id.to_string();
        conn.call(move |c| {
            let mut stmt = c.prepare("SELECT full_json FROM org_members WHERE org_id = ?1")?;
            let rows = stmt.query_map(rusqlite::params![oid], |row| {
                let json: String = row.get(0)?;
                from_json::<OrgMember>(&json)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(store_err)
    }

    async fn get_member(&self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>> {
        let conn = match self.find_tenant_for_org(org_id).await? {
            Some(c) => c,
            None => return Ok(None),
        };
        let oid = org_id.to_string();
        let uid = user_id.to_string();
        conn.call(move |c| {
            let mut stmt =
                c.prepare("SELECT full_json FROM org_members WHERE org_id = ?1 AND user_id = ?2")?;
            let mut rows = stmt.query(rusqlite::params![oid, uid])?;
            if let Some(row) = rows.next()? {
                let json: String = row.get(0)?;
                Ok(Some(from_json::<OrgMember>(&json)?))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(store_err)
    }

    async fn update_member_role(&self, org_id: &str, user_id: &str, role: OrgRole) -> Result<()> {
        let conn = match self.find_tenant_for_org(org_id).await? {
            Some(c) => c,
            None => return Err(MuliError::Storage(format!("org {org_id} not found"))),
        };
        let oid = org_id.to_string();
        let uid = user_id.to_string();
        let rows = conn
            .call(move |c| {
                // Read-modify-write
                let existing: Option<String> = {
                    let mut stmt = c.prepare(
                        "SELECT full_json FROM org_members WHERE org_id = ?1 AND user_id = ?2",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![oid, uid])?;
                    rows.next()?
                        .map(|row| row.get::<_, String>(0))
                        .transpose()?
                };
                let Some(json) = existing else {
                    return Ok(0usize);
                };
                let mut member: OrgMember = from_json(&json)?;
                member.role = role;
                let new_json = to_json(&member)?;
                let rows = c.execute(
                    "UPDATE org_members SET full_json = ?1 WHERE org_id = ?2 AND user_id = ?3",
                    rusqlite::params![new_json, member.org_id, member.user_id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "member user_id={user_id} not found in org {org_id}"
            )));
        }
        Ok(())
    }
}
