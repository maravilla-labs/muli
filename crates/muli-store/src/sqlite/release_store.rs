// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite release + asset store. Releases are stored with `full_json`; assets
//! live in a separate table and are joined in on read.

use std::sync::Arc;

use async_trait::async_trait;

use muli_core::error::{MuliError, Result};
use muli_core::release::{Release, ReleaseAsset};
use muli_core::traits::ReleaseStore;

use super::factory::SqliteStoreFactory;
use super::util::{from_json, store_err, to_json};

pub struct SqliteReleaseStore {
    factory: Arc<SqliteStoreFactory>,
}

impl SqliteReleaseStore {
    pub fn new(factory: Arc<SqliteStoreFactory>) -> Self {
        Self { factory }
    }

    /// Load a release's assets (same tenant shard), oldest first.
    async fn load_assets(&self, tenant_id: &str, release_id: &str) -> Result<Vec<ReleaseAsset>> {
        let conn = self.factory.tenant_conn(tenant_id).await?;
        let rid = release_id.to_string();
        let mut assets: Vec<ReleaseAsset> = conn
            .call(move |c| {
                let mut stmt =
                    c.prepare("SELECT full_json FROM release_assets WHERE release_id = ?1")?;
                let mut rows = stmt.query(rusqlite::params![rid])?;
                let mut out = Vec::new();
                while let Some(row) = rows.next()? {
                    let json: String = row.get(0)?;
                    out.push(from_json::<ReleaseAsset>(&json)?);
                }
                Ok(out)
            })
            .await
            .map_err(store_err)?;
        assets.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(assets)
    }
}

#[async_trait]
impl ReleaseStore for SqliteReleaseStore {
    async fn create_release(&self, release: &Release) -> Result<String> {
        let conn = self.factory.tenant_conn(&release.tenant_id).await?;
        let mut rel = release.clone();
        rel.assets = Vec::new(); // assets are persisted separately
        let id = rel.id.clone();
        conn.call(move |c| {
            let json = to_json(&rel)?;
            c.execute(
                "INSERT INTO releases (id, tenant_id, repo_id, tag, draft, created_at, full_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    rel.id,
                    rel.tenant_id,
                    rel.repo_id,
                    rel.tag,
                    rel.draft as i64,
                    rel.created_at.to_rfc3339(),
                    json,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn get_release(&self, release_id: &str) -> Result<Option<Release>> {
        let rid = release_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare("SELECT full_json FROM releases WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![r])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Release>(&json)?))
                    } else {
                        Ok(None)
                    }
                })
                .await
                .map_err(store_err)?;
            if let Some(mut rel) = result {
                rel.assets = self.load_assets(&tenant_id, &rel.id).await?;
                return Ok(Some(rel));
            }
        }
        Ok(None)
    }

    async fn get_release_by_tag(&self, repo_id: &str, tag: &str) -> Result<Option<Release>> {
        let rid = repo_id.to_string();
        let t = tag.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let (rr, tt) = (rid.clone(), t.clone());
            let result = conn
                .call(move |c| {
                    let mut stmt = c.prepare(
                        "SELECT full_json FROM releases WHERE repo_id = ?1 AND tag = ?2",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![rr, tt])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<Release>(&json)?))
                    } else {
                        Ok(None)
                    }
                })
                .await
                .map_err(store_err)?;
            if let Some(mut rel) = result {
                rel.assets = self.load_assets(&tenant_id, &rel.id).await?;
                return Ok(Some(rel));
            }
        }
        Ok(None)
    }

    async fn list_releases(
        &self,
        repo_id: &str,
        published_only: bool,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<Release>> {
        let rid = repo_id.to_string();
        let mut all: Vec<(String, Release)> = Vec::new();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let rows: Vec<Release> = conn
                .call(move |c| {
                    let sql = if published_only {
                        "SELECT full_json FROM releases WHERE repo_id = ?1 AND draft = 0 ORDER BY created_at DESC"
                    } else {
                        "SELECT full_json FROM releases WHERE repo_id = ?1 ORDER BY created_at DESC"
                    };
                    let mut stmt = c.prepare(sql)?;
                    let mut rows = stmt.query(rusqlite::params![r])?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        out.push(from_json::<Release>(&json)?);
                    }
                    Ok(out)
                })
                .await
                .map_err(store_err)?;
            for rel in rows {
                all.push((tenant_id.clone(), rel));
            }
        }
        all.sort_by(|a, b| b.1.created_at.cmp(&a.1.created_at));
        let slice: Vec<(String, Release)> = all
            .into_iter()
            .skip(offset as usize)
            .take(if limit == 0 {
                usize::MAX
            } else {
                limit as usize
            })
            .collect();
        let mut out = Vec::with_capacity(slice.len());
        for (tenant_id, mut rel) in slice {
            rel.assets = self.load_assets(&tenant_id, &rel.id).await?;
            out.push(rel);
        }
        Ok(out)
    }

    async fn update_release(&self, release: &Release) -> Result<()> {
        let conn = self.factory.tenant_conn(&release.tenant_id).await?;
        let mut rel = release.clone();
        rel.assets = Vec::new();
        let id_for_err = rel.id.clone();
        let rows = conn
            .call(move |c| {
                let json = to_json(&rel)?;
                let rows = c.execute(
                    "UPDATE releases SET draft = ?1, tag = ?2, full_json = ?3 WHERE id = ?4",
                    rusqlite::params![rel.draft as i64, rel.tag, json, rel.id],
                )?;
                Ok(rows)
            })
            .await
            .map_err(store_err)?;
        if rows == 0 {
            return Err(MuliError::Storage(format!(
                "release {id_for_err} not found"
            )));
        }
        Ok(())
    }

    async fn delete_release(&self, release_id: &str) -> Result<()> {
        let rid = release_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let r = rid.clone();
            let deleted = conn
                .call(move |c| {
                    let n =
                        c.execute("DELETE FROM releases WHERE id = ?1", rusqlite::params![r])?;
                    c.execute(
                        "DELETE FROM release_assets WHERE release_id = ?1",
                        rusqlite::params![r],
                    )?;
                    Ok(n)
                })
                .await
                .map_err(store_err)?;
            if deleted > 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn add_asset(&self, asset: &ReleaseAsset) -> Result<String> {
        let conn = self.factory.tenant_conn(&asset.tenant_id).await?;
        let asset = asset.clone();
        let id = asset.id.clone();
        conn.call(move |c| {
            let json = to_json(&asset)?;
            c.execute(
                "INSERT INTO release_assets (id, tenant_id, release_id, full_json)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![asset.id, asset.tenant_id, asset.release_id, json],
            )?;
            Ok(())
        })
        .await
        .map_err(store_err)?;
        Ok(id)
    }

    async fn list_assets(&self, release_id: &str) -> Result<Vec<ReleaseAsset>> {
        for tenant_id in self.factory.all_tenant_ids().await? {
            let assets = self.load_assets(&tenant_id, release_id).await?;
            if !assets.is_empty() {
                return Ok(assets);
            }
        }
        Ok(Vec::new())
    }

    async fn get_asset(&self, asset_id: &str) -> Result<Option<ReleaseAsset>> {
        let aid = asset_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let a = aid.clone();
            let result = conn
                .call(move |c| {
                    let mut stmt =
                        c.prepare("SELECT full_json FROM release_assets WHERE id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![a])?;
                    if let Some(row) = rows.next()? {
                        let json: String = row.get(0)?;
                        Ok(Some(from_json::<ReleaseAsset>(&json)?))
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

    async fn delete_asset(&self, asset_id: &str) -> Result<()> {
        let aid = asset_id.to_string();
        for tenant_id in self.factory.all_tenant_ids().await? {
            let conn = self.factory.tenant_conn(&tenant_id).await?;
            let a = aid.clone();
            let deleted = conn
                .call(move |c| {
                    Ok(c.execute(
                        "DELETE FROM release_assets WHERE id = ?1",
                        rusqlite::params![a],
                    )?)
                })
                .await
                .map_err(store_err)?;
            if deleted > 0 {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muli_core::release::{NewRelease, ReleaseAsset};

    async fn make_factory() -> (Arc<SqliteStoreFactory>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let factory = SqliteStoreFactory::new(dir.path()).await.unwrap();
        (factory, dir)
    }

    fn new_release(draft: bool) -> Release {
        Release::new(NewRelease {
            tenant_id: "t1".into(),
            repo_id: "repo-1".into(),
            tag: if draft {
                "v0.1.0".into()
            } else {
                "v1.0.0".into()
            },
            target_commitish: "main".into(),
            name: String::new(),
            body: "notes".into(),
            draft,
            prerelease: false,
            created_by: "u1".into(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn test_release_crud_and_assets() {
        let (factory, _dir) = make_factory().await;
        let store = SqliteReleaseStore::new(factory);

        let rel = new_release(false);
        let id = store.create_release(&rel).await.unwrap();

        // get by id + by tag, name defaulted to tag
        let fetched = store.get_release(&id).await.unwrap().unwrap();
        assert_eq!(fetched.tag, "v1.0.0");
        assert_eq!(fetched.name, "v1.0.0");
        assert!(fetched.published_at.is_some());
        let by_tag = store
            .get_release_by_tag("repo-1", "v1.0.0")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_tag.id, id);

        // asset round-trip
        let asset = ReleaseAsset::new(
            "t1".into(),
            id.clone(),
            "app-linux-x64.tar.gz".into(),
            123,
            "deadbeef".into(),
            "application/gzip".into(),
            "releases/app.tar.gz".into(),
        );
        store.add_asset(&asset).await.unwrap();
        let with_assets = store.get_release(&id).await.unwrap().unwrap();
        assert_eq!(with_assets.assets.len(), 1);
        assert_eq!(with_assets.assets[0].name, "app-linux-x64.tar.gz");

        // published_only filtering
        let draft = new_release(true);
        store.create_release(&draft).await.unwrap();
        assert_eq!(
            store
                .list_releases("repo-1", false, 0, 0)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store
                .list_releases("repo-1", true, 0, 0)
                .await
                .unwrap()
                .len(),
            1
        );

        // delete cascades assets
        store.delete_release(&id).await.unwrap();
        assert!(store.get_release(&id).await.unwrap().is_none());
        assert!(store.get_asset(&asset.id).await.unwrap().is_none());
    }
}
