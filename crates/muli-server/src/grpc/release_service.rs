// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Release management gRPC service.
//!
//! Access is tenant-scoped: every RPC validates the `x-tenant-id` metadata
//! against the request and checks the release belongs to the caller's tenant +
//! repo. That is the default-safe (private) gate; a public, anonymous-pull
//! download path (mirroring git read) is a follow-up.

use std::sync::Arc;

use chrono::Utc;
use tonic::{Request, Response, Status};

use muli_core::release::{NewRelease, Release as CoreRelease, ReleaseAsset as CoreAsset};
use muli_core::traits::ReleaseStore;

use muli_proto::release_service_server::ReleaseService;
use muli_proto::{
    CreateReleaseRequest, DeleteReleaseAssetRequest, DeleteReleaseAssetResponse,
    DeleteReleaseRequest, DeleteReleaseResponse, DownloadReleaseAssetRequest,
    DownloadReleaseAssetResponse, GetReleaseByTagRequest, GetReleaseRequest,
    ListReleaseAssetsRequest, ListReleaseAssetsResponse, ListReleasesRequest, ListReleasesResponse,
    Release as ProtoRelease, ReleaseAsset as ProtoAsset, ReleaseAssetChunk, UpdateReleaseRequest,
    UploadReleaseAssetRequest,
};

use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio_util::io::ReaderStream;

/// Bytes per streamed frame. Matches the artifact download path.
const ASSET_CHUNK_BYTES: usize = 64 * 1024;

use super::util::{datetime_to_proto, domain_err_to_grpc, validate_tenant};
use crate::release_storage::ReleaseAssetStorage;

pub struct ReleaseServiceImpl {
    pub store: Arc<dyn ReleaseStore>,
    pub asset_storage: Arc<ReleaseAssetStorage>,
}

fn asset_to_proto(a: CoreAsset) -> ProtoAsset {
    ProtoAsset {
        id: a.id,
        release_id: a.release_id,
        name: a.name,
        size: a.size,
        sha256: a.sha256,
        content_type: a.content_type,
        storage_key: a.storage_key,
        created_at: Some(datetime_to_proto(a.created_at)),
        tenant_id: a.tenant_id,
    }
}

fn release_to_proto(r: CoreRelease) -> ProtoRelease {
    ProtoRelease {
        id: r.id,
        tenant_id: r.tenant_id,
        repo_id: r.repo_id,
        tag: r.tag,
        target_commitish: r.target_commitish,
        name: r.name,
        body: r.body,
        draft: r.draft,
        prerelease: r.prerelease,
        created_by: r.created_by,
        created_at: Some(datetime_to_proto(r.created_at)),
        updated_at: Some(datetime_to_proto(r.updated_at)),
        published_at: r.published_at.map(datetime_to_proto),
        assets: r.assets.into_iter().map(asset_to_proto).collect(),
    }
}

impl ReleaseServiceImpl {
    /// Fetch a release and enforce tenant + repo scoping.
    async fn owned_release(
        &self,
        tenant_id: &str,
        repo_id: &str,
        release_id: &str,
    ) -> Result<CoreRelease, Status> {
        self.store
            .get_release(release_id)
            .await
            .map_err(domain_err_to_grpc)?
            .filter(|r| r.tenant_id == tenant_id && r.repo_id == repo_id)
            .ok_or_else(|| Status::not_found("release not found"))
    }
}

#[tonic::async_trait]
impl ReleaseService for ReleaseServiceImpl {
    type DownloadReleaseAssetStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReleaseAssetChunk, Status>> + Send>>;

    async fn create_release(
        &self,
        request: Request<CreateReleaseRequest>,
    ) -> Result<Response<ProtoRelease>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        // `create_tag` is accepted but tag creation via git is a follow-up; the
        // common flow pushes the tag, then records the release against it.
        let rel = CoreRelease::new(NewRelease {
            tenant_id: req.tenant_id,
            repo_id: req.repo_id,
            tag: req.tag,
            target_commitish: req.target_commitish,
            name: req.name,
            body: req.body,
            draft: req.draft,
            prerelease: req.prerelease,
            created_by: req.created_by,
        })
        .map_err(domain_err_to_grpc)?;
        self.store
            .create_release(&rel)
            .await
            .map_err(domain_err_to_grpc)?;
        Ok(Response::new(release_to_proto(rel)))
    }

    async fn get_release(
        &self,
        request: Request<GetReleaseRequest>,
    ) -> Result<Response<ProtoRelease>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let rel = self
            .owned_release(&req.tenant_id, &req.repo_id, &req.release_id)
            .await?;
        Ok(Response::new(release_to_proto(rel)))
    }

    async fn get_release_by_tag(
        &self,
        request: Request<GetReleaseByTagRequest>,
    ) -> Result<Response<ProtoRelease>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let rel = self
            .store
            .get_release_by_tag(&req.repo_id, &req.tag)
            .await
            .map_err(domain_err_to_grpc)?
            .filter(|r| r.tenant_id == req.tenant_id)
            .ok_or_else(|| Status::not_found("release not found"))?;
        Ok(Response::new(release_to_proto(rel)))
    }

    async fn list_releases(
        &self,
        request: Request<ListReleasesRequest>,
    ) -> Result<Response<ListReleasesResponse>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        // Fetch all (filtered) so we can return an accurate `total` with the page.
        let all = self
            .store
            .list_releases(&req.repo_id, req.published_only, 0, 0)
            .await
            .map_err(domain_err_to_grpc)?;
        let all: Vec<CoreRelease> = all
            .into_iter()
            .filter(|r| r.tenant_id == req.tenant_id)
            .collect();
        let total = all.len() as u32;
        let take = if req.limit == 0 {
            usize::MAX
        } else {
            req.limit as usize
        };
        let releases = all
            .into_iter()
            .skip(req.offset as usize)
            .take(take)
            .map(release_to_proto)
            .collect();
        Ok(Response::new(ListReleasesResponse { releases, total }))
    }

    async fn update_release(
        &self,
        request: Request<UpdateReleaseRequest>,
    ) -> Result<Response<ProtoRelease>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let mut rel = self
            .owned_release(&req.tenant_id, &req.repo_id, &req.release_id)
            .await?;
        if let Some(name) = req.name {
            rel.name = name;
        }
        if let Some(body) = req.body {
            rel.body = body;
        }
        if let Some(prerelease) = req.prerelease {
            rel.prerelease = prerelease;
        }
        if let Some(draft) = req.draft {
            // Publishing a previously-draft release stamps `published_at`.
            if rel.draft && !draft && rel.published_at.is_none() {
                rel.published_at = Some(Utc::now());
            }
            rel.draft = draft;
        }
        rel.updated_at = Utc::now();
        self.store
            .update_release(&rel)
            .await
            .map_err(domain_err_to_grpc)?;
        Ok(Response::new(release_to_proto(rel)))
    }

    async fn delete_release(
        &self,
        request: Request<DeleteReleaseRequest>,
    ) -> Result<Response<DeleteReleaseResponse>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let rel = self
            .owned_release(&req.tenant_id, &req.repo_id, &req.release_id)
            .await?;
        for asset in &rel.assets {
            let _ = self
                .asset_storage
                .delete(&req.tenant_id, &rel.id, &asset.id)
                .await;
        }
        self.store
            .delete_release(&rel.id)
            .await
            .map_err(domain_err_to_grpc)?;
        Ok(Response::new(DeleteReleaseResponse {}))
    }

    async fn upload_release_asset(
        &self,
        request: Request<UploadReleaseAssetRequest>,
    ) -> Result<Response<ProtoAsset>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let rel = self
            .owned_release(&req.tenant_id, &req.repo_id, &req.release_id)
            .await?;
        let mut asset = CoreAsset::new(
            req.tenant_id.clone(),
            rel.id.clone(),
            req.name,
            0,
            String::new(),
            req.content_type,
            String::new(),
        );
        let (size, sha256) = self
            .asset_storage
            .upload(&req.tenant_id, &rel.id, &asset.id, &req.data)
            .await
            .map_err(domain_err_to_grpc)?;
        asset.size = size;
        asset.sha256 = sha256;
        asset.storage_key = ReleaseAssetStorage::key(&req.tenant_id, &rel.id, &asset.id);
        self.store
            .add_asset(&asset)
            .await
            .map_err(domain_err_to_grpc)?;
        Ok(Response::new(asset_to_proto(asset)))
    }

    async fn download_release_asset(
        &self,
        request: Request<DownloadReleaseAssetRequest>,
    ) -> Result<Response<DownloadReleaseAssetResponse>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let asset = self
            .store
            .get_asset(&req.asset_id)
            .await
            .map_err(domain_err_to_grpc)?
            .filter(|a| a.tenant_id == req.tenant_id)
            .ok_or_else(|| Status::not_found("asset not found"))?;
        let data = self
            .asset_storage
            .download(&req.tenant_id, &asset.release_id, &asset.id)
            .await
            .map_err(domain_err_to_grpc)?;
        Ok(Response::new(DownloadReleaseAssetResponse {
            data,
            content_type: asset.content_type,
            name: asset.name,
        }))
    }

    /// Stream an asset off disk.
    ///
    /// Unlike the unary form above — and unlike the artifact download path, which
    /// reads the whole file into a `Vec` before chunking it — this never holds more
    /// than one frame in memory: `ReaderStream` pulls from the file handle lazily as
    /// the client consumes. That removes the message-size ceiling entirely rather
    /// than raising it.
    ///
    /// The first frame carries metadata and no data so a proxy can set response
    /// headers before forwarding bytes.
    async fn download_release_asset_stream(
        &self,
        request: Request<DownloadReleaseAssetRequest>,
    ) -> Result<Response<Self::DownloadReleaseAssetStreamStream>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let asset = self
            .store
            .get_asset(&req.asset_id)
            .await
            .map_err(domain_err_to_grpc)?
            .filter(|a| a.tenant_id == req.tenant_id)
            .ok_or_else(|| Status::not_found("asset not found"))?;

        let (file, size) = self
            .asset_storage
            .open(&req.tenant_id, &asset.release_id, &asset.id)
            .await
            .map_err(domain_err_to_grpc)?;

        let head = ReleaseAssetChunk {
            data: Vec::new(),
            content_type: asset.content_type,
            name: asset.name,
            size,
        };

        let body = ReaderStream::with_capacity(file, ASSET_CHUNK_BYTES).map(|frame| {
            frame
                .map(|bytes| ReleaseAssetChunk {
                    data: bytes.to_vec(),
                    ..Default::default()
                })
                .map_err(|e| Status::internal(format!("read asset: {e}")))
        });

        let stream = futures::stream::once(async move { Ok(head) }).chain(body);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_release_assets(
        &self,
        request: Request<ListReleaseAssetsRequest>,
    ) -> Result<Response<ListReleaseAssetsResponse>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        // Ensure the release belongs to the caller before listing its assets.
        let _ = self
            .owned_release(&req.tenant_id, &req.repo_id, &req.release_id)
            .await?;
        let assets = self
            .store
            .list_assets(&req.release_id)
            .await
            .map_err(domain_err_to_grpc)?
            .into_iter()
            .map(asset_to_proto)
            .collect();
        Ok(Response::new(ListReleaseAssetsResponse { assets }))
    }

    async fn delete_release_asset(
        &self,
        request: Request<DeleteReleaseAssetRequest>,
    ) -> Result<Response<DeleteReleaseAssetResponse>, Status> {
        let (_t, req) = validate_tenant(request, |r| &r.tenant_id)?;
        let _ = self
            .owned_release(&req.tenant_id, &req.repo_id, &req.release_id)
            .await?;
        let asset = self
            .store
            .get_asset(&req.asset_id)
            .await
            .map_err(domain_err_to_grpc)?
            .filter(|a| a.tenant_id == req.tenant_id && a.release_id == req.release_id)
            .ok_or_else(|| Status::not_found("asset not found"))?;
        let _ = self
            .asset_storage
            .delete(&req.tenant_id, &asset.release_id, &asset.id)
            .await;
        self.store
            .delete_asset(&asset.id)
            .await
            .map_err(domain_err_to_grpc)?;
        Ok(Response::new(DeleteReleaseAssetResponse {}))
    }
}
