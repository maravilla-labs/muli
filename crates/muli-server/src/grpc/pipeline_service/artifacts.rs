// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Artifact, cache, watch, and config RPCs.

use tonic::{Request, Response, Status};
use tracing::info;

use muli_proto::{
    ArtifactChunk, DeleteCacheRequest, DeleteCacheResponse, DownloadArtifactRequest,
    GetPipelineConfigRequest, GetPipelineConfigResponse, ListArtifactsRequest,
    ListArtifactsResponse, ListCachesRequest, ListCachesResponse, PipelineRunEvent,
    WatchPipelineRunRequest,
};

use super::PipelineServiceImpl;
use super::conversions::{artifact_to_proto, cache_to_proto};
use crate::grpc::util::validate_tenant;

impl PipelineServiceImpl {
    pub async fn list_artifacts_impl(
        &self,
        request: Request<ListArtifactsRequest>,
    ) -> Result<Response<ListArtifactsResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.run_id.is_empty() {
            return Err(Status::invalid_argument("run_id is required"));
        }

        let artifacts = self
            .artifact_store
            .list_by_run(&caller_tenant, &req.run_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list artifacts: {e}")))?;

        Ok(Response::new(ListArtifactsResponse {
            artifacts: artifacts.iter().map(artifact_to_proto).collect(),
        }))
    }

    pub async fn download_artifact_impl(
        &self,
        _request: Request<DownloadArtifactRequest>,
    ) -> Result<Response<super::BoxStream<ArtifactChunk>>, Status> {
        Err(Status::unimplemented(
            "DownloadArtifact requires blob storage integration",
        ))
    }

    pub async fn list_caches_impl(
        &self,
        request: Request<ListCachesRequest>,
    ) -> Result<Response<ListCachesResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        let caches = self
            .cache_store
            .list_by_repo(&caller_tenant, &req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to list caches: {e}")))?;

        Ok(Response::new(ListCachesResponse {
            caches: caches.iter().map(cache_to_proto).collect(),
        }))
    }

    pub async fn delete_cache_impl(
        &self,
        request: Request<DeleteCacheRequest>,
    ) -> Result<Response<DeleteCacheResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }
        if req.cache_key.is_empty() {
            return Err(Status::invalid_argument("cache_key is required"));
        }

        self.cache_store
            .delete_cache(&caller_tenant, &req.repo_id, &req.cache_key)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete cache: {e}")))?;

        info!(
            operation = "delete_cache",
            tenant_id = %caller_tenant,
            repo_id = %req.repo_id,
            cache_key = %req.cache_key,
            "audit: pipeline cache deleted"
        );

        Ok(Response::new(DeleteCacheResponse {}))
    }

    pub async fn watch_pipeline_run_impl(
        &self,
        _request: Request<WatchPipelineRunRequest>,
    ) -> Result<Response<super::BoxStream<PipelineRunEvent>>, Status> {
        Err(Status::unimplemented(
            "WatchPipelineRun requires event streaming integration",
        ))
    }

    pub async fn get_pipeline_config_impl(
        &self,
        _request: Request<GetPipelineConfigRequest>,
    ) -> Result<Response<GetPipelineConfigResponse>, Status> {
        Err(Status::unimplemented(
            "GetPipelineConfig requires git repository access",
        ))
    }
}
