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
        request: Request<DownloadArtifactRequest>,
    ) -> Result<Response<super::BoxStream<ArtifactChunk>>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.artifact_id.is_empty() {
            return Err(Status::invalid_argument("artifact_id is required"));
        }

        // Look up the artifact record to get run_id and name
        let artifact = self
            .artifact_store
            .get_artifact(&caller_tenant, &req.artifact_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get artifact: {e}")))?
            .ok_or_else(|| Status::not_found("Artifact not found"))?;

        let data = self
            .artifact_storage
            .download(&caller_tenant, &artifact.run_id, &artifact.name)
            .await
            .map_err(|e| Status::not_found(format!("Artifact data not found: {e}")))?;

        // Stream in 64KB chunks
        const CHUNK_SIZE: usize = 64 * 1024;
        let chunks: Vec<Result<ArtifactChunk, Status>> = data
            .chunks(CHUNK_SIZE)
            .map(|chunk| {
                Ok(ArtifactChunk {
                    data: chunk.to_vec(),
                })
            })
            .collect();

        info!(
            operation = "download_artifact",
            tenant_id = %caller_tenant,
            run_id = %artifact.run_id,
            artifact_id = %req.artifact_id,
            artifact_name = %artifact.name,
            "audit: artifact downloaded"
        );

        let stream = tokio_stream::iter(chunks);
        Ok(Response::new(Box::pin(stream)))
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
        request: Request<GetPipelineConfigRequest>,
    ) -> Result<Response<GetPipelineConfigResponse>, Status> {
        let (caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.repo_id.is_empty() {
            return Err(Status::invalid_argument("repo_id is required"));
        }

        // Look up repo to get namespace + name for the filesystem path
        let repo = self
            .repo_store
            .get_repository(&req.repo_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get repository: {e}")))?
            .ok_or_else(|| Status::not_found("repository not found"))?;

        if repo.tenant_id != caller_tenant {
            return Err(Status::not_found("repository not found"));
        }

        let repo_path = self
            .git_root
            .join(&caller_tenant)
            .join(&repo.namespace)
            .join(format!("{}.git", &repo.name));

        let commit_ref = req.commit_sha.clone();

        let yaml_content = tokio::task::spawn_blocking({
            let repo_path = repo_path.clone();
            move || -> Result<Option<String>, Status> {
                let git_repo = git2::Repository::open(&repo_path)
                    .map_err(|e| Status::internal(format!("Cannot open git repository: {e}")))?;

                // Resolve the ref; fall back to HEAD when commit_sha was not provided.
                let resolved = if commit_ref.is_empty() {
                    git_repo
                        .head()
                        .and_then(|h| h.peel_to_commit())
                        .map(|c| c.id().to_string())
                        .map_err(|e| Status::not_found(format!("Repository has no commits: {e}")))?
                } else {
                    commit_ref.clone()
                };

                let obj = git_repo
                    .revparse_single(&resolved)
                    .map_err(|e| Status::not_found(format!("Cannot resolve ref '{resolved}': {e}")))?;

                let commit = obj
                    .peel_to_commit()
                    .map_err(|e| Status::internal(format!("Cannot peel to commit: {e}")))?;

                let tree = commit
                    .tree()
                    .map_err(|e| Status::internal(format!("Cannot read tree: {e}")))?;

                let entry = match tree.get_path(std::path::Path::new(".maravilla/pipeline.yml")) {
                    Ok(e) => e,
                    Err(_) => return Ok(None),
                };

                let blob = git_repo
                    .find_blob(entry.id())
                    .map_err(|e| Status::internal(format!("Cannot read blob: {e}")))?;

                let content = std::str::from_utf8(blob.content())
                    .map_err(|e| Status::internal(format!("pipeline.yml is not valid UTF-8: {e}")))?;

                Ok(Some(content.to_string()))
            }
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking failed: {e}")))??;

        match yaml_content {
            Some(content) => Ok(Response::new(GetPipelineConfigResponse {
                yaml_content: content,
                pipeline_detected: true,
            })),
            None => Ok(Response::new(GetPipelineConfigResponse {
                yaml_content: String::new(),
                pipeline_detected: false,
            })),
        }
    }
}
