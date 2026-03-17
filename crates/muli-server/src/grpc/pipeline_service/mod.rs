// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline CI/CD gRPC service.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use muli_core::traits::{
    ArtifactStore, CacheStore, GitTokenStore, JobLogStore, JobStore, PipelineRunStore,
    PipelineSecretStore, PipelineStore, RepositoryStore, StepRunStore,
};
use muli_pipeline::dag::executor::JobSubmitter;

use muli_proto::pipeline_service_server::PipelineService;
use muli_proto::{
    ArtifactChunk, CancelPipelineRequest, CancelPipelineResponse, DeleteCacheRequest,
    DeleteCacheResponse, DownloadArtifactRequest, GetPipelineConfigRequest,
    GetPipelineConfigResponse, GetPipelineRunRequest, GetStepLogsRequest, GetStepLogsResponse,
    ListArtifactsRequest, ListArtifactsResponse, ListCachesRequest, ListCachesResponse,
    ListPipelineRunsRequest, ListPipelineRunsResponse, LogLine, PipelineRunEvent,
    PipelineRunResponse, RetryPipelineRequest, StreamStepLogsRequest, TriggerPipelineRequest,
    WatchPipelineRunRequest,
};

mod artifacts;
mod conversions;
mod helpers;
mod logs;
mod runs;

pub struct PipelineServiceImpl {
    pub pipeline_store: Arc<dyn PipelineStore>,
    pub run_store: Arc<dyn PipelineRunStore>,
    pub step_store: Arc<dyn StepRunStore>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub cache_store: Arc<dyn CacheStore>,
    pub secret_store: Arc<dyn PipelineSecretStore>,
    pub job_store: Arc<dyn JobStore>,
    pub job_log_store: Arc<dyn JobLogStore>,
    pub job_submitter: Arc<dyn JobSubmitter>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub git_root: PathBuf,
    pub token_store: Arc<dyn GitTokenStore>,
    pub git_base_url: String,
}

pub(crate) type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl PipelineService for PipelineServiceImpl {
    type StreamStepLogsStream = BoxStream<LogLine>;
    type DownloadArtifactStream = BoxStream<ArtifactChunk>;
    type WatchPipelineRunStream = BoxStream<PipelineRunEvent>;

    async fn trigger_pipeline(
        &self,
        request: Request<TriggerPipelineRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        self.trigger_pipeline_impl(request).await
    }

    async fn get_pipeline_run(
        &self,
        request: Request<GetPipelineRunRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        self.get_pipeline_run_impl(request).await
    }

    async fn list_pipeline_runs(
        &self,
        request: Request<ListPipelineRunsRequest>,
    ) -> Result<Response<ListPipelineRunsResponse>, Status> {
        self.list_pipeline_runs_impl(request).await
    }

    async fn cancel_pipeline(
        &self,
        request: Request<CancelPipelineRequest>,
    ) -> Result<Response<CancelPipelineResponse>, Status> {
        self.cancel_pipeline_impl(request).await
    }

    async fn retry_pipeline(
        &self,
        request: Request<RetryPipelineRequest>,
    ) -> Result<Response<PipelineRunResponse>, Status> {
        self.retry_pipeline_impl(request).await
    }

    async fn get_step_logs(
        &self,
        request: Request<GetStepLogsRequest>,
    ) -> Result<Response<GetStepLogsResponse>, Status> {
        self.get_step_logs_impl(request).await
    }

    async fn stream_step_logs(
        &self,
        request: Request<StreamStepLogsRequest>,
    ) -> Result<Response<Self::StreamStepLogsStream>, Status> {
        self.stream_step_logs_impl(request).await
    }

    async fn list_artifacts(
        &self,
        request: Request<ListArtifactsRequest>,
    ) -> Result<Response<ListArtifactsResponse>, Status> {
        self.list_artifacts_impl(request).await
    }

    async fn download_artifact(
        &self,
        request: Request<DownloadArtifactRequest>,
    ) -> Result<Response<Self::DownloadArtifactStream>, Status> {
        self.download_artifact_impl(request).await
    }

    async fn list_caches(
        &self,
        request: Request<ListCachesRequest>,
    ) -> Result<Response<ListCachesResponse>, Status> {
        self.list_caches_impl(request).await
    }

    async fn delete_cache(
        &self,
        request: Request<DeleteCacheRequest>,
    ) -> Result<Response<DeleteCacheResponse>, Status> {
        self.delete_cache_impl(request).await
    }

    async fn watch_pipeline_run(
        &self,
        request: Request<WatchPipelineRunRequest>,
    ) -> Result<Response<Self::WatchPipelineRunStream>, Status> {
        self.watch_pipeline_run_impl(request).await
    }

    async fn get_pipeline_config(
        &self,
        request: Request<GetPipelineConfigRequest>,
    ) -> Result<Response<GetPipelineConfigResponse>, Status> {
        self.get_pipeline_config_impl(request).await
    }
}
