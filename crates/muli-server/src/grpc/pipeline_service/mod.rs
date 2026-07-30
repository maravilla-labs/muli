// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pipeline CI/CD gRPC service.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use dashmap::DashMap;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use muli_core::traits::{
    ArtifactStore, CacheStore, GitTokenStore, JobLogStore, JobStore, OrgSecretStore,
    PipelineRunStore, PipelineSecretStore, PipelineStore, RepositoryStore, StepRunStore,
};
use muli_engine::docker::logs::LogCollector;
use muli_pipeline::dag::executor::JobSubmitter;

use muli_proto::pipeline_service_server::PipelineService;
use muli_proto::{
    ArtifactChunk, CancelPipelineRequest, CancelPipelineResponse, DeleteCacheRequest,
    DeleteCacheResponse, DeleteOrgSecretRequest, DeleteOrgSecretResponse, DeleteSecretRequest,
    DeleteSecretResponse, DownloadArtifactRequest, GetPipelineConfigRequest,
    GetPipelineConfigResponse, GetPipelineRunRequest, GetStepLogsRequest, GetStepLogsResponse,
    ListArtifactsRequest, ListArtifactsResponse, ListCachesRequest, ListCachesResponse,
    ListOrgSecretsRequest, ListOrgSecretsResponse, ListPipelineRunsRequest,
    ListPipelineRunsResponse, ListSecretsRequest, ListSecretsResponse, LogLine, PipelineRunEvent,
    PipelineRunResponse, RetryPipelineRequest, SetOrgSecretRequest, SetOrgSecretResponse,
    SetSecretRequest, SetSecretResponse, StreamStepLogsRequest, TriggerPipelineRequest,
    WatchPipelineRunRequest,
};

mod artifacts;
mod conversions;
mod helpers;
mod logs;
mod runs;
mod secrets;

pub struct PipelineServiceImpl {
    pub pipeline_store: Arc<dyn PipelineStore>,
    pub run_store: Arc<dyn PipelineRunStore>,
    pub step_store: Arc<dyn StepRunStore>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub artifact_storage: Arc<muli_pipeline::artifact::storage::ArtifactStorage>,
    pub cache_store: Arc<dyn CacheStore>,
    pub secret_store: Arc<dyn PipelineSecretStore>,
    pub job_store: Arc<dyn JobStore>,
    pub job_log_store: Arc<dyn JobLogStore>,
    pub log_collectors: Arc<DashMap<String, Arc<LogCollector>>>,
    pub max_log_lines: usize,
    pub job_submitter: Arc<dyn JobSubmitter>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub git_root: PathBuf,
    pub token_store: Arc<dyn GitTokenStore>,
    pub git_base_url: String,
    pub org_secret_store: Arc<dyn OrgSecretStore>,
    /// Shared with the git push hooks. A manual trigger runs the exact same
    /// phases as a push-triggered one; `None` when pipelines or git are disabled.
    pub pipeline_trigger: Option<Arc<crate::pipeline_trigger::PipelineTriggerImpl>>,
    pub encryption_key: Option<[u8; 32]>,
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

    async fn set_secret(
        &self,
        request: Request<SetSecretRequest>,
    ) -> Result<Response<SetSecretResponse>, Status> {
        self.set_secret_impl(request).await
    }

    async fn delete_secret(
        &self,
        request: Request<DeleteSecretRequest>,
    ) -> Result<Response<DeleteSecretResponse>, Status> {
        self.delete_secret_impl(request).await
    }

    async fn list_secrets(
        &self,
        request: Request<ListSecretsRequest>,
    ) -> Result<Response<ListSecretsResponse>, Status> {
        self.list_secrets_impl(request).await
    }

    async fn set_org_secret(
        &self,
        request: Request<SetOrgSecretRequest>,
    ) -> Result<Response<SetOrgSecretResponse>, Status> {
        self.set_org_secret_impl(request).await
    }

    async fn delete_org_secret(
        &self,
        request: Request<DeleteOrgSecretRequest>,
    ) -> Result<Response<DeleteOrgSecretResponse>, Status> {
        self.delete_org_secret_impl(request).await
    }

    async fn list_org_secrets(
        &self,
        request: Request<ListOrgSecretsRequest>,
    ) -> Result<Response<ListOrgSecretsResponse>, Status> {
        self.list_org_secrets_impl(request).await
    }
}
