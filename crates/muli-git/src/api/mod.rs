// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REST API router for git repository management.

pub mod blame;
pub mod blobs;
pub mod commits;
pub mod forks;
pub mod helpers;
pub mod protocol;
pub mod pulls;
pub mod pulls_comments;
pub mod pulls_diff;
pub mod pulls_merge;
pub mod pulls_types;
pub mod refs;
pub mod repos;
pub mod tags;
pub mod tokens;
pub mod tree;
pub mod webhooks;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    routing::{delete, get, post},
};

use muli_core::traits::{
    GitTokenStore, PrCommentStore, PullRequestStore, RepositoryStore, SshKeyStore,
    TreeCommitCacheStore, WebhookStore,
};

use crate::auth::GitAuth;
use crate::lfs;
use crate::storage::FilesystemStorage;
use crate::tenant::TenantConfig;

// Re-export for backward compatibility
pub use helpers::strip_git_suffix;

/// Shared application state for the git service.
#[derive(Clone)]
pub struct GitState {
    pub storage: Arc<FilesystemStorage>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub token_store: Arc<dyn GitTokenStore>,
    pub webhook_store: Arc<dyn WebhookStore>,
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    pub pr_store: Arc<dyn PullRequestStore>,
    pub pr_comment_store: Arc<dyn PrCommentStore>,
    pub http_client: Arc<reqwest::Client>,
    pub cache_store: Option<Arc<dyn TreeCommitCacheStore>>,
    pub webhook_semaphore: Arc<tokio::sync::Semaphore>,
    /// When true, skip SSRF checks on webhook URLs (for testing only).
    pub allow_localhost_webhooks: bool,
    /// LFS object storage backend (None = LFS disabled).
    pub lfs_storage: Option<Arc<dyn lfs::storage::LfsStorage>>,
}

/// Configuration for building the git router.
pub struct GitRouterConfig {
    pub storage: Arc<FilesystemStorage>,
    pub repo_store: Arc<dyn RepositoryStore>,
    pub token_store: Arc<dyn GitTokenStore>,
    pub webhook_store: Arc<dyn WebhookStore>,
    pub ssh_key_store: Arc<dyn SshKeyStore>,
    pub pr_store: Arc<dyn PullRequestStore>,
    pub pr_comment_store: Arc<dyn PrCommentStore>,
    pub auth: Option<GitAuth>,
    pub tenant_config: TenantConfig,
    pub cache_store: Option<Arc<dyn TreeCommitCacheStore>>,
    /// When true, skip SSRF checks on webhook URLs (for testing only).
    pub allow_localhost_webhooks: bool,
    /// LFS object storage backend (None = LFS disabled).
    pub lfs_storage: Option<Arc<dyn lfs::storage::LfsStorage>>,
}

/// Build the complete git service router.
pub fn git_router(cfg: GitRouterConfig) -> Router {
    let GitRouterConfig {
        storage,
        repo_store,
        token_store,
        webhook_store,
        ssh_key_store,
        pr_store,
        pr_comment_store,
        auth,
        tenant_config,
        cache_store,
        allow_localhost_webhooks,
        lfs_storage,
    } = cfg;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build reqwest client");

    let state = Arc::new(GitState {
        storage,
        repo_store,
        token_store,
        webhook_store,
        ssh_key_store,
        pr_store,
        pr_comment_store,
        http_client: Arc::new(http_client),
        cache_store,
        webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
        allow_localhost_webhooks,
        lfs_storage,
    });

    // Git Smart HTTP protocol routes.
    let git_protocol = Router::new()
        .route("/{namespace}/{repo}/info/refs", get(protocol::info_refs))
        .route(
            "/{namespace}/{repo}/git-upload-pack",
            post(protocol::upload_pack),
        )
        .route(
            "/{namespace}/{repo}/git-receive-pack",
            post(protocol::receive_pack),
        );

    // REST API routes
    let rest_api = Router::new()
        .route(
            "/api/v1/repos",
            post(repos::create_repo).get(repos::list_repos),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}",
            delete(repos::delete_repo),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/transfer",
            post(repos::transfer_repo),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/forks",
            post(forks::fork_repo),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/refs",
            get(refs::list_refs),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/branches",
            post(refs::create_branch),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/commits",
            get(commits::list_commits),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/commits/{sha}",
            get(commits::get_commit),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/commits/{sha}/diff",
            get(commits::get_commit_diff),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/tree-commits",
            get(tree::list_tree_commits),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/blame/{*path}",
            get(blame::get_blame),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/contents",
            get(blobs::get_root_contents),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/contents/{*path}",
            get(blobs::get_blob),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/hooks",
            post(webhooks::create_webhook).get(webhooks::list_webhooks),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/hooks/{hook_id}",
            delete(webhooks::delete_webhook),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/tags",
            post(tags::create_tag),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/tags/{tag}",
            delete(tags::delete_tag),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pulls",
            post(pulls::create_pr).get(pulls::list_prs),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pulls/{number}",
            get(pulls::get_pr).patch(pulls::patch_pr),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pulls/{number}/diff",
            get(pulls::get_pr_diff),
        )
        .route(
            "/api/v1/repos/{namespace}/{repo}/pulls/{number}/comments",
            post(pulls::add_comment).get(pulls::list_comments),
        )
        .route("/api/v1/tokens", post(tokens::create_token))
        .route("/api/v1/tokens/{token_id}", delete(tokens::revoke_token));

    // LFS Batch API routes (active when lfs_storage is Some).
    let lfs_api = Router::new()
        .route(
            "/{namespace}/{repo}/info/lfs/objects/batch",
            post(lfs::api::batch),
        )
        .route(
            "/{namespace}/{repo}/info/lfs/objects/verify",
            post(lfs::api::verify),
        )
        .route(
            "/{namespace}/{repo}/info/lfs/objects/{oid}",
            get(lfs::api::download).put(lfs::api::upload),
        );

    let mut app = Router::new()
        .merge(git_protocol)
        .merge(rest_api)
        .merge(lfs_api)
        .with_state(state);

    if let Some(git_auth) = auth {
        app = app
            .layer(axum::middleware::from_fn(crate::auth::auth_middleware))
            .layer(axum::Extension(git_auth));
    }

    app = app
        .layer(axum::middleware::from_fn(crate::tenant::tenant_middleware))
        .layer(axum::Extension(tenant_config));

    Router::new()
        .route("/-/health", get(|| async { "ok" }))
        .merge(app)
}
