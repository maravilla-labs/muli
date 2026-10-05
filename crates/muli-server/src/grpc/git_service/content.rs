// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Repository content read/write RPCs.

use base64::Engine as _;
use tonic::{Request, Response, Status};
use tracing::info;

use muli_proto::{
    CreateFilesBatchRequest, CreateFilesBatchResponse, CreateOrUpdateFileRequest,
    CreateOrUpdateFileResponse, GetFileContentRequest, GetFileContentResponse,
};

use super::GitServiceImpl;
use crate::grpc::util::validate_tenant;

impl GitServiceImpl {
    pub async fn get_file_content_impl(
        &self,
        request: Request<GetFileContentRequest>,
    ) -> Result<Response<GetFileContentResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        // Validate required fields
        if req.namespace.is_empty() || req.repo.is_empty() || req.path.is_empty() {
            return Err(Status::invalid_argument(
                "namespace, repo, and path are required",
            ));
        }

        // Verify repo exists
        self.repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.repo)
            .await
            .map_err(|e| Status::internal(format!("failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.repo
                ))
            })?;

        let repo_fs_path = self
            .git_storage
            .repo_path(&req.tenant_id, &req.namespace, &req.repo);

        let git_ref = req.git_ref.unwrap_or_else(|| "HEAD".to_string());
        let file_path = req.path.clone();
        let git_ref_clone = git_ref.clone();

        let result = tokio::task::spawn_blocking(move || {
            let repo = git2::Repository::open_bare(&repo_fs_path).map_err(|e| e.to_string())?;
            let obj = repo
                .revparse_single(&git_ref_clone)
                .map_err(|e| format!("ref not found: {e}"))?;
            let commit = obj
                .peel_to_commit()
                .map_err(|e| format!("not a commit: {e}"))?;
            let tree = commit.tree().map_err(|e| e.to_string())?;
            let entry = tree
                .get_path(std::path::Path::new(&file_path))
                .map_err(|e| format!("path not found: {e}"))?;
            let blob = repo
                .find_blob(entry.id())
                .map_err(|e| format!("not a blob: {e}"))?;
            Ok::<Vec<u8>, String>(blob.content().to_vec())
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?;

        match result {
            Ok(content) => {
                let size = content.len() as u64;
                let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
                Ok(Response::new(GetFileContentResponse {
                    path: req.path,
                    git_ref,
                    size,
                    encoding: "base64".to_string(),
                    content: encoded,
                }))
            }
            Err(e) if e.contains("not found") => Err(Status::not_found(e)),
            Err(e) => Err(Status::internal(e)),
        }
    }

    pub async fn create_or_update_file_impl(
        &self,
        request: Request<CreateOrUpdateFileRequest>,
    ) -> Result<Response<CreateOrUpdateFileResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.namespace.is_empty() || req.repo.is_empty() || req.path.is_empty() {
            return Err(Status::invalid_argument(
                "namespace, repo, and path are required",
            ));
        }
        if req.content.is_empty() {
            return Err(Status::invalid_argument("content is required"));
        }
        if req.message.is_empty() {
            return Err(Status::invalid_argument("message is required"));
        }

        let branch = if req.branch.is_empty() {
            "main".to_string()
        } else {
            req.branch
        };

        // Verify repo exists
        let repository = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.repo)
            .await
            .map_err(|e| Status::internal(format!("failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.repo
                ))
            })?;

        let repo_fs_path = self
            .git_storage
            .repo_path(&req.tenant_id, &req.namespace, &req.repo);
        let repo_size_before = muli_git::hooks::compute_dir_size(&repo_fs_path).await.ok();
        let git_path = repo_fs_path.clone();

        let content = base64::engine::general_purpose::STANDARD
            .decode(&req.content)
            .map_err(|_| Status::invalid_argument("invalid base64 content"))?;

        let file_path = req.path.clone();
        let message = req.message;

        let result = tokio::task::spawn_blocking(move || {
            let repo = git2::Repository::open_bare(&git_path).map_err(|e| e.to_string())?;
            let sig = git2::Signature::now("Muli", "muli@localhost").map_err(|e| e.to_string())?;

            let ref_name = format!("refs/heads/{branch}");
            let parent_commit = repo
                .find_reference(&ref_name)
                .and_then(|r| r.peel_to_commit())
                .map_err(|e| format!("branch not found: {e}"))?;

            let base_tree = parent_commit.tree().map_err(|e| e.to_string())?;
            let blob_oid = repo.blob(&content).map_err(|e| e.to_string())?;

            let segments: Vec<&str> = file_path.split('/').collect();
            let new_tree_oid = muli_git::api::blobs::insert_blob_in_tree(
                &repo,
                Some(&base_tree),
                &segments,
                blob_oid,
            )?;
            let new_tree = repo.find_tree(new_tree_oid).map_err(|e| e.to_string())?;

            let commit_oid = repo
                .commit(
                    Some(&ref_name),
                    &sig,
                    &sig,
                    &message,
                    &new_tree,
                    &[&parent_commit],
                )
                .map_err(|e| e.to_string())?;

            let update = muli_git::RefUpdate {
                old_sha: parent_commit.id().to_string(),
                new_sha: commit_oid.to_string(),
                ref_name,
            };
            Ok::<(String, muli_git::RefUpdate), String>((file_path, update))
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?;

        match result {
            Ok((path, update)) => {
                let sha = update.new_sha.clone();
                self.fire_push_hooks(
                    &req.tenant_id,
                    repository.id,
                    req.repo.clone(),
                    update,
                    repo_size_before,
                    repo_fs_path,
                );
                info!(
                    operation = "create_or_update_file",
                    tenant_id = %req.tenant_id,
                    namespace = %req.namespace,
                    repo = %req.repo,
                    path = %path,
                    "audit: file created/updated via gRPC"
                );
                Ok(Response::new(CreateOrUpdateFileResponse { path, sha }))
            }
            Err(e) if e.contains("branch not found") => Err(Status::not_found(e)),
            Err(e) => Err(Status::internal(e)),
        }
    }

    pub async fn create_files_batch_impl(
        &self,
        request: Request<CreateFilesBatchRequest>,
    ) -> Result<Response<CreateFilesBatchResponse>, Status> {
        let (_caller_tenant, req) = validate_tenant(request, |r| &r.tenant_id)?;

        if req.namespace.is_empty() || req.repo.is_empty() {
            return Err(Status::invalid_argument("namespace and repo are required"));
        }
        if req.files.is_empty() {
            return Err(Status::invalid_argument("files must not be empty"));
        }
        if req.message.is_empty() {
            return Err(Status::invalid_argument("message is required"));
        }

        let branch = if req.branch.is_empty() {
            "main".to_string()
        } else {
            req.branch
        };

        // Verify repo exists
        let repository = self
            .repo_store
            .get_repository_by_name(&req.tenant_id, &req.namespace, &req.repo)
            .await
            .map_err(|e| Status::internal(format!("failed to look up repository: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository {}/{} not found",
                    req.namespace, req.repo
                ))
            })?;

        let repo_fs_path = self
            .git_storage
            .repo_path(&req.tenant_id, &req.namespace, &req.repo);
        let repo_size_before = muli_git::hooks::compute_dir_size(&repo_fs_path).await.ok();
        let git_path = repo_fs_path.clone();

        // Decode all files upfront
        let mut decoded_files = Vec::with_capacity(req.files.len());
        for entry in &req.files {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&entry.content)
                .map_err(|_| {
                    Status::invalid_argument(format!(
                        "invalid base64 content for path: {}",
                        entry.path
                    ))
                })?;
            decoded_files.push((entry.path.clone(), bytes));
        }

        let message = req.message;

        let result = tokio::task::spawn_blocking(move || {
            let repo = git2::Repository::open_bare(&git_path).map_err(|e| e.to_string())?;
            let sig = git2::Signature::now("Muli", "muli@localhost").map_err(|e| e.to_string())?;

            let ref_name = format!("refs/heads/{branch}");
            let parent_commit = repo
                .find_reference(&ref_name)
                .and_then(|r| r.peel_to_commit())
                .ok();

            let base_tree = match &parent_commit {
                Some(c) => Some(c.tree().map_err(|e| e.to_string())?),
                None => None,
            };

            let mut current_tree_oid = match &base_tree {
                Some(t) => t.id(),
                None => {
                    let builder = repo.treebuilder(None).map_err(|e| e.to_string())?;
                    builder.write().map_err(|e| e.to_string())?
                }
            };

            for (path, content) in &decoded_files {
                let blob_oid = repo.blob(content).map_err(|e| e.to_string())?;
                let current_tree = repo
                    .find_tree(current_tree_oid)
                    .map_err(|e| e.to_string())?;
                let segments: Vec<&str> = path.split('/').collect();
                current_tree_oid = muli_git::api::blobs::insert_blob_in_tree(
                    &repo,
                    Some(&current_tree),
                    &segments,
                    blob_oid,
                )?;
            }

            let final_tree = repo
                .find_tree(current_tree_oid)
                .map_err(|e| e.to_string())?;

            let parents: Vec<&git2::Commit<'_>> = match &parent_commit {
                Some(c) => vec![c],
                None => vec![],
            };
            let commit_oid = repo
                .commit(Some(&ref_name), &sig, &sig, &message, &final_tree, &parents)
                .map_err(|e| e.to_string())?;

            let update = muli_git::RefUpdate {
                // The first commit of an empty repository has no parent.
                old_sha: parent_commit
                    .as_ref()
                    .map_or_else(|| "0".repeat(40), |c| c.id().to_string()),
                new_sha: commit_oid.to_string(),
                ref_name,
            };
            Ok::<(usize, muli_git::RefUpdate), String>((decoded_files.len(), update))
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?;

        match result {
            Ok((count, update)) => {
                let sha = update.new_sha.clone();
                self.fire_push_hooks(
                    &req.tenant_id,
                    repository.id,
                    req.repo.clone(),
                    update,
                    repo_size_before,
                    repo_fs_path,
                );
                info!(
                    operation = "create_files_batch",
                    tenant_id = %req.tenant_id,
                    namespace = %req.namespace,
                    repo = %req.repo,
                    file_count = count,
                    "audit: batch files created via gRPC"
                );
                Ok(Response::new(CreateFilesBatchResponse {
                    files_committed: count as u32,
                    sha,
                }))
            }
            Err(e) => Err(Status::internal(e)),
        }
    }

    /// A commit made here moves a branch like a push does, so it fires the
    /// same hooks: pipelines, webhooks, cache, quota.
    fn fire_push_hooks(
        &self,
        tenant_id: &str,
        repo_id: String,
        repo_name: String,
        update: muli_git::RefUpdate,
        repo_size_before: Option<u64>,
        repo_path: std::path::PathBuf,
    ) {
        if let Some(hooks) = &self.post_push_hooks {
            hooks.fire(
                tenant_id.to_string(),
                repo_id,
                repo_name,
                vec![update],
                repo_size_before,
                repo_path,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use muli_core::git::Repository;
    use muli_core::traits::RepositoryStore;
    use muli_proto::{BatchFileEntry, CreateFilesBatchRequest};
    use muli_store::memory::{MemoryRepositoryStore, MemoryWebhookStore};
    use muli_store::sqlite::{SqliteCollaboratorStore, SqliteStoreFactory};
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tonic::Request;

    use super::GitServiceImpl;

    /// Records every push it is told about.
    struct RecordingTrigger(mpsc::UnboundedSender<(String, String, String)>);

    #[async_trait::async_trait]
    impl muli_git::api::PipelineTriggerHook for RecordingTrigger {
        async fn on_push(&self, _: &str, repo_id: &str, old: &str, new: &str, ref_name: &str) {
            let _ = self
                .0
                .send((repo_id.into(), format!("{old}..{new}"), ref_name.into()));
        }
        async fn on_pr_event(&self, _: &str, _: &str, _: u64, _: &str) {}
    }

    #[tokio::test]
    async fn batch_commit_triggers_the_pipeline_like_a_push() {
        let tmp = TempDir::new().expect("temp dir");
        let repo_store = Arc::new(MemoryRepositoryStore::new());
        let webhook_store = Arc::new(MemoryWebhookStore::new());
        let git_storage = Arc::new(
            muli_git::storage::FilesystemStorage::new(tmp.path().to_str().unwrap())
                .await
                .expect("git storage"),
        );
        let repo = Repository::new(
            "tenant-1".into(),
            "acme".into(),
            "site".into(),
            String::new(),
            false,
        )
        .expect("repo");
        repo_store
            .create_repository(&repo)
            .await
            .expect("create repo");
        git_storage
            .init_repo("tenant-1", "acme", "site")
            .await
            .expect("init repo");

        let (tx, mut rx) = mpsc::unbounded_channel();
        let hooks = muli_git::hooks::PostPushHooks {
            pipeline_trigger: Some(Arc::new(RecordingTrigger(tx))),
            webhook_store: webhook_store.clone(),
            http_client: Arc::new(muli_git::hooks::webhook_http_client()),
            webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            allow_localhost_webhooks: false,
            cache_store: None,
            quota_store: None,
        };
        let service = GitServiceImpl {
            repo_store: repo_store.clone(),
            token_store: Arc::new(muli_store::memory::MemoryGitTokenStore::new()),
            ssh_key_store: Arc::new(muli_store::memory::MemorySshKeyStore::new()),
            webhook_store,
            collaborator_store: Arc::new(SqliteCollaboratorStore::new(
                SqliteStoreFactory::new(tmp.path()).await.expect("sqlite"),
            )),
            git_storage: git_storage.clone(),
            allow_localhost_webhooks: false,
            repo_service: Arc::new(muli_core::service::RepositoryService::new(
                repo_store,
                git_storage,
            )),
            tenant_limits_store: None,
            post_push_hooks: Some(hooks),
        };

        let mut req = Request::new(CreateFilesBatchRequest {
            tenant_id: "tenant-1".into(),
            namespace: "acme".into(),
            repo: "site".into(),
            files: vec![BatchFileEntry {
                path: "README.md".into(),
                content: "aGVsbG8K".into(),
            }],
            message: "first commit".into(),
            branch: "main".into(),
        });
        req.metadata_mut().insert(
            "x-tenant-id",
            tonic::metadata::MetadataValue::from_static("tenant-1"),
        );
        let sha = service
            .create_files_batch_impl(req)
            .await
            .expect("batch commit")
            .into_inner()
            .sha;

        let (repo_id, range, ref_name) =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("pipeline trigger not called")
                .expect("channel closed");
        assert_eq!(repo_id, repo.id);
        assert_eq!(range, format!("{}..{sha}", "0".repeat(40)));
        assert_eq!(ref_name, "refs/heads/main");
    }
}
