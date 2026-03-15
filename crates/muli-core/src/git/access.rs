// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared repo-level access control logic used by both HTTP and SSH paths.

use crate::git::{GitPermission, HasPermissions, Repository};
use crate::traits::CollaboratorStore;

/// Result of a repo-level access check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoAccessVerdict {
    Allowed,
    Denied { reason: &'static str },
}

/// Check whether `user_id` may perform `required` on `repo`.
///
/// Rules:
/// - Public repo + read-only → allowed (no auth needed)
/// - Owner → allowed
/// - Collaborator with the required permission → allowed
/// - Otherwise → denied
pub async fn check_repo_access(
    repo: &Repository,
    user_id: Option<&str>,
    required: GitPermission,
    collaborator_store: &dyn CollaboratorStore,
) -> RepoAccessVerdict {
    let is_write = matches!(required, GitPermission::Push | GitPermission::Admin);

    // Public repo, read-only — no ACL needed
    if !repo.is_private && !is_write {
        return RepoAccessVerdict::Allowed;
    }

    // From here on, a user_id is required
    let user_id = match user_id {
        Some(uid) if !uid.is_empty() => uid,
        _ => {
            return RepoAccessVerdict::Denied {
                reason: "access denied",
            };
        }
    };

    // Owner check
    if !repo.owner_id.is_empty() && repo.owner_id == user_id {
        return RepoAccessVerdict::Allowed;
    }

    // Collaborator check
    let is_collaborator = collaborator_store
        .get_collaborator(&repo.id, user_id)
        .await
        .ok()
        .flatten()
        .map(|c| c.has_permission(required))
        .unwrap_or(false);

    if is_collaborator {
        RepoAccessVerdict::Allowed
    } else {
        RepoAccessVerdict::Denied {
            reason: "access denied",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::error::Result;
    use crate::git::{GitPermission, RepositoryCollaborator};

    /// A minimal in-memory `CollaboratorStore` for unit tests.
    struct MockCollaboratorStore {
        /// Keyed by (repo_id, user_id).
        data: Mutex<HashMap<(String, String), RepositoryCollaborator>>,
    }

    impl MockCollaboratorStore {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }

        fn add(&self, collab: RepositoryCollaborator) {
            let key = (collab.repo_id.clone(), collab.user_id.clone());
            self.data.lock().unwrap().insert(key, collab);
        }
    }

    #[async_trait]
    impl CollaboratorStore for MockCollaboratorStore {
        async fn upsert_collaborator(
            &self,
            collaborator: &RepositoryCollaborator,
        ) -> Result<String> {
            let key = (collaborator.repo_id.clone(), collaborator.user_id.clone());
            self.data.lock().unwrap().insert(key, collaborator.clone());
            Ok(collaborator.id.clone())
        }

        async fn remove_collaborator(&self, repo_id: &str, user_id: &str) -> Result<()> {
            self.data
                .lock()
                .unwrap()
                .remove(&(repo_id.to_string(), user_id.to_string()));
            Ok(())
        }

        async fn list_collaborators(&self, repo_id: &str) -> Result<Vec<RepositoryCollaborator>> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .values()
                .filter(|c| c.repo_id == repo_id)
                .cloned()
                .collect())
        }

        async fn get_collaborator(
            &self,
            repo_id: &str,
            user_id: &str,
        ) -> Result<Option<RepositoryCollaborator>> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(&(repo_id.to_string(), user_id.to_string()))
                .cloned())
        }
    }

    /// Helper: build a Repository with the given visibility and owner.
    fn make_repo(is_private: bool, owner_id: &str) -> Repository {
        Repository {
            id: "repo-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            namespace: "acme".to_string(),
            name: "my-repo".to_string(),
            description: String::new(),
            is_private,
            default_branch: "main".to_string(),
            fork_of: None,
            owner_id: owner_id.to_string(),
            owner_type: crate::git::OwnerType::User,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    // ---------------------------------------------------------------
    // Public repo + read (Pull) → Allowed (no auth needed)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_public_repo_read_allowed_without_auth() {
        let repo = make_repo(false, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, None, GitPermission::Pull, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    #[tokio::test]
    async fn test_public_repo_read_allowed_with_any_user() {
        let repo = make_repo(false, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict =
            check_repo_access(&repo, Some("random-user"), GitPermission::Pull, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    // ---------------------------------------------------------------
    // Private repo + no user → Denied
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_private_repo_denied_without_user() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, None, GitPermission::Pull, &store).await;
        assert_eq!(
            verdict,
            RepoAccessVerdict::Denied {
                reason: "access denied"
            }
        );
    }

    // ---------------------------------------------------------------
    // Owner → Allowed
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_owner_allowed() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, Some("owner-1"), GitPermission::Pull, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    #[tokio::test]
    async fn test_owner_allowed_for_push() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, Some("owner-1"), GitPermission::Push, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    #[tokio::test]
    async fn test_owner_allowed_for_admin() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, Some("owner-1"), GitPermission::Admin, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    // ---------------------------------------------------------------
    // Collaborator with required permission → Allowed
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_collaborator_with_pull_permission_allowed() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();
        store.add(RepositoryCollaborator::new(
            "tenant-1".into(),
            "repo-1".into(),
            "collab-1".into(),
            vec![GitPermission::Pull],
        ));

        let verdict = check_repo_access(&repo, Some("collab-1"), GitPermission::Pull, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    #[tokio::test]
    async fn test_collaborator_with_push_permission_allowed() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();
        store.add(RepositoryCollaborator::new(
            "tenant-1".into(),
            "repo-1".into(),
            "collab-1".into(),
            vec![GitPermission::Pull, GitPermission::Push],
        ));

        let verdict = check_repo_access(&repo, Some("collab-1"), GitPermission::Push, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    // ---------------------------------------------------------------
    // Collaborator WITHOUT required permission → Denied
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_collaborator_without_required_permission_denied() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();
        store.add(RepositoryCollaborator::new(
            "tenant-1".into(),
            "repo-1".into(),
            "collab-1".into(),
            vec![GitPermission::Pull], // only Pull
        ));

        let verdict = check_repo_access(&repo, Some("collab-1"), GitPermission::Push, &store).await;
        assert_eq!(
            verdict,
            RepoAccessVerdict::Denied {
                reason: "access denied"
            }
        );
    }

    #[tokio::test]
    async fn test_collaborator_without_admin_permission_denied() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();
        store.add(RepositoryCollaborator::new(
            "tenant-1".into(),
            "repo-1".into(),
            "collab-1".into(),
            vec![GitPermission::Pull, GitPermission::Push],
        ));

        let verdict =
            check_repo_access(&repo, Some("collab-1"), GitPermission::Admin, &store).await;
        assert_eq!(
            verdict,
            RepoAccessVerdict::Denied {
                reason: "access denied"
            }
        );
    }

    // ---------------------------------------------------------------
    // Not owner, not collaborator → Denied
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_not_owner_not_collaborator_denied() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, Some("stranger"), GitPermission::Pull, &store).await;
        assert_eq!(
            verdict,
            RepoAccessVerdict::Denied {
                reason: "access denied"
            }
        );
    }

    // ---------------------------------------------------------------
    // Empty user_id → Denied
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_empty_user_id_denied() {
        let repo = make_repo(true, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, Some(""), GitPermission::Pull, &store).await;
        assert_eq!(
            verdict,
            RepoAccessVerdict::Denied {
                reason: "access denied"
            }
        );
    }

    // ---------------------------------------------------------------
    // Public repo + write (Push) with no user → Denied
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_public_repo_push_denied_without_user() {
        let repo = make_repo(false, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, None, GitPermission::Push, &store).await;
        assert_eq!(
            verdict,
            RepoAccessVerdict::Denied {
                reason: "access denied"
            }
        );
    }

    // ---------------------------------------------------------------
    // Public repo + write (Push) by owner → Allowed
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_public_repo_push_by_owner_allowed() {
        let repo = make_repo(false, "owner-1");
        let store = MockCollaboratorStore::new();

        let verdict = check_repo_access(&repo, Some("owner-1"), GitPermission::Push, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }

    // ---------------------------------------------------------------
    // Public repo + write (Push) by collaborator with Push → Allowed
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_public_repo_push_by_collaborator_with_push_allowed() {
        let repo = make_repo(false, "owner-1");
        let store = MockCollaboratorStore::new();
        store.add(RepositoryCollaborator::new(
            "tenant-1".into(),
            "repo-1".into(),
            "collab-1".into(),
            vec![GitPermission::Pull, GitPermission::Push],
        ));

        let verdict = check_repo_access(&repo, Some("collab-1"), GitPermission::Push, &store).await;
        assert_eq!(verdict, RepoAccessVerdict::Allowed);
    }
}
