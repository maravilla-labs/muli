// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The repository an HTTP request is about, decided ONCE.
//!
//! `repo_target_middleware` reads the raw request path, resolves the
//! repository it names and stores the outcome as a [`RepoTarget`] request
//! extension. The auth middleware authorizes against that repository and
//! every handler serves that repository (extractor [`TargetRepo`]); nothing
//! downstream derives a repository from the path again. One parser means the
//! repository that was authorized is the repository that is served.
//!
//! Grammar (on the raw, still percent-encoded path, split on `/`):
//! - `/api/v1/repos/{ns}/{name}[/...]`: REST. `name` is the repository name
//!   exactly as stored (no suffix handling).
//! - `/{ns}/{repo}/info/refs`, `/{ns}/{repo}/git-upload-pack`,
//!   `/{ns}/{repo}/git-receive-pack`, `/{ns}/{repo}/info/lfs/...`: git
//!   transport. `repo` is the name with at most ONE trailing `.git` removed
//!   (`x.git` = `x`, `x.git.git` = `x.git`).
//! - anything else names no repository.
//!
//! Both components must satisfy the rules repositories are created with
//! (`muli_core::git::is_valid_repo_component`): no `%` (percent-encoding is
//! refused, never decoded), no `..`, no separators, nothing empty. A path
//! that names a repository but is malformed, unknown in the tenant or cannot
//! be looked up is [`RepoTarget::Unresolved`]: refused, never "no
//! repository" (which would skip the per-repository checks).

use std::sync::Arc;

use axum::{
    extract::{FromRequestParts, Request, State},
    http::{StatusCode, request::Parts},
    middleware::Next,
    response::Response,
};
use muli_core::git::{Repository, is_valid_repo_component};
use muli_core::traits::RepositoryStore;

use crate::api::helpers::error_response;
use crate::tenant::TenantContext;

/// Which family of endpoints a repository path belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepoPathKind {
    /// `/api/v1/repos/{ns}/{name}/...`
    Rest,
    /// Git smart HTTP and Git LFS: `/{ns}/{repo}/(info/refs|git-*-pack|info/lfs/...)`.
    Transport,
}

/// What the raw path says about a repository. Pure result of [`parse`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepoPath {
    /// The path names no repository.
    None,
    /// The path names `namespace/name` (canonical, valid components).
    Named {
        kind: RepoPathKind,
        namespace: String,
        name: String,
    },
    /// The path has the shape of a repository path but its components are
    /// not a valid repository (encoded, traversal, empty, bad characters).
    Invalid { kind: RepoPathKind },
}

impl RepoPath {
    pub fn kind(&self) -> Option<RepoPathKind> {
        match self {
            RepoPath::None => None,
            RepoPath::Named { kind, .. } | RepoPath::Invalid { kind } => Some(*kind),
        }
    }
}

/// The transport tails after `/{ns}/{repo}`. Matches exactly the router's
/// transport and LFS routes.
fn is_transport_tail(tail: &[&str]) -> bool {
    match tail {
        ["info", "refs"] | ["git-upload-pack"] | ["git-receive-pack"] => true,
        ["info", "lfs", "objects", object] => !object.is_empty(),
        _ => false,
    }
}

/// Parse the raw request path. Pure; the only repository path parser of
/// the HTTP service.
pub fn parse(path: &str) -> RepoPath {
    let Some(rest) = path.strip_prefix('/') else {
        return RepoPath::None;
    };
    let segs: Vec<&str> = rest.split('/').collect();

    let (kind, namespace, name) = match segs.as_slice() {
        ["api", "v1", "repos", ns, name, ..] => (RepoPathKind::Rest, *ns, *name),
        ["api", ..] => return RepoPath::None,
        [ns, repo, tail @ ..] if is_transport_tail(tail) => {
            let name = repo.strip_suffix(".git").unwrap_or(repo);
            (RepoPathKind::Transport, *ns, name)
        }
        _ => return RepoPath::None,
    };

    if is_valid_repo_component(namespace) && is_valid_repo_component(name) {
        RepoPath::Named {
            kind,
            namespace: namespace.to_string(),
            name: name.to_string(),
        }
    } else {
        RepoPath::Invalid { kind }
    }
}

/// The resolved target of a request, stored as a request extension.
#[derive(Clone, Debug)]
pub enum RepoTarget {
    /// The path names no repository (repository list/create, tokens).
    None,
    /// The path names this repository of the request's tenant.
    Resolved {
        kind: RepoPathKind,
        repo: Box<Repository>,
    },
    /// The path names a repository that could not be resolved. Refused by
    /// the auth middleware (after authentication) and by every handler.
    Unresolved { kind: RepoPathKind },
}

impl RepoTarget {
    pub fn kind(&self) -> Option<RepoPathKind> {
        match self {
            RepoTarget::None => None,
            RepoTarget::Resolved { kind, .. } | RepoTarget::Unresolved { kind } => Some(*kind),
        }
    }

    pub fn repo(&self) -> Option<&Repository> {
        match self {
            RepoTarget::Resolved { repo, .. } => Some(repo),
            _ => None,
        }
    }
}

/// Resolve the request's repository once and store it as [`RepoTarget`].
/// Never answers by itself (an unauthenticated caller must not learn which
/// repositories exist); refusing is the auth middleware's and the handlers'
/// job.
pub async fn repo_target_middleware(
    State(repo_store): State<Arc<dyn RepositoryStore>>,
    mut request: Request,
    next: Next,
) -> Response {
    let target = match parse(request.uri().path()) {
        RepoPath::None => RepoTarget::None,
        RepoPath::Invalid { kind } => RepoTarget::Unresolved { kind },
        RepoPath::Named {
            kind,
            namespace,
            name,
        } => {
            let tenant_id = request
                .extensions()
                .get::<TenantContext>()
                .map(|t| t.tenant_id.clone())
                .unwrap_or_default();
            if tenant_id.is_empty() {
                RepoTarget::Unresolved { kind }
            } else {
                match repo_store
                    .get_repository_by_name(&tenant_id, &namespace, &name)
                    .await
                {
                    // The store matched on these exact strings; check anyway,
                    // so a looser store can never hand back another repository.
                    Ok(Some(repo))
                        if repo.tenant_id == tenant_id
                            && repo.namespace == namespace
                            && repo.name == name =>
                    {
                        RepoTarget::Resolved {
                            kind,
                            repo: Box::new(repo),
                        }
                    }
                    Ok(_) => RepoTarget::Unresolved { kind },
                    Err(e) => {
                        tracing::error!(error = %e, "repository lookup failed");
                        RepoTarget::Unresolved { kind }
                    }
                }
            }
        }
    };
    request.extensions_mut().insert(target);
    next.run(request).await
}

/// Extractor: the repository the request was resolved (and authorized)
/// against. Rejects with 404 when the path named no resolvable repository or
/// the resolver did not run.
pub struct TargetRepo(pub Repository);

impl<S: Send + Sync> FromRequestParts<S> for TargetRepo {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<RepoTarget>() {
            Some(RepoTarget::Resolved { repo, .. }) => Ok(TargetRepo((**repo).clone())),
            _ => Err(error_response(
                StatusCode::NOT_FOUND,
                "repository not found",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(kind: RepoPathKind, ns: &str, name: &str) -> RepoPath {
        RepoPath::Named {
            kind,
            namespace: ns.into(),
            name: name.into(),
        }
    }

    #[test]
    fn transport_strips_exactly_one_git_suffix() {
        use RepoPathKind::Transport as T;
        assert_eq!(parse("/acme/x/info/refs"), named(T, "acme", "x"));
        assert_eq!(parse("/acme/x.git/info/refs"), named(T, "acme", "x"));
        assert_eq!(
            parse("/acme/x.git.git/info/refs"),
            named(T, "acme", "x.git")
        );
        assert_eq!(parse("/acme/x.git/git-upload-pack"), named(T, "acme", "x"));
        assert_eq!(parse("/acme/x.git/git-receive-pack"), named(T, "acme", "x"));
        assert_eq!(
            parse("/acme/x.git.git/info/lfs/objects/batch"),
            named(T, "acme", "x.git")
        );
        assert_eq!(parse("/acme/x/info/lfs/objects/abc"), named(T, "acme", "x"));
    }

    #[test]
    fn rest_takes_the_name_verbatim() {
        use RepoPathKind::Rest as R;
        assert_eq!(parse("/api/v1/repos/acme/x"), named(R, "acme", "x"));
        assert_eq!(
            parse("/api/v1/repos/acme/x.git/tree"),
            named(R, "acme", "x.git")
        );
        assert_eq!(parse("/api/v1/repos/acme/X/tree"), named(R, "acme", "X"));
    }

    #[test]
    fn no_repository() {
        for p in [
            "/",
            "",
            "/-/health",
            "/api/v1/repos",
            "/api/v1/repos/acme",
            "/api/v1/tokens",
            "/api/v1/tokens/t1",
            "/acme/x",
            "/acme/x/info",
            "/acme/x/info/lfs/objects/",
            "/acme/x/info/refs/",
            "/acme/x/info/lfs/objects/a/b",
        ] {
            assert_eq!(parse(p), RepoPath::None, "{p}");
        }
    }

    #[test]
    fn malformed_components_are_invalid_not_none() {
        use RepoPathKind::{Rest, Transport};
        for p in [
            "/acme/%78/info/refs",
            "/acme/x%2Egit/info/refs",
            "/%61cme/x/info/refs",
            "/acme/../info/refs",
            "/../x/info/refs",
            "/acme/%2e%2e/info/refs",
            "/acme//info/refs",
            "/acme/.git/info/refs",
            "//x/info/refs",
            "/acme/x%2Fy/git-upload-pack",
        ] {
            assert_eq!(parse(p), RepoPath::Invalid { kind: Transport }, "{p}");
        }
        for p in [
            "/api/v1/repos/acme/%78",
            "/api/v1/repos/acme/../tree",
            "/api/v1/repos/acme//tree",
            "/api/v1/repos/%2e%2e/x",
        ] {
            assert_eq!(parse(p), RepoPath::Invalid { kind: Rest }, "{p}");
        }
    }
}
