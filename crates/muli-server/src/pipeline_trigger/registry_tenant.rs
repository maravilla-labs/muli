// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resolve which registry tenant a pipeline's ambient publish credential targets.
//!
//! Git and the registry isolate differently. Git is isolated per-repo (owner /
//! collaborator / org-role ACL), so many repos safely share one git tenant. The
//! registry is isolated ONLY per-tenant — there is no per-package ACL — so an
//! org's packages must live in the org's own registry tenant, or any token for
//! the shared tenant could overwrite them.
//!
//! An org-owned repo carries `owner_type == Organization` and a `namespace` equal
//! to the org handle (flightdeck sets it that way when it creates the repo via the
//! gRPC `create_repository`; git push cannot create repos). So the org's registry
//! tenant is simply the repo namespace. `owner_type` is the trust anchor: only
//! membership-gated flightdeck can set it, so a non-member cannot obtain a
//! `solutas`-namespace org repo and thus cannot mint a `solutas` registry token.
//!
//! Note: muli's own org tables are unpopulated on deployments where orgs are owned
//! by an external control plane, so this deliberately does NOT consult `OrgStore`
//! — it relies only on the repo's own trusted `owner_type` + `namespace`.

use muli_core::git::{OwnerType, Repository};

use super::PipelineTriggerImpl;

/// Pure resolution logic (see module docs). Kept free-standing so it is unit
/// testable without constructing a full `PipelineTriggerImpl`.
///
/// - flag off → the git tenant (today's behaviour; also the rollback switch).
/// - org-owned repo → the org handle (= `repo.namespace`), its own registry.
/// - user/personal repo → the shared git tenant.
pub(crate) fn registry_tenant_for<'a>(
    per_handle: bool,
    git_tenant: &'a str,
    repo: &'a Repository,
) -> &'a str {
    if !per_handle {
        return git_tenant;
    }
    match repo.owner_type {
        OwnerType::Organization => &repo.namespace,
        OwnerType::User => git_tenant,
    }
}

impl PipelineTriggerImpl {
    /// The registry tenant an org repo's ambient publish credential should target.
    pub(crate) fn resolve_registry_tenant<'a>(
        &self,
        git_tenant: &'a str,
        repo: &'a Repository,
    ) -> &'a str {
        registry_tenant_for(self.registry_tenant_per_handle, git_tenant, repo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(owner_type: OwnerType, namespace: &str) -> Repository {
        let mut r = Repository::new(
            "local".to_string(),
            namespace.to_string(),
            "some-repo".to_string(),
            String::new(),
            false,
        )
        .expect("valid repo");
        r.owner_type = owner_type;
        r
    }

    #[test]
    fn org_repo_with_flag_on_uses_org_handle() {
        let r = repo(OwnerType::Organization, "solutas");
        assert_eq!(registry_tenant_for(true, "local", &r), "solutas");
    }

    #[test]
    fn user_repo_uses_shared_git_tenant() {
        let r = repo(OwnerType::User, "labertasch");
        assert_eq!(registry_tenant_for(true, "local", &r), "local");
    }

    #[test]
    fn flag_off_always_uses_git_tenant() {
        let org = repo(OwnerType::Organization, "solutas");
        assert_eq!(registry_tenant_for(false, "local", &org), "local");
        let user = repo(OwnerType::User, "labertasch");
        assert_eq!(registry_tenant_for(false, "local", &user), "local");
    }
}
