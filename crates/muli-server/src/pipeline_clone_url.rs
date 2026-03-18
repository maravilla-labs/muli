// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CiCloneUrlTarget {
    HostCheckout,
    ContainerClone,
}

pub(crate) fn build_ci_clone_url(
    git_base_url: &str,
    token: &str,
    namespace: &str,
    name: &str,
    target: CiCloneUrlTarget,
) -> String {
    let base = git_base_url.trim_end_matches('/');
    let (scheme, host) = if let Some(i) = base.find("://") {
        (&base[..i], base[i + 3..].to_string())
    } else {
        ("http", base.to_string())
    };
    let host = match target {
        CiCloneUrlTarget::HostCheckout => host,
        CiCloneUrlTarget::ContainerClone => rewrite_localhost_for_container(host),
    };
    format!("{scheme}://x-pipeline-token:{token}@{host}/{namespace}/{name}.git")
}

fn rewrite_localhost_for_container(host: String) -> String {
    if host.starts_with("localhost") {
        host.replacen("localhost", "host.docker.internal", 1)
    } else if host.starts_with("127.0.0.1") {
        host.replacen("127.0.0.1", "host.docker.internal", 1)
    } else {
        host
    }
}

#[cfg(test)]
mod tests {
    use super::{CiCloneUrlTarget, build_ci_clone_url};

    #[test]
    fn host_checkout_keeps_local_ipv4_base_url() {
        let url = build_ci_clone_url(
            "http://127.0.0.1:7000",
            "secret",
            "acme",
            "repo",
            CiCloneUrlTarget::HostCheckout,
        );
        assert_eq!(
            url,
            "http://x-pipeline-token:secret@127.0.0.1:7000/acme/repo.git"
        );
    }

    #[test]
    fn container_clone_rewrites_local_ipv4_base_url() {
        let url = build_ci_clone_url(
            "http://127.0.0.1:7000",
            "secret",
            "acme",
            "repo",
            CiCloneUrlTarget::ContainerClone,
        );
        assert_eq!(
            url,
            "http://x-pipeline-token:secret@host.docker.internal:7000/acme/repo.git"
        );
    }
}
