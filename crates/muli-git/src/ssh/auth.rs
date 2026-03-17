// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SSH command parsing and path validation.

use crate::api::helpers::validate_ssh_path_segment;

/// Parse `git-upload-pack '/ns/repo.git'` or `git-receive-pack '/ns/repo.git'`
/// into `(git_binary, repo_path_string)`.
pub fn parse_git_ssh_command(command: &str) -> Option<(String, String)> {
    let command = command.trim();

    let (cmd, rest) = if let Some(r) = command.strip_prefix("git-upload-pack ") {
        ("git-upload-pack", r)
    } else if let Some(r) = command.strip_prefix("git-receive-pack ") {
        ("git-receive-pack", r)
    } else if let Some(r) = command.strip_prefix("git upload-pack ") {
        ("git-upload-pack", r)
    } else if let Some(r) = command.strip_prefix("git receive-pack ") {
        ("git-receive-pack", r)
    } else if let Some(r) = command.strip_prefix("git-lfs-authenticate ") {
        ("git-lfs-authenticate", r)
    } else {
        return None;
    };

    // For git-lfs-authenticate, preserve the full argument string (path + operation).
    // For regular git commands, trim quotes from the path.
    let path = if cmd == "git-lfs-authenticate" {
        rest.trim().to_string()
    } else {
        rest.trim().trim_matches('\'').trim_matches('"').to_string()
    };
    Some((cmd.to_string(), path))
}

/// Extract `(namespace, repo_name)` from `/ns/repo.git` or `ns/repo.git`.
///
/// Validates each path segment to prevent path traversal attacks:
/// - Rejects `..`, `.`, empty segments, null bytes, slashes, whitespace
/// - Rejects leading/trailing hyphens
/// - Enforces max segment length of 255 chars
pub fn parse_repo_path(path: &str) -> Option<(String, String)> {
    let path = path.trim_start_matches('/');
    let mut parts = path.splitn(2, '/');
    let namespace = parts.next()?.to_string();
    let repo_raw = parts.next()?;
    let repo = repo_raw
        .strip_suffix(".git")
        .unwrap_or(repo_raw)
        .to_string();

    if namespace.is_empty() || repo.is_empty() {
        return None;
    }

    // Validate segments to prevent path traversal
    if !validate_ssh_path_segment(&namespace) || !validate_ssh_path_segment(&repo) {
        return None;
    }

    Some((namespace, repo))
}

/// Parse the LFS authenticate path: `<repo_path> upload|download`
/// Returns `(repo_path, operation)`.
pub fn parse_lfs_authenticate_args(args: &str) -> Option<(String, String)> {
    let args = args.trim();
    // The last token is the operation, everything before is the repo path.
    let last_space = args.rfind(' ')?;
    let repo_path = args[..last_space]
        .trim()
        .trim_matches('\'')
        .trim_matches('"');
    let operation = args[last_space + 1..].trim();
    if operation != "upload" && operation != "download" {
        return None;
    }
    if repo_path.is_empty() {
        return None;
    }
    Some((repo_path.to_string(), operation.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_git_ssh_command_upload_pack() {
        let (cmd, path) = parse_git_ssh_command("git-upload-pack '/acme/my-repo.git'").unwrap();
        assert_eq!(cmd, "git-upload-pack");
        assert_eq!(path, "/acme/my-repo.git");
    }

    #[test]
    fn test_parse_git_ssh_command_receive_pack_no_slash() {
        let (cmd, path) = parse_git_ssh_command("git-receive-pack 'acme/my-repo.git'").unwrap();
        assert_eq!(cmd, "git-receive-pack");
        assert_eq!(path, "acme/my-repo.git");
    }

    #[test]
    fn test_parse_git_ssh_command_space_form() {
        let (cmd, path) = parse_git_ssh_command("git receive-pack '/acme/my-repo.git'").unwrap();
        assert_eq!(cmd, "git-receive-pack");
        assert_eq!(path, "/acme/my-repo.git");
    }

    #[test]
    fn test_parse_git_ssh_command_unknown() {
        assert!(parse_git_ssh_command("ls -la").is_none());
    }

    #[test]
    fn test_parse_repo_path_with_leading_slash() {
        let (ns, repo) = parse_repo_path("/acme/my-repo.git").unwrap();
        assert_eq!(ns, "acme");
        assert_eq!(repo, "my-repo");
    }

    #[test]
    fn test_parse_repo_path_no_leading_slash() {
        let (ns, repo) = parse_repo_path("acme/my-repo.git").unwrap();
        assert_eq!(ns, "acme");
        assert_eq!(repo, "my-repo");
    }

    #[test]
    fn test_parse_repo_path_no_git_suffix() {
        let (ns, repo) = parse_repo_path("/acme/my-repo").unwrap();
        assert_eq!(ns, "acme");
        assert_eq!(repo, "my-repo");
    }

    #[test]
    fn test_parse_repo_path_invalid() {
        assert!(parse_repo_path("no-slash").is_none());
        assert!(parse_repo_path("").is_none());
    }

    // Security: path traversal tests
    #[test]
    fn test_parse_repo_path_rejects_dot_dot_namespace() {
        assert!(parse_repo_path("../etc/passwd").is_none());
    }

    #[test]
    fn test_parse_repo_path_rejects_dot_dot_repo() {
        assert!(parse_repo_path("acme/../../etc").is_none());
    }

    #[test]
    fn test_parse_repo_path_rejects_null_bytes() {
        assert!(parse_repo_path("acme/repo\0evil").is_none());
    }

    #[test]
    fn test_parse_repo_path_rejects_dot_namespace() {
        assert!(parse_repo_path("./repo").is_none());
    }

    #[test]
    fn test_parse_repo_path_rejects_whitespace() {
        assert!(parse_repo_path("acme/my repo").is_none());
    }

    #[test]
    fn test_parse_repo_path_rejects_leading_hyphen() {
        assert!(parse_repo_path("-acme/repo").is_none());
    }

    // -- git-lfs-authenticate --

    #[test]
    fn test_parse_git_ssh_command_lfs_authenticate() {
        let (cmd, args) =
            parse_git_ssh_command("git-lfs-authenticate '/acme/my-repo.git' upload").unwrap();
        assert_eq!(cmd, "git-lfs-authenticate");
        assert_eq!(args, "'/acme/my-repo.git' upload");
        // Full parsing via parse_lfs_authenticate_args
        let (repo, op) = parse_lfs_authenticate_args(&args).unwrap();
        assert_eq!(repo, "/acme/my-repo.git");
        assert_eq!(op, "upload");
    }

    #[test]
    fn test_parse_lfs_authenticate_args_upload() {
        let (repo, op) = parse_lfs_authenticate_args("'/acme/my-repo.git' upload").unwrap();
        assert_eq!(repo, "/acme/my-repo.git");
        assert_eq!(op, "upload");
    }

    #[test]
    fn test_parse_lfs_authenticate_args_download() {
        let (repo, op) = parse_lfs_authenticate_args("/acme/my-repo.git download").unwrap();
        assert_eq!(repo, "/acme/my-repo.git");
        assert_eq!(op, "download");
    }

    #[test]
    fn test_parse_lfs_authenticate_args_invalid_op() {
        assert!(parse_lfs_authenticate_args("/acme/repo.git fetch").is_none());
    }

    #[test]
    fn test_parse_lfs_authenticate_args_empty() {
        assert!(parse_lfs_authenticate_args("").is_none());
    }
}
