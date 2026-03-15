// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end integration tests for muli-git.
//!
//! Tests are organized into focused submodules:
//! - `harness`               — shared test infrastructure (server, helpers)
//! - `api_tests`             — REST API tests (no git binary required)
//! - `git_protocol_tests`    — Git Smart HTTP: clone, push, refs, commits, blobs
//! - `git_fork_webhook_tests`— Fork and webhook delivery
//! - `git_branch_tag_tests`  — Branches, lightweight tags, annotated tags, tag REST API
//! - `pr_crud_tests`         — PR create, list, get, sequential numbering
//! - `pr_lifecycle_tests`    — PR close, merge, and full lifecycle
//! - `pr_diff_comment_tests` — PR structured diff and comments
//! - `pr_error_tests`        — PR error cases (conflict, invalid action, missing fields)
//! - `ssh_tests`             — SSH clone and push

#[path = "e2e/acl_tests.rs"]
mod acl_tests;
#[path = "e2e/api_tests.rs"]
mod api_tests;
#[path = "e2e/git_branch_tag_tests.rs"]
mod git_branch_tag_tests;
#[path = "e2e/git_fork_webhook_tests.rs"]
mod git_fork_webhook_tests;
#[path = "e2e/git_protocol_tests.rs"]
mod git_protocol_tests;
#[path = "e2e/harness.rs"]
mod harness;
#[path = "e2e/pr_crud_tests.rs"]
mod pr_crud_tests;
#[path = "e2e/pr_diff_comment_tests.rs"]
mod pr_diff_comment_tests;
#[path = "e2e/pr_error_tests.rs"]
mod pr_error_tests;
#[path = "e2e/pr_lifecycle_tests.rs"]
mod pr_lifecycle_tests;
#[path = "e2e/ssh_tests.rs"]
mod ssh_tests;
