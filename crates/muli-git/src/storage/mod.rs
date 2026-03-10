// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Git repository storage abstraction.

mod filesystem;
pub use filesystem::{FilesystemStorage, GitStorageError, check_git_available};
