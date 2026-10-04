// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Package listing and search over the per-package summaries.

use tokio::fs;

use crate::storage::{FilesystemStorage, StorageResult};

use super::storage::{PackageSummary, packages_root};

/// Largest page size a client may ask for.
pub const MAX_PER_PAGE: usize = 100;
pub const DEFAULT_PER_PAGE: usize = 20;

/// Read every package summary of a tenant.
pub async fn list_summaries(
    storage: &FilesystemStorage,
    tenant_id: &str,
) -> StorageResult<Vec<PackageSummary>> {
    let root = packages_root(storage, tenant_id);
    let mut out = Vec::new();
    let mut scopes = match fs::read_dir(&root).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    while let Some(scope) = scopes.next_entry().await? {
        if !scope.file_type().await?.is_dir() {
            continue;
        }
        let mut packages = fs::read_dir(scope.path()).await?;
        while let Some(pkg) = packages.next_entry().await? {
            let path = pkg.path().join("summary.json");
            if let Ok(bytes) = fs::read(&path).await
                && let Ok(summary) = serde_json::from_slice::<PackageSummary>(&bytes)
            {
                out.push(summary);
            }
        }
    }
    Ok(out)
}

/// Case-insensitive match on name, description and keywords.
fn matches(summary: &PackageSummary, needle: &str) -> bool {
    summary.name.to_lowercase().contains(needle)
        || summary
            .description
            .as_deref()
            .is_some_and(|d| d.to_lowercase().contains(needle))
        || summary
            .keywords
            .iter()
            .any(|k| k.to_lowercase().contains(needle))
}

/// Filter, order and paginate. Returns the page and the total match count.
///
/// Without a query every package is listed, most recently updated first. With
/// one, exact name matches come first, then the rest by recency.
pub fn search(
    mut all: Vec<PackageSummary>,
    query: Option<&str>,
    page: usize,
    per_page: usize,
) -> (Vec<PackageSummary>, usize) {
    let needle = query
        .map(|q| q.trim().to_lowercase())
        .filter(|q| !q.is_empty());
    if let Some(needle) = &needle {
        all.retain(|s| matches(s, needle));
    }
    all.sort_by(|a, b| {
        let exact = |s: &PackageSummary| {
            needle.as_deref().is_some_and(|n| {
                let lower = s.name.to_lowercase();
                lower == n || lower.rsplit('/').next() == Some(n)
            })
        };
        exact(b)
            .cmp(&exact(a))
            .then(b.updated_at.cmp(&a.updated_at))
            .then(a.name.cmp(&b.name))
    });
    let total = all.len();
    let per_page = per_page.clamp(1, MAX_PER_PAGE);
    let start = page.max(1).saturating_sub(1).saturating_mul(per_page);
    let page_items = all.into_iter().skip(start).take(per_page).collect();
    (page_items, total)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;

    fn summary(name: &str, desc: &str, keywords: &[&str], age_min: i64) -> PackageSummary {
        PackageSummary {
            name: name.into(),
            description: Some(desc.into()),
            latest: Some("1.0.0".into()),
            keywords: keywords.iter().map(|k| k.to_string()).collect(),
            updated_at: Utc::now() - Duration::minutes(age_min),
        }
    }

    fn sample() -> Vec<PackageSummary> {
        vec![
            summary("@acme/ui", "Cards and buttons", &["components"], 30),
            summary("@acme/icons", "SVG icons", &["ui"], 10),
            summary("@other/forms", "Form helpers", &["input"], 20),
        ]
    }

    #[test]
    fn no_query_lists_most_recent_first() {
        let (page, total) = search(sample(), None, 1, 20);
        assert_eq!(total, 3);
        let names: Vec<_> = page.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["@acme/icons", "@other/forms", "@acme/ui"]);
    }

    #[test]
    fn query_matches_name_description_and_keywords_case_insensitively() {
        let (page, total) = search(sample(), Some("UI"), 1, 20);
        assert_eq!(total, 2);
        // Exact name match first, keyword match after.
        assert_eq!(page[0].name, "@acme/ui");
        assert_eq!(page[1].name, "@acme/icons");
        assert_eq!(search(sample(), Some("helpers"), 1, 20).1, 1);
        assert_eq!(search(sample(), Some("nothing"), 1, 20).1, 0);
    }

    #[test]
    fn pagination() {
        let (page, total) = search(sample(), None, 2, 2);
        assert_eq!(total, 3);
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].name, "@acme/ui");
        assert!(search(sample(), None, 9, 2).0.is_empty());
        assert_eq!(search(sample(), None, 0, 1).0[0].name, "@acme/icons");
    }
}
