// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `luat.toml` parsing: the `[package]` table and `[dependencies]`.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::validation;

/// The parts of a package's `luat.toml` the registry stores and serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub license: Option<String>,
    pub repository: Option<String>,
    pub keywords: Vec<String>,
    /// Semver requirement against the luat version (`luat = ">=0.2"`).
    pub luat: Option<String>,
    /// Dependency name → semver requirement, sorted by name.
    pub dependencies: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct RawManifest {
    package: Option<RawPackage>,
    #[serde(default)]
    dependencies: BTreeMap<String, toml::Value>,
}

#[derive(Deserialize)]
struct RawPackage {
    name: Option<String>,
    version: Option<String>,
    description: Option<String>,
    license: Option<String>,
    repository: Option<String>,
    #[serde(default)]
    keywords: Vec<String>,
    luat: Option<String>,
}

/// Parse and validate `luat.toml`. Every error is a client error (HTTP 400).
pub fn parse(source: &str) -> Result<Manifest, String> {
    let raw: RawManifest =
        toml::from_str(source).map_err(|e| format!("luat.toml is not valid: {e}"))?;
    let pkg = raw
        .package
        .ok_or("luat.toml has no [package] table".to_string())?;
    let name = pkg.name.ok_or("luat.toml [package] has no name")?;
    let version = pkg.version.ok_or("luat.toml [package] has no version")?;
    validation::parse_full_name(&name).map_err(|e| e.to_string())?;
    validation::parse_version(&version).map_err(|e| e.to_string())?;
    if let Some(req) = &pkg.luat {
        validation::validate_requirement(req)
            .map_err(|e| format!("luat.toml [package] luat: {e}"))?;
    }

    let mut dependencies = BTreeMap::new();
    for (dep, value) in raw.dependencies {
        validation::parse_full_name(&dep).map_err(|e| format!("dependency: {e}"))?;
        let req = value
            .as_str()
            .ok_or_else(|| format!("dependency `{dep}` must be a version requirement string"))?;
        validation::validate_requirement(req).map_err(|e| format!("dependency `{dep}`: {e}"))?;
        if dep == name {
            return Err(format!("package `{name}` depends on itself"));
        }
        dependencies.insert(dep, req.to_string());
    }

    Ok(Manifest {
        name,
        version,
        description: pkg.description,
        license: pkg.license,
        repository: pkg.repository,
        keywords: pkg.keywords,
        luat: pkg.luat,
        dependencies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
[package]
name = "@acme/ui"
version = "1.2.0"
description = "Cards, buttons and layouts"
license = "MIT"
repository = "https://example.com/acme/ui"
keywords = ["ui", "components"]
luat = ">=0.2"
include = ["src/**", "README.md"]

[dependencies]
"@acme/icons" = "^2.0"
"#;

    #[test]
    fn parses_the_spec_example() {
        let m = parse(FULL).unwrap();
        assert_eq!(m.name, "@acme/ui");
        assert_eq!(m.version, "1.2.0");
        assert_eq!(m.keywords, vec!["ui", "components"]);
        assert_eq!(m.luat.as_deref(), Some(">=0.2"));
        assert_eq!(m.dependencies["@acme/icons"], "^2.0");
    }

    #[test]
    fn minimal_manifest() {
        let m = parse("[package]\nname = \"@a/b\"\nversion = \"0.1.0\"\n").unwrap();
        assert!(m.dependencies.is_empty());
        assert!(m.description.is_none());
    }

    #[test]
    fn rejects_bad_manifests() {
        for bad in [
            "not toml [",
            "[dependencies]\n",
            "[package]\nversion = \"1.0.0\"\n",
            "[package]\nname = \"@a/b\"\n",
            "[package]\nname = \"a/b\"\nversion = \"1.0.0\"\n",
            "[package]\nname = \"@a/b\"\nversion = \"1.0\"\n",
            "[package]\nname = \"@a/b\"\nversion = \"1.0.0\"\nluat = \"??\"\n",
            "[package]\nname = \"@a/b\"\nversion = \"1.0.0\"\n[dependencies]\n\"Bad/Name\" = \"^1\"\n",
            "[package]\nname = \"@a/b\"\nversion = \"1.0.0\"\n[dependencies]\n\"@a/c\" = \"what\"\n",
            "[package]\nname = \"@a/b\"\nversion = \"1.0.0\"\n[dependencies]\n\"@a/c\" = 3\n",
            "[package]\nname = \"@a/b\"\nversion = \"1.0.0\"\n[dependencies]\n\"@a/b\" = \"^1\"\n",
        ] {
            assert!(parse(bad).is_err(), "should reject: {bad}");
        }
    }
}
