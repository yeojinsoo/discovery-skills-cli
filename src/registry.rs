use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config;

// ---------------------------------------------------------------------------
// Data structures matching registry.toml layout
// ---------------------------------------------------------------------------

/// Top-level registry structure parsed from registry.toml.
///
/// ```toml
/// [metadata]
/// repo = "yeojinsoo/discovery-skills-registry"
///
/// [skills.logical-analysis]
/// version = "1.0.0"
/// description = "..."
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    pub metadata: RegistryMetadata,
    pub skills: HashMap<String, SkillEntry>,
}

/// Metadata section of the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryMetadata {
    pub repo: String,
}

/// A dependency reference: which skill is required and at what version it was tested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub name: String,
    pub ref_version: String,
}

/// A single skill entry inside `[skills.<name>]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillEntry {
    pub version: String,
    pub description: String,
    #[serde(default)]
    pub depends_on: Vec<Dependency>,
}

/// Flattened skill info returned to callers (name is the map key).
#[derive(Debug, Clone)]
pub struct SkillInfo {
    pub name: String,
    pub version: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build a reusable HTTP client with connect and response timeouts.
fn http_client() -> Result<reqwest::blocking::Client, Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()?;
    Ok(client)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Fetch the registry index from the remote GitHub raw URL and parse it.
pub fn fetch_registry() -> Result<Registry, Box<dyn std::error::Error>> {
    let url = config::registry_raw_url();
    let client = http_client()?;
    let response = client.get(&url).send()?;
    if !response.status().is_success() {
        return Err(format!("Failed to fetch registry: HTTP {}", response.status()).into());
    }
    let body = response.text()?;
    let registry: Registry = toml::from_str(&body)?;
    Ok(registry)
}

/// Parse a registry from a TOML string (useful for testing).
#[cfg(test)]
pub fn parse_registry(toml_str: &str) -> Result<Registry, Box<dyn std::error::Error>> {
    let registry: Registry = toml::from_str(toml_str)?;
    Ok(registry)
}

/// Return a sorted list of all available skills in the registry.
pub fn list_available_skills(registry: &Registry) -> Vec<SkillInfo> {
    let mut skills: Vec<SkillInfo> = registry
        .skills
        .iter()
        .map(|(name, entry)| SkillInfo {
            name: name.clone(),
            version: entry.version.clone(),
            description: entry.description.clone(),

        })
        .collect();
    skills.sort_by(|a, b| a.name.cmp(&b.name));
    skills
}

/// Find a specific skill by name.
pub fn find_skill(registry: &Registry, name: &str) -> Option<SkillInfo> {
    registry.skills.get(name).map(|entry| SkillInfo {
        name: name.to_string(),
        version: entry.version.clone(),
        description: entry.description.clone(),
    })
}

/// Download a skill release archive from GitHub Releases, decompress, and
/// extract into `target_dir/{name}/`.
///
/// The archive is expected to be a `.tar.gz` containing the skill files.
// TODO: Add sha256 checksum verification when registry.toml supports it
pub fn download_skill(
    name: &str,
    version: &str,
    target_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = config::release_download_url(name, version);

    let client = http_client()?;
    let response = client.get(&url).send()?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to download skill '{}' v{}: HTTP {}",
            name,
            version,
            response.status()
        )
        .into());
    }

    let bytes = response.bytes()?;
    println!(
        "  Downloaded '{}' v{} ({} bytes)",
        name,
        version,
        bytes.len()
    );

    // Decompress gzip → tar, filtering out registry-only files
    let gz_decoder = flate2::read::GzDecoder::new(bytes.as_ref());
    let mut archive = tar::Archive::new(gz_decoder);

    let dest = target_dir.join(name);
    std::fs::create_dir_all(&dest)?;

    // Skip files that belong to the registry only (e.g., CHANGELOG.md)
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            if file_name == "CHANGELOG.md" {
                continue;
            }
        }
        entry.unpack_in(&dest)?;
    }

    Ok(())
}

/// Download multiple skills in parallel. Returns a Vec of (name, version, Result).
///
/// Each skill is downloaded in its own thread using `std::thread::scope`.
/// A failure in one download does not affect the others.
/// The error type is `String` because `Box<dyn Error>` from `download_skill` is not `Send`.
pub fn download_skills_parallel(
    skills: &[(&str, &str)],
    target_dir: &Path,
) -> Vec<(String, String, Result<(), String>)> {
    std::thread::scope(|s| {
        let handles: Vec<_> = skills
            .iter()
            .map(|&(name, version)| {
                let target = target_dir.to_path_buf();
                s.spawn(move || {
                    let result = download_skill(name, version, &target);
                    (
                        name.to_string(),
                        version.to_string(),
                        result.map_err(|e| e.to_string()),
                    )
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_TOML: &str = r#"
[metadata]
repo = "yeojinsoo/discovery-skills-registry"

[skills.logical-analysis]
version = "1.0.0"
description = "Logical analysis skill"

[skills.project-planner]
version = "2.0.0"
description = "Project planning skill"
"#;

    #[test]
    fn test_parse_registry() {
        let registry = parse_registry(SAMPLE_TOML).expect("Failed to parse TOML");

        // metadata
        assert_eq!(registry.metadata.repo, "yeojinsoo/discovery-skills-registry");

        // skills count
        assert_eq!(registry.skills.len(), 2);

        // individual skill
        let la = registry.skills.get("logical-analysis").unwrap();
        assert_eq!(la.version, "1.0.0");
        assert_eq!(la.description, "Logical analysis skill");

        let pp = registry.skills.get("project-planner").unwrap();
        assert_eq!(pp.version, "2.0.0");
        assert_eq!(pp.description, "Project planning skill");
    }

    #[test]
    fn test_list_available_skills() {
        let registry = parse_registry(SAMPLE_TOML).unwrap();
        let skills = list_available_skills(&registry);

        assert_eq!(skills.len(), 2);
        // sorted alphabetically
        assert_eq!(skills[0].name, "logical-analysis");
        assert_eq!(skills[1].name, "project-planner");
    }

    #[test]
    fn test_find_skill_exists() {
        let registry = parse_registry(SAMPLE_TOML).unwrap();

        let found = find_skill(&registry, "logical-analysis");
        assert!(found.is_some());
        let info = found.unwrap();
        assert_eq!(info.name, "logical-analysis");
        assert_eq!(info.version, "1.0.0");
    }

    #[test]
    fn test_find_skill_not_exists() {
        let registry = parse_registry(SAMPLE_TOML).unwrap();
        let found = find_skill(&registry, "nonexistent-skill");
        assert!(found.is_none());
    }

    #[test]
    #[ignore] // Requires network access
    fn test_fetch_registry() {
        let registry = fetch_registry().expect("Failed to fetch registry from GitHub");

        // Basic structural assertions
        assert_eq!(registry.metadata.repo, "yeojinsoo/discovery-skills-registry");
        assert!(!registry.skills.is_empty(), "Registry should have at least one skill");

        // We know logical-analysis exists in the real registry
        let la = registry.skills.get("logical-analysis");
        assert!(la.is_some(), "logical-analysis should exist in registry");
    }
}
