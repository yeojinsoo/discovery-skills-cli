use crate::config;
use crate::lockfile::Lockfile;
use crate::registry;

/// Compare two version strings using semver when possible.
/// Falls back to string comparison if either version is not valid semver.
fn versions_equal(a: &str, b: &str) -> bool {
    match (semver::Version::parse(a), semver::Version::parse(b)) {
        (Ok(va), Ok(vb)) => va == vb,
        _ => a == b, // fallback to string comparison
    }
}

/// Update installed skills.
/// If `name` is Some, update only that skill.
/// If `name` is None, update all installed skills.
pub fn run(name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Fetching registry...");
    let reg = registry::fetch_registry()?;

    let lockfile_path = config::lockfile_path();
    let mut lockfile = Lockfile::load(&lockfile_path)?;

    let targets: Vec<String> = match name {
        Some(n) => {
            if lockfile.get_skill(n).is_none() {
                return Err(format!("Skill '{}' is not installed.", n).into());
            }
            vec![n.to_string()]
        }
        None => {
            let installed: Vec<String> = lockfile.skills.keys().cloned().collect();
            if installed.is_empty() {
                println!("No skills are installed. Nothing to update.");
                return Ok(());
            }
            installed
        }
    };

    let skills_dir = config::skills_dir();
    let mut updated_count = 0;

    for skill_name in &targets {
        let registry_skill = match registry::find_skill(&reg, skill_name) {
            Some(s) => s,
            None => {
                eprintln!(
                    "Warning: '{}' not found in registry, skipping.",
                    skill_name
                );
                continue;
            }
        };

        let installed = lockfile.get_skill(skill_name).unwrap();
        if versions_equal(&installed.version, &registry_skill.version) {
            println!(
                "'{}' is already up to date (v{}).",
                skill_name, installed.version
            );
            continue;
        }

        println!(
            "Updating '{}': v{} -> v{}...",
            skill_name, installed.version, registry_skill.version
        );

        let skill_path = skills_dir.join(skill_name);
        let staging_dir = skills_dir.join(format!(".{}-staging", skill_name));

        // Clean up any leftover staging directory from a previous failed attempt
        if staging_dir.exists() {
            std::fs::remove_dir_all(&staging_dir)?;
        }

        // Download new version to staging directory first (atomic update).
        // download_skill creates staging_dir/{skill_name}/ internally.
        match registry::download_skill(skill_name, &registry_skill.version, &staging_dir) {
            Ok(()) => {
                // Download succeeded — swap directories
                let downloaded = staging_dir.join(skill_name);
                if skill_path.exists() {
                    std::fs::remove_dir_all(&skill_path)?;
                }
                std::fs::rename(&downloaded, &skill_path)?;
                // Remove the now-empty staging directory
                let _ = std::fs::remove_dir_all(&staging_dir);
                lockfile.add_skill(skill_name, &registry_skill.version);
                updated_count += 1;
                println!("  Updated '{}'.", skill_name);
            }
            Err(e) => {
                // Download failed — clean up staging, preserve existing skill
                eprintln!(
                    "Error: failed to download '{}' v{}: {}",
                    skill_name, registry_skill.version, e
                );
                if staging_dir.exists() {
                    let _ = std::fs::remove_dir_all(&staging_dir);
                }
                eprintln!("  Existing installation preserved.");
            }
        }
    }

    lockfile.save(&lockfile_path)?;

    if updated_count > 0 {
        println!("\nDone. {} skill(s) updated.", updated_count);
    } else {
        println!("\nAll skills are up to date.");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versions_equal_semver() {
        assert!(versions_equal("1.0.0", "1.0.0"));
        assert!(!versions_equal("1.0.0", "1.0.1"));
        assert!(!versions_equal("1.0.0", "2.0.0"));
    }

    #[test]
    fn test_versions_equal_fallback() {
        // Non-semver strings fall back to string comparison
        assert!(versions_equal("abc", "abc"));
        assert!(!versions_equal("abc", "def"));
    }

    #[test]
    fn test_versions_equal_mixed() {
        // One valid semver, one not — falls back to string comparison
        assert!(!versions_equal("1.0.0", "latest"));
        assert!(versions_equal("latest", "latest"));
    }
}
