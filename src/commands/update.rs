use crate::config;
use crate::lockfile::Lockfile;
use crate::registry;

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
        if installed.version == registry_skill.version {
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

        // Remove old directory
        let skill_path = skills_dir.join(skill_name);
        if skill_path.exists() {
            std::fs::remove_dir_all(&skill_path)?;
        }

        // Download new version
        registry::download_skill(skill_name, &registry_skill.version, &skills_dir)?;
        lockfile.add_skill(skill_name, &registry_skill.version);
        updated_count += 1;
        println!("  Updated '{}'.", skill_name);
    }

    lockfile.save(&lockfile_path)?;

    if updated_count > 0 {
        println!("\nDone. {} skill(s) updated.", updated_count);
    } else {
        println!("\nAll skills are up to date.");
    }

    Ok(())
}
