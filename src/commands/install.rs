use std::io::{self, Write};

use crate::config;
use crate::lockfile::Lockfile;
use crate::registry;

/// Install a skill from the registry.
/// If `name` is Some, install that specific skill.
/// If `name` is None, show all available skills and ask for confirmation.
pub fn run(name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Fetching registry...");
    let reg = registry::fetch_registry()?;

    let targets: Vec<registry::SkillInfo> = match name {
        Some(n) => {
            let skill = registry::find_skill(&reg, n)
                .ok_or_else(|| format!("Skill '{}' not found in registry", n))?;
            vec![skill]
        }
        None => {
            let all = registry::list_available_skills(&reg);
            if all.is_empty() {
                println!("No skills available in registry.");
                return Ok(());
            }
            println!("\nAvailable skills:");
            for s in &all {
                println!("  - {} (v{}) : {}", s.name, s.version, s.description);
            }
            println!();
            if !confirm("Install all skills?")? {
                println!("Cancelled.");
                return Ok(());
            }
            all
        }
    };

    let lockfile_path = config::lockfile_path();
    let mut lockfile = Lockfile::load(&lockfile_path)?;
    let skills_dir = config::skills_dir();

    let mut installed_count = 0;
    for skill in &targets {
        if let Some(existing) = lockfile.get_skill(&skill.name) {
            println!(
                "Skipping '{}': already installed (v{})",
                skill.name, existing.version
            );
            continue;
        }

        println!("Installing '{}' v{}...", skill.name, skill.version);
        registry::download_skill(&skill.name, &skill.version, &skills_dir)?;
        lockfile.add_skill(&skill.name, &skill.version);
        installed_count += 1;
        println!("  Installed '{}'.", skill.name);
    }

    lockfile.save(&lockfile_path)?;

    if installed_count > 0 {
        println!("\nDone. {} skill(s) installed.", installed_count);
    } else {
        println!("\nNo new skills were installed.");
    }

    Ok(())
}

/// Prompt the user for y/n confirmation.
fn confirm(message: &str) -> Result<bool, Box<dyn std::error::Error>> {
    print!("{} (y/n): ", message);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let answer = input.trim().to_lowercase();
    Ok(answer == "y" || answer == "yes")
}
