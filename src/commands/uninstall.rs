use std::io::{self, Write};

use crate::config;
use crate::lockfile::Lockfile;

/// Uninstall an installed skill.
/// If `name` is Some, uninstall that specific skill.
/// If `name` is None, uninstall all installed skills after confirmation.
pub fn run(name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
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
                println!("No skills are installed.");
                return Ok(());
            }

            println!("Installed skills:");
            for s in &installed {
                let info = lockfile.get_skill(s).unwrap();
                println!("  - {} (v{})", s, info.version);
            }
            println!();
            if !confirm("Uninstall all installed skills?")? {
                println!("Cancelled.");
                return Ok(());
            }
            installed
        }
    };

    let skills_dir = config::skills_dir();
    let mut removed_count = 0;

    for skill_name in &targets {
        let skill_path = skills_dir.join(skill_name);
        if skill_path.exists() {
            std::fs::remove_dir_all(&skill_path)?;
            println!("Removed directory: {}", skill_path.display());
        }
        lockfile.remove_skill(skill_name);
        removed_count += 1;
        println!("Uninstalled '{}'.", skill_name);
    }

    lockfile.save(&lockfile_path)?;

    println!("\nDone. {} skill(s) uninstalled.", removed_count);

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
