use crate::config;
use crate::deps;
use crate::lockfile::Lockfile;
use crate::ui;

/// Uninstall an installed skill.
/// If `name` is Some, uninstall that specific skill.
/// If `name` is None, uninstall all installed skills after confirmation.
pub fn run(name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let lockfile_path = config::lockfile_path();
    let mut lockfile = Lockfile::load(&lockfile_path)?;

    let targets: Vec<String> = match name {
        Some(n) => {
            if lockfile.get_skill(n).is_none() {
                return Err(format!("'{}' 스킬이 설치되어 있지 않습니다.", n).into());
            }

            // Use offline dependents check (no network needed)
            let dependents = deps::find_dependents_offline(n, &lockfile);

            let mut removal_targets = vec![n.to_string()];

            if !dependents.is_empty() {
                println!("'{}'에 의존하는 스킬이 있습니다:", n);
                for dep in &dependents {
                    println!("  - {} (v{})", dep.name, dep.version);
                }
                if !ui::confirm("모두 함께 제거하시겠습니까?")? {
                    println!("취소되었습니다.");
                    return Ok(());
                }
                // Add dependents before the target so they are removed first
                let mut ordered = Vec::new();
                for dep in &dependents {
                    ordered.push(dep.name.clone());
                }
                ordered.push(n.to_string());
                removal_targets = ordered;
            }

            removal_targets
        }
        None => {
            let installed: Vec<String> = lockfile.skills.keys().cloned().collect();
            if installed.is_empty() {
                println!("설치된 스킬이 없습니다.");
                return Ok(());
            }

            println!("설치된 스킬:");
            for s in &installed {
                let info = lockfile.get_skill(s).unwrap();
                println!("  - {} (v{})", s, info.version);
            }
            println!();
            if !ui::confirm("설치된 모든 스킬을 제거하시겠습니까?")? {
                println!("취소되었습니다.");
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
            println!("디렉토리 제거: {}", skill_path.display());
        }
        lockfile.remove_skill(skill_name);
        removed_count += 1;
        println!("'{}' 제거 완료.", skill_name);
    }

    lockfile.save(&lockfile_path)?;

    println!("\n완료. {}개 스킬 제거됨.", removed_count);

    Ok(())
}
