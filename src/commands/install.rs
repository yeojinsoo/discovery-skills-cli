use crate::config;
use crate::deps;
use crate::lockfile::Lockfile;
use crate::registry;
use crate::ui;

/// Install a skill from the registry.
/// If `name` is Some, install that specific skill.
/// If `name` is None, show all available skills and ask for confirmation.
pub fn run(name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    println!("레지스트리 조회 중...");
    let reg = registry::fetch_registry()?;

    let targets: Vec<registry::SkillInfo> = match name {
        Some(n) => {
            let skill = registry::find_skill(&reg, n)
                .ok_or_else(|| format!("'{}' 스킬을 레지스트리에서 찾을 수 없습니다.", n))?;
            vec![skill]
        }
        None => {
            let all = registry::list_available_skills(&reg);
            if all.is_empty() {
                println!("레지스트리에 사용 가능한 스킬이 없습니다.");
                return Ok(());
            }
            println!("\n사용 가능한 스킬 목록:");
            for s in &all {
                println!("  - {} (v{}) : {}", s.name, s.version, s.description);
            }
            println!();
            if !ui::confirm("모든 스킬을 설치하시겠습니까?")? {
                println!("취소되었습니다.");
                return Ok(());
            }
            all
        }
    };

    let lockfile_path = config::lockfile_path();
    let mut lockfile = Lockfile::load(&lockfile_path)?;
    let skills_dir = config::skills_dir();

    let install_all = name.is_none();

    let mut installed_count = 0;
    for skill in &targets {
        if let Some(existing) = lockfile.get_skill(&skill.name) {
            println!(
                "'{}' 건너뜀: 이미 설치됨 (v{})",
                skill.name, existing.version
            );
            // Even if already installed, check version drift for its deps
            let drifts = deps::check_version_drift(&skill.name, &reg, &lockfile);
            for d in &drifts {
                println!(
                    "\u{26a0} '{}' 설치 버전(v{})이 참조 버전(v{})과 다릅니다. 호환되지 않을 수 있습니다.",
                    d.dep_name, d.installed_version, d.ref_version
                );
            }
            continue;
        }

        // Resolve missing dependencies (BFS, transitive)
        let missing = deps::resolve_install_deps(&skill.name, &reg, &lockfile);

        // Check for deps not in registry — if any, skip this skill entirely
        let has_unresolvable = missing.iter().any(|dep| !dep.in_registry);
        if has_unresolvable {
            for dep in missing.iter().filter(|d| !d.in_registry) {
                println!(
                    "오류: '{}' 스킬이 레지스트리에 존재하지 않습니다.",
                    dep.name
                );
            }
            println!(
                "'{}' 스킬의 의존성을 해결할 수 없어 설치를 건너뜁니다.",
                skill.name
            );
            continue;
        }

        if !missing.is_empty() && !install_all {
            // Ask user to confirm installing dependencies
            println!(
                "'{}'\u{c740}(\u{b294}) 다음 스킬에 의존합니다:",
                skill.name
            );
            for dep in &missing {
                println!("  - {} (ref: v{})", dep.name, dep.ref_version);
            }
            if !ui::confirm("함께 설치하시겠습니까?")? {
                println!("취소되었습니다.");
                continue;
            }
        }

        // Install missing dependencies
        for dep in &missing {
            if lockfile.get_skill(&dep.name).is_some() {
                continue; // may have been installed as dep of a previous skill
            }
            println!("'{}' v{} 설치 중...", dep.name, dep.registry_version);
            registry::download_skill(&dep.name, &dep.registry_version, &skills_dir)?;
            let locked_deps = deps::to_locked_deps(&reg, &dep.name);
            lockfile.add_skill(&dep.name, &dep.registry_version, locked_deps);
            installed_count += 1;
            println!("  '{}' 설치 완료.", dep.name);
        }

        // Check version drift for already-installed dependencies
        let drifts = deps::check_version_drift(&skill.name, &reg, &lockfile);
        for d in &drifts {
            println!(
                "\u{26a0} '{}' 설치 버전(v{})이 참조 버전(v{})과 다릅니다. 호환되지 않을 수 있습니다.",
                d.dep_name, d.installed_version, d.ref_version
            );
        }

        // Install the target skill itself
        println!("'{}' v{} 설치 중...", skill.name, skill.version);
        registry::download_skill(&skill.name, &skill.version, &skills_dir)?;
        let locked_deps = deps::to_locked_deps(&reg, &skill.name);
        lockfile.add_skill(&skill.name, &skill.version, locked_deps);
        installed_count += 1;
        println!("  '{}' 설치 완료.", skill.name);
    }

    lockfile.save(&lockfile_path)?;

    if installed_count > 0 {
        println!("\n완료. {}개 스킬 설치됨.", installed_count);
    } else {
        println!("\n새로 설치된 스킬이 없습니다.");
    }

    Ok(())
}
