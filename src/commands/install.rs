use std::collections::HashSet;

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

    let lockfile_path = config::lockfile_path()?;
    let mut lockfile = Lockfile::load(&lockfile_path)?;
    let skills_dir = config::skills_dir()?;

    if name.is_none() {
        run_install_all_parallel(&targets, &reg, &mut lockfile, &lockfile_path, &skills_dir)
    } else {
        run_install_single(&targets, &reg, &mut lockfile, &lockfile_path, &skills_dir)
    }
}

/// Install all skills in parallel (used when name is None / "install all" mode).
///
/// 1. Collect all skills + deps that need downloading (dedup, skip already installed).
/// 2. Download them all in parallel.
/// 3. Process results sequentially, updating the lockfile after each success.
fn run_install_all_parallel(
    targets: &[registry::SkillInfo],
    reg: &registry::Registry,
    lockfile: &mut Lockfile,
    lockfile_path: &std::path::Path,
    skills_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Phase 1: Collect all (name, version) pairs to download.
    // We need to preserve install order for lockfile updates, but download in parallel.
    // Use an ordered list for the final processing and a set for dedup.
    let mut to_download: Vec<(String, String)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    // Track which targets had unresolvable deps so we skip them later
    let mut skip_targets: HashSet<String> = HashSet::new();

    for skill in targets {
        if lockfile.get_skill(&skill.name).is_some() {
            println!(
                "'{}' 건너뜀: 이미 설치됨 (v{})",
                skill.name,
                lockfile.get_skill(&skill.name).unwrap().version
            );
            let drifts = deps::check_version_drift(&skill.name, reg, lockfile);
            for d in &drifts {
                println!(
                    "\u{26a0} '{}' 설치 버전(v{})이 참조 버전(v{})과 다릅니다. 호환되지 않을 수 있습니다.",
                    d.dep_name, d.installed_version, d.ref_version
                );
            }
            continue;
        }

        // Resolve missing dependencies
        let missing = deps::resolve_install_deps(&skill.name, reg, lockfile);

        // Check for deps not in registry
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
            skip_targets.insert(skill.name.clone());
            continue;
        }

        // Add deps first (in topological order), then the skill itself
        for dep in &missing {
            if !seen.contains(&dep.name) && lockfile.get_skill(&dep.name).is_none() {
                seen.insert(dep.name.clone());
                to_download.push((dep.name.clone(), dep.registry_version.clone()));
            }
        }
        if !seen.contains(&skill.name) {
            seen.insert(skill.name.clone());
            to_download.push((skill.name.clone(), skill.version.clone()));
        }
    }

    if to_download.is_empty() {
        println!("\n새로 설치된 스킬이 없습니다.");
        return Ok(());
    }

    // Phase 2: Download all in parallel
    println!("\n{}개 스킬 동시 다운로드 중...", to_download.len());
    let download_refs: Vec<(&str, &str)> = to_download
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    let results = registry::download_skills_parallel(&download_refs, skills_dir);

    // Phase 3: Process results sequentially in the original order.
    // Build a lookup from name -> result for quick access.
    let mut result_map: std::collections::HashMap<String, Result<(), String>> =
        std::collections::HashMap::new();
    for (rname, _rversion, rresult) in results {
        result_map.insert(rname, rresult);
    }

    let mut installed_count = 0;
    // Track which skills failed so we can skip their dependents
    let mut failed: HashSet<String> = HashSet::new();

    for (dl_name, dl_version) in &to_download {
        // Check if any of this skill's deps failed
        if let Some(entry) = reg.skills.get(dl_name) {
            let dep_failed = entry.depends_on.iter().any(|d| failed.contains(&d.name));
            if dep_failed {
                eprintln!(
                    "'{}' 스킬의 의존성 설치에 실패하여 건너뜁니다.",
                    dl_name
                );
                failed.insert(dl_name.clone());
                continue;
            }
        }

        match result_map.remove(dl_name) {
            Some(Ok(())) => {
                let locked_deps = deps::to_locked_deps(reg, dl_name);
                lockfile.add_skill(dl_name, dl_version, locked_deps);
                lockfile.save(lockfile_path)?;
                installed_count += 1;
                println!("  '{}' 설치 완료.", dl_name);
            }
            Some(Err(e)) => {
                eprintln!(
                    "오류: '{}' v{} 다운로드 실패: {}",
                    dl_name, dl_version, e
                );
                failed.insert(dl_name.clone());
            }
            None => {
                // Should not happen, but handle gracefully
                eprintln!("오류: '{}' 다운로드 결과를 찾을 수 없습니다.", dl_name);
                failed.insert(dl_name.clone());
            }
        }
    }

    // Check version drifts for all successfully installed targets
    for skill in targets {
        if skip_targets.contains(&skill.name) || failed.contains(&skill.name) {
            continue;
        }
        if lockfile.get_skill(&skill.name).is_some() {
            let drifts = deps::check_version_drift(&skill.name, reg, lockfile);
            for d in &drifts {
                println!(
                    "\u{26a0} '{}' 설치 버전(v{})이 참조 버전(v{})과 다릅니다. 호환되지 않을 수 있습니다.",
                    d.dep_name, d.installed_version, d.ref_version
                );
            }
        }
    }

    if installed_count > 0 {
        println!("\n완료. {}개 스킬 설치됨.", installed_count);
    } else {
        println!("\n새로 설치된 스킬이 없습니다.");
    }

    Ok(())
}

/// Install a single skill sequentially (original behavior for single-skill install).
fn run_install_single(
    targets: &[registry::SkillInfo],
    reg: &registry::Registry,
    lockfile: &mut Lockfile,
    lockfile_path: &std::path::Path,
    skills_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut installed_count = 0;
    for skill in targets {
        if let Some(existing) = lockfile.get_skill(&skill.name) {
            println!(
                "'{}' 건너뜀: 이미 설치됨 (v{})",
                skill.name, existing.version
            );
            let drifts = deps::check_version_drift(&skill.name, reg, lockfile);
            for d in &drifts {
                println!(
                    "\u{26a0} '{}' 설치 버전(v{})이 참조 버전(v{})과 다릅니다. 호환되지 않을 수 있습니다.",
                    d.dep_name, d.installed_version, d.ref_version
                );
            }
            continue;
        }

        // Resolve missing dependencies (BFS, transitive)
        let missing = deps::resolve_install_deps(&skill.name, reg, lockfile);

        // Check for deps not in registry -- if any, skip this skill entirely
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

        if !missing.is_empty() {
            // Ask user to confirm installing dependencies
            println!(
                "'{}'은(는) 다음 스킬에 의존합니다:",
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
        let mut dep_failed = false;
        for dep in &missing {
            if lockfile.get_skill(&dep.name).is_some() {
                continue; // may have been installed as dep of a previous skill
            }
            println!("'{}' v{} 설치 중...", dep.name, dep.registry_version);
            match registry::download_skill(&dep.name, &dep.registry_version, skills_dir) {
                Ok(()) => {
                    let locked_deps = deps::to_locked_deps(reg, &dep.name);
                    lockfile.add_skill(&dep.name, &dep.registry_version, locked_deps);
                    lockfile.save(lockfile_path)?;
                    installed_count += 1;
                    println!("  '{}' 설치 완료.", dep.name);
                }
                Err(e) => {
                    eprintln!(
                        "오류: '{}' v{} 다운로드 실패: {}",
                        dep.name, dep.registry_version, e
                    );
                    dep_failed = true;
                    break;
                }
            }
        }

        if dep_failed {
            eprintln!(
                "'{}' 스킬의 의존성 설치에 실패하여 건너뜁니다.",
                skill.name
            );
            continue;
        }

        // Check version drift for already-installed dependencies
        let drifts = deps::check_version_drift(&skill.name, reg, lockfile);
        for d in &drifts {
            println!(
                "\u{26a0} '{}' 설치 버전(v{})이 참조 버전(v{})과 다릅니다. 호환되지 않을 수 있습니다.",
                d.dep_name, d.installed_version, d.ref_version
            );
        }

        // Install the target skill itself
        println!("'{}' v{} 설치 중...", skill.name, skill.version);
        match registry::download_skill(&skill.name, &skill.version, skills_dir) {
            Ok(()) => {
                let locked_deps = deps::to_locked_deps(reg, &skill.name);
                lockfile.add_skill(&skill.name, &skill.version, locked_deps);
                lockfile.save(lockfile_path)?;
                installed_count += 1;
                println!("  '{}' 설치 완료.", skill.name);
            }
            Err(e) => {
                eprintln!(
                    "오류: '{}' v{} 다운로드 실패: {}",
                    skill.name, skill.version, e
                );
            }
        }
    }

    if installed_count > 0 {
        println!("\n완료. {}개 스킬 설치됨.", installed_count);
    } else {
        println!("\n새로 설치된 스킬이 없습니다.");
    }

    Ok(())
}
