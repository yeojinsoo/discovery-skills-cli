use crate::config;
use crate::deps;
use crate::lockfile::Lockfile;
use crate::registry;
use crate::ui;

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
    println!("레지스트리 조회 중...");
    let reg = registry::fetch_registry()?;

    let lockfile_path = config::lockfile_path()?;
    let mut lockfile = Lockfile::load(&lockfile_path)?;

    let targets: Vec<String> = match name {
        Some(n) => {
            if lockfile.get_skill(n).is_none() {
                return Err(format!("'{}' 스킬이 설치되어 있지 않습니다.", n).into());
            }
            vec![n.to_string()]
        }
        None => {
            let installed: Vec<String> = lockfile.skills.keys().cloned().collect();
            if installed.is_empty() {
                println!("설치된 스킬이 없습니다.");
                return Ok(());
            }
            installed
        }
    };

    let skills_dir = config::skills_dir()?;

    if name.is_none() {
        run_update_all_parallel(&targets, &reg, &mut lockfile, &lockfile_path, &skills_dir)
    } else {
        run_update_single(&targets, &reg, &mut lockfile, &lockfile_path, &skills_dir)
    }
}

/// Update all installed skills in parallel (used when name is None / "update all" mode).
///
/// 1. Filter skills that actually need updating (version differs).
/// 2. Check dependency drift warnings (skip interactive prompts in bulk mode).
/// 3. Download all to staging dirs in parallel.
/// 4. Process atomic swap results sequentially, updating lockfile.
fn run_update_all_parallel(
    targets: &[String],
    reg: &registry::Registry,
    lockfile: &mut Lockfile,
    lockfile_path: &std::path::Path,
    skills_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Phase 1: Collect skills that need updating.
    // Each entry is (skill_name, installed_version, registry_version, staging_dir).
    let mut to_update: Vec<(String, String, String, std::path::PathBuf)> = Vec::new();

    for skill_name in targets {
        let registry_skill = match registry::find_skill(reg, skill_name) {
            Some(s) => s,
            None => {
                eprintln!(
                    "경고: '{}' 스킬이 레지스트리에 없습니다. 건너뜁니다.",
                    skill_name
                );
                continue;
            }
        };

        let installed = lockfile.get_skill(skill_name).unwrap();
        if versions_equal(&installed.version, &registry_skill.version) {
            println!(
                "'{}' 이미 최신 버전입니다 (v{}).",
                skill_name, installed.version
            );
            continue;
        }

        println!(
            "'{}' 업데이트 예정: v{} \u{2192} v{}",
            skill_name, installed.version, registry_skill.version
        );

        let staging_dir = skills_dir.join(format!(".{}-staging", skill_name));

        // Clean up any leftover staging directory from a previous failed attempt
        match std::fs::remove_dir_all(&staging_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                eprintln!("경고: 이전 스테이징 디렉토리 정리 실패: {}", e);
            }
        }

        to_update.push((
            skill_name.clone(),
            installed.version.clone(),
            registry_skill.version.clone(),
            staging_dir,
        ));
    }

    if to_update.is_empty() {
        println!("\n모든 스킬이 최신 버전입니다.");
        return Ok(());
    }

    // Phase 2: Download all to staging dirs in parallel.
    // Build (name, version) pairs pointing to each skill's staging dir.
    // Since download_skills_parallel takes a single target_dir, but each update
    // needs its own staging dir, we use per-skill staging dirs.
    println!("\n{}개 스킬 동시 다운로드 중...", to_update.len());
    let download_refs: Vec<(&str, &str, &std::path::Path)> = to_update
        .iter()
        .map(|(name, _old_ver, new_ver, staging)| (name.as_str(), new_ver.as_str(), staging.as_path()))
        .collect();

    // Use std::thread::scope for parallel downloads to individual staging dirs
    let download_results: Vec<(String, String, Result<(), String>)> = std::thread::scope(|s| {
        let handles: Vec<_> = download_refs
            .iter()
            .map(|&(name, version, staging_dir)| {
                s.spawn(move || {
                    let result = registry::download_skill(name, version, staging_dir);
                    (
                        name.to_string(),
                        version.to_string(),
                        result.map_err(|e| e.to_string()),
                    )
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Build a lookup from name -> download result
    let mut result_map: std::collections::HashMap<String, Result<(), String>> =
        std::collections::HashMap::new();
    for (rname, _rversion, rresult) in download_results {
        result_map.insert(rname, rresult);
    }

    // Phase 3: Process results sequentially — atomic swap for each.
    let mut updated_count = 0;

    for (skill_name, _old_version, new_version, staging_dir) in &to_update {
        let download_result = result_map.remove(skill_name).unwrap();

        match download_result {
            Ok(()) => {
                // Download succeeded — backup-swap to prevent data loss
                let skill_path = skills_dir.join(skill_name);
                let downloaded = staging_dir.join(skill_name);
                let backup_path = skills_dir.join(format!(".{}-backup", skill_name));

                // Step 1: Move existing skill to backup (if it exists)
                let had_existing = skill_path.exists();
                if had_existing {
                    // Clean up any stale backup from a previous run
                    if backup_path.exists() {
                        let _ = std::fs::remove_dir_all(&backup_path);
                    }
                    std::fs::rename(&skill_path, &backup_path)?;
                }

                // Step 2: Move downloaded skill into place
                match std::fs::rename(&downloaded, &skill_path) {
                    Ok(()) => {
                        // Step 3a: Success — remove backup
                        if had_existing {
                            let _ = std::fs::remove_dir_all(&backup_path);
                        }
                    }
                    Err(e) => {
                        // Step 3b: Failed — restore from backup
                        eprintln!("오류: '{}' 교체 실패: {}", skill_name, e);
                        if had_existing {
                            if let Err(restore_err) = std::fs::rename(&backup_path, &skill_path) {
                                eprintln!(
                                    "치명적: '{}' 백업 복원 실패: {}. 백업 경로: {}",
                                    skill_name, restore_err, backup_path.display()
                                );
                            } else {
                                eprintln!("  기존 설치가 백업에서 복원되었습니다.");
                            }
                        }
                        // Clean up staging
                        let _ = std::fs::remove_dir_all(staging_dir);
                        continue;
                    }
                }

                // Remove the now-empty staging directory
                let _ = std::fs::remove_dir_all(staging_dir);
                lockfile.add_skill(skill_name, new_version, deps::to_locked_deps(reg, skill_name));
                lockfile.save(lockfile_path)?;
                updated_count += 1;
                println!("  '{}' 업데이트 완료.", skill_name);
            }
            Err(e) => {
                // Download failed — clean up staging, preserve existing skill
                eprintln!(
                    "오류: '{}' v{} 다운로드 실패: {}",
                    skill_name, new_version, e
                );
                if staging_dir.exists() {
                    let _ = std::fs::remove_dir_all(staging_dir);
                }
                eprintln!("  기존 설치가 유지됩니다.");
            }
        }
    }

    if updated_count > 0 {
        println!("\n완료. {}개 스킬 업데이트됨.", updated_count);
    } else {
        println!("\n모든 스킬이 최신 버전입니다.");
    }

    Ok(())
}

/// Update a single skill sequentially (original behavior for single-skill update).
fn run_update_single(
    targets: &[String],
    reg: &registry::Registry,
    lockfile: &mut Lockfile,
    lockfile_path: &std::path::Path,
    skills_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut updated_count = 0;

    for skill_name in targets {
        let registry_skill = match registry::find_skill(reg, skill_name) {
            Some(s) => s,
            None => {
                eprintln!(
                    "경고: '{}' 스킬이 레지스트리에 없습니다. 건너뜁니다.",
                    skill_name
                );
                continue;
            }
        };

        let installed = lockfile.get_skill(skill_name).unwrap();
        if versions_equal(&installed.version, &registry_skill.version) {
            println!(
                "'{}' 이미 최신 버전입니다 (v{}).",
                skill_name, installed.version
            );
            continue;
        }

        // Check for dependency version drift warnings
        let dependents = deps::find_dependents(skill_name, reg, lockfile);
        let mut skip = false;
        for dep in &dependents {
            if let Some(ref_ver) = deps::get_ref_version(&dep.name, skill_name, reg) {
                if !versions_equal(&registry_skill.version, &ref_ver) {
                    let msg = format!(
                        "\u{26a0} '{}'이(가) '{}' v{}을 참조합니다.\n  업데이트 후 호환되지 않을 수 있습니다. 계속하시겠습니까?",
                        dep.name, skill_name, ref_ver
                    );
                    if !ui::confirm(&msg)? {
                        println!("  '{}' 업데이트를 건너뜁니다.", skill_name);
                        skip = true;
                        break;
                    }
                }
            }
        }
        if skip {
            continue;
        }

        println!(
            "'{}' 업데이트 중: v{} \u{2192} v{}...",
            skill_name, installed.version, registry_skill.version
        );

        let skill_path = skills_dir.join(skill_name);
        let staging_dir = skills_dir.join(format!(".{}-staging", skill_name));

        // Clean up any leftover staging directory from a previous failed attempt
        match std::fs::remove_dir_all(&staging_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                eprintln!("경고: 이전 스테이징 디렉토리 정리 실패: {}", e);
            }
        }

        // Download new version to staging directory first (atomic update).
        // download_skill creates staging_dir/{skill_name}/ internally.
        match registry::download_skill(skill_name, &registry_skill.version, &staging_dir) {
            Ok(()) => {
                // Download succeeded — backup-swap to prevent data loss
                let downloaded = staging_dir.join(skill_name);
                let backup_path = skills_dir.join(format!(".{}-backup", skill_name));

                // Step 1: Move existing skill to backup (if it exists)
                let had_existing = skill_path.exists();
                if had_existing {
                    // Clean up any stale backup from a previous run
                    if backup_path.exists() {
                        let _ = std::fs::remove_dir_all(&backup_path);
                    }
                    std::fs::rename(&skill_path, &backup_path)?;
                }

                // Step 2: Move downloaded skill into place
                match std::fs::rename(&downloaded, &skill_path) {
                    Ok(()) => {
                        // Step 3a: Success — remove backup
                        if had_existing {
                            let _ = std::fs::remove_dir_all(&backup_path);
                        }
                    }
                    Err(e) => {
                        // Step 3b: Failed — restore from backup
                        eprintln!("오류: '{}' 교체 실패: {}", skill_name, e);
                        if had_existing {
                            if let Err(restore_err) = std::fs::rename(&backup_path, &skill_path) {
                                eprintln!(
                                    "치명적: '{}' 백업 복원 실패: {}. 백업 경로: {}",
                                    skill_name, restore_err, backup_path.display()
                                );
                            } else {
                                eprintln!("  기존 설치가 백업에서 복원되었습니다.");
                            }
                        }
                        // Clean up staging
                        let _ = std::fs::remove_dir_all(&staging_dir);
                        continue;
                    }
                }

                // Remove the now-empty staging directory
                let _ = std::fs::remove_dir_all(&staging_dir);
                lockfile.add_skill(skill_name, &registry_skill.version, deps::to_locked_deps(reg, skill_name));
                updated_count += 1;
                println!("  '{}' 업데이트 완료.", skill_name);
            }
            Err(e) => {
                // Download failed — clean up staging, preserve existing skill
                eprintln!(
                    "오류: '{}' v{} 다운로드 실패: {}",
                    skill_name, registry_skill.version, e
                );
                if staging_dir.exists() {
                    let _ = std::fs::remove_dir_all(&staging_dir);
                }
                eprintln!("  기존 설치가 유지됩니다.");
            }
        }
    }

    lockfile.save(lockfile_path)?;

    if updated_count > 0 {
        println!("\n완료. {}개 스킬 업데이트됨.", updated_count);
    } else {
        println!("\n모든 스킬이 최신 버전입니다.");
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
