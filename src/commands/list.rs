use crate::config;
use crate::lockfile::Lockfile;

/// List all currently installed skills in a table format.
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let lockfile_path = config::lockfile_path();
    let lockfile = Lockfile::load(&lockfile_path)?;

    if lockfile.skills.is_empty() {
        println!("설치된 스킬이 없습니다.");
        return Ok(());
    }

    // Collect and sort by name
    let mut entries: Vec<(&String, &crate::lockfile::InstalledSkill)> =
        lockfile.skills.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    // Calculate column widths
    let name_width = entries
        .iter()
        .map(|(n, _)| n.len())
        .max()
        .unwrap_or(4)
        .max(4); // "이름"
    let version_width = entries
        .iter()
        .map(|(_, s)| s.version.len())
        .max()
        .unwrap_or(7)
        .max(4); // "버전"
    let date_header = "설치 일시";
    let date_width = entries
        .iter()
        .map(|(_, s)| s.installed_at.len())
        .max()
        .unwrap_or(date_header.len())
        .max(date_header.len());

    // Print header
    println!(
        "{:<name_w$}  {:<ver_w$}  {:<date_w$}",
        "이름",
        "버전",
        "설치 일시",
        name_w = name_width,
        ver_w = version_width,
        date_w = date_width,
    );
    println!(
        "{:-<name_w$}  {:-<ver_w$}  {:-<date_w$}",
        "",
        "",
        "",
        name_w = name_width,
        ver_w = version_width,
        date_w = date_width,
    );

    // Print rows
    for (name, skill) in &entries {
        println!(
            "{:<name_w$}  {:<ver_w$}  {:<date_w$}",
            name,
            skill.version,
            skill.installed_at,
            name_w = name_width,
            ver_w = version_width,
            date_w = date_width,
        );
    }

    println!("\n총 {}개 스킬 설치됨.", entries.len());

    Ok(())
}
