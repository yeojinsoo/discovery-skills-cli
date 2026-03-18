/// GitHub repository for the skill registry (owner/repo format).
pub const REGISTRY_REPO: &str = "yeojinsoo/discovery-skills-registry";

/// Base directory where skills are installed.
/// Resolved at runtime via `dirs::home_dir()`.
pub const SKILLS_DIR: &str = ".claude/skills";

/// Name of the lockfile that tracks installed skills.
pub const LOCKFILE_NAME: &str = ".skill-manager.toml";

/// Construct the full path to the skills directory.
pub fn skills_dir() -> std::path::PathBuf {
    dirs::home_dir()
        .expect("Could not determine home directory")
        .join(SKILLS_DIR)
}

/// Construct the full path to the lockfile.
pub fn lockfile_path() -> std::path::PathBuf {
    skills_dir().join(LOCKFILE_NAME)
}

/// Construct the GitHub API URL for the registry index.
pub fn registry_api_url() -> String {
    format!(
        "https://api.github.com/repos/{}/contents/index.json",
        REGISTRY_REPO
    )
}

/// Construct the raw GitHub URL for downloading registry files.
pub fn registry_raw_url(path: &str) -> String {
    format!(
        "https://raw.githubusercontent.com/{}/main/{}",
        REGISTRY_REPO, path
    )
}
