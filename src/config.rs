use std::path::PathBuf;

/// GitHub repository for the skill registry (owner/repo format).
pub const REGISTRY_REPO: &str = "yeojinsoo/discovery-skills-registry";

/// Branch to use for fetching registry data.
pub const REGISTRY_BRANCH: &str = "main";

/// Relative directory (from home) where skills are installed.
pub const SKILLS_DIR_NAME: &str = ".claude/skills";

/// Name of the lockfile that tracks installed skills.
pub const LOCKFILE_NAME: &str = ".skill-manager.toml";

/// Construct the full path to the skills directory.
pub fn skills_dir() -> PathBuf {
    dirs::home_dir()
        .expect("Could not determine home directory")
        .join(SKILLS_DIR_NAME)
}

/// Construct the full path to the lockfile.
pub fn lockfile_path() -> PathBuf {
    skills_dir().join(LOCKFILE_NAME)
}

/// Construct the raw GitHub URL for the registry.toml index file.
pub fn registry_raw_url() -> String {
    format!(
        "https://raw.githubusercontent.com/{}/{}/registry.toml",
        REGISTRY_REPO, REGISTRY_BRANCH
    )
}

/// Construct the GitHub Releases download URL for a specific skill version.
///
/// Pattern: `https://github.com/{REPO}/releases/download/{name}-v{version}/{name}-{version}.tar.gz`
/// Note: tag uses v-prefix but asset filename does not.
pub fn release_download_url(skill_name: &str, version: &str) -> String {
    format!(
        "https://github.com/{}/releases/download/{}-v{}/{}-{}.tar.gz",
        REGISTRY_REPO, skill_name, version, skill_name, version
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_raw_url() {
        let url = registry_raw_url();
        assert_eq!(
            url,
            "https://raw.githubusercontent.com/yeojinsoo/discovery-skills-registry/main/registry.toml"
        );
    }

    #[test]
    fn test_release_download_url() {
        let url = release_download_url("logical-analysis", "1.0.0");
        assert_eq!(
            url,
            "https://github.com/yeojinsoo/discovery-skills-registry/releases/download/logical-analysis-v1.0.0/logical-analysis-1.0.0.tar.gz"
        );
    }

    #[test]
    fn test_skills_dir_is_under_home() {
        let dir = skills_dir();
        let home = dirs::home_dir().unwrap();
        assert!(dir.starts_with(&home));
        assert!(dir.ends_with(".claude/skills"));
    }

    #[test]
    fn test_lockfile_path_is_under_skills_dir() {
        let lf = lockfile_path();
        let sd = skills_dir();
        assert!(lf.starts_with(&sd));
        assert!(lf.ends_with(".skill-manager.toml"));
    }
}
