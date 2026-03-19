use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A dependency record stored in the lockfile for offline dependency resolution.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockedDependency {
    pub name: String,
    pub ref_version: String,
}

/// Represents a single installed skill entry in the lockfile.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstalledSkill {
    pub version: String,
    pub installed_at: String, // ISO 8601
    #[serde(default)]
    pub depends_on: Vec<LockedDependency>,
}

/// The lockfile tracks all currently installed skills.
/// Stored as `~/.claude/skills/.skill-manager.toml`.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Lockfile {
    #[serde(default)]
    pub skills: HashMap<String, InstalledSkill>,
}

impl Lockfile {
    /// Load the lockfile from disk.
    /// Returns an empty `Lockfile` if the file does not exist.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(Lockfile::default());
        }
        let content = std::fs::read_to_string(path)?;
        if content.trim().is_empty() {
            return Ok(Lockfile::default());
        }
        let lockfile: Lockfile = toml::from_str(&content)?;
        Ok(lockfile)
    }

    /// Save the lockfile to disk.
    /// Creates parent directories if they do not exist.
    pub fn save(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Add or update a skill entry. Sets `installed_at` to the current UTC time (ISO 8601).
    pub fn add_skill(&mut self, name: &str, version: &str, depends_on: Vec<LockedDependency>) {
        let now = chrono::Utc::now().to_rfc3339();
        self.skills.insert(
            name.to_string(),
            InstalledSkill {
                version: version.to_string(),
                installed_at: now,
                depends_on,
            },
        );
    }

    /// Remove a skill entry by name.
    pub fn remove_skill(&mut self, name: &str) {
        self.skills.remove(name);
    }

    /// Look up an installed skill by name.
    pub fn get_skill(&self, name: &str) -> Option<&InstalledSkill> {
        self.skills.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Helper: create a unique temporary lockfile path.
    /// Returns the parent directory and the lockfile path.
    /// The caller is responsible for cleaning up.
    fn temp_lockfile_path(test_name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("discovery-skills-test")
            .join(test_name);
        let _ = std::fs::create_dir_all(&dir);
        dir.join(".skill-manager.toml")
    }

    /// Cleanup helper
    fn cleanup(path: &Path) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    /// CRUD cycle: create empty -> add -> get -> remove -> get(None)
    #[test]
    fn test_lockfile_crud() {
        let mut lockfile = Lockfile::default();
        assert!(lockfile.skills.is_empty());

        // add
        lockfile.add_skill("logical-analysis", "1.0.0", vec![]);
        assert_eq!(lockfile.skills.len(), 1);

        // get
        let skill = lockfile.get_skill("logical-analysis");
        assert!(skill.is_some());
        let skill = skill.unwrap();
        assert_eq!(skill.version, "1.0.0");
        assert!(!skill.installed_at.is_empty());

        // remove
        lockfile.remove_skill("logical-analysis");
        assert!(lockfile.get_skill("logical-analysis").is_none());
        assert!(lockfile.skills.is_empty());
    }

    /// Load from a non-existent path returns an empty Lockfile.
    /// Save it, reload, and confirm it round-trips correctly.
    #[test]
    fn test_lockfile_create() {
        let path = temp_lockfile_path("test_lockfile_create");
        // ensure clean state
        let _ = std::fs::remove_file(&path);

        // file does not exist yet
        assert!(!path.exists());

        let lockfile = Lockfile::load(&path).expect("load should succeed for missing file");
        assert!(lockfile.skills.is_empty());

        // save the empty lockfile
        lockfile.save(&path).expect("save should succeed");
        assert!(path.exists());

        // reload
        let reloaded = Lockfile::load(&path).expect("reload should succeed");
        assert!(reloaded.skills.is_empty());

        cleanup(&path);
    }

    /// Add two skills, save, reload, and verify both are present.
    #[test]
    fn test_lockfile_save_load() {
        let path = temp_lockfile_path("test_lockfile_save_load");

        let mut lockfile = Lockfile::default();
        lockfile.add_skill("logical-analysis", "1.2.0", vec![]);
        lockfile.add_skill("project-planner", "0.9.1", vec![]);
        assert_eq!(lockfile.skills.len(), 2);

        lockfile.save(&path).expect("save should succeed");

        let reloaded = Lockfile::load(&path).expect("reload should succeed");
        assert_eq!(reloaded.skills.len(), 2);

        let la = reloaded.get_skill("logical-analysis").expect("should exist");
        assert_eq!(la.version, "1.2.0");

        let pp = reloaded.get_skill("project-planner").expect("should exist");
        assert_eq!(pp.version, "0.9.1");

        cleanup(&path);
    }
}
