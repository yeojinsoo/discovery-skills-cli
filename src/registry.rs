use serde::{Deserialize, Serialize};

/// Metadata for a single skill in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
}

/// Represents the registry index containing all available skills.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    pub skills: Vec<SkillInfo>,
}

impl Registry {
    /// Fetch the registry index from the remote repository.
    pub fn fetch() -> Result<Self, Box<dyn std::error::Error>> {
        // TODO: Implement in S4
        todo!("Registry::fetch not yet implemented")
    }

    /// Find a skill by name in the registry.
    pub fn find_skill(&self, name: &str) -> Option<&SkillInfo> {
        self.skills.iter().find(|s| s.name == name)
    }
}
