use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents a single installed skill entry in the lockfile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledSkill {
    pub name: String,
    pub version: String,
    pub installed_at: DateTime<Utc>,
}

/// The lockfile tracks all currently installed skills.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lockfile {
    pub skills: Vec<InstalledSkill>,
}

impl Lockfile {
    /// Load the lockfile from disk, or return an empty lockfile if it doesn't exist.
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        // TODO: Implement in S4
        Ok(Lockfile { skills: vec![] })
    }

    /// Save the lockfile to disk.
    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Implement in S4
        Ok(())
    }
}
