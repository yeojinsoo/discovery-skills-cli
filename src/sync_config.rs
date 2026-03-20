use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::config;

/// Name of the sync configuration file stored under DATA_DIR.
const SYNC_CONFIG_FILE: &str = ".sync-config.toml";

/// Top-level TOML wrapper: `[sync]` table.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SyncConfigFile {
    sync: SyncConfig,
}

/// Sync configuration for cloud storage synchronisation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncConfig {
    pub bucket: String,
    pub region: String,
    pub device_id: String,
}

impl SyncConfig {
    /// Return the path to the sync-config file: `~/.discovery-skills/.sync-config.toml`.
    pub fn path() -> Result<PathBuf, Box<dyn std::error::Error>> {
        Ok(config::data_dir()?.join(SYNC_CONFIG_FILE))
    }

    /// Load the sync configuration from disk.
    /// Returns `Err` if the file does not exist or cannot be parsed.
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let path = Self::path()?;
        if !path.exists() {
            return Err(format!(
                "Sync 설정 파일이 없습니다: {}. `discovery-skills sync init`을 먼저 실행하세요.",
                path.display()
            )
            .into());
        }
        let content = std::fs::read_to_string(&path)?;
        let file: SyncConfigFile = toml::from_str(&content)?;
        Ok(file.sync)
    }

    /// Serialize this configuration and write it to disk.
    /// Creates parent directories if they don't exist.
    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let wrapper = SyncConfigFile {
            sync: self.clone(),
        };
        let content = toml::to_string_pretty(&wrapper)?;
        std::fs::write(&path, content)?;
        Ok(())
    }

    /// Derive a default device id by running `hostname`.
    pub fn default_device_id() -> Result<String, Box<dyn std::error::Error>> {
        let output = std::process::Command::new("hostname").output()?;
        if !output.status.success() {
            return Err("hostname 명령 실행에 실패했습니다.".into());
        }
        let id = String::from_utf8(output.stdout)?.trim().to_string();
        if id.is_empty() {
            return Err("hostname이 빈 문자열을 반환했습니다.".into());
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip test: save a SyncConfig to a temp directory, then load it back
    /// and verify all fields match.
    #[test]
    fn test_save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join(SYNC_CONFIG_FILE);

        let original = SyncConfig {
            bucket: "my-test-bucket".to_string(),
            region: "ap-northeast-2".to_string(),
            device_id: "test-device-42".to_string(),
        };

        // Save to the temp path (bypass Self::path() which uses real home dir).
        let wrapper = SyncConfigFile {
            sync: original.clone(),
        };
        let content = toml::to_string_pretty(&wrapper).unwrap();
        std::fs::write(&config_path, &content).unwrap();

        // Load back from the same path.
        let loaded_content = std::fs::read_to_string(&config_path).unwrap();
        let loaded_file: SyncConfigFile = toml::from_str(&loaded_content).unwrap();
        let loaded = loaded_file.sync;

        assert_eq!(original, loaded);
        assert_eq!(loaded.bucket, "my-test-bucket");
        assert_eq!(loaded.region, "ap-northeast-2");
        assert_eq!(loaded.device_id, "test-device-42");
    }

    /// Verify that the serialised TOML output contains the expected `[sync]` table.
    #[test]
    fn test_toml_format() {
        let cfg = SyncConfig {
            bucket: "b".to_string(),
            region: "r".to_string(),
            device_id: "d".to_string(),
        };
        let wrapper = SyncConfigFile { sync: cfg };
        let content = toml::to_string_pretty(&wrapper).unwrap();
        assert!(content.contains("[sync]"));
        assert!(content.contains("bucket = \"b\""));
        assert!(content.contains("region = \"r\""));
        assert!(content.contains("device_id = \"d\""));
    }

    /// Verify default_device_id returns a non-empty string.
    #[test]
    fn test_default_device_id() {
        let id = SyncConfig::default_device_id().unwrap();
        assert!(!id.is_empty());
        // Should not contain trailing newline
        assert!(!id.contains('\n'));
    }
}
