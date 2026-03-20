use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

/// Files to exclude from sync tracking.
const EXCLUDED_FILES: &[&str] = &[
    ".sync-config.toml",
    ".sync-manifest.toml",
    "lockfile.toml",
    "sync.log",
];

/// A single tracked file entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileEntry {
    pub hash: String,
    pub last_synced: String,
}

/// Manifest that tracks the sync state of files under the data directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncManifest {
    pub files: HashMap<String, FileEntry>,
}

/// Result of comparing local files against the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct DiffResult {
    /// Files whose hash has changed since the last sync.
    pub modified: Vec<String>,
    /// Files that exist locally but are not in the manifest.
    pub new: Vec<String>,
    /// Files that are in the manifest but no longer exist locally.
    pub deleted: Vec<String>,
}

impl SyncManifest {
    /// Create an empty manifest.
    pub fn empty() -> Self {
        Self {
            files: HashMap::new(),
        }
    }
}

/// Compute the SHA-256 hash of a file's contents and return it as a lowercase hex string.
pub fn compute_hash(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    let hash = hasher.finalize();
    Ok(format!("{:x}", hash))
}

/// Load a `SyncManifest` from the given path.
/// Returns an empty manifest if the file does not exist.
pub fn load(path: &Path) -> Result<SyncManifest, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(SyncManifest::empty());
    }
    let content = std::fs::read_to_string(path)?;
    let manifest: SyncManifest = toml::from_str(&content)?;
    Ok(manifest)
}

/// Save the manifest atomically by writing to a temporary file in the same
/// directory and then renaming it into place.
pub fn save_atomic(
    manifest: &SyncManifest,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = toml::to_string_pretty(manifest)?;

    let parent = path
        .parent()
        .ok_or("manifest path has no parent directory")?;
    std::fs::create_dir_all(parent)?;

    let tmp_path = parent.join(format!(
        ".sync-manifest.tmp.{}",
        uuid::Uuid::new_v4()
    ));

    std::fs::write(&tmp_path, &content)?;

    if let Err(e) = std::fs::rename(&tmp_path, path) {
        // Clean up the temp file on failure, then propagate the error.
        let _ = std::fs::remove_file(&tmp_path);
        return Err(Box::new(e));
    }

    Ok(())
}

/// Check whether a relative path should be excluded from sync tracking.
fn is_excluded(relative: &str) -> bool {
    EXCLUDED_FILES.iter().any(|exc| relative == *exc)
}

/// Recursively collect all file paths under `dir`, returning paths relative to `base`.
fn collect_files_recursive(
    dir: &Path,
    base: &Path,
    out: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, base, out)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(base)
                .map_err(|e| format!("strip_prefix failed: {}", e))?
                .to_string_lossy()
                .to_string();
            // Normalise to forward-slash separators for cross-platform consistency.
            let relative = relative.replace('\\', "/");
            if !is_excluded(&relative) {
                out.push(relative);
            }
        }
    }
    Ok(())
}

/// Compare the current state of files under `data_dir` against the manifest,
/// producing a `DiffResult` that categorises changes into modified, new, and deleted files.
pub fn diff_local(
    manifest: &SyncManifest,
    data_dir: &Path,
) -> Result<DiffResult, Box<dyn std::error::Error>> {
    let mut local_files: Vec<String> = Vec::new();
    if data_dir.exists() {
        collect_files_recursive(data_dir, data_dir, &mut local_files)?;
    }

    let mut modified = Vec::new();
    let mut new = Vec::new();

    for rel in &local_files {
        let full = data_dir.join(rel);
        let hash = compute_hash(&full)?;
        match manifest.files.get(rel) {
            Some(entry) => {
                if entry.hash != hash {
                    modified.push(rel.clone());
                }
            }
            None => {
                new.push(rel.clone());
            }
        }
    }

    let local_set: std::collections::HashSet<&String> = local_files.iter().collect();
    let mut deleted: Vec<String> = manifest
        .files
        .keys()
        .filter(|k| !local_set.contains(k))
        .cloned()
        .collect();

    // Sort for deterministic output.
    modified.sort();
    new.sort();
    deleted.sort();

    Ok(DiffResult {
        modified,
        new,
        deleted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_compute_hash_correctness() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("hello.txt");
        std::fs::write(&file_path, b"hello world").unwrap();

        let hash = compute_hash(&file_path).unwrap();

        // SHA-256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_compute_hash_empty_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("empty.txt");
        std::fs::write(&file_path, b"").unwrap();

        let hash = compute_hash(&file_path).unwrap();

        // SHA-256 of empty input
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_load_nonexistent_returns_empty() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join(".sync-manifest.toml");
        let m = load(&manifest_path).unwrap();
        assert!(m.files.is_empty());
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join(".sync-manifest.toml");

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "spec-agent/repos/myapp/history.jsonl".to_string(),
            FileEntry {
                hash: "abcdef1234567890".to_string(),
                last_synced: "2026-03-20T15:00:00+09:00".to_string(),
            },
        );

        save_atomic(&manifest, &manifest_path).unwrap();

        assert!(manifest_path.exists());

        let loaded = load(&manifest_path).unwrap();
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn test_atomic_write_replaces_existing() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join(".sync-manifest.toml");

        // Write an initial manifest.
        let mut m1 = SyncManifest::empty();
        m1.files.insert(
            "file_a.txt".to_string(),
            FileEntry {
                hash: "hash_a".to_string(),
                last_synced: "2026-01-01T00:00:00Z".to_string(),
            },
        );
        save_atomic(&m1, &manifest_path).unwrap();

        // Overwrite with a different manifest.
        let mut m2 = SyncManifest::empty();
        m2.files.insert(
            "file_b.txt".to_string(),
            FileEntry {
                hash: "hash_b".to_string(),
                last_synced: "2026-02-01T00:00:00Z".to_string(),
            },
        );
        save_atomic(&m2, &manifest_path).unwrap();

        let loaded = load(&manifest_path).unwrap();
        assert_eq!(loaded, m2);
        assert!(!loaded.files.contains_key("file_a.txt"));
    }

    #[test]
    fn test_atomic_write_no_leftover_tmp() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join(".sync-manifest.toml");

        let manifest = SyncManifest::empty();
        save_atomic(&manifest, &manifest_path).unwrap();

        // No .tmp files should remain.
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .contains(".sync-manifest.tmp")
            })
            .collect();
        assert!(entries.is_empty(), "temporary files should be cleaned up");
    }

    #[test]
    fn test_diff_detects_new_files() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("new_file.txt"), b"content").unwrap();

        let manifest = SyncManifest::empty();
        let diff = diff_local(&manifest, dir.path()).unwrap();

        assert_eq!(diff.new, vec!["new_file.txt".to_string()]);
        assert!(diff.modified.is_empty());
        assert!(diff.deleted.is_empty());
    }

    #[test]
    fn test_diff_detects_modified_files() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("tracked.txt");
        std::fs::write(&file_path, b"original").unwrap();

        let original_hash = compute_hash(&file_path).unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "tracked.txt".to_string(),
            FileEntry {
                hash: original_hash,
                last_synced: "2026-03-20T00:00:00Z".to_string(),
            },
        );

        // Modify the file.
        std::fs::write(&file_path, b"modified content").unwrap();

        let diff = diff_local(&manifest, dir.path()).unwrap();

        assert_eq!(diff.modified, vec!["tracked.txt".to_string()]);
        assert!(diff.new.is_empty());
        assert!(diff.deleted.is_empty());
    }

    #[test]
    fn test_diff_detects_deleted_files() {
        let dir = TempDir::new().unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "gone.txt".to_string(),
            FileEntry {
                hash: "somehash".to_string(),
                last_synced: "2026-03-20T00:00:00Z".to_string(),
            },
        );

        let diff = diff_local(&manifest, dir.path()).unwrap();

        assert!(diff.new.is_empty());
        assert!(diff.modified.is_empty());
        assert_eq!(diff.deleted, vec!["gone.txt".to_string()]);
    }

    #[test]
    fn test_diff_detects_unchanged_files() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("stable.txt");
        std::fs::write(&file_path, b"stable content").unwrap();

        let hash = compute_hash(&file_path).unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "stable.txt".to_string(),
            FileEntry {
                hash,
                last_synced: "2026-03-20T00:00:00Z".to_string(),
            },
        );

        let diff = diff_local(&manifest, dir.path()).unwrap();

        assert!(diff.new.is_empty());
        assert!(diff.modified.is_empty());
        assert!(diff.deleted.is_empty());
    }

    #[test]
    fn test_excluded_files_are_skipped() {
        let dir = TempDir::new().unwrap();
        // Create excluded files.
        std::fs::write(dir.path().join(".sync-config.toml"), b"config").unwrap();
        std::fs::write(dir.path().join(".sync-manifest.toml"), b"manifest").unwrap();
        std::fs::write(dir.path().join("lockfile.toml"), b"lock").unwrap();
        std::fs::write(dir.path().join("sync.log"), b"log").unwrap();
        // Create a non-excluded file.
        std::fs::write(dir.path().join("real_data.txt"), b"data").unwrap();

        let manifest = SyncManifest::empty();
        let diff = diff_local(&manifest, dir.path()).unwrap();

        assert_eq!(diff.new, vec!["real_data.txt".to_string()]);
    }

    #[test]
    fn test_diff_with_nested_directories() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("sub").join("deep");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("nested.txt"), b"nested").unwrap();

        let manifest = SyncManifest::empty();
        let diff = diff_local(&manifest, dir.path()).unwrap();

        assert_eq!(diff.new, vec!["sub/deep/nested.txt".to_string()]);
    }

    #[test]
    fn test_toml_format_matches_spec() {
        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "spec-agent/repos/myapp/history.jsonl".to_string(),
            FileEntry {
                hash: "sha256hex_example".to_string(),
                last_synced: "2026-03-20T15:00:00+09:00".to_string(),
            },
        );

        let content = toml::to_string_pretty(&manifest).unwrap();
        // Verify the TOML can be deserialized back.
        let loaded: SyncManifest = toml::from_str(&content).unwrap();
        assert_eq!(loaded, manifest);
        // Verify the structure contains the expected key format.
        assert!(content.contains("[files.\"spec-agent/repos/myapp/history.jsonl\"]"));
    }
}
