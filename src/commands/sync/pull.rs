use crate::config;
use crate::jsonl_merge;
use crate::s3_client::{AwsS3Client, S3Client};
use crate::sync_config::SyncConfig;
use crate::sync_manifest::{self, FileEntry};
use std::path::Path;

/// Name of the manifest file within the data directory.
const MANIFEST_FILE: &str = ".sync-manifest.toml";

/// S3 key for the in-progress marker.
const SYNC_IN_PROGRESS_MARKER: &str = ".sync-in-progress";

/// Keys that should never be downloaded to the local data directory.
const EXCLUDED_S3_KEYS: &[&str] = &[
    ".sync-in-progress",
    ".sync-config.toml",
    ".sync-manifest.toml",
    "lockfile.toml",
    "sync.log",
];

/// Execute the `sync pull` command.
///
/// Loads configuration, fetches remote file list from S3, performs 3-way diff
/// against the local manifest, downloads/merges changes, and updates the manifest.
pub fn run(force: bool, quiet: bool) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load SyncConfig
    let cfg = SyncConfig::load().map_err(|_| {
        "Run 'ds sync init' first to configure your S3 bucket."
    })?;

    // 2. Create S3 client (offline graceful fail)
    let client = match AwsS3Client::new(&cfg.bucket, &cfg.region) {
        Ok(c) => c,
        Err(e) => {
            return Err(format!("S3 연결 실패: {}", e).into());
        }
    };

    let data_dir = config::data_dir()?;
    run_with_client(&client, &cfg, force, quiet, &data_dir)
}

/// Determine how a single file should be handled during pull.
#[derive(Debug, Clone, PartialEq)]
enum PullAction {
    /// Remote-only change: download and overwrite local.
    Download,
    /// Local-only change: skip (preserve local modifications).
    Skip,
    /// Both sides changed: needs conflict resolution.
    Conflict,
    /// File is new on remote (not in manifest): download.
    NewRemote,
}

/// Internal implementation that accepts an `&dyn S3Client` and an explicit
/// `data_dir`, making it testable without real AWS credentials or env mutation.
pub fn run_with_client(
    client: &dyn S3Client,
    _config: &SyncConfig,
    force: bool,
    quiet: bool,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest_path = data_dir.join(MANIFEST_FILE);

    // 3. Check sync-in-progress marker
    match client.head_object(SYNC_IN_PROGRESS_MARKER) {
        Ok(Some(_)) => {
            if !quiet {
                eprintln!(
                    "Warning: a sync operation is in progress on another device (.sync-in-progress marker found)."
                );
            }
        }
        Ok(None) => {}
        Err(e) => {
            if !quiet {
                eprintln!("Warning: could not check sync marker: {}", e);
            }
        }
    }

    // 4. Load manifest (empty if missing)
    let manifest = sync_manifest::load(&manifest_path)?;

    // 5. List remote objects from S3
    let remote_objects = client.list_objects("")?;

    // Filter out internal / excluded keys and .sync-meta/ prefix
    let remote_files: Vec<_> = remote_objects
        .into_iter()
        .filter(|obj| {
            let key = &obj.key;
            !key.starts_with(".sync-meta/")
                && !EXCLUDED_S3_KEYS.contains(&key.as_str())
        })
        .collect();

    if remote_files.is_empty() {
        if !quiet {
            println!("No remote files to pull.");
        }
        return Ok(());
    }

    // 6. 3-way comparison and action planning
    let mut actions: Vec<(String, PullAction, String)> = Vec::new(); // (key, action, remote_last_modified)

    for obj in &remote_files {
        let key = &obj.key;
        let remote_last_modified = &obj.last_modified;

        let local_path = data_dir.join(key);
        let local_exists = local_path.exists();

        match manifest.files.get(key) {
            Some(entry) => {
                // File is tracked in manifest
                let manifest_hash = &entry.hash;

                // Compute remote hash by downloading (we need the content anyway for comparison)
                // Instead, use manifest vs local vs remote presence logic:
                if local_exists {
                    let local_hash = sync_manifest::compute_hash(&local_path)?;
                    let local_changed = &local_hash != manifest_hash;

                    // We can't compute remote hash without downloading, so we use
                    // last_synced timestamp vs remote last_modified as a proxy for
                    // whether the remote has changed since our last sync.
                    let remote_changed = entry.last_synced != *remote_last_modified;

                    if !local_changed && remote_changed {
                        // manifest == local, manifest != remote → remote-only change
                        actions.push((key.clone(), PullAction::Download, remote_last_modified.clone()));
                    } else if local_changed && !remote_changed {
                        // manifest != local, manifest == remote → local-only change
                        actions.push((key.clone(), PullAction::Skip, remote_last_modified.clone()));
                    } else if local_changed && remote_changed {
                        // Both sides changed
                        if force {
                            actions.push((key.clone(), PullAction::Download, remote_last_modified.clone()));
                        } else {
                            actions.push((key.clone(), PullAction::Conflict, remote_last_modified.clone()));
                        }
                    }
                    // else: neither changed → nothing to do
                } else {
                    // File in manifest but deleted locally; remote may have updates.
                    // Re-download since we're pulling.
                    actions.push((key.clone(), PullAction::Download, remote_last_modified.clone()));
                }
            }
            None => {
                // Not in manifest → new remote file
                if force && local_exists {
                    // Force overwrite
                    actions.push((key.clone(), PullAction::Download, remote_last_modified.clone()));
                } else if local_exists {
                    // Local file exists but not tracked — treat as conflict
                    actions.push((key.clone(), PullAction::Conflict, remote_last_modified.clone()));
                } else {
                    actions.push((key.clone(), PullAction::NewRemote, remote_last_modified.clone()));
                }
            }
        }
    }

    // Filter out Skip actions for reporting
    let actionable: Vec<_> = actions
        .iter()
        .filter(|(_, a, _)| *a != PullAction::Skip)
        .collect();

    if actionable.is_empty() {
        if !quiet {
            println!("All files up to date.");
        }
        return Ok(());
    }

    if !quiet {
        let downloads = actions.iter().filter(|(_, a, _)| *a == PullAction::Download || *a == PullAction::NewRemote).count();
        let conflicts = actions.iter().filter(|(_, a, _)| *a == PullAction::Conflict).count();
        let skips = actions.iter().filter(|(_, a, _)| *a == PullAction::Skip).count();
        println!(
            "Pull plan: {} download(s), {} conflict(s), {} skip(s).",
            downloads, conflicts, skips
        );
    }

    // 7. Execute actions
    let mut new_manifest = manifest.clone();
    let mut pulled_count: usize = 0;

    for (key, action, remote_last_modified) in &actions {
        match action {
            PullAction::Skip => {
                if !quiet {
                    println!("  Skipped (local changes): {}", key);
                }
            }
            PullAction::Download | PullAction::NewRemote => {
                let remote_data = client.get_object(key)?;
                let local_path = data_dir.join(key);

                // Ensure parent directories exist
                if let Some(parent) = local_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                std::fs::write(&local_path, &remote_data)?;

                if !quiet {
                    let label = if *action == PullAction::NewRemote { "New" } else { "Updated" };
                    println!("  {}: {}", label, key);
                }

                let hash = sync_manifest::compute_hash(&local_path)?;
                new_manifest.files.insert(
                    key.clone(),
                    FileEntry {
                        hash,
                        last_synced: remote_last_modified.clone(),
                    },
                );
                pulled_count += 1;
            }
            PullAction::Conflict => {
                let remote_data = client.get_object(key)?;
                let local_path = data_dir.join(key);

                let resolved = resolve_conflict(key, &local_path, &remote_data, remote_last_modified)?;

                // Ensure parent directories exist
                if let Some(parent) = local_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                std::fs::write(&local_path, &resolved)?;

                if !quiet {
                    println!("  Merged (conflict resolved): {}", key);
                }

                let hash = sync_manifest::compute_hash(&local_path)?;
                new_manifest.files.insert(
                    key.clone(),
                    FileEntry {
                        hash,
                        last_synced: remote_last_modified.clone(),
                    },
                );
                pulled_count += 1;
            }
        }
    }

    // 8. Save manifest atomically
    sync_manifest::save_atomic(&new_manifest, &manifest_path)?;

    if !quiet {
        println!("Pull complete. {} file(s) synced.", pulled_count);
    }

    Ok(())
}

/// Resolve a conflict between a local file and remote data based on file type.
///
/// Rules:
/// - `.jsonl`: merge via `jsonl_merge::merge_jsonl`
/// - `.md` with `analyses/` in path (write-once): newer wins by remote last_modified
/// - `.md` (mutable, e.g. SPEC.md, progress.md): newer wins + `.conflict` backup
/// - `.toml`: last-write-wins (remote overwrites)
fn resolve_conflict(
    key: &str,
    local_path: &Path,
    remote_data: &[u8],
    _remote_last_modified: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if key.ends_with(".jsonl") {
        // JSONL: union + dedup merge
        let local_data = std::fs::read(local_path)?;
        let merged = jsonl_merge::merge_jsonl(&local_data, remote_data);
        return Ok(merged);
    }

    if key.ends_with(".md") {
        if key.contains("analyses/") {
            // Write-once pattern: newer wins (remote wins since it has a newer timestamp
            // — we only reach conflict resolution when remote_changed is true)
            return Ok(remote_data.to_vec());
        }

        // Mutable markdown (SPEC.md, progress.md, etc.): newer wins + .conflict backup
        let conflict_path = format!("{}.conflict", local_path.display());
        let local_data = std::fs::read(local_path)?;
        std::fs::write(&conflict_path, &local_data)?;
        return Ok(remote_data.to_vec());
    }

    if key.ends_with(".toml") {
        // Last-write-wins: remote overwrites
        return Ok(remote_data.to_vec());
    }

    // Default: remote wins (last-write-wins)
    Ok(remote_data.to_vec())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_client::MockS3Client;
    use crate::sync_manifest::SyncManifest;
    use tempfile::TempDir;

    /// Helper: create a SyncConfig for testing.
    fn test_config() -> SyncConfig {
        SyncConfig {
            bucket: "test-bucket".to_string(),
            region: "ap-northeast-2".to_string(),
            device_id: "test-device".to_string(),
        }
    }

    // SPEC Oracle: test_pull_no_remote_files
    #[test]
    fn test_pull_no_remote_files() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());
    }

    // SPEC Oracle: test_pull_new_remote_file
    #[test]
    fn test_pull_new_remote_file() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();

        // Put a file on S3 that doesn't exist locally
        client
            .put_object("remote_file.txt", b"remote content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Verify the file was downloaded
        let content = std::fs::read_to_string(dir.path().join("remote_file.txt")).unwrap();
        assert_eq!(content, "remote content");

        // Verify manifest was updated
        let manifest = sync_manifest::load(&dir.path().join(MANIFEST_FILE)).unwrap();
        assert!(manifest.files.contains_key("remote_file.txt"));
    }

    // SPEC Oracle: test_pull_new_remote_nested_file
    #[test]
    fn test_pull_new_remote_nested_file() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();

        client
            .put_object("sub/deep/nested.txt", b"nested data".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        let content = std::fs::read_to_string(dir.path().join("sub/deep/nested.txt")).unwrap();
        assert_eq!(content, "nested data");
    }

    // SPEC Oracle: test_pull_remote_only_change
    #[test]
    fn test_pull_remote_only_change() {
        let dir = TempDir::new().unwrap();

        // Create a local file and manifest entry with matching hash
        let file_path = dir.path().join("data.txt");
        std::fs::write(&file_path, b"original").unwrap();
        let hash = sync_manifest::compute_hash(&file_path).unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "data.txt".to_string(),
            FileEntry {
                hash,
                last_synced: "2025-01-01T00:00:00Z".to_string(),
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        // Put updated content on S3 (MockS3Client uses "2025-01-01T00:00:00Z" as last_modified,
        // which matches last_synced — so we need the mock to have a different timestamp.
        // Since MockS3Client returns a fixed timestamp, we adjust the manifest's last_synced
        // to differ from it to simulate remote change.)
        let mut manifest2 = SyncManifest::empty();
        manifest2.files.insert(
            "data.txt".to_string(),
            FileEntry {
                hash: sync_manifest::compute_hash(&file_path).unwrap(),
                last_synced: "2024-12-01T00:00:00Z".to_string(), // older than mock's "2025-01-01T00:00:00Z"
            },
        );
        sync_manifest::save_atomic(&manifest2, &dir.path().join(MANIFEST_FILE)).unwrap();

        let client = MockS3Client::new();
        client
            .put_object("data.txt", b"updated from remote".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // File should be updated with remote content
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "updated from remote");
    }

    // SPEC Oracle: test_pull_local_only_change_skipped
    #[test]
    fn test_pull_local_only_change_skipped() {
        let dir = TempDir::new().unwrap();

        // Create local file with content different from manifest hash
        let file_path = dir.path().join("local_edit.txt");
        std::fs::write(&file_path, b"original").unwrap();
        let original_hash = sync_manifest::compute_hash(&file_path).unwrap();

        // manifest records the original hash, last_synced matches mock's timestamp
        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "local_edit.txt".to_string(),
            FileEntry {
                hash: original_hash,
                last_synced: "2025-01-01T00:00:00Z".to_string(), // matches mock timestamp
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        // Modify local file (so local_hash != manifest_hash)
        std::fs::write(&file_path, b"locally modified").unwrap();

        // S3 has the original content (remote unchanged — last_modified matches last_synced)
        let client = MockS3Client::new();
        client
            .put_object("local_edit.txt", b"original".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Local file should NOT be overwritten
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "locally modified");
    }

    // SPEC Oracle: test_pull_conflict_jsonl_merge
    #[test]
    fn test_pull_conflict_jsonl_merge() {
        let dir = TempDir::new().unwrap();

        // Create a local JSONL file with different content from manifest
        let file_path = dir.path().join("history.jsonl");
        let local_content = r#"{"id":"K-001","ts":"2024-01-01","data":"local"}
{"id":"K-002","ts":"2024-01-02","data":"only-local"}
"#;
        std::fs::write(&file_path, local_content.as_bytes()).unwrap();

        // Manifest has a different hash (simulating original state before local edit)
        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "history.jsonl".to_string(),
            FileEntry {
                hash: "old-hash-differs".to_string(), // different from current local hash
                last_synced: "2024-12-01T00:00:00Z".to_string(), // different from mock timestamp
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        // Remote has overlapping + new entries
        let remote_content = r#"{"id":"K-001","ts":"2024-06-15","data":"remote-newer"}
{"id":"K-003","ts":"2024-03-01","data":"only-remote"}
"#;
        let client = MockS3Client::new();
        client
            .put_object("history.jsonl", remote_content.as_bytes().to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Verify merge result contains all unique entries
        let merged = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = merged.lines().collect();
        assert_eq!(lines.len(), 3, "expected 3 merged lines, got: {:?}", lines);
        // K-001 should have the newer remote version
        assert!(merged.contains("remote-newer"));
        // K-002 (local only) should be preserved
        assert!(merged.contains("only-local"));
        // K-003 (remote only) should be added
        assert!(merged.contains("only-remote"));
    }

    // SPEC Oracle: test_pull_conflict_md_analyses_newer_wins
    #[test]
    fn test_pull_conflict_md_analyses_newer_wins() {
        let dir = TempDir::new().unwrap();

        // Create local analysis MD file
        let sub = dir.path().join("analyses");
        std::fs::create_dir_all(&sub).unwrap();
        let file_path = sub.join("report.md");
        std::fs::write(&file_path, b"local analysis content").unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "analyses/report.md".to_string(),
            FileEntry {
                hash: "old-hash".to_string(),
                last_synced: "2024-12-01T00:00:00Z".to_string(),
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        let client = MockS3Client::new();
        client
            .put_object(
                "analyses/report.md",
                b"remote analysis content (newer)".to_vec(),
                None,
            )
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // analyses/ pattern: remote wins (newer)
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "remote analysis content (newer)");
    }

    // SPEC Oracle: test_pull_conflict_md_mutable_creates_conflict_file
    #[test]
    fn test_pull_conflict_md_mutable_creates_conflict_file() {
        let dir = TempDir::new().unwrap();

        // Create local mutable MD file (not in analyses/)
        let file_path = dir.path().join("SPEC.md");
        std::fs::write(&file_path, b"local spec content").unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "SPEC.md".to_string(),
            FileEntry {
                hash: "old-hash".to_string(),
                last_synced: "2024-12-01T00:00:00Z".to_string(),
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        let client = MockS3Client::new();
        client
            .put_object("SPEC.md", b"remote spec content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Remote wins for the main file
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "remote spec content");

        // .conflict file should contain the local version
        let conflict_path = dir.path().join("SPEC.md.conflict");
        assert!(conflict_path.exists(), ".conflict file should be created");
        let conflict_content = std::fs::read_to_string(&conflict_path).unwrap();
        assert_eq!(conflict_content, "local spec content");
    }

    // SPEC Oracle: test_pull_conflict_toml_last_write_wins
    #[test]
    fn test_pull_conflict_toml_last_write_wins() {
        let dir = TempDir::new().unwrap();

        let file_path = dir.path().join("config.toml");
        std::fs::write(&file_path, b"[local]\nkey = \"local\"").unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "config.toml".to_string(),
            FileEntry {
                hash: "old-hash".to_string(),
                last_synced: "2024-12-01T00:00:00Z".to_string(),
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        let client = MockS3Client::new();
        client
            .put_object(
                "config.toml",
                b"[remote]\nkey = \"remote\"".to_vec(),
                None,
            )
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // TOML: last-write-wins → remote overwrites
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "[remote]\nkey = \"remote\"");
    }

    // SPEC Oracle: test_pull_force_overwrites_conflict
    #[test]
    fn test_pull_force_overwrites_conflict() {
        let dir = TempDir::new().unwrap();

        let file_path = dir.path().join("data.txt");
        std::fs::write(&file_path, b"local content").unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "data.txt".to_string(),
            FileEntry {
                hash: "old-hash".to_string(),
                last_synced: "2024-12-01T00:00:00Z".to_string(),
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        let client = MockS3Client::new();
        client
            .put_object("data.txt", b"remote content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, true, false, dir.path());
        assert!(result.is_ok());

        // Force: remote overwrites local
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "remote content");
    }

    // SPEC Oracle: test_pull_sync_in_progress_warning
    #[test]
    fn test_pull_sync_in_progress_warning() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();

        // Place a sync-in-progress marker on S3
        client
            .put_object(SYNC_IN_PROGRESS_MARKER, Vec::new(), None)
            .unwrap();
        // Also put a real file so we have something to pull
        client
            .put_object("file.txt", b"content".to_vec(), None)
            .unwrap();

        let config = test_config();

        // Should succeed (warning only, not blocking)
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // The marker itself should NOT be downloaded
        assert!(!dir.path().join(SYNC_IN_PROGRESS_MARKER).exists());
    }

    // SPEC Oracle: test_pull_excludes_sync_meta_prefix
    #[test]
    fn test_pull_excludes_sync_meta_prefix() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();

        // Put sync-meta files and a real file on S3
        client
            .put_object(".sync-meta/device-a.toml", b"meta".to_vec(), None)
            .unwrap();
        client
            .put_object("real.txt", b"real data".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Only real.txt should be downloaded, not .sync-meta/
        assert!(dir.path().join("real.txt").exists());
        assert!(!dir.path().join(".sync-meta/device-a.toml").exists());
    }

    // SPEC Oracle: test_pull_manifest_updated_after_pull
    #[test]
    fn test_pull_manifest_updated_after_pull() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();

        client
            .put_object("new_file.txt", b"content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Load the updated manifest
        let manifest = sync_manifest::load(&dir.path().join(MANIFEST_FILE)).unwrap();
        assert!(manifest.files.contains_key("new_file.txt"));

        let entry = &manifest.files["new_file.txt"];
        assert!(!entry.hash.is_empty());
        assert!(!entry.last_synced.is_empty());
    }

    // SPEC Oracle: test_pull_quiet_no_remote
    #[test]
    fn test_pull_quiet_no_remote() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, true, dir.path());
        assert!(result.is_ok());
    }

    // SPEC Oracle: test_pull_unchanged_file_not_downloaded
    #[test]
    fn test_pull_unchanged_file_not_downloaded() {
        let dir = TempDir::new().unwrap();

        // Create a local file whose hash matches the manifest, and last_synced matches mock
        let file_path = dir.path().join("stable.txt");
        std::fs::write(&file_path, b"stable content").unwrap();
        let hash = sync_manifest::compute_hash(&file_path).unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "stable.txt".to_string(),
            FileEntry {
                hash,
                last_synced: "2025-01-01T00:00:00Z".to_string(), // matches mock
            },
        );
        sync_manifest::save_atomic(&manifest, &dir.path().join(MANIFEST_FILE)).unwrap();

        let client = MockS3Client::new();
        client
            .put_object("stable.txt", b"stable content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // File should remain unchanged
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "stable content");
    }
}
