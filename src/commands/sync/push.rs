use crate::config;
use crate::s3_client::{AwsS3Client, S3Client};
use crate::sync_config::SyncConfig;
use crate::sync_manifest::{self, FileEntry};
use std::path::Path;

/// Name of the manifest file within the data directory.
const MANIFEST_FILE: &str = ".sync-manifest.toml";

/// S3 key for the in-progress marker.
const SYNC_IN_PROGRESS_MARKER: &str = ".sync-in-progress";

/// Execute the `sync push` command.
///
/// Loads configuration, diffs local state, uploads changed files to S3,
/// and updates the manifest.
pub fn run(force: bool, quiet: bool, _changed_only: bool) -> Result<(), Box<dyn std::error::Error>> {
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

/// Internal implementation that accepts an `&dyn S3Client` and an explicit
/// `data_dir`, making it testable without real AWS credentials or env mutation.
pub fn run_with_client(
    client: &dyn S3Client,
    config: &SyncConfig,
    force: bool,
    quiet: bool,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest_path = data_dir.join(MANIFEST_FILE);

    // 3. Load manifest (empty if missing)
    let manifest = sync_manifest::load(&manifest_path)?;

    // 4. Diff local files against manifest
    let diff = sync_manifest::diff_local(&manifest, data_dir)?;

    let changed_files: Vec<String> = diff
        .modified
        .iter()
        .chain(diff.new.iter())
        .cloned()
        .collect();

    if changed_files.is_empty() {
        if !quiet {
            println!("No changes to push.");
        }
        return Ok(());
    }

    if !quiet {
        println!(
            "Found {} changed file(s) ({} modified, {} new).",
            changed_files.len(),
            diff.modified.len(),
            diff.new.len()
        );
    }

    // 5. Conflict detection via HeadObject
    if !force {
        let mut conflicts: Vec<String> = Vec::new();
        for rel in &changed_files {
            let s3_key = rel.replace('\\', "/");
            match client.head_object(&s3_key) {
                Ok(Some(remote_meta)) => {
                    // Check if the remote object differs from what we last synced
                    if let Some(entry) = manifest.files.get(rel) {
                        // If the remote last_modified differs from what we recorded,
                        // another device may have pushed.
                        if let Some(ref remote_etag) = remote_meta.e_tag {
                            if entry.last_synced != remote_meta.last_modified {
                                conflicts.push(format!(
                                    "  {} (remote modified: {}, etag: {})",
                                    rel, remote_meta.last_modified, remote_etag
                                ));
                            }
                        }
                    }
                    // New file that already exists on S3 is a conflict
                    else if diff.new.contains(rel) {
                        conflicts.push(format!(
                            "  {} (exists on remote but not in local manifest)",
                            rel
                        ));
                    }
                }
                Ok(None) => {
                    // Not on S3 yet — no conflict
                }
                Err(e) => {
                    // S3 connectivity issue → graceful fail
                    return Err(format!("S3 HeadObject 실패 ({}): {}", rel, e).into());
                }
            }
        }

        if !conflicts.is_empty() {
            let msg = format!(
                "Conflict detected — the following file(s) have been modified remotely:\n{}\n\
                 Use --force to overwrite remote changes.",
                conflicts.join("\n")
            );
            return Err(msg.into());
        }
    }

    // 6. Place sync-in-progress marker (conditional put for concurrent push protection)
    match client.put_object_conditional(SYNC_IN_PROGRESS_MARKER, Vec::new()) {
        Ok(true) => { /* marker created successfully */ }
        Ok(false) => {
            // Marker already exists — another device is syncing
            if !force {
                return Err(
                    "다른 디바이스에서 sync가 진행 중입니다. 잠시 후 재시도하세요.".into(),
                );
            }
            // --force: overwrite the existing marker
            if let Err(e) = client.put_object(SYNC_IN_PROGRESS_MARKER, Vec::new(), None) {
                return Err(format!("마커 생성 실패: {}", e).into());
            }
        }
        Err(e) => {
            return Err(format!("마커 생성 실패: {}", e).into());
        }
    }

    // 7. Upload changed files
    let mut new_manifest = manifest.clone();

    for rel in &changed_files {
        let full_path = data_dir.join(rel);
        let body = match std::fs::read(&full_path) {
            Ok(b) => b,
            Err(e) => {
                // Clean up marker before returning error
                let _ = client.delete_object(SYNC_IN_PROGRESS_MARKER);
                return Err(format!("파일 읽기 실패 ({}): {}", rel, e).into());
            }
        };

        let s3_key = rel.replace('\\', "/");
        // Note: AWS SDK handles Content-MD5 internally via checksum
        // Retry up to 3 times with 1-second delay between attempts
        let mut last_err: Option<Box<dyn std::error::Error>> = None;
        let mut uploaded = false;
        for attempt in 1..=3 {
            match client.put_object(&s3_key, body.clone(), None) {
                Ok(()) => {
                    uploaded = true;
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 3 {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
            }
        }
        if !uploaded {
            // Clean up marker before returning error
            let _ = client.delete_object(SYNC_IN_PROGRESS_MARKER);
            return Err(format!(
                "S3 업로드 실패 ({}, 3회 재시도 후): {}",
                rel,
                last_err.unwrap()
            )
            .into());
        }

        if !quiet {
            println!("  Uploaded: {}", rel);
        }

        // Compute hash for the manifest entry
        let hash = sync_manifest::compute_hash(&full_path)?;

        // Retrieve the S3-side last_modified timestamp via HeadObject so that
        // the manifest records the authoritative remote timestamp instead of the
        // local clock.  This ensures pull-side comparison
        // (`entry.last_synced == remote_last_modified`) is exact.
        let last_synced = match client.head_object(&s3_key)? {
            Some(meta) => meta.last_modified,
            None => {
                // Fallback: should not happen right after a successful put,
                // but use local time as a safety net.
                humantime::format_rfc3339(std::time::SystemTime::now()).to_string()
            }
        };

        new_manifest.files.insert(
            rel.clone(),
            FileEntry {
                hash,
                last_synced,
            },
        );
    }

    // 8. All uploads succeeded → save manifest atomically + backup to S3
    sync_manifest::save_atomic(&new_manifest, &manifest_path)?;

    // Backup manifest to S3 as .sync-meta/{device-id}.toml
    let manifest_backup_key = format!(".sync-meta/{}.toml", config.device_id);
    let manifest_content = toml::to_string_pretty(&new_manifest)?;
    if let Err(e) = client.put_object(
        &manifest_backup_key,
        manifest_content.into_bytes(),
        None,
    ) {
        // Non-fatal: warn but don't roll back
        if !quiet {
            eprintln!(
                "Warning: manifest backup to S3 failed ({}): {}",
                manifest_backup_key, e
            );
        }
    }

    // 9. Delete sync-in-progress marker
    let _ = client.delete_object(SYNC_IN_PROGRESS_MARKER);

    if !quiet {
        println!("Push complete. {} file(s) synced.", changed_files.len());
    }

    Ok(())
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

    // SPEC Oracle: test_push_no_changes
    #[test]
    fn test_push_no_changes() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());
    }

    // SPEC Oracle: test_push_new_file
    #[test]
    fn test_push_new_file() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("hello.txt"), b"hello world").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Verify file was uploaded to S3
        let data = client.get_object("hello.txt").unwrap();
        assert_eq!(data, b"hello world");

        // Verify manifest backup was uploaded
        let backup = client
            .get_object(".sync-meta/test-device.toml")
            .unwrap();
        let backup_str = String::from_utf8(backup).unwrap();
        assert!(backup_str.contains("hello.txt"));

        // Verify marker was cleaned up
        assert!(client.head_object(".sync-in-progress").unwrap().is_none());
    }

    // SPEC Oracle: test_push_modified_file
    #[test]
    fn test_push_modified_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("data.txt");
        std::fs::write(&file_path, b"original").unwrap();
        let old_hash = sync_manifest::compute_hash(&file_path).unwrap();

        // Create manifest with the old hash
        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "data.txt".to_string(),
            FileEntry {
                hash: old_hash,
                last_synced: "2026-01-01T00:00:00Z".to_string(),
            },
        );
        let manifest_path = dir.path().join(MANIFEST_FILE);
        sync_manifest::save_atomic(&manifest, &manifest_path).unwrap();

        // Now modify the file
        std::fs::write(&file_path, b"modified content").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Verify modified content was uploaded
        let data = client.get_object("data.txt").unwrap();
        assert_eq!(data, b"modified content");
    }

    // SPEC Oracle: test_push_conflict_without_force
    #[test]
    fn test_push_conflict_without_force() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("conflict.txt"), b"local content").unwrap();

        let client = MockS3Client::new();
        // Pre-populate S3 with a file (simulates remote having a file)
        client
            .put_object("conflict.txt", b"remote content".to_vec(), None)
            .unwrap();

        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());

        // Should fail due to conflict (new local file that already exists on remote)
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Conflict detected"));
    }

    // SPEC Oracle: test_push_conflict_with_force
    #[test]
    fn test_push_conflict_with_force() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("conflict.txt"), b"local content").unwrap();

        let client = MockS3Client::new();
        // Pre-populate S3 with a file
        client
            .put_object("conflict.txt", b"remote content".to_vec(), None)
            .unwrap();

        let config = test_config();

        let result = run_with_client(&client, &config, true, false, dir.path());

        // Should succeed with --force
        assert!(result.is_ok());

        // Verify local content overwrote remote
        let data = client.get_object("conflict.txt").unwrap();
        assert_eq!(data, b"local content");
    }

    // SPEC Oracle: test_push_quiet_no_changes
    #[test]
    fn test_push_quiet_no_changes() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, true, dir.path());
        assert!(result.is_ok());
    }

    // SPEC Oracle: test_push_marker_cleaned_up_on_success
    #[test]
    fn test_push_marker_cleaned_up_on_success() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("file.txt"), b"data").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Marker should be deleted after successful push
        assert!(client.head_object(".sync-in-progress").unwrap().is_none());
    }

    // SPEC Oracle: test_push_manifest_updated_after_push
    #[test]
    fn test_push_manifest_updated_after_push() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("new_file.txt"), b"content").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Load the updated manifest and check the entry exists
        let manifest_path = dir.path().join(MANIFEST_FILE);
        let manifest = sync_manifest::load(&manifest_path).unwrap();
        assert!(manifest.files.contains_key("new_file.txt"));

        let entry = &manifest.files["new_file.txt"];
        assert!(!entry.hash.is_empty());
        assert!(!entry.last_synced.is_empty());
    }

    // SPEC Oracle: test_push_nested_directory
    #[test]
    fn test_push_nested_directory() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("sub").join("deep");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("nested.txt"), b"nested data").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Verify the nested file was uploaded with forward-slash key
        let data = client.get_object("sub/deep/nested.txt").unwrap();
        assert_eq!(data, b"nested data");
    }

    // SPEC Oracle: test_push_excluded_files_not_uploaded
    #[test]
    fn test_push_excluded_files_not_uploaded() {
        let dir = TempDir::new().unwrap();
        // Create excluded files — .sync-manifest.toml must be valid TOML
        // because load() will attempt to parse it.
        std::fs::write(dir.path().join(".sync-config.toml"), b"[sync]\nbucket = \"b\"\nregion = \"r\"\ndevice_id = \"d\"\n").unwrap();
        let empty_manifest = SyncManifest::empty();
        sync_manifest::save_atomic(&empty_manifest, &dir.path().join(MANIFEST_FILE)).unwrap();
        std::fs::write(dir.path().join("lockfile.toml"), b"lock").unwrap();
        std::fs::write(dir.path().join("sync.log"), b"log").unwrap();
        // Create a non-excluded file
        std::fs::write(dir.path().join("real.txt"), b"real data").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_ok());

        // Only real.txt should be uploaded
        assert!(client.get_object("real.txt").is_ok());
        assert!(client.get_object(".sync-config.toml").is_err());
        assert!(client.get_object(".sync-manifest.toml").is_err());
        assert!(client.get_object("lockfile.toml").is_err());
        assert!(client.get_object("sync.log").is_err());
    }

    // SPEC Oracle: test_push_records_s3_timestamp
    // Verifies that after push, the manifest's last_synced matches the S3
    // HeadObject last_modified (not a local SystemTime::now()).
    #[test]
    fn test_push_records_s3_timestamp() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("ts_check.txt"), b"timestamp test").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, false, true, dir.path());
        assert!(result.is_ok());

        // Load the manifest written by push
        let manifest_path = dir.path().join(MANIFEST_FILE);
        let manifest = sync_manifest::load(&manifest_path).unwrap();
        let entry = manifest.files.get("ts_check.txt").expect("entry must exist");

        // MockS3Client.head_object always returns last_modified = "2025-01-01T00:00:00Z"
        assert_eq!(
            entry.last_synced, "2025-01-01T00:00:00Z",
            "last_synced must be the S3 timestamp, not local clock"
        );
    }

    // SPEC Oracle: test_push_pull_roundtrip_no_redownload
    // After push, an immediate pull should detect zero changes (no re-download)
    // because the manifest's last_synced exactly matches the S3 last_modified.
    #[test]
    fn test_push_pull_roundtrip_no_redownload() {
        use crate::commands::sync::pull;

        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("roundtrip.txt"), b"round trip data").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        // Push
        let push_result = run_with_client(&client, &config, false, true, dir.path());
        assert!(push_result.is_ok());

        // Verify the file is on S3
        let s3_data = client.get_object("roundtrip.txt").unwrap();
        assert_eq!(s3_data, b"round trip data");

        // Pull — should find nothing to download because last_synced matches
        // the mock's list_objects last_modified ("2025-01-01T00:00:00Z") which
        // also equals head_object's last_modified recorded during push.
        let pull_result = pull::run_with_client(&client, &config, false, true, dir.path());
        assert!(pull_result.is_ok());

        // File content unchanged (not re-downloaded / overwritten)
        let content = std::fs::read_to_string(dir.path().join("roundtrip.txt")).unwrap();
        assert_eq!(content, "round trip data");

        // Manifest should still have exactly one user file entry
        let manifest_path = dir.path().join(MANIFEST_FILE);
        let manifest = sync_manifest::load(&manifest_path).unwrap();
        assert_eq!(manifest.files.len(), 1);
        assert!(manifest.files.contains_key("roundtrip.txt"));
    }

    // SPEC Oracle: test_push_aborts_when_marker_exists
    #[test]
    fn test_push_aborts_when_marker_exists() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("file.txt"), b"data").unwrap();

        let client = MockS3Client::new();
        // Pre-create the sync-in-progress marker (simulates another device syncing)
        client
            .put_object(SYNC_IN_PROGRESS_MARKER, Vec::new(), None)
            .unwrap();

        let config = test_config();

        // Without --force, push should abort
        let result = run_with_client(&client, &config, false, false, dir.path());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("다른 디바이스에서 sync가 진행 중입니다"),
            "expected concurrent sync error, got: {}",
            err_msg
        );
    }

    // SPEC Oracle: test_push_force_ignores_marker
    #[test]
    fn test_push_force_ignores_marker() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("file.txt"), b"data").unwrap();

        let client = MockS3Client::new();
        // Pre-create the sync-in-progress marker
        client
            .put_object(SYNC_IN_PROGRESS_MARKER, Vec::new(), None)
            .unwrap();

        let config = test_config();

        // With --force, push should succeed despite the existing marker
        let result = run_with_client(&client, &config, true, false, dir.path());
        assert!(result.is_ok());

        // Verify file was uploaded
        let data = client.get_object("file.txt").unwrap();
        assert_eq!(data, b"data");

        // Verify marker was cleaned up after push
        assert!(client.head_object(SYNC_IN_PROGRESS_MARKER).unwrap().is_none());
    }
}
