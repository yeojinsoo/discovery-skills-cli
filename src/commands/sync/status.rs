use crate::config;
use crate::s3_client::{AwsS3Client, S3Client};
use crate::sync_config::SyncConfig;
use crate::sync_manifest;
use std::path::Path;

/// Name of the manifest file within the data directory.
const MANIFEST_FILE: &str = ".sync-manifest.toml";

/// A single entry in the status table.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusEntry {
    pub status: FileStatus,
    pub path: String,
    pub local_info: String,
    pub remote_info: String,
}

/// Classification of a file's sync state.
#[derive(Debug, Clone, PartialEq)]
pub enum FileStatus {
    Modified,
    New,
    Behind,
    #[allow(dead_code)] // used in sort ordering and label(); synced files counted but not listed individually
    Synced,
}

impl FileStatus {
    fn label(&self) -> &'static str {
        match self {
            FileStatus::Modified => "modified",
            FileStatus::New => "new",
            FileStatus::Behind => "behind",
            FileStatus::Synced => "synced",
        }
    }
}

/// Aggregated status result.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusResult {
    pub bucket: String,
    pub region: String,
    pub device_id: String,
    pub last_synced: Option<String>,
    pub entries: Vec<StatusEntry>,
    pub synced_count: usize,
    pub to_push: usize,
    pub to_pull: usize,
}

/// Execute the `sync status` command.
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = SyncConfig::load().map_err(|_| {
        "Run 'ds sync init' first to configure your S3 bucket."
    })?;

    let client = AwsS3Client::new(&cfg.bucket, &cfg.region)?;
    let data_dir = config::data_dir()?;
    let result = run_with_client(&client, &cfg, &data_dir)?;
    print_status(&result);
    Ok(())
}

/// Internal implementation that accepts an `&dyn S3Client` and an explicit
/// `data_dir`, making it testable without real AWS credentials.
pub fn run_with_client(
    client: &dyn S3Client,
    config: &SyncConfig,
    data_dir: &Path,
) -> Result<StatusResult, Box<dyn std::error::Error>> {
    let manifest_path = data_dir.join(MANIFEST_FILE);
    let manifest = sync_manifest::load(&manifest_path)?;

    // Determine last synced time from the most recent entry in the manifest.
    let last_synced = manifest
        .files
        .values()
        .map(|e| e.last_synced.as_str())
        .max()
        .map(|s| s.to_string());

    // Diff local files against manifest to find modified / new files.
    let diff = sync_manifest::diff_local(&manifest, data_dir)?;

    let mut entries: Vec<StatusEntry> = Vec::new();
    let mut to_push: usize = 0;
    let mut to_pull: usize = 0;

    // Modified files → to push
    for rel in &diff.modified {
        let full_path = data_dir.join(rel);
        let meta = std::fs::metadata(&full_path).ok();
        let local_info = match meta {
            Some(m) => format_size(m.len()),
            None => "-".to_string(),
        };
        entries.push(StatusEntry {
            status: FileStatus::Modified,
            path: rel.clone(),
            local_info,
            remote_info: "unchanged".to_string(),
        });
        to_push += 1;
    }

    // New files → to push
    for rel in &diff.new {
        let full_path = data_dir.join(rel);
        let meta = std::fs::metadata(&full_path).ok();
        let local_info = match meta {
            Some(m) => format_size(m.len()),
            None => "-".to_string(),
        };
        entries.push(StatusEntry {
            status: FileStatus::New,
            path: rel.clone(),
            local_info,
            remote_info: "-".to_string(),
        });
        to_push += 1;
    }

    // Check remote for files that are ahead of local (behind).
    // List all objects in the bucket and compare against manifest.
    let remote_objects = client.list_objects("")?;
    for obj in &remote_objects {
        // Skip internal sync metadata files.
        if obj.key.starts_with(".sync-meta/")
            || obj.key == ".sync-in-progress"
            || obj.key == ".sync-config.toml"
            || obj.key == ".sync-manifest.toml"
        {
            continue;
        }
        // If we already classified it as modified or new locally, skip.
        if diff.modified.contains(&obj.key) || diff.new.contains(&obj.key) {
            continue;
        }
        // Check if the remote version is newer than what we last synced.
        if let Some(entry) = manifest.files.get(&obj.key) {
            // Use head_object for accurate last_modified comparison.
            match client.head_object(&obj.key) {
                Ok(Some(remote_meta)) => {
                    if remote_meta.last_modified != entry.last_synced {
                        entries.push(StatusEntry {
                            status: FileStatus::Behind,
                            path: obj.key.clone(),
                            local_info: truncate_date(&entry.last_synced),
                            remote_info: truncate_date(&remote_meta.last_modified),
                        });
                        to_pull += 1;
                    }
                    // Otherwise it's synced — counted below.
                }
                Ok(None) => {
                    // Object disappeared between list and head — ignore.
                }
                Err(_) => {
                    // S3 error — skip gracefully.
                }
            }
        } else {
            // File exists on remote but not in manifest at all and not in
            // diff.new — this means we've never seen it. Treat as behind.
            entries.push(StatusEntry {
                status: FileStatus::Behind,
                path: obj.key.clone(),
                local_info: "-".to_string(),
                remote_info: format_size(obj.size as u64),
            });
            to_pull += 1;
        }
    }

    // Count synced: files in manifest that are neither modified, new, deleted,
    // nor behind.
    let behind_paths: std::collections::HashSet<&str> = entries
        .iter()
        .filter(|e| e.status == FileStatus::Behind)
        .map(|e| e.path.as_str())
        .collect();
    let synced_count = manifest
        .files
        .keys()
        .filter(|k| {
            !diff.modified.contains(k)
                && !diff.new.contains(k)
                && !diff.deleted.contains(k)
                && !behind_paths.contains(k.as_str())
        })
        .count();

    // Sort entries: modified, new, behind (synced files are not listed individually).
    entries.sort_by(|a, b| {
        let order = |s: &FileStatus| match s {
            FileStatus::Modified => 0,
            FileStatus::New => 1,
            FileStatus::Behind => 2,
            FileStatus::Synced => 3,
        };
        order(&a.status).cmp(&order(&b.status)).then(a.path.cmp(&b.path))
    });

    Ok(StatusResult {
        bucket: config.bucket.clone(),
        region: config.region.clone(),
        device_id: config.device_id.clone(),
        last_synced,
        entries,
        synced_count,
        to_push,
        to_pull,
    })
}

/// Print the status result as a formatted table.
fn print_status(result: &StatusResult) {
    println!("Sync target: s3://{}/", result.bucket);
    println!(
        "Last synced:  {}",
        result.last_synced.as_deref().unwrap_or("never")
    );
    println!("Device:       {}", result.device_id);
    println!();

    if result.entries.is_empty() && result.synced_count == 0 {
        println!("No files tracked.");
        return;
    }

    if !result.entries.is_empty() {
        // Column headers
        println!(
            "  {:<10}  {:<40}  {:<12}  {:<12}",
            "Status", "Path", "Local", "Remote"
        );
        println!(
            "  {:<10}  {:<40}  {:<12}  {:<12}",
            repeat_char('-', 10),
            repeat_char('-', 40),
            repeat_char('-', 12),
            repeat_char('-', 12),
        );

        for entry in &result.entries {
            let path_display = truncate_path(&entry.path, 40);
            println!(
                "  {:<10}  {:<40}  {:<12}  {:<12}",
                entry.status.label(),
                path_display,
                entry.local_info,
                entry.remote_info,
            );
        }
        println!();
    }

    // Summary line
    let mut parts: Vec<String> = Vec::new();
    if result.to_push > 0 {
        parts.push(format!("{} to push", result.to_push));
    }
    if result.to_pull > 0 {
        parts.push(format!("{} to pull", result.to_pull));
    }
    if result.synced_count > 0 {
        parts.push(format!("{} synced", result.synced_count));
    }
    if parts.is_empty() {
        println!("Summary: everything up to date");
    } else {
        println!("Summary: {}", parts.join(", "));
    }
}

/// Format a byte count into a human-readable size string.
fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Truncate a date string to just the date portion (first 10 chars) for display.
fn truncate_date(date_str: &str) -> String {
    if date_str.len() >= 10 {
        date_str[..10].to_string()
    } else {
        date_str.to_string()
    }
}

/// Truncate a path string to fit within `max_len`, adding "..." prefix if needed.
fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else {
        let suffix = &path[path.len() - (max_len - 3)..];
        format!("...{}", suffix)
    }
}

/// Repeat a character `n` times to produce a separator string.
fn repeat_char(ch: char, n: usize) -> String {
    std::iter::repeat(ch).take(n).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_client::MockS3Client;
    use crate::sync_manifest::{FileEntry, SyncManifest};
    use tempfile::TempDir;

    fn test_config() -> SyncConfig {
        SyncConfig {
            bucket: "test-bucket".to_string(),
            region: "ap-northeast-2".to_string(),
            device_id: "test-device".to_string(),
        }
    }

    // SPEC Oracle: test_status_empty
    #[test]
    fn test_status_empty() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.to_push, 0);
        assert_eq!(result.to_pull, 0);
        assert_eq!(result.synced_count, 0);
        assert!(result.last_synced.is_none());
        assert!(result.entries.is_empty());
    }

    // SPEC Oracle: test_status_new_local_file
    #[test]
    fn test_status_new_local_file() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("new_file.txt"), b"hello").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.to_push, 1);
        assert_eq!(result.to_pull, 0);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].status, FileStatus::New);
        assert_eq!(result.entries[0].path, "new_file.txt");
    }

    // SPEC Oracle: test_status_modified_local_file
    #[test]
    fn test_status_modified_local_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("tracked.txt");
        std::fs::write(&file_path, b"original").unwrap();
        let original_hash = sync_manifest::compute_hash(&file_path).unwrap();

        // Create manifest with original hash
        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "tracked.txt".to_string(),
            FileEntry {
                hash: original_hash,
                last_synced: "2026-03-20T00:00:00Z".to_string(),
            },
        );
        let manifest_path = dir.path().join(MANIFEST_FILE);
        sync_manifest::save_atomic(&manifest, &manifest_path).unwrap();

        // Modify the file
        std::fs::write(&file_path, b"modified content").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.to_push, 1);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].status, FileStatus::Modified);
        assert_eq!(result.entries[0].path, "tracked.txt");
    }

    // SPEC Oracle: test_status_behind_remote_file
    #[test]
    fn test_status_behind_remote_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("remote_updated.txt");
        std::fs::write(&file_path, b"local content").unwrap();
        let hash = sync_manifest::compute_hash(&file_path).unwrap();

        // Create manifest recording the last sync time
        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "remote_updated.txt".to_string(),
            FileEntry {
                hash,
                last_synced: "2026-03-19T00:00:00Z".to_string(),
            },
        );
        let manifest_path = dir.path().join(MANIFEST_FILE);
        sync_manifest::save_atomic(&manifest, &manifest_path).unwrap();

        // Put a version on S3 (MockS3Client uses "2025-01-01T00:00:00Z" as
        // last_modified, which differs from our manifest's last_synced)
        let client = MockS3Client::new();
        client
            .put_object("remote_updated.txt", b"remote content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.to_pull, 1);
        let behind_entries: Vec<_> = result
            .entries
            .iter()
            .filter(|e| e.status == FileStatus::Behind)
            .collect();
        assert_eq!(behind_entries.len(), 1);
        assert_eq!(behind_entries[0].path, "remote_updated.txt");
    }

    // SPEC Oracle: test_status_synced_count
    #[test]
    fn test_status_synced_count() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("synced.txt");
        std::fs::write(&file_path, b"stable content").unwrap();
        let hash = sync_manifest::compute_hash(&file_path).unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "synced.txt".to_string(),
            FileEntry {
                hash,
                last_synced: "2025-01-01T00:00:00Z".to_string(),
            },
        );
        let manifest_path = dir.path().join(MANIFEST_FILE);
        sync_manifest::save_atomic(&manifest, &manifest_path).unwrap();

        // Put the same file on S3 with the same last_modified as last_synced
        // MockS3Client returns "2025-01-01T00:00:00Z" as last_modified
        let client = MockS3Client::new();
        client
            .put_object("synced.txt", b"stable content".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.synced_count, 1);
        assert_eq!(result.to_push, 0);
        assert_eq!(result.to_pull, 0);
        // Synced files are not listed individually
        assert!(result.entries.is_empty());
    }

    // SPEC Oracle: test_status_mixed
    #[test]
    fn test_status_mixed() {
        let dir = TempDir::new().unwrap();

        // Create a new file (not in manifest)
        std::fs::write(dir.path().join("brand_new.txt"), b"new").unwrap();

        // Create a modified file
        let mod_path = dir.path().join("modified.txt");
        std::fs::write(&mod_path, b"original").unwrap();
        let old_hash = sync_manifest::compute_hash(&mod_path).unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "modified.txt".to_string(),
            FileEntry {
                hash: old_hash,
                last_synced: "2026-03-18T00:00:00Z".to_string(),
            },
        );
        let manifest_path = dir.path().join(MANIFEST_FILE);
        sync_manifest::save_atomic(&manifest, &manifest_path).unwrap();

        // Modify the file after recording its hash
        std::fs::write(&mod_path, b"changed").unwrap();

        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.to_push, 2); // modified + new
        assert_eq!(result.entries.len(), 2);
    }

    // SPEC Oracle: test_status_remote_only_file
    #[test]
    fn test_status_remote_only_file() {
        // File on S3 that is not in local manifest and not on disk at all
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        client
            .put_object("only_on_remote.txt", b"data".to_vec(), None)
            .unwrap();

        let config = test_config();
        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.to_pull, 1);
        let behind: Vec<_> = result
            .entries
            .iter()
            .filter(|e| e.status == FileStatus::Behind)
            .collect();
        assert_eq!(behind.len(), 1);
        assert_eq!(behind[0].path, "only_on_remote.txt");
    }

    // SPEC Oracle: test_format_size
    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(14540), "14.2 KB");
        assert_eq!(format_size(1048576), "1.0 MB");
    }

    // SPEC Oracle: test_truncate_path
    #[test]
    fn test_truncate_path() {
        assert_eq!(truncate_path("short.txt", 40), "short.txt");
        let long = "a".repeat(50);
        let truncated = truncate_path(&long, 40);
        assert_eq!(truncated.len(), 40);
        assert!(truncated.starts_with("..."));
    }

    // SPEC Oracle: test_truncate_date
    #[test]
    fn test_truncate_date() {
        assert_eq!(truncate_date("2026-03-20T08:30:00+09:00"), "2026-03-20");
        assert_eq!(truncate_date("short"), "short");
    }

    // SPEC Oracle: test_last_synced_from_manifest
    #[test]
    fn test_last_synced_from_manifest() {
        let dir = TempDir::new().unwrap();
        let file_a = dir.path().join("a.txt");
        let file_b = dir.path().join("b.txt");
        std::fs::write(&file_a, b"aaa").unwrap();
        std::fs::write(&file_b, b"bbb").unwrap();

        let mut manifest = SyncManifest::empty();
        manifest.files.insert(
            "a.txt".to_string(),
            FileEntry {
                hash: sync_manifest::compute_hash(&file_a).unwrap(),
                last_synced: "2026-03-19T00:00:00Z".to_string(),
            },
        );
        manifest.files.insert(
            "b.txt".to_string(),
            FileEntry {
                hash: sync_manifest::compute_hash(&file_b).unwrap(),
                last_synced: "2026-03-20T08:30:00Z".to_string(),
            },
        );
        let manifest_path = dir.path().join(MANIFEST_FILE);
        sync_manifest::save_atomic(&manifest, &manifest_path).unwrap();

        // Put both on S3 so they show as synced
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, dir.path()).unwrap();

        // last_synced should be the max of the two
        assert_eq!(
            result.last_synced.as_deref(),
            Some("2026-03-20T08:30:00Z")
        );
    }

    // SPEC Oracle: test_status_header_info
    #[test]
    fn test_status_header_info() {
        let dir = TempDir::new().unwrap();
        let client = MockS3Client::new();
        let config = test_config();

        let result = run_with_client(&client, &config, dir.path()).unwrap();

        assert_eq!(result.bucket, "test-bucket");
        assert_eq!(result.region, "ap-northeast-2");
        assert_eq!(result.device_id, "test-device");
    }
}
