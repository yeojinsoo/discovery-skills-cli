use crate::s3_client::{AwsS3Client, S3Client};
use crate::sync_config::SyncConfig;

/// Prompt the user for a value via stdin (reuses the pattern from crate::ui).
fn prompt(label: &str) -> Result<String, Box<dyn std::error::Error>> {
    use std::io::{self, Write};
    print!("{}: ", label);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let value = input.trim().to_string();
    if value.is_empty() {
        return Err(format!("{} 값을 입력해주세요.", label).into());
    }
    Ok(value)
}

/// Execute the `sync init` command.
///
/// 1. If bucket/region are None, prompt interactively.
/// 2. Create an S3 client for the given bucket/region.
/// 3. Call `head_bucket()` to verify credentials and bucket access.
/// 4. Derive a default device_id from the hostname.
/// 5. Persist the configuration to disk.
pub fn run(bucket: Option<&str>, region: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve bucket and region — prompt if not provided
    let bucket = match bucket {
        Some(b) => b.to_string(),
        None => prompt("S3 bucket name")?,
    };
    let region = match region {
        Some(r) => r.to_string(),
        None => prompt("AWS region (e.g. ap-northeast-2)")?,
    };

    // 1. Verify bucket access via HeadBucket
    eprintln!(
        "Verifying access to bucket '{}' in region '{}'...",
        bucket, region
    );
    let client = AwsS3Client::new(&bucket, &region)?;
    run_with_client(&client, &bucket, &region)
}

/// Internal implementation that accepts an `&dyn S3Client`, making it testable
/// without real AWS credentials.
pub fn run_with_client(
    client: &dyn S3Client,
    bucket: &str,
    region: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client.head_bucket().map_err(|e| {
        format!(
            "S3 버킷 '{}'에 접근할 수 없습니다 (region={}): {}",
            bucket, region, e
        )
    })?;

    // 2. Derive device_id from hostname
    let device_id = SyncConfig::default_device_id()?;
    eprintln!("Device ID: {}", device_id);

    // 3. Build and save the configuration
    let config = SyncConfig {
        bucket: bucket.to_string(),
        region: region.to_string(),
        device_id,
    };
    config.save()?;

    let config_path = SyncConfig::path()?;
    eprintln!("Sync configuration saved to {}", config_path.display());
    eprintln!("Sync initialized successfully.");

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_client::MockS3Client;

    // SPEC Oracle: test_init_creates_config
    #[test]
    fn test_init_creates_config() {
        // MockS3Client::new() has head_bucket() returning Ok(())
        let client = MockS3Client::new();
        let result = run_with_client(&client, "test-bucket", "ap-northeast-2");
        assert!(result.is_ok());

        // Verify config was persisted
        let config_path = SyncConfig::path().unwrap();
        assert!(config_path.exists(), "config file should be created");

        let loaded = SyncConfig::load().unwrap();
        assert_eq!(loaded.bucket, "test-bucket");
        assert_eq!(loaded.region, "ap-northeast-2");
    }

    // SPEC Oracle: test_init_fails_on_bad_bucket
    #[test]
    fn test_init_fails_on_bad_bucket() {
        let client = MockS3ClientBadBucket;
        let result = run_with_client(&client, "bad-bucket", "us-east-1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bad-bucket"));
    }

    /// Mock that always fails head_bucket.
    struct MockS3ClientBadBucket;

    impl S3Client for MockS3ClientBadBucket {
        fn head_bucket(&self) -> Result<(), Box<dyn std::error::Error>> {
            Err("NoSuchBucket".into())
        }
        fn put_object(
            &self,
            _key: &str,
            _body: Vec<u8>,
            _content_md5: Option<&str>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            unimplemented!()
        }
        fn get_object(&self, _key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
            unimplemented!()
        }
        fn head_object(
            &self,
            _key: &str,
        ) -> Result<Option<crate::s3_client::ObjectMeta>, Box<dyn std::error::Error>> {
            unimplemented!()
        }
        fn list_objects(
            &self,
            _prefix: &str,
        ) -> Result<Vec<crate::s3_client::ObjectInfo>, Box<dyn std::error::Error>> {
            unimplemented!()
        }
        fn delete_object(&self, _key: &str) -> Result<(), Box<dyn std::error::Error>> {
            unimplemented!()
        }
        fn put_object_conditional(
            &self,
            _key: &str,
            _body: Vec<u8>,
        ) -> Result<bool, Box<dyn std::error::Error>> {
            unimplemented!()
        }
    }
}
