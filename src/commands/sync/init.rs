use crate::s3_client::{AwsS3Client, HeadBucketResult, S3Client};
use crate::sync_config::SyncConfig;

/// Default S3 bucket name.
const DEFAULT_BUCKET: &str = "discovery-skills";

/// Default AWS region.
const DEFAULT_REGION: &str = "ap-northeast-2";

/// Execute the `sync init` command.
///
/// 1. If bucket/region are None, use defaults and log.
/// 2. Check AWS credentials before creating the client.
/// 3. Create an S3 client for the given bucket/region.
/// 4. Call `head_bucket()` to check bucket state and create if needed.
/// 5. Derive a default device_id from the hostname.
/// 6. Persist the configuration to disk.
pub fn run(bucket: Option<&str>, region: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve bucket and region — use defaults if not provided
    let bucket = match bucket {
        Some(b) => b.to_string(),
        None => {
            eprintln!("기본값 사용: bucket={}", DEFAULT_BUCKET);
            DEFAULT_BUCKET.to_string()
        }
    };
    let region = match region {
        Some(r) => r.to_string(),
        None => {
            eprintln!("기본값 사용: region={}", DEFAULT_REGION);
            DEFAULT_REGION.to_string()
        }
    };

    // Check AWS credentials before creating the client
    check_aws_credentials()?;

    // Verify bucket access via HeadBucket
    eprintln!(
        "Verifying access to bucket '{}' in region '{}'...",
        bucket, region
    );
    let client = AwsS3Client::new(&bucket, &region)?;
    run_with_client(&client, &bucket, &region)
}

/// Check that AWS credentials are available (file-based or env-based).
/// Returns a user-friendly error if no credentials can be found.
fn check_aws_credentials() -> Result<(), Box<dyn std::error::Error>> {
    let home = dirs::home_dir().ok_or("홈 디렉토리를 찾을 수 없습니다")?;

    let has_credentials_file = home.join(".aws").join("credentials").exists();
    let has_env_key = std::env::var("AWS_ACCESS_KEY_ID").is_ok();
    let has_sso_config = home.join(".aws").join("config").exists();

    if !has_credentials_file && !has_env_key && !has_sso_config {
        return Err(
            "AWS 자격증명을 찾을 수 없습니다.\n\n\
             다음 방법 중 하나로 설정하세요:\n\n\
             1. AWS CLI 설치 후 configure:\n\
             \x20  brew install awscli\n\
             \x20  aws configure\n\n\
             2. 환경변수 직접 설정:\n\
             \x20  export AWS_ACCESS_KEY_ID=<your-key>\n\
             \x20  export AWS_SECRET_ACCESS_KEY=<your-secret>\n\n\
             3. ~/.aws/credentials 파일 직접 생성:\n\
             \x20  [default]\n\
             \x20  aws_access_key_id = <your-key>\n\
             \x20  aws_secret_access_key = <your-secret>"
                .into(),
        );
    }

    Ok(())
}

/// Internal implementation that accepts an `&dyn S3Client`, making it testable
/// without real AWS credentials.
pub fn run_with_client(
    client: &dyn S3Client,
    bucket: &str,
    region: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match client.head_bucket()? {
        HeadBucketResult::Ok => {
            eprintln!("버킷 접근 확인 완료: '{}'", bucket);
        }
        HeadBucketResult::NotFound => {
            eprintln!("버킷 '{}'이 존재하지 않습니다. 생성합니다...", bucket);
            client.create_and_configure_bucket(region).map_err(|e| {
                format!("버킷 '{}' 생성 실패: {}", bucket, e)
            })?;
            eprintln!("버킷 생성 완료: '{}'", bucket);
        }
        HeadBucketResult::Forbidden => {
            return Err(format!(
                "S3 버킷 '{}'에 접근이 거부되었습니다 (region={}). 자격증명이 잘못되었습니다.",
                bucket, region
            )
            .into());
        }
    }

    // Derive device_id from hostname
    let device_id = SyncConfig::default_device_id()?;
    eprintln!("Device ID: {}", device_id);

    // Build and save the configuration
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
        // MockS3Client::new() has head_bucket() returning Ok(HeadBucketResult::Ok)
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

    // --- Mock: head_bucket → NotFound, create_and_configure_bucket → Ok ---
    struct MockS3ClientNotFound;

    impl S3Client for MockS3ClientNotFound {
        fn head_bucket(&self) -> Result<HeadBucketResult, Box<dyn std::error::Error>> {
            Ok(HeadBucketResult::NotFound)
        }
        fn create_and_configure_bucket(
            &self,
            _region: &str,
        ) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
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
        fn delete_bucket(&self) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }
    }

    // --- Mock: head_bucket → Forbidden ---
    struct MockS3ClientForbidden;

    impl S3Client for MockS3ClientForbidden {
        fn head_bucket(&self) -> Result<HeadBucketResult, Box<dyn std::error::Error>> {
            Ok(HeadBucketResult::Forbidden)
        }
        fn create_and_configure_bucket(
            &self,
            _region: &str,
        ) -> Result<(), Box<dyn std::error::Error>> {
            unimplemented!()
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
        fn delete_bucket(&self) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }
    }

    // --- Mock: head_bucket → NotFound, create_and_configure_bucket → Err ---
    struct MockS3ClientCreateFails;

    impl S3Client for MockS3ClientCreateFails {
        fn head_bucket(&self) -> Result<HeadBucketResult, Box<dyn std::error::Error>> {
            Ok(HeadBucketResult::NotFound)
        }
        fn create_and_configure_bucket(
            &self,
            _region: &str,
        ) -> Result<(), Box<dyn std::error::Error>> {
            Err("CreateBucket access denied".into())
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
        fn delete_bucket(&self) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }
    }

    // SPEC Oracle: test_init_auto_creates_bucket_on_not_found
    #[test]
    fn test_init_auto_creates_bucket_on_not_found() {
        let client = MockS3ClientNotFound;
        let result = run_with_client(&client, "new-bucket", "ap-northeast-2");
        assert!(result.is_ok());

        let loaded = SyncConfig::load().unwrap();
        assert_eq!(loaded.bucket, "new-bucket");
    }

    // SPEC Oracle: test_init_fails_on_forbidden
    #[test]
    fn test_init_fails_on_forbidden() {
        let client = MockS3ClientForbidden;
        let result = run_with_client(&client, "forbidden-bucket", "us-east-1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("자격증명이 잘못되었습니다"),
            "expected forbidden error, got: {}",
            err
        );
    }

    // SPEC Oracle: test_init_fails_on_bucket_create_error
    #[test]
    fn test_init_fails_on_bucket_create_error() {
        let client = MockS3ClientCreateFails;
        let result = run_with_client(&client, "fail-bucket", "ap-northeast-2");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("생성 실패"),
            "expected creation failure error, got: {}",
            err
        );
    }
}
