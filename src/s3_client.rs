use std::error::Error;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// Metadata returned by head_object.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub last_modified: String,
    #[allow(dead_code)] // reserved for future use (e.g. size-based conflict detection)
    pub content_length: i64,
    pub e_tag: Option<String>,
}

/// Entry returned by list_objects.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub last_modified: String,
    pub size: i64,
}

// ---------------------------------------------------------------------------
// Trait (sync, object-safe)
// ---------------------------------------------------------------------------

pub trait S3Client {
    /// Check that the configured bucket exists and is accessible.
    fn head_bucket(&self) -> Result<(), Box<dyn Error>>;

    /// Upload an object.  `content_md5` is an optional base64-encoded MD5.
    fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        content_md5: Option<&str>,
    ) -> Result<(), Box<dyn Error>>;

    /// Download an object and return its bytes.
    fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn Error>>;

    /// Return metadata for a single object, or `None` if the key does not exist.
    fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, Box<dyn Error>>;

    /// List objects whose key starts with `prefix`.
    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Box<dyn Error>>;

    /// Delete a single object (used for marker deletion).
    fn delete_object(&self, key: &str) -> Result<(), Box<dyn Error>>;

    /// Conditional put: upload only if the key does NOT already exist.
    /// Returns `Ok(true)` if the object was created, `Ok(false)` if it
    /// already existed (i.e. another writer won the race).
    fn put_object_conditional(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> Result<bool, Box<dyn Error>>;
}

// ---------------------------------------------------------------------------
// AwsS3Client — real implementation backed by aws-sdk-s3
// ---------------------------------------------------------------------------

pub struct AwsS3Client {
    bucket: String,
    client: aws_sdk_s3::Client,
    rt: tokio::runtime::Runtime,
}

impl AwsS3Client {
    /// Create a new client.  Builds a Tokio runtime internally so the public
    /// API stays synchronous.
    pub fn new(bucket: &str, region: &str) -> Result<Self, Box<dyn Error>> {
        let rt = tokio::runtime::Runtime::new()?;
        let client = rt.block_on(async {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(region.to_string()))
                .load()
                .await;
            aws_sdk_s3::Client::new(&config)
        });
        Ok(Self {
            bucket: bucket.to_string(),
            client,
            rt,
        })
    }
}

impl S3Client for AwsS3Client {
    fn head_bucket(&self) -> Result<(), Box<dyn Error>> {
        self.rt.block_on(async {
            self.client
                .head_bucket()
                .bucket(&self.bucket)
                .send()
                .await?;
            Ok(())
        })
    }

    fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        content_md5: Option<&str>,
    ) -> Result<(), Box<dyn Error>> {
        self.rt.block_on(async {
            let mut req = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body.into());
            if let Some(md5) = content_md5 {
                req = req.content_md5(md5);
            }
            req.send().await?;
            Ok(())
        })
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        self.rt.block_on(async {
            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await?;
            let bytes = resp.body.collect().await?.into_bytes();
            Ok(bytes.to_vec())
        })
    }

    fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, Box<dyn Error>> {
        self.rt.block_on(async {
            match self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => {
                    let last_modified = resp
                        .last_modified()
                        .map(|t| t.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).unwrap_or_default())
                        .unwrap_or_default();
                    let content_length = resp.content_length().unwrap_or(0);
                    let e_tag = resp.e_tag().map(|s| s.to_string());
                    Ok(Some(ObjectMeta {
                        last_modified,
                        content_length,
                        e_tag,
                    }))
                }
                Err(sdk_err) => {
                    // If the error is a 404 / NotFound, return None instead of
                    // propagating the error.
                    if let aws_sdk_s3::error::SdkError::ServiceError(ref se) = sdk_err {
                        if se.err().is_not_found() {
                            return Ok(None);
                        }
                    }
                    Err(Box::new(sdk_err) as Box<dyn Error>)
                }
            }
        })
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Box<dyn Error>> {
        self.rt.block_on(async {
            let mut objects = Vec::new();
            let mut continuation_token: Option<String> = None;

            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(prefix);
                if let Some(token) = continuation_token.take() {
                    req = req.continuation_token(token);
                }
                let resp = req.send().await?;

                for obj in resp.contents() {
                    let key = obj.key().unwrap_or_default().to_string();
                    let last_modified = obj
                        .last_modified()
                        .map(|t: &aws_sdk_s3::primitives::DateTime| {
                            t.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();
                    let size = obj.size().unwrap_or(0);
                    objects.push(ObjectInfo {
                        key,
                        last_modified,
                        size,
                    });
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }

            Ok(objects)
        })
    }

    fn delete_object(&self, key: &str) -> Result<(), Box<dyn Error>> {
        self.rt.block_on(async {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await?;
            Ok(())
        })
    }

    fn put_object_conditional(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> Result<bool, Box<dyn Error>> {
        self.rt.block_on(async {
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .if_none_match("*")
                .body(body.into())
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(sdk_err) => {
                    // Check for PreconditionFailed (HTTP 412)
                    if let aws_sdk_s3::error::SdkError::ServiceError(ref se) = sdk_err {
                        let raw = se.raw();
                        if raw.status().as_u16() == 412 {
                            return Ok(false);
                        }
                    }
                    Err(Box::new(sdk_err) as Box<dyn Error>)
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// MockS3Client — in-memory implementation for unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub struct MockS3Client {
    storage: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

#[cfg(test)]
impl MockS3Client {
    pub fn new() -> Self {
        Self {
            storage: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[cfg(test)]
impl S3Client for MockS3Client {
    fn head_bucket(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        _content_md5: Option<&str>,
    ) -> Result<(), Box<dyn Error>> {
        self.storage
            .lock()
            .map_err(|e| format!("lock error: {}", e))?
            .insert(key.to_string(), body);
        Ok(())
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let store = self
            .storage
            .lock()
            .map_err(|e| format!("lock error: {}", e))?;
        store
            .get(key)
            .cloned()
            .ok_or_else(|| format!("key not found: {}", key).into())
    }

    fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, Box<dyn Error>> {
        let store = self
            .storage
            .lock()
            .map_err(|e| format!("lock error: {}", e))?;
        match store.get(key) {
            Some(data) => Ok(Some(ObjectMeta {
                last_modified: "2025-01-01T00:00:00Z".to_string(),
                content_length: data.len() as i64,
                e_tag: Some("\"mock-etag\"".to_string()),
            })),
            None => Ok(None),
        }
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Box<dyn Error>> {
        let store = self
            .storage
            .lock()
            .map_err(|e| format!("lock error: {}", e))?;
        let mut results: Vec<ObjectInfo> = store
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectInfo {
                key: k.clone(),
                last_modified: "2025-01-01T00:00:00Z".to_string(),
                size: v.len() as i64,
            })
            .collect();
        results.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(results)
    }

    fn delete_object(&self, key: &str) -> Result<(), Box<dyn Error>> {
        self.storage
            .lock()
            .map_err(|e| format!("lock error: {}", e))?
            .remove(key);
        Ok(())
    }

    fn put_object_conditional(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> Result<bool, Box<dyn Error>> {
        let mut store = self
            .storage
            .lock()
            .map_err(|e| format!("lock error: {}", e))?;
        if store.contains_key(key) {
            Ok(false)
        } else {
            store.insert(key.to_string(), body);
            Ok(true)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_head_bucket() {
        let client = MockS3Client::new();
        assert!(client.head_bucket().is_ok());
    }

    #[test]
    fn mock_put_and_get_object() {
        let client = MockS3Client::new();
        let data = b"hello world".to_vec();
        client
            .put_object("test/key.txt", data.clone(), None)
            .unwrap();
        let fetched = client.get_object("test/key.txt").unwrap();
        assert_eq!(fetched, data);
    }

    #[test]
    fn mock_get_object_not_found() {
        let client = MockS3Client::new();
        assert!(client.get_object("nonexistent").is_err());
    }

    #[test]
    fn mock_head_object_exists() {
        let client = MockS3Client::new();
        client
            .put_object("obj", vec![1, 2, 3], None)
            .unwrap();
        let meta = client.head_object("obj").unwrap();
        assert!(meta.is_some());
        let meta = meta.unwrap();
        assert_eq!(meta.content_length, 3);
        assert!(meta.e_tag.is_some());
    }

    #[test]
    fn mock_head_object_not_found() {
        let client = MockS3Client::new();
        let meta = client.head_object("missing").unwrap();
        assert!(meta.is_none());
    }

    #[test]
    fn mock_list_objects() {
        let client = MockS3Client::new();
        client.put_object("a/1", vec![1], None).unwrap();
        client.put_object("a/2", vec![2, 3], None).unwrap();
        client.put_object("b/1", vec![4], None).unwrap();

        let list = client.list_objects("a/").unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].key, "a/1");
        assert_eq!(list[1].key, "a/2");
        assert_eq!(list[1].size, 2);
    }

    #[test]
    fn mock_delete_object() {
        let client = MockS3Client::new();
        client.put_object("to-delete", vec![1], None).unwrap();
        assert!(client.head_object("to-delete").unwrap().is_some());

        client.delete_object("to-delete").unwrap();
        assert!(client.head_object("to-delete").unwrap().is_none());
    }

    #[test]
    fn mock_put_with_content_md5() {
        let client = MockS3Client::new();
        // content_md5 is ignored by mock, but the call should succeed
        client
            .put_object("md5-test", vec![9, 8, 7], Some("dGVzdA=="))
            .unwrap();
        let fetched = client.get_object("md5-test").unwrap();
        assert_eq!(fetched, vec![9, 8, 7]);
    }

    #[test]
    fn trait_object_safety() {
        // Verify S3Client can be used as a trait object
        let client: Box<dyn S3Client> = Box::new(MockS3Client::new());
        assert!(client.head_bucket().is_ok());
    }

    #[test]
    fn test_conditional_put_succeeds_when_no_marker() {
        let client = MockS3Client::new();
        let result = client
            .put_object_conditional(".sync-in-progress", Vec::new())
            .unwrap();
        assert!(result, "conditional put should succeed when key does not exist");
        // Verify the key now exists
        assert!(client.head_object(".sync-in-progress").unwrap().is_some());
    }

    #[test]
    fn test_conditional_put_fails_when_marker_exists() {
        let client = MockS3Client::new();
        // Pre-create the marker
        client
            .put_object(".sync-in-progress", Vec::new(), None)
            .unwrap();

        let result = client
            .put_object_conditional(".sync-in-progress", Vec::new())
            .unwrap();
        assert!(!result, "conditional put should return false when key already exists");
    }
}
