use crate::backup::BackupResult;
use crate::config::AppConfig;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::StorageClass;
use std::fs;

/// Error type for storage operations
#[derive(Debug)]
pub enum StorageError {
    /// AWS S3 SDK error
    S3(String),
    /// IO error
    Io(std::io::Error),
    /// Invalid configuration
    InvalidConfig(String),
    /// Failed to list objects
    ListFailed(String),
    /// Failed to delete old backup
    DeleteFailed(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::S3(msg) => write!(f, "S3 error: {}", msg),
            StorageError::Io(e) => write!(f, "IO error: {}", e),
            StorageError::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            StorageError::ListFailed(msg) => write!(f, "Failed to list objects: {}", msg),
            StorageError::DeleteFailed(msg) => write!(f, "Failed to delete old backup: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

/// Result of uploading backup to S3
#[derive(Debug, Clone)]
pub struct UploadResult {
    /// S3 key (path) where the backup was uploaded
    pub s3_key: String,
    /// S3 bucket name
    pub bucket: String,
    /// Size of uploaded file in bytes
    pub file_size: u64,
}

/// Create S3 client from configuration
///
/// # Arguments
/// * `config` - Application configuration
///
/// # Returns
/// * `Result<S3Client, StorageError>` - S3 client or error
async fn create_s3_client(config: &AppConfig) -> Result<S3Client, StorageError> {
    let region = Region::new(config.s3.region.clone());

    let mut s3_config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            &config.s3.access_key_id,
            &config.s3.secret_access_key,
            None,
            None,
            "lifeguard",
        ));

    // Handle custom endpoint (for S3-compatible services like MinIO)
    if let Some(endpoint) = &config.s3.endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }

    let s3_config = s3_config_builder.load().await;
    let client = S3Client::new(&s3_config);

    Ok(client)
}

/// Build S3 key (object path) from backup filename and prefix
///
/// # Arguments
/// * `filename` - Backup filename
/// * `prefix` - Optional S3 key prefix
///
/// # Returns
/// * `String` - Full S3 key
fn build_s3_key(filename: &str, prefix: Option<&String>) -> String {
    let base_key = filename;
    if let Some(prefix) = prefix {
        // Ensure prefix ends with / if not empty
        let normalized_prefix = if prefix.ends_with('/') {
            prefix.as_str()
        } else if prefix.is_empty() {
            ""
        } else {
            // We'll add / after prefix
            prefix.as_str()
        };

        if normalized_prefix.is_empty() {
            base_key.to_string()
        } else {
            format!("{}{}", normalized_prefix, base_key)
        }
    } else {
        base_key.to_string()
    }
}

/// Upload backup file to AWS S3
///
/// # Arguments
/// * `config` - Application configuration
/// * `backup_result` - Backup result containing file path
///
/// # Returns
/// * `Result<UploadResult, StorageError>` - Upload result with S3 key and metadata, or error
///
/// # Example
/// ```no_run
/// use lifeguard::config::AppConfig;
/// use lifeguard::backup::BackupResult;
/// use lifeguard::storage::upload_to_s3;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let config = AppConfig::load()?;
/// let backup_result = BackupResult { /* ... */ };
/// let upload_result = upload_to_s3(&config, &backup_result).await?;
/// println!("Uploaded to s3://{}/{}", upload_result.bucket, upload_result.s3_key);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// ```
pub async fn upload_to_s3(
    config: &AppConfig,
    backup_result: &BackupResult,
) -> Result<UploadResult, StorageError> {
    // Extract filename from backup path
    let filename = backup_result
        .backup_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| StorageError::InvalidConfig("Invalid backup file path".to_string()))?;

    // Build S3 key with prefix
    let s3_key = build_s3_key(filename, config.s3.prefix.as_ref());

    // Create S3 client
    let client = create_s3_client(config).await?;

    // Read file content
    let file_content = fs::read(&backup_result.backup_path)?;
    let body = ByteStream::from(file_content);

    // Upload to S3
    let request = client
        .put_object()
        .bucket(&config.s3.bucket)
        .key(&s3_key)
        .body(body)
        .content_type("application/gzip")
        .storage_class(StorageClass::Standard);

    // Add server-side encryption if needed (optional)
    // request = request.server_side_encryption(ServerSideEncryption::Aes256);

    let output = request
        .send()
        .await
        .map_err(|e| StorageError::S3(format!("Failed to upload to S3: {}", e)))?;

    // Verify upload was successful
    if let Some(etag) = output.e_tag {
        // ETag indicates successful upload
        tracing::debug!("Upload successful. ETag: {}", etag);
    }

    Ok(UploadResult {
        s3_key: s3_key.clone(),
        bucket: config.s3.bucket.clone(),
        file_size: backup_result.file_size,
    })
}

/// Clean up old backup files from S3 based on retention count
///
/// This function lists all backup files in the S3 bucket (matching the prefix),
/// sorts them by modification time, and deletes the oldest ones if they exceed
/// the retention count specified in the configuration.
///
/// # Arguments
/// * `config` - Application configuration
/// * `current_backup_key` - S3 key of the backup that was just uploaded (to exclude from deletion)
///
/// # Returns
/// * `Result<usize, StorageError>` - Number of old backups deleted, or error
pub async fn cleanup_old_backups(
    config: &AppConfig,
    current_backup_key: &str,
) -> Result<usize, StorageError> {
    // Get retention count from config
    let retention_count = config.backup.retention_count;

    // Create S3 client
    let client = create_s3_client(config).await?;

    // Build prefix for listing (include trailing / for proper prefix matching)
    let list_prefix = config
        .s3
        .prefix
        .as_ref()
        .map(|p| {
            if p.ends_with('/') {
                p.clone()
            } else {
                format!("{}/", p)
            }
        })
        .unwrap_or_else(|| String::new());

    // List objects in the bucket with the prefix
    let mut list_request = client.list_objects_v2().bucket(&config.s3.bucket);

    if !list_prefix.is_empty() {
        list_request = list_request.prefix(&list_prefix);
    }

    let mut objects = Vec::new();

    // Paginate through all objects
    let mut continuation_token = None;
    loop {
        let mut request = list_request.clone();
        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let output = request
            .send()
            .await
            .map_err(|e| StorageError::ListFailed(format!("Failed to list S3 objects: {}", e)))?;

        if let Some(contents) = output.contents {
            for object in contents {
                // Skip the current backup
                if let Some(key) = &object.key {
                    if key == current_backup_key {
                        continue;
                    }

                    // Only process files that match our backup filename pattern
                    // (e.g., ending with .sql.gz)
                    if key.ends_with(".sql.gz") {
                        objects.push(object);
                    }
                }
            }
        }

        // Check if there are more objects
        if output.is_truncated.unwrap_or(false) {
            continuation_token = output.next_continuation_token;
        } else {
            break;
        }
    }

    // Sort by last modified time (oldest first)
    let default_time = DateTime::from_secs(0);
    objects.sort_by(|a, b| {
        let time_a = a.last_modified().unwrap_or(&default_time);
        let time_b = b.last_modified().unwrap_or(&default_time);
        time_a.cmp(time_b)
    });

    // Delete old backups if count exceeds retention
    let mut deleted_count = 0;
    if objects.len() > retention_count {
        let to_delete = objects.len() - retention_count;

        for object in objects.iter().take(to_delete) {
            if let Some(key) = &object.key {
                client
                    .delete_object()
                    .bucket(&config.s3.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| {
                        StorageError::DeleteFailed(format!(
                            "Failed to delete old backup {}: {}",
                            key, e
                        ))
                    })?;
                deleted_count += 1;
            }
        }
    }

    Ok(deleted_count)
}

/// Complete upload workflow: upload backup and cleanup old backups
///
/// This function combines uploading the backup to S3 and cleaning up old backups
/// in a single operation.
///
/// # Arguments
/// * `config` - Application configuration
/// * `backup_result` - Backup result containing file path
///
/// # Returns
/// * `Result<(UploadResult, usize), StorageError>` - Upload result and number of deleted old backups, or error
pub async fn upload_and_cleanup(
    config: &AppConfig,
    backup_result: &BackupResult,
) -> Result<(UploadResult, usize), StorageError> {
    // Upload backup
    let upload_result = upload_to_s3(config, backup_result).await?;

    // Cleanup old backups
    let deleted_count = cleanup_old_backups(config, &upload_result.s3_key).await?;

    Ok((upload_result, deleted_count))
}
