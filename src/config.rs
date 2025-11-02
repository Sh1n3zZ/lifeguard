use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Application configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// MySQL database connection settings
    pub mysql: MysqlConfig,
    /// AWS S3 storage settings
    pub s3: S3Config,
    /// Backup settings
    pub backup: BackupConfig,
}

/// MySQL database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlConfig {
    /// Database host address
    pub host: String,
    /// Database port
    pub port: u16,
    /// Database username
    pub username: String,
    /// Database password
    pub password: String,
    /// Database name to backup
    pub database: String,
}

/// AWS S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region (e.g., us-east-1, ap-southeast-1)
    pub region: String,
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
    /// Optional S3 endpoint for custom S3-compatible services
    #[serde(default)]
    pub endpoint: Option<String>,
    /// S3 key prefix for backups (optional)
    #[serde(default)]
    pub prefix: Option<String>,
}

/// Backup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Local temporary directory for backups before uploading
    pub temp_dir: PathBuf,
    /// Cron expression for backup schedule (e.g., "0 2 * * *" for daily at 2 AM)
    #[serde(default = "default_cron")]
    pub schedule: String,
    /// Maximum number of backup files to retain in S3 (older backups will be deleted)
    #[serde(default = "default_retention")]
    pub retention_count: usize,
    /// Backup file name format (supports placeholders like {timestamp}, {database})
    #[serde(default = "default_backup_format")]
    pub file_format: String,
}

// Default values
fn default_cron() -> String {
    "0 2 * * *".to_string() // Daily at 2 AM
}

fn default_retention() -> usize {
    30 // Keep 30 backups
}

fn default_backup_format() -> String {
    "{database}_{timestamp}.sql.gz".to_string()
}

impl AppConfig {
    /// Load configuration from file
    ///
    /// # Arguments
    /// * `config_path` - Path to the configuration file (supports .yaml, .yml, .toml, .json)
    ///
    /// # Returns
    /// * `Result<AppConfig, ConfigError>` - Configuration or error
    pub fn from_file<P: AsRef<std::path::Path>>(config_path: P) -> Result<Self, ConfigError> {
        let config_path = config_path.as_ref();

        // Try to load from various file formats
        let builder =
            Config::builder().add_source(File::with_name(config_path.to_str().ok_or_else(
                || ConfigError::Foreign("Invalid config file path".to_string().into()),
            )?));

        let config = builder.build()?;
        config.try_deserialize()
    }

    /// Load configuration from default locations
    ///
    /// Searches for configuration files in the following order (under `config/`):
    /// 1. `config/default.yaml`
    /// 2. `config/default.yml`
    /// 3. `config/default.toml`
    ///
    /// # Returns
    /// * `Result<AppConfig, ConfigError>` - Configuration or error
    pub fn load() -> Result<Self, ConfigError> {
        let candidates = [
            "config/default.yaml",
            "config/default.yml",
            "config/default.toml",
        ];

        for path in candidates.iter() {
            if std::path::Path::new(path).exists() {
                return Self::from_file(path);
            }
        }

        let candidates_str = candidates.join(", ");
        Err(ConfigError::Foreign(
            format!(
                "No configuration file found. Please create one of: {}",
                candidates_str
            )
            .into(),
        ))
    }

    // /// Get MySQL connection string
    // pub fn mysql_connection_string(&self) -> String {
    //     format!(
    //         "mysql://{}:{}@{}:{}/{}",
    //         self.mysql.username,
    //         self.mysql.password,
    //         self.mysql.host,
    //         self.mysql.port,
    //         self.mysql.database
    //     )
    // }
}
