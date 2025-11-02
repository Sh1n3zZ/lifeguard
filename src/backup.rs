use crate::config::AppConfig;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Error type for backup operations
#[derive(Debug)]
pub enum BackupError {
    /// IO error
    Io(io::Error),
    /// mysqldump command execution failed
    MysqlDumpFailed(String),
    /// Compression failed
    CompressionFailed(String),
    /// Invalid file path or directory
    InvalidPath(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::Io(e) => write!(f, "IO error: {}", e),
            BackupError::MysqlDumpFailed(msg) => write!(f, "mysqldump failed: {}", msg),
            BackupError::CompressionFailed(msg) => write!(f, "Compression failed: {}", msg),
            BackupError::InvalidPath(msg) => write!(f, "Invalid path: {}", msg),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<io::Error> for BackupError {
    fn from(err: io::Error) -> Self {
        BackupError::Io(err)
    }
}

/// Backup result containing the path to the backup file
#[derive(Debug, Clone)]
pub struct BackupResult {
    /// Path to the created backup file
    pub backup_path: PathBuf,
    /// Size of the backup file in bytes
    pub file_size: u64,
}

/// Perform a complete MySQL database backup
///
/// # Arguments
/// * `config` - Application configuration
///
/// # Returns
/// * `Result<BackupResult, BackupError>` - Backup result with file path and size, or error
///
/// # Example
/// ```no_run
/// use lifeguard::config::AppConfig;
/// use lifeguard::backup::perform_backup;
///
/// let config = AppConfig::load()?;
/// let result = perform_backup(&config)?;
/// println!("Backup created at: {:?}", result.backup_path);
/// ```
pub fn perform_backup(config: &AppConfig) -> Result<BackupResult, BackupError> {
    // Ensure temp directory exists
    ensure_temp_dir(&config.backup.temp_dir)?;

    // Generate backup filename
    let backup_filename =
        generate_backup_filename(&config.backup.file_format, &config.mysql.database)?;
    let temp_sql_path = config
        .backup
        .temp_dir
        .join(&backup_filename.replace(".gz", ".sql"));
    let final_backup_path = config.backup.temp_dir.join(&backup_filename);

    // Run mysqldump
    run_mysqldump(config, &temp_sql_path)?;

    // Compress the backup file
    compress_backup(&temp_sql_path, &final_backup_path)?;

    // Remove uncompressed file
    fs::remove_file(&temp_sql_path).map_err(|e| {
        BackupError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to remove temporary file: {}", e),
        ))
    })?;

    // Get file size
    let file_size = fs::metadata(&final_backup_path)
        .map_err(|e| {
            BackupError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to get backup file metadata: {}", e),
            ))
        })?
        .len();

    Ok(BackupResult {
        backup_path: final_backup_path,
        file_size,
    })
}

/// Ensure the temporary directory exists, create it if necessary
///
/// # Arguments
/// * `temp_dir` - Path to the temporary directory
///
/// # Returns
/// * `Result<(), BackupError>` - Success or error
fn ensure_temp_dir(temp_dir: &Path) -> Result<(), BackupError> {
    if !temp_dir.exists() {
        fs::create_dir_all(temp_dir).map_err(|e| {
            BackupError::Io(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("Failed to create temp directory {:?}: {}", temp_dir, e),
            ))
        })?;
    }

    if !temp_dir.is_dir() {
        return Err(BackupError::InvalidPath(format!(
            "Path exists but is not a directory: {:?}",
            temp_dir
        )));
    }

    Ok(())
}

/// Generate backup filename by replacing placeholders in the format string
///
/// # Arguments
/// * `file_format` - Format string with placeholders ({timestamp}, {database})
/// * `database_name` - Database name to replace {database} placeholder
///
/// # Returns
/// * `Result<String, BackupError>` - Generated filename or error
///
/// # Placeholders
/// * `{timestamp}` - Replaced with current timestamp in format: YYYYMMDD_HHMMSS
/// * `{database}` - Replaced with the database name
fn generate_backup_filename(file_format: &str, database_name: &str) -> Result<String, BackupError> {
    use chrono::Local;

    // Generate timestamp: YYYYMMDD_HHMMSS
    let now = Local::now();
    let timestamp_str = now.format("%Y%m%d_%H%M%S").to_string();

    let filename = file_format
        .replace("{timestamp}", &timestamp_str)
        .replace("{database}", database_name);

    Ok(filename)
}

/// Run mysqldump command to create database backup
///
/// # Arguments
/// * `config` - Application configuration
/// * `output_path` - Path where the SQL dump should be saved
///
/// # Returns
/// * `Result<(), BackupError>` - Success or error
fn run_mysqldump(config: &AppConfig, output_path: &Path) -> Result<(), BackupError> {
    // Build mysqldump command
    let mut cmd = Command::new("mysqldump");
    cmd.arg("--host")
        .arg(&config.mysql.host)
        .arg("--port")
        .arg(config.mysql.port.to_string())
        .arg("--user")
        .arg(&config.mysql.username);

    if !config.mysql.password.is_empty() {
        cmd.arg(format!("--password={}", config.mysql.password));
    }

    cmd.arg("--single-transaction")
        .arg("--routines")
        .arg("--triggers")
        .arg("--events")
        .arg(&config.mysql.database);

    // Create output file
    let output_file = fs::File::create(output_path).map_err(|e| {
        BackupError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("Failed to create backup file {:?}: {}", output_path, e),
        ))
    })?;

    // Set stdout to the output file
    cmd.stdout(Stdio::from(output_file));
    cmd.stderr(Stdio::piped());

    // Execute mysqldump
    let output = cmd.output().map_err(|e| {
        BackupError::MysqlDumpFailed(format!(
            "Failed to execute mysqldump command: {}. Make sure mysqldump is installed and in PATH.",
            e
        ))
    })?;

    // Check if mysqldump was successful
    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(BackupError::MysqlDumpFailed(format!(
            "mysqldump exited with code {}: {}",
            output.status.code().unwrap_or(-1),
            error_msg
        )));
    }

    Ok(())
}

/// Compress backup file using gzip
///
/// # Arguments
/// * `input_path` - Path to the uncompressed SQL file
/// * `output_path` - Path where the compressed file should be saved
///
/// # Returns
/// * `Result<(), BackupError>` - Success or error
fn compress_backup(input_path: &Path, output_path: &Path) -> Result<(), BackupError> {
    // Try to use system gzip command first
    let gzip_result = compress_with_gzip_command(input_path, output_path);

    if gzip_result.is_ok() {
        return Ok(());
    }

    // Fallback to flate2 if gzip command is not available
    compress_with_flate2(input_path, output_path)
}

/// Compress file using system gzip command
fn compress_with_gzip_command(input_path: &Path, output_path: &Path) -> Result<(), BackupError> {
    let mut cmd = Command::new("gzip");
    cmd.arg("-c")
        .arg(input_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = cmd
        .output()
        .map_err(|e| BackupError::CompressionFailed(format!("Failed to execute gzip: {}", e)))?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(BackupError::CompressionFailed(format!(
            "gzip failed: {}",
            error_msg
        )));
    }

    let mut output_file = fs::File::create(output_path).map_err(|e| {
        BackupError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to create compressed file: {}", e),
        ))
    })?;

    output_file.write_all(&output.stdout).map_err(|e| {
        BackupError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to write compressed data: {}", e),
        ))
    })?;

    Ok(())
}

/// Compress file using flate2 library (fallback method)
fn compress_with_flate2(input_path: &Path, output_path: &Path) -> Result<(), BackupError> {
    use std::io::Read;

    // Read input file
    let mut input_file = fs::File::open(input_path)?;
    let mut input_data = Vec::new();
    input_file.read_to_end(&mut input_data)?;

    // Compress using flate2
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let output_file = fs::File::create(output_path)?;
    let mut encoder = GzEncoder::new(output_file, Compression::default());
    encoder.write_all(&input_data)?;
    encoder.finish()?;

    Ok(())
}

// /// Save backup file to local directory according to configuration
// ///
// /// This function ensures the backup file is saved in the configured temp directory
// /// and returns the final path. This is essentially what `perform_backup` does,
// /// but provided as a separate function for explicit file saving operations.
// ///
// /// # Arguments
// /// * `config` - Application configuration
// /// * `backup_result` - Result from previous backup operation
// ///
// /// # Returns
// /// * `Result<PathBuf, BackupError>` - Final path where backup is saved
// pub fn save_backup_to_local(
//     config: &AppConfig,
//     backup_result: &BackupResult,
// ) -> Result<PathBuf, BackupError> {
//     // Ensure temp directory exists
//     ensure_temp_dir(&config.backup.temp_dir)?;

//     // The backup file is already in the correct location from perform_backup
//     // This function mainly serves as a validation/utility function
//     if !backup_result.backup_path.exists() {
//         return Err(BackupError::InvalidPath(format!(
//             "Backup file does not exist: {:?}",
//             backup_result.backup_path
//         )));
//     }

//     Ok(backup_result.backup_path.clone())
// }
