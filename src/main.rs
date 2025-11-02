mod backup;
mod config;
mod storage;

/// Minimal CLI: default action `run` performs backup -> upload -> cleanup
#[tokio::main]
async fn main() {
    // Very small CLI parser
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "run".to_string());

    match cmd.as_str() {
        "run" => {
            if let Err(e) = cmd_run().await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "backup" => {
            if let Err(e) = cmd_backup().await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "help" | "-h" | "--help" => {
            print_help();
        }
        _ => {
            eprintln!("Unknown command: {}", cmd);
            print_help();
            std::process::exit(2);
        }
    }
}

/// Print minimal CLI help
fn print_help() {
    println!("lifeguard - MySQL backup to S3");
    println!("Usage:");
    println!("  lifeguard run      # backup -> upload -> cleanup");
    println!("  lifeguard backup   # backup locally only");
    println!("  lifeguard help     # show this help");
}

/// Execute full workflow: backup -> upload -> cleanup
async fn cmd_run() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::AppConfig::load()?;
    let backup_res = backup::perform_backup(&cfg).map_err(|e| format!("Backup failed: {}", e))?;

    let (upload_res, deleted) = storage::upload_and_cleanup(&cfg, &backup_res)
        .await
        .map_err(|e| format!("Upload/Cleanup failed: {}", e))?;

    println!(
        "Backup uploaded: s3://{}/{} ({} bytes). Old backups deleted: {}",
        upload_res.bucket, upload_res.s3_key, upload_res.file_size, deleted
    );
    println!("Local file: {}", backup_res.backup_path.display());
    Ok(())
}

/// Perform only local backup
async fn cmd_backup() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::AppConfig::load()?;
    let backup_res = backup::perform_backup(&cfg).map_err(|e| format!("Backup failed: {}", e))?;
    println!(
        "Backup created locally: {} ({} bytes)",
        backup_res.backup_path.display(),
        backup_res.file_size
    );
    Ok(())
}
