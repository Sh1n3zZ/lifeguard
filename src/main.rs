mod backup;
mod cli;
mod config;
mod scheduler;
mod service;
mod storage;

/// CLI entrypoint using clap
#[tokio::main]
async fn main() {
    use clap::Parser;
    let args = cli::Cli::parse();

    match args.command {
        cli::Commands::Run => {
            if let Err(e) = cmd_run().await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        cli::Commands::Backup => {
            if let Err(e) = cmd_backup().await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        cli::Commands::Start => {
            if let Err(e) = service::start() {
                eprintln!("Failed to start daemon: {}", e);
                std::process::exit(1);
            }
        }
        cli::Commands::Stop => {
            if let Err(e) = service::stop() {
                eprintln!("Failed to stop daemon: {}", e);
                std::process::exit(1);
            }
        }
        cli::Commands::Restart => {
            if let Err(e) = service::restart() {
                eprintln!("Failed to restart daemon: {}", e);
                std::process::exit(1);
            }
        }
    }
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
