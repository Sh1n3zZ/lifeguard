use crate::{backup, config::AppConfig, storage};
use std::error::Error;
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};

/// Execute one full cycle: backup -> upload -> cleanup
pub async fn run_one_cycle(cfg: &AppConfig) -> Result<(), Box<dyn Error>> {
    let backup_res = backup::perform_backup(cfg).map_err(|e| format!("Backup failed: {}", e))?;
    let (_uploaded, _deleted) = storage::upload_and_cleanup(cfg, &backup_res)
        .await
        .map_err(|e| format!("Upload/Cleanup failed: {}", e))?;
    Ok(())
}

/// Start the cron scheduler and block until a shutdown signal is received
pub async fn run_scheduler_forever(cfg: AppConfig) -> Result<(), Box<dyn Error>> {
    let mut scheduler = JobScheduler::new().await?;

    let cfg_arc = Arc::new(cfg);
    let schedule = cfg_arc.backup.schedule.clone();

    // Add the cron job (async)
    let cfg_for_job = cfg_arc.clone();
    let job = Job::new_async(schedule.as_str(), move |_uuid, _l| {
        let cfg_inner = cfg_for_job.clone();
        Box::pin(async move {
            if let Err(e) = run_one_cycle(&cfg_inner).await {
                eprintln!("Scheduled run failed: {}", e);
            }
        })
    })?;
    scheduler.add(job).await?;

    scheduler.start().await?;

    // Wait for shutdown signals
    wait_for_shutdown_signal().await;

    // Graceful stop
    let _ = scheduler.shutdown().await;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("register SIGTERM");
        let mut sigint = signal(SignalKind::interrupt()).expect("register SIGINT");
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        // Fallback to Ctrl-C on non-Unix platforms
        let _ = tokio::signal::ctrl_c().await;
    }
}
