use crate::{backup, config::AppConfig, storage};
use chrono::{DateTime, Local};
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio_cron_scheduler::{Job, JobScheduler};

pub const MAX_HISTORY_ENTRIES: usize = 100;

#[derive(Debug, Clone, Copy)]
pub enum TaskLogStatus {
    Success,
    Failure,
    Info,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TaskLogEntry {
    pub timestamp: DateTime<Local>,
    pub status: TaskLogStatus,
    pub message: String,
}

impl TaskLogEntry {
    #[allow(dead_code)]
    pub fn success(message: String) -> Self {
        Self::new(TaskLogStatus::Success, message)
    }

    #[allow(dead_code)]
    pub fn failure(message: String) -> Self {
        Self::new(TaskLogStatus::Failure, message)
    }

    #[allow(dead_code)]
    pub fn info(message: String) -> Self {
        Self::new(TaskLogStatus::Info, message)
    }

    #[allow(dead_code)]
    pub fn format_line(&self) -> String {
        let ts = self.timestamp.format("%Y-%m-%d %H:%M:%S");
        let status = match self.status {
            TaskLogStatus::Success => "OK",
            TaskLogStatus::Failure => "ERR",
            TaskLogStatus::Info => "INFO",
        };
        format!("[{}] {} {}", ts, status, &self.message)
    }

    fn new(status: TaskLogStatus, message: String) -> Self {
        Self {
            timestamp: Local::now(),
            status,
            message: sanitize_message(message),
        }
    }
}

fn sanitize_message(message: String) -> String {
    message.replace(['\n', '\r'], " ")
}

fn record_history(history: &Arc<Mutex<Vec<TaskLogEntry>>>, entry: TaskLogEntry) {
    if let Ok(mut buffer) = history.lock() {
        buffer.push(entry);
        if buffer.len() > MAX_HISTORY_ENTRIES {
            let overflow = buffer.len() - MAX_HISTORY_ENTRIES;
            buffer.drain(0..overflow);
        }
    }
}

#[allow(dead_code)]
async fn run_one_cycle(cfg: &AppConfig) -> Result<String, Box<dyn Error>> {
    let backup_res = backup::perform_backup(cfg).map_err(|e| format!("Backup failed: {}", e))?;
    let (upload_res, deleted) = storage::upload_and_cleanup(cfg, &backup_res)
        .await
        .map_err(|e| format!("Upload/Cleanup failed: {}", e))?;

    Ok(format!(
        "Backup stored at {} and uploaded to s3://{}/{} ({} bytes). Deleted {} old backups.",
        backup_res.backup_path.display(),
        upload_res.bucket,
        upload_res.s3_key,
        upload_res.file_size,
        deleted
    ))
}

#[allow(dead_code)]
pub async fn run_scheduler_forever(
    cfg: AppConfig,
    history: Arc<Mutex<Vec<TaskLogEntry>>>,
) -> Result<(), Box<dyn Error>> {
    let mut scheduler = JobScheduler::new().await?;

    let cfg_arc = Arc::new(cfg);
    let schedule = cfg_arc.backup.schedule.clone();

    record_history(
        &history,
        TaskLogEntry::info(format!("Scheduler started with cron: {}", schedule)),
    );

    let cfg_for_job = cfg_arc.clone();
    let history_for_job = history.clone();
    let job = Job::new_async(schedule.as_str(), move |_uuid, _l| {
        let cfg_inner = cfg_for_job.clone();
        let history_inner = history_for_job.clone();
        Box::pin(async move {
            match run_one_cycle(&cfg_inner).await {
                Ok(summary) => {
                    record_history(&history_inner, TaskLogEntry::success(summary));
                }
                Err(err) => {
                    record_history(&history_inner, TaskLogEntry::failure(err.to_string()));
                    eprintln!("Scheduled run failed: {}", err);
                }
            }
        })
    })?;
    scheduler.add(job).await?;

    scheduler.start().await?;
    wait_for_shutdown_signal().await;
    let _ = scheduler.shutdown().await;

    record_history(
        &history,
        TaskLogEntry::info("Scheduler stopped".to_string()),
    );
    Ok(())
}

#[allow(dead_code)]
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
        let _ = tokio::signal::ctrl_c().await;
    }
}
