use clap::{Parser, Subcommand};

/// CLI definition for lifeguard
#[derive(Debug, Parser)]
#[command(
    name = "lifeguard",
    version,
    about = "MySQL backup to S3 with optional daemon scheduler"
)]
pub struct Cli {
    /// Subcommand to execute
    #[command(subcommand)]
    pub command: Commands,
}

/// Available subcommands
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run one full cycle: backup -> upload -> cleanup, then exit
    Run,

    /// Create a local backup only, then exit
    Backup,

    /// Start the background scheduler (daemon)
    Start,

    /// Stop the background scheduler (daemon)
    Stop,

    /// Restart the background scheduler (daemon)
    Restart,

    /// Show execution history for scheduled tasks
    Logs,
}
