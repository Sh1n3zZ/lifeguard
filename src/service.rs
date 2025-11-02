use std::error::Error;
use std::path::PathBuf;

fn pid_file_path() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
        .join("lifeguard.pid")
}

#[cfg(all(unix, feature = "daemon"))]
mod unix_daemon {
    use super::*;
    use crate::{config::AppConfig, scheduler};
    use daemonize::Daemonize;
    use fs2::FileExt;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::path::Path;
    use std::thread;
    use std::time::{Duration, Instant};

    fn is_process_running(pid: i32) -> bool {
        match kill(Pid::from_raw(pid), None) {
            Ok(_) => true,
            Err(nix::errno::Errno::ESRCH) => false,
            Err(_) => true,
        }
    }

    fn read_pid_from_file(path: &Path) -> Option<i32> {
        if !path.exists() {
            return None;
        }
        let mut content = String::new();
        if File::open(path)
            .and_then(|mut f| f.read_to_string(&mut content))
            .is_ok()
        {
            content.trim().parse::<i32>().ok()
        } else {
            None
        }
    }

    fn write_and_lock_pid(path: &Path, pid: i32) -> std::io::Result<()> {
        let file = File::create(path)?;
        // Take exclusive lock and keep it for the process lifetime
        file.lock_exclusive()?;
        writeln!(&file, "{}", pid)?;
        // Leak the file handle to keep the lock held
        std::mem::forget(file);
        Ok(())
    }

    pub fn start() -> Result<(), Box<dyn Error>> {
        let pid_path = super::pid_file_path();
        if let Some(pid) = read_pid_from_file(&pid_path) {
            if is_process_running(pid) {
                return Err(format!("Daemon already running with PID {}", pid).into());
            }
        }

        // Prepare daemonization
        let working_dir = pid_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let stdout = fs::File::create(working_dir.join("lifeguard.out")).ok();
        let stderr = fs::File::create(working_dir.join("lifeguard.err")).ok();

        let daemonize = Daemonize::new()
            .working_directory(&working_dir)
            .umask(0o027)
            .stdout(stdout.unwrap_or_else(|| fs::File::create("/dev/null").unwrap()))
            .stderr(stderr.unwrap_or_else(|| fs::File::create("/dev/null").unwrap()));

        match daemonize.start() {
            Ok(_) => {
                // We are now in the child process (daemon)
                let pid = nix::unistd::getpid().as_raw();
                let _ = write_and_lock_pid(&pid_path, pid);

                // Build runtime and run scheduler forever
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(async move {
                    match AppConfig::load() {
                        Ok(cfg) => {
                            if let Err(e) = scheduler::run_scheduler_forever(cfg).await {
                                eprintln!("Scheduler error: {}", e);
                            }
                        }
                        Err(e) => eprintln!("Failed to load config: {}", e),
                    }
                });
                Ok(())
            }
            Err(e) => Err(format!("Failed to daemonize: {}", e).into()),
        }
    }

    pub fn stop() -> Result<(), Box<dyn Error>> {
        let pid_path = super::pid_file_path();
        let Some(pid) = read_pid_from_file(&pid_path) else {
            return Err("Daemon not running (no PID file)".into());
        };

        // Send SIGTERM
        kill(Pid::from_raw(pid), Signal::SIGTERM)?;

        // Wait for process to exit
        let start_wait = Instant::now();
        let timeout = Duration::from_secs(10);
        while is_process_running(pid) {
            if start_wait.elapsed() > timeout {
                return Err("Timed out waiting for daemon to stop".into());
            }
            thread::sleep(Duration::from_millis(200));
        }

        // Cleanup PID file
        let _ = fs::remove_file(pid_path);
        Ok(())
    }

    pub fn restart() -> Result<(), Box<dyn Error>> {
        let _ = stop();
        start()
    }

    pub fn status() -> Result<Option<i32>, Box<dyn Error>> {
        let pid_path = super::pid_file_path();
        let pid = read_pid_from_file(&pid_path);
        Ok(pid)
    }
}

#[cfg(not(all(unix, feature = "daemon")))]
mod no_daemon {
    use super::*;

    pub fn start() -> Result<(), Box<dyn Error>> {
        Err("Daemon mode is only available on Unix with feature=daemon".into())
    }
    pub fn stop() -> Result<(), Box<dyn Error>> {
        Err("Daemon mode is only available on Unix with feature=daemon".into())
    }
    pub fn restart() -> Result<(), Box<dyn Error>> {
        Err("Daemon mode is only available on Unix with feature=daemon".into())
    }
    pub fn status() -> Result<Option<i32>, Box<dyn Error>> {
        Ok(None)
    }
}

pub fn start() -> Result<(), Box<dyn Error>> {
    #[cfg(all(unix, feature = "daemon"))]
    {
        return unix_daemon::start();
    }
    #[cfg(not(all(unix, feature = "daemon")))]
    {
        return no_daemon::start();
    }
}

pub fn stop() -> Result<(), Box<dyn Error>> {
    #[cfg(all(unix, feature = "daemon"))]
    {
        return unix_daemon::stop();
    }
    #[cfg(not(all(unix, feature = "daemon")))]
    {
        return no_daemon::stop();
    }
}

pub fn restart() -> Result<(), Box<dyn Error>> {
    #[cfg(all(unix, feature = "daemon"))]
    {
        return unix_daemon::restart();
    }
    #[cfg(not(all(unix, feature = "daemon")))]
    {
        return no_daemon::restart();
    }
}

pub fn status() -> Result<Option<i32>, Box<dyn Error>> {
    #[cfg(all(unix, feature = "daemon"))]
    {
        return unix_daemon::status();
    }
    #[cfg(not(all(unix, feature = "daemon")))]
    {
        return no_daemon::status();
    }
}
