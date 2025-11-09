use std::error::Error;

#[cfg(all(unix, feature = "daemon"))]
mod unix_daemon {
    use super::*;
    use crate::{
        config::AppConfig,
        scheduler::{self, TaskLogEntry},
    };
    use nix::sys::signal::{self, Signal};
    use nix::sys::socket::{
        AddressFamily, SockAddr, SockFlag, SockType, UnixAddr, bind, connect, listen, socket,
    };
    use nix::sys::stat::{Mode, umask};
    use nix::sys::wait::waitpid;
    use nix::unistd::{ForkResult, Pid, chdir, dup2, fork, getpid, pipe, read, setsid, write};
    use nix::{errno::Errno, libc};
    use std::fs::OpenOptions;
    use std::io::{self, Read, Write};
    use std::net::Shutdown;
    use std::os::fd::{AsFd, FromRawFd, OwnedFd};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    const CONTROL_SOCKET_NAME: &[u8] = b"lifeguard_daemon_control";
    const PIPE_BUFFER_SIZE: usize = 64;

    fn nix_err_to_io(err: nix::Error) -> io::Error {
        match err {
            nix::Error::Errno(errno) => io::Error::from_raw_os_error(errno as i32),
            other => io::Error::new(io::ErrorKind::Other, other.to_string()),
        }
    }

    fn connect_control_socket() -> io::Result<UnixStream> {
        let fd = socket(
            AddressFamily::Unix,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )
        .map_err(nix_err_to_io)?;

        let addr = UnixAddr::new_abstract(CONTROL_SOCKET_NAME)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        if let Err(err) = connect(fd, &SockAddr::Unix(addr)) {
            let io_err = nix_err_to_io(err);
            unsafe {
                libc::close(fd);
            }
            return Err(io_err);
        }

        Ok(unsafe { UnixStream::from_raw_fd(fd) })
    }

    fn send_control_command(command: &str) -> io::Result<Option<String>> {
        let mut stream = match connect_control_socket() {
            Ok(stream) => stream,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::AddrNotAvailable
                ) =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err),
        };

        stream.write_all(command.as_bytes())?;
        stream.flush()?;
        stream.shutdown(Shutdown::Write)?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        if response.is_empty() {
            Ok(Some(String::new()))
        } else {
            Ok(Some(response.trim().to_string()))
        }
    }

    fn request_existing_pid() -> Result<Option<i32>, Box<dyn Error>> {
        match send_control_command("pid\n") {
            Ok(Some(pid_str)) if !pid_str.is_empty() => {
                let pid = pid_str.parse::<i32>()?;
                Ok(Some(pid))
            }
            Ok(_) => Ok(None),
            Err(err) => match err.kind() {
                io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::NotFound
                | io::ErrorKind::AddrNotAvailable => Ok(None),
                _ => Err(Box::new(err)),
            },
        }
    }

    fn create_control_listener() -> io::Result<UnixListener> {
        let fd = socket(
            AddressFamily::Unix,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )
        .map_err(nix_err_to_io)?;

        if let Err(err) = nix::fcntl::fcntl(
            fd,
            nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::FD_CLOEXEC),
        ) {
            unsafe {
                libc::close(fd);
            }
            return Err(nix_err_to_io(err));
        }

        let addr = UnixAddr::new_abstract(CONTROL_SOCKET_NAME)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        if let Err(err) = bind(fd, &SockAddr::Unix(addr)) {
            unsafe {
                libc::close(fd);
            }
            return Err(nix_err_to_io(err));
        }

        if let Err(err) = listen(fd, 32) {
            unsafe {
                libc::close(fd);
            }
            return Err(nix_err_to_io(err));
        }

        Ok(unsafe { UnixListener::from_raw_fd(fd) })
    }

    fn configure_daemon_environment() -> Result<(), Box<dyn Error>> {
        umask(Mode::from_bits_truncate(0o027));
        chdir("/")?;

        let dev_null = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?;
        dup2(&dev_null, libc::STDIN_FILENO)?;
        dup2(&dev_null, libc::STDOUT_FILENO)?;
        dup2(&dev_null, libc::STDERR_FILENO)?;
        Ok(())
    }

    fn write_pid_to_pipe(pipe_fd: OwnedFd) -> Result<(), Box<dyn Error>> {
        let pid_bytes = format!("{}\n", getpid().as_raw()).into_bytes();
        let mut written = 0usize;
        while written < pid_bytes.len() {
            let chunk = write(pipe_fd.as_fd(), &pid_bytes[written..])?;
            written += chunk;
        }
        Ok(())
    }

    fn spawn_control_thread(listener: UnixListener, history: Arc<Mutex<Vec<TaskLogEntry>>>) {
        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        let mut buf = String::new();
                        if stream.read_to_string(&mut buf).is_ok() {
                            let command = buf.trim();
                            match command {
                                "pid" => {
                                    let _ = writeln!(stream, "{}", getpid().as_raw());
                                }
                                "stop" => {
                                    let _ = stream.write_all(b"ok\n");
                                    let _ = stream.flush();
                                    let _ = signal::kill(Pid::this(), Signal::SIGTERM);
                                }
                                "logs" => {
                                    let response = {
                                        if let Ok(buffer) = history.lock() {
                                            if buffer.is_empty() {
                                                String::from("No logs available\n")
                                            } else {
                                                let mut lines: Vec<String> = buffer
                                                    .iter()
                                                    .rev()
                                                    .map(|entry| entry.format_line())
                                                    .collect();
                                                lines.push(String::new());
                                                lines.join("\n")
                                            }
                                        } else {
                                            String::from("Failed to read logs\n")
                                        }
                                    };
                                    let _ = stream.write_all(response.as_bytes());
                                    let _ = stream.flush();
                                }
                                _ => {
                                    let _ = stream.write_all(b"unknown\n");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if err.kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });
    }

    fn finalize_daemon(pipe_fd: OwnedFd) -> Result<(), Box<dyn Error>> {
        configure_daemon_environment()?;
        write_pid_to_pipe(pipe_fd)?;

        let history: Arc<Mutex<Vec<TaskLogEntry>>> = Arc::new(Mutex::new(Vec::new()));
        let listener = create_control_listener()?;
        spawn_control_thread(listener, history.clone());

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let history_for_scheduler = history.clone();
            match AppConfig::load() {
                Ok(cfg) => {
                    if let Err(e) =
                        scheduler::run_scheduler_forever(cfg, history_for_scheduler).await
                    {
                        eprintln!("Scheduler error: {}", e);
                    }
                }
                Err(e) => {
                    if let Ok(mut buffer) = history.lock() {
                        buffer.push(TaskLogEntry::failure(format!(
                            "Failed to load config: {}",
                            e
                        )));
                        if buffer.len() > scheduler::MAX_HISTORY_ENTRIES {
                            let overflow = buffer.len() - scheduler::MAX_HISTORY_ENTRIES;
                            buffer.drain(0..overflow);
                        }
                    }
                    eprintln!("Failed to load config: {}", e);
                }
            }
        });
        Ok(())
    }

    pub fn start() -> Result<(), Box<dyn Error>> {
        if let Some(pid) = request_existing_pid()? {
            return Err(format!("Daemon already running with PID {}", pid).into());
        }

        let (read_fd, write_fd) = pipe().map_err(|e| format!("Failed to create pipe: {}", e))?;
        match unsafe { fork() } {
            Ok(ForkResult::Parent { child, .. }) => {
                drop(write_fd);
                let mut buffer = [0u8; PIPE_BUFFER_SIZE];
                let n = read(read_fd.as_fd(), &mut buffer)
                    .map_err(|e| format!("Failed to read daemon PID: {}", e))?;
                drop(read_fd);
                let pid_str = std::str::from_utf8(&buffer[..n])
                    .map_err(|e| format!("Failed to parse daemon PID: {}", e))?
                    .trim()
                    .to_string();
                let _ = waitpid(child, None);
                println!("Daemon started with PID {}", pid_str);
                Ok(())
            }
            Ok(ForkResult::Child) => {
                drop(read_fd);
                setsid()?;
                spawn_grandchild(write_fd)
            }
            Err(e) => {
                drop(read_fd);
                drop(write_fd);
                Err(format!("Failed to fork: {}", e).into())
            }
        }
    }

    fn spawn_grandchild(write_fd: OwnedFd) -> Result<(), Box<dyn Error>> {
        match unsafe { fork() } {
            Ok(ForkResult::Parent { .. }) => {
                drop(write_fd);
                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Child) => finalize_daemon(write_fd),
            Err(e) => {
                drop(write_fd);
                Err(format!("Failed to fork daemon: {}", e).into())
            }
        }
    }

    pub fn stop() -> Result<(), Box<dyn Error>> {
        match send_control_command("stop\n") {
            Ok(Some(response)) if response == "ok" || response.is_empty() => Ok(()),
            Ok(Some(other)) => Err(format!("Unexpected response: {}", other).into()),
            Ok(None) => Err("Daemon not running".into()),
            Err(err) => Err(Box::new(err)),
        }
    }

    pub fn restart() -> Result<(), Box<dyn Error>> {
        let _ = stop();
        start()
    }

    #[allow(dead_code)]
    pub fn status() -> Result<Option<i32>, Box<dyn Error>> {
        request_existing_pid()
    }

    pub fn logs() -> Result<(), Box<dyn Error>> {
        match send_control_command("logs\n") {
            Ok(Some(response)) if response.is_empty() => {
                println!("No logs available");
                Ok(())
            }
            Ok(Some(response)) => {
                println!("{}", response);
                Ok(())
            }
            Ok(None) => Err("Daemon not running".into()),
            Err(err) => Err(Box::new(err)),
        }
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
    #[allow(dead_code)]
    pub fn status() -> Result<Option<i32>, Box<dyn Error>> {
        Ok(None)
    }
    pub fn logs() -> Result<(), Box<dyn Error>> {
        Err("Daemon mode is only available on Unix with feature=daemon".into())
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

#[allow(dead_code)]
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

pub fn logs() -> Result<(), Box<dyn Error>> {
    #[cfg(all(unix, feature = "daemon"))]
    {
        return unix_daemon::logs();
    }
    #[cfg(not(all(unix, feature = "daemon")))]
    {
        return no_daemon::logs();
    }
}
