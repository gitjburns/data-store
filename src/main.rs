mod config;
mod docling;
mod error;
mod http;
mod inference;
mod logging;
mod source;
mod state;
mod storage;
mod types;
mod units;

use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    os::unix::{
        fs::OpenOptionsExt,
        io::{AsRawFd, FromRawFd},
        net::UnixStream,
        process::CommandExt,
    },
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use crate::{
    config::{CliOptions, ServiceConfig, resolve_cli_options_from_args},
    error::ApiError,
    http::build_router,
    inference::InferenceRuntime,
    logging::init_file_logging,
    state::AppState,
    storage::{StorageRuntime, setup_storage},
};

enum ServiceProcessRole {
    ParentComplete,
    Service(StartupReporter),
}

enum StartupReporter {
    Stdout { progress_active: bool },
    Pipe(UnixStream),
}

struct AdminTokenFile {
    path: PathBuf,
    token: String,
}

const STARTUP_PROGRESS_PREFIX: &str = "__data_store_progress__";
const BACKGROUND_STARTUP_FD_ENV: &str = "DATA_STORE_BACKGROUND_STARTUP_FD";
const F_GETFD: i32 = 1;
const F_SETFD: i32 = 2;
const FD_CLOEXEC: i32 = 1;

unsafe extern "C" {
    fn setsid() -> i32;
    fn fcntl(fd: i32, cmd: i32, ...) -> i32;
}

impl StartupReporter {
    /// Return the human-readable execution mode for startup handoff output.
    fn mode_label(&self) -> &'static str {
        match self {
            Self::Stdout { .. } => "foreground",
            Self::Pipe(_) => "background",
        }
    }

    /// Emit one operator-visible startup status line.
    fn report(&mut self, message: impl AsRef<str>) -> Result<(), ApiError> {
        match self {
            Self::Stdout { progress_active } => {
                if *progress_active {
                    println!();
                    *progress_active = false;
                }
                println!("{}", message.as_ref());
                std::io::stdout()
                    .flush()
                    .map_err(|source| ApiError::InternalIo {
                        message: format!("failed to flush startup stdout: {source}"),
                    })?;
            }
            Self::Pipe(stream) => {
                writeln!(stream, "{}", message.as_ref()).map_err(|source| {
                    ApiError::InternalIo {
                        message: format!("failed to write startup status to parent: {source}"),
                    }
                })?;
                stream.flush().map_err(|source| ApiError::InternalIo {
                    message: format!("failed to flush startup status to parent: {source}"),
                })?;
            }
        }

        Ok(())
    }

    /// Emit a transient startup progress update that the terminal can overwrite in place.
    fn report_progress(&mut self, message: impl AsRef<str>) -> Result<(), ApiError> {
        match self {
            Self::Stdout { progress_active } => {
                print!("\r\u{1b}[2K{}", message.as_ref());
                *progress_active = true;
                std::io::stdout()
                    .flush()
                    .map_err(|source| ApiError::InternalIo {
                        message: format!("failed to flush startup progress stdout: {source}"),
                    })?;
            }
            Self::Pipe(stream) => {
                writeln!(stream, "{STARTUP_PROGRESS_PREFIX}{}", message.as_ref()).map_err(
                    |source| ApiError::InternalIo {
                        message: format!("failed to write startup progress to parent: {source}"),
                    },
                )?;
                stream.flush().map_err(|source| ApiError::InternalIo {
                    message: format!("failed to flush startup progress to parent: {source}"),
                })?;
            }
        }

        Ok(())
    }

    /// Close the startup handoff channel so the background parent can exit.
    fn close(self) {
        if let Self::Stdout {
            progress_active: true,
        } = self
        {
            println!();
        }
    }
}

/// Start the standalone Data Store service.
fn main() -> anyhow::Result<()> {
    let cli_options = resolve_cli_options_from_args()?;
    // Bootstrap output stays on stdout so a launcher can find config/log
    // diagnostics before it backgrounds the service.
    println!(
        "data-store bootstrap config_path={}",
        cli_options.config_path.display()
    );
    let config = ServiceConfig::load(cli_options.config_path.clone())?;
    let resolved_log_path = config.logging.resolved_file_path();
    println!(
        "data-store bootstrap logging.file_path={} logging.resolved_file_path={} logging.level={}",
        config.logging.file_path.display(),
        resolved_log_path.display(),
        config.logging.level.as_str()
    );
    // Operational logs switch to the configured file here. Config/CLI failures
    // before this point still surface through stdout/stderr.
    let logging = init_file_logging(&config.logging)?;
    println!(
        "data-store bootstrap file_logging=initialized path={}",
        logging.resolved_file_path.display()
    );
    info!(
        event = "service.bootstrap",
        config_path = %cli_options.config_path.display(),
        log_file_path = %logging.resolved_file_path.display(),
        log_level = config.logging.level.as_str(),
        "service bootstrap completed"
    );
    if cli_options.setup_storage {
        match setup_storage(&config.storage) {
            Ok(db_path) => {
                println!("storage schema ready at {}", db_path.display());
                info!(
                    event = "storage.setup.completed",
                    db_path = %db_path.display(),
                    "storage setup completed"
                );
                return Ok(());
            }
            Err(source) => {
                error!(
                    event = "storage.setup.failed",
                    error = %source,
                    "storage setup failed"
                );
                return Err(source.into());
            }
        }
    }
    if cli_options.smoke_dense {
        run_dense_smoke(&cli_options, &config)?;
        return Ok(());
    }

    let bind_address = config.bind_address();
    println!("data-store bootstrap bind_address={bind_address}");

    let process_role = enter_service_process(cli_options.foreground)?;
    let reporter = match process_role {
        ServiceProcessRole::ParentComplete => return Ok(()),
        ServiceProcessRole::Service(reporter) => reporter,
    };
    let admin_shutdown_token = generate_admin_shutdown_token()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_http_service(config, admin_shutdown_token, reporter))
}

/// Initialize service dependencies, bind HTTP, report readiness, and serve until shutdown.
async fn run_http_service(
    config: ServiceConfig,
    admin_shutdown_token: String,
    mut reporter: StartupReporter,
) -> anyhow::Result<()> {
    let bind_address = config.bind_address();
    reporter.report(format!(
        "data-store startup mode={} bind_address={bind_address}",
        reporter.mode_label()
    ))?;
    reporter.report(format!("admin_shutdown_token={admin_shutdown_token}"))?;
    let admin_token_file = match AdminTokenFile::write_current(&config, &admin_shutdown_token) {
        Ok(token_file) => token_file,
        Err(source) => {
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            return Err(source.into());
        }
    };
    reporter.report(format!(
        "data-store startup admin_token_file=ready path={}",
        admin_token_file.path.display()
    ))?;
    info!(
        event = "service.initializing",
        %bind_address,
        "initializing inference and storage"
    );

    reporter.report("data-store startup inference=initializing")?;
    let mut report_inference_progress = |message: &str| {
        let startup_message = format!("data-store startup inference={message}");
        if uses_count_progress(message) {
            reporter.report_progress(startup_message)
        } else {
            reporter.report(startup_message)
        }
    };
    let inference_result =
        InferenceRuntime::initialize_with_progress(&config, &mut report_inference_progress);
    drop(report_inference_progress);
    let inference = match inference_result {
        Ok(runtime) => {
            reporter.report(format!(
                "data-store startup inference=ready details=\"{}\"",
                runtime.health_details().join(" | ")
            ))?;
            info!(
                event = "inference.initialized",
                "inference initialized successfully"
            );
            runtime
        }
        Err(source) => {
            reporter.report(format!(
                "data-store startup inference=not_ready error=\"{source}\""
            ))?;
            error!(
                event = "inference.initialization_failed",
                error = %source,
                "inference initialization failed"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };

    reporter.report("data-store startup storage_cache=initializing")?;
    let storage_result = StorageRuntime::open(
        &config.storage,
        &config.models.dense,
        &config.models.colbert,
    );
    let storage = match storage_result {
        Ok(runtime) => {
            reporter.report(format!(
                "data-store startup storage_cache=ready details=\"{}\"",
                runtime.health_details().join(" | ")
            ))?;
            info!(
                event = "storage.initialized",
                "storage initialized successfully"
            );
            runtime
        }
        Err(source) => {
            reporter.report(format!(
                "data-store startup storage_cache=not_ready error=\"{source}\""
            ))?;
            error!(
                event = "storage.initialization_failed",
                error = %source,
                "storage initialization failed"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let state = Arc::new(AppState::new(
        config,
        Ok(inference),
        Ok(storage),
        admin_shutdown_token.clone(),
        shutdown_sender,
    ));
    let app = build_router(state).layer(TraceLayer::new_for_http());
    reporter.report(format!(
        "data-store startup http=binding bind_address={bind_address}"
    ))?;
    let listener = match TcpListener::bind(bind_address).await {
        Ok(listener) => listener,
        Err(source) => {
            reporter.report(format!(
                "data-store startup http=bind_failed bind_address={bind_address} error=\"{source}\""
            ))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };

    reporter.report(format!(
        "data-store startup http=listening bind_address={bind_address}"
    ))?;
    reporter.report(format!(
        "data-store startup ready=true inference=true storage_cache=true health_url=http://{bind_address}/v1/health"
    ))?;
    reporter.close();
    info!(
        event = "service.listening",
        %bind_address,
        ready = true,
        inference_ready = true,
        storage_ready = true,
        "data store service listening"
    );
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal(shutdown_receiver))
        .await;
    admin_token_file.cleanup_if_current();
    serve_result?;
    info!(event = "service.stopped", "data store service stopped");

    Ok(())
}

impl AdminTokenFile {
    /// Publish the current startup-scoped admin token to the configured owner-only runtime file.
    fn write_current(config: &ServiceConfig, token: &str) -> Result<Self, ApiError> {
        let path = config.admin.resolved_token_file_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| ApiError::InternalIo {
                message: format!(
                    "failed to create admin token file directory {}: {source}",
                    parent.display()
                ),
            })?;
        }
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to replace stale admin token file {}: {source}",
                        path.display()
                    ),
                });
            }
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(|source| ApiError::InternalIo {
                message: format!(
                    "failed to create admin token file {}: {source}",
                    path.display()
                ),
            })?;
        writeln!(file, "{token}").map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to write admin token file {}: {source}",
                path.display()
            ),
        })?;
        file.sync_all().map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to flush admin token file {}: {source}",
                path.display()
            ),
        })?;

        Ok(Self {
            path,
            token: token.to_string(),
        })
    }

    /// Remove the runtime token file only when it still contains this service's current token.
    fn cleanup_if_current(&self) {
        let mut contents = String::new();
        let read_result = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .and_then(|mut file| {
                file.read_to_string(&mut contents)?;
                Ok(())
            });
        match read_result {
            Ok(()) => {
                if contents.trim_end_matches(['\r', '\n']) != self.token {
                    warn!(
                        event = "admin_token_file.cleanup_skipped",
                        path = %self.path.display(),
                        "admin token file did not contain the current service token"
                    );
                    return;
                }
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return,
            Err(source) => {
                warn!(
                    event = "admin_token_file.cleanup_failed",
                    path = %self.path.display(),
                    error = %source,
                    "failed to read admin token file before cleanup"
                );
                return;
            }
        }
        if let Err(source) = fs::remove_file(&self.path) {
            warn!(
                event = "admin_token_file.cleanup_failed",
                path = %self.path.display(),
                error = %source,
                "failed to remove admin token file"
            );
        }
    }
}

/// Decide whether this invocation should continue as the service or relay child startup output as the original parent.
fn enter_service_process(foreground: bool) -> Result<ServiceProcessRole, ApiError> {
    if let Some(fd_value) = env::var_os(BACKGROUND_STARTUP_FD_ENV) {
        let fd_text = fd_value.to_string_lossy();
        let fd = fd_text
            .parse::<i32>()
            .map_err(|source| ApiError::InternalIo {
                message: format!("invalid background startup fd {fd_text}: {source}"),
            })?;
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        return Ok(ServiceProcessRole::Service(StartupReporter::Pipe(stream)));
    }

    if foreground {
        println!("data-store bootstrap mode=foreground");
        return Ok(ServiceProcessRole::Service(StartupReporter::Stdout {
            progress_active: false,
        }));
    }

    enter_background_process()
}

#[cfg(unix)]
/// Spawn a freshly exec'd detached service child while the parent relays startup status.
fn enter_background_process() -> Result<ServiceProcessRole, ApiError> {
    let (parent_stream, child_stream) =
        UnixStream::pair().map_err(|source| ApiError::InternalIo {
            message: format!("failed to create startup status pipe: {source}"),
        })?;
    let startup_fd = child_stream.as_raw_fd();
    clear_close_on_exec(startup_fd)?;
    let executable = env::current_exe().map_err(|source| ApiError::InternalIo {
        message: format!("failed to resolve current executable for background spawn: {source}"),
    })?;
    let mut command = Command::new(executable);
    command
        .args(env::args_os().skip(1))
        .env(BACKGROUND_STARTUP_FD_ENV, startup_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(|| {
            if setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().map_err(|source| ApiError::InternalIo {
        message: format!("failed to spawn background service process: {source}"),
    })?;
    drop(child_stream);
    println!(
        "data-store bootstrap mode=background parent_pid={} child_pid={}",
        std::process::id(),
        child.id()
    );
    let startup_failed = relay_startup_status(parent_stream)?;
    if startup_failed {
        let _ = child.wait();
        return Err(ApiError::InternalIo {
            message: "background service failed during startup".to_string(),
        });
    }
    if let Some(status) = child.try_wait().map_err(|source| ApiError::InternalIo {
        message: format!("failed to inspect background service status: {source}"),
    })? {
        if !status.success() {
            return Err(ApiError::InternalIo {
                message: format!("background service exited during startup with status {status}"),
            });
        }
    }

    Ok(ServiceProcessRole::ParentComplete)
}

#[cfg(not(unix))]
/// Report the unsupported background mode clearly on non-Unix targets.
fn enter_background_process() -> Result<ServiceProcessRole, ApiError> {
    Err(ApiError::InvalidCli {
        message: "--foreground is required on non-Unix targets".to_string(),
    })
}

/// Relay newline-delimited startup status from the background child to the invoking terminal.
fn relay_startup_status(stream: UnixStream) -> Result<bool, ApiError> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let mut startup_failed = false;
    let mut progress_active = false;
    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .map_err(|source| ApiError::InternalIo {
                message: format!("failed to read background startup status: {source}"),
            })?;
        if bytes_read == 0 {
            break;
        }
        if let Some(progress) = line.strip_prefix(STARTUP_PROGRESS_PREFIX) {
            print!("\r\u{1b}[2K{}", progress.trim_end_matches(['\r', '\n']));
            std::io::stdout()
                .flush()
                .map_err(|source| ApiError::InternalIo {
                    message: format!("failed to flush startup progress stdout: {source}"),
                })?;
            progress_active = true;
            continue;
        }
        if progress_active {
            println!();
            progress_active = false;
        }
        if line.contains("data-store startup fatal=")
            || line.contains("data-store startup http=bind_failed")
        {
            startup_failed = true;
        }
        print!("{line}");
        std::io::stdout()
            .flush()
            .map_err(|source| ApiError::InternalIo {
                message: format!("failed to flush startup status stdout: {source}"),
            })?;
    }
    if progress_active {
        println!();
    }

    Ok(startup_failed)
}

#[cfg(unix)]
/// Keep the startup-status pipe open across exec so the fresh child can report readiness.
fn clear_close_on_exec(fd: i32) -> Result<(), ApiError> {
    let flags = unsafe { fcntl(fd, F_GETFD) };
    if flags < 0 {
        return Err(ApiError::InternalIo {
            message: format!(
                "failed to inspect startup status fd flags: {}",
                io::Error::last_os_error()
            ),
        });
    }
    if unsafe { fcntl(fd, F_SETFD, flags & !FD_CLOEXEC) } < 0 {
        return Err(ApiError::InternalIo {
            message: format!(
                "failed to clear close-on-exec for startup status fd: {}",
                io::Error::last_os_error()
            ),
        });
    }

    Ok(())
}

fn uses_count_progress(message: &str) -> bool {
    message.split_whitespace().any(|field| {
        let Some((_, value)) = field.split_once('=') else {
            return false;
        };
        let Some((current, total)) = value.split_once('/') else {
            return false;
        };
        !current.is_empty()
            && !total.is_empty()
            && current.chars().all(|value| value.is_ascii_digit())
            && total.chars().all(|value| value.is_ascii_digit())
    })
}

/// Initialize inference, report dense readiness, and exit without starting HTTP.
fn run_dense_smoke(cli_options: &CliOptions, config: &ServiceConfig) -> anyhow::Result<()> {
    let runtime = InferenceRuntime::initialize(config)?;
    println!(
        "dense smoke initialized from {}",
        cli_options.config_path.display()
    );
    for detail in runtime.health_details() {
        println!("{detail}");
    }

    info!(
        event = "inference.smoke.completed",
        config_path = %cli_options.config_path.display(),
        "dense smoke completed"
    );
    Ok(())
}

/// Generate one startup-scoped admin shutdown token from OS randomness.
fn generate_admin_shutdown_token() -> anyhow::Result<String> {
    let mut token_bytes = [0u8; 32];
    getrandom::fill(&mut token_bytes)?;
    Ok(hex_encode(&token_bytes))
}

/// Wait until the protected admin shutdown route signals service termination.
async fn wait_for_shutdown_signal(receiver: oneshot::Receiver<()>) {
    let _ = receiver.await;
}

/// Encode bytes as lowercase hexadecimal without adding another dependency.
fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for value in bytes {
        output.push_str(&format!("{value:02x}"));
    }

    output
}
