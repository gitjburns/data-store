mod config;
mod docling;
mod docling_activity;
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
    time::Instant,
};

use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use crate::{
    config::{CliOptions, ServiceConfig, resolve_cli_options_from_args},
    error::ApiError,
    http::build_router,
    inference::InferenceRuntime,
    logging::init_file_logging,
    state::{AppState, ShutdownSignal},
    storage::{StorageRuntime, setup_storage},
};

enum ServiceProcessRole {
    ParentComplete,
    Service(StartupReporter),
}

enum StartupReporter {
    Stdout {
        active_progress_chars: usize,
        started_at: Instant,
    },
    Pipe {
        stream: UnixStream,
        started_at: Instant,
    },
}

struct AdminTokenFile {
    path: PathBuf,
    token: String,
}

struct StartupRelayOutcome {
    startup_failed: bool,
    last_status_line: Option<String>,
    last_progress_line: Option<String>,
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
            Self::Pipe { .. } => "background",
        }
    }

    /// Return elapsed startup time from the reporter's service-process boundary.
    fn elapsed_ms(&self) -> u64 {
        self.started_at().elapsed().as_millis() as u64
    }

    /// Return the monotonic clock captured when this service process began reporting startup.
    fn started_at(&self) -> Instant {
        match self {
            Self::Stdout { started_at, .. } | Self::Pipe { started_at, .. } => *started_at,
        }
    }

    /// Emit one operator-visible startup status line.
    fn report(&mut self, message: impl AsRef<str>) -> Result<(), ApiError> {
        let message = message.as_ref();
        if message.starts_with("admin_shutdown_token=") {
            self.write_status_line(message)?;
            info!(
                event = "startup.admin_token_reported",
                mode = self.mode_label(),
                token_present = true,
                elapsed_ms = self.elapsed_ms(),
                "startup admin token reported to operator channel"
            );
            return Ok(());
        }
        self.write_status_line(message)?;
        info!(
            event = "startup.status_reported",
            mode = self.mode_label(),
            stage = startup_message_stage(message),
            message,
            elapsed_ms = self.elapsed_ms(),
            "startup status reported"
        );

        Ok(())
    }

    /// Emit the admin token to the operator channel while logging only a sanitized durable event.
    fn report_admin_shutdown_token(&mut self, token: &str) -> Result<(), ApiError> {
        self.write_status_line(&format!("admin_shutdown_token={token}"))?;
        info!(
            event = "startup.admin_token_reported",
            mode = self.mode_label(),
            token_present = true,
            elapsed_ms = self.elapsed_ms(),
            "startup admin token reported to operator channel"
        );

        Ok(())
    }

    /// Write one startup status line to stdout or the background parent pipe.
    fn write_status_line(&mut self, message: &str) -> Result<(), ApiError> {
        match self {
            Self::Stdout {
                active_progress_chars,
                ..
            } => {
                if *active_progress_chars > 0 {
                    println!();
                    *active_progress_chars = 0;
                }
                println!("{message}");
                std::io::stdout()
                    .flush()
                    .map_err(|source| ApiError::InternalIo {
                        message: format!("failed to flush startup stdout: {source}"),
                    })?;
            }
            Self::Pipe { stream, .. } => {
                writeln!(stream, "{message}").map_err(|source| ApiError::InternalIo {
                    message: format!("failed to write startup status to parent: {source}"),
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
        let message = message.as_ref();
        match self {
            Self::Stdout {
                active_progress_chars,
                ..
            } => {
                *active_progress_chars =
                    write_active_terminal_line(message, *active_progress_chars)?;
            }
            Self::Pipe { stream, .. } => {
                writeln!(stream, "{STARTUP_PROGRESS_PREFIX}{message}").map_err(|source| {
                    ApiError::InternalIo {
                        message: format!("failed to write startup progress to parent: {source}"),
                    }
                })?;
                stream.flush().map_err(|source| ApiError::InternalIo {
                    message: format!("failed to flush startup progress to parent: {source}"),
                })?;
            }
        }
        info!(
            event = "startup.progress_reported",
            mode = self.mode_label(),
            stage = startup_message_stage(message),
            message,
            elapsed_ms = self.elapsed_ms(),
            "startup progress reported"
        );

        Ok(())
    }

    /// Close the startup handoff channel so the background parent can exit.
    fn close(self) {
        if let Self::Stdout {
            active_progress_chars,
            ..
        } = self
            && active_progress_chars > 0
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

/// Bind HTTP before lengthy dependency initialization, then report readiness and serve until shutdown.
async fn run_http_service(
    config: ServiceConfig,
    admin_shutdown_token: String,
    mut reporter: StartupReporter,
) -> anyhow::Result<()> {
    let startup_started_at = Instant::now();
    let bind_address = config.bind_address();
    reporter.report(format!(
        "data-store startup mode={} bind_address={bind_address}",
        reporter.mode_label()
    ))?;
    reporter.report(format!(
        "data-store startup http=binding bind_address={bind_address}"
    ))?;
    let http_bind_started_at = Instant::now();
    info!(
        event = "startup.http_bind_started",
        %bind_address,
        elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
        "startup HTTP bind started"
    );
    let listener = match TcpListener::bind(bind_address).await {
        Ok(listener) => {
            info!(
                event = "startup.http_bound",
                %bind_address,
                elapsed_ms = http_bind_started_at.elapsed().as_millis() as u64,
                "startup HTTP bind completed"
            );
            listener
        }
        Err(source) => {
            reporter.report(format!(
                "data-store startup http=bind_failed bind_address={bind_address} error=\"{source}\""
            ))?;
            error!(
                event = "startup.http_bind_failed",
                %bind_address,
                error = %source,
                elapsed_ms = http_bind_started_at.elapsed().as_millis() as u64,
                "startup HTTP bind failed"
            );
            error!(
                event = "startup.fatal",
                stage = "http_bind",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed during HTTP bind"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            return Err(source.into());
        }
    };
    reporter.report_admin_shutdown_token(&admin_shutdown_token)?;
    let admin_token_file = match AdminTokenFile::write_current(&config, &admin_shutdown_token) {
        Ok(token_file) => token_file,
        Err(source) => {
            error!(
                event = "startup.fatal",
                stage = "admin_token_file_publish",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed while publishing admin token file"
            );
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
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "inference initialization failed"
            );
            error!(
                event = "startup.fatal",
                stage = "inference_initialization",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed during inference initialization"
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
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "storage initialization failed"
            );
            error!(
                event = "startup.fatal",
                stage = "storage_cache_initialization",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed during storage/cache initialization"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };
    let shutdown_signal = Arc::new(ShutdownSignal::default());
    let state = Arc::new(AppState::new(
        config,
        Ok(inference),
        Ok(storage),
        admin_shutdown_token.clone(),
        Arc::clone(&shutdown_signal),
    ));
    let app = build_router(state).layer(TraceLayer::new_for_http());
    reporter.report(format!(
        "data-store startup http=listening bind_address={bind_address}"
    ))?;
    info!(
        event = "startup.http_listening",
        %bind_address,
        elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
        "startup HTTP listener is ready"
    );
    let health_url = format!("http://{bind_address}/v1/health");
    reporter.report(format!(
        "data-store startup ready=true inference=true storage_cache=true health_url={health_url}"
    ))?;
    info!(
        event = "startup.ready",
        %bind_address,
        health_url = %health_url,
        inference_ready = true,
        storage_ready = true,
        elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
        "startup readiness completed"
    );
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
        .with_graceful_shutdown(wait_for_shutdown_signal(shutdown_signal))
        .await;
    admin_token_file.cleanup_if_current();
    serve_result?;
    info!(event = "service.stopped", "data store service stopped");

    Ok(())
}

impl AdminTokenFile {
    /// Publish the current startup-scoped admin token to the configured owner-only runtime file.
    fn write_current(config: &ServiceConfig, token: &str) -> Result<Self, ApiError> {
        let started_at = Instant::now();
        let path = config.admin.resolved_token_file_path();
        info!(
            event = "admin_token_file.publish_started",
            path = %path.display(),
            stage = "start",
            permissions = "0600",
            "admin token file publication started"
        );
        if let Some(parent) = path.parent() {
            if let Err(source) = fs::create_dir_all(parent) {
                error!(
                    event = "admin_token_file.publish_directory_failed",
                    path = %path.display(),
                    directory = %parent.display(),
                    stage = "create_parent_directory",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to create admin token file directory"
                );
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to create admin token file directory {}: {source}",
                        parent.display()
                    ),
                });
            }
            info!(
                event = "admin_token_file.publish_directory_ready",
                path = %path.display(),
                directory = %parent.display(),
                stage = "create_parent_directory",
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "admin token file directory ready"
            );
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                info!(
                    event = "admin_token_file.stale_file_removed",
                    path = %path.display(),
                    stage = "remove_stale_file",
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "stale admin token file removed"
                );
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    event = "admin_token_file.stale_file_absent",
                    path = %path.display(),
                    stage = "remove_stale_file",
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "no stale admin token file was present"
                );
            }
            Err(source) => {
                error!(
                    event = "admin_token_file.stale_file_remove_failed",
                    path = %path.display(),
                    stage = "remove_stale_file",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to remove stale admin token file"
                );
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to replace stale admin token file {}: {source}",
                        path.display()
                    ),
                });
            }
        }
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => file,
            Err(source) => {
                error!(
                    event = "admin_token_file.publish_create_failed",
                    path = %path.display(),
                    stage = "create_token_file",
                    permissions = "0600",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to create admin token file"
                );
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to create admin token file {}: {source}",
                        path.display()
                    ),
                });
            }
        };
        if let Err(source) = writeln!(file, "{token}") {
            error!(
                event = "admin_token_file.publish_write_failed",
                path = %path.display(),
                stage = "write_token_file",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to write admin token file"
            );
            return Err(ApiError::InternalIo {
                message: format!(
                    "failed to write admin token file {}: {source}",
                    path.display()
                ),
            });
        }
        if let Err(source) = file.sync_all() {
            error!(
                event = "admin_token_file.publish_sync_failed",
                path = %path.display(),
                stage = "sync_token_file",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to flush admin token file"
            );
            return Err(ApiError::InternalIo {
                message: format!(
                    "failed to flush admin token file {}: {source}",
                    path.display()
                ),
            });
        }
        info!(
            event = "admin_token_file.publish_completed",
            path = %path.display(),
            stage = "complete",
            permissions = "0600",
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "admin token file publication completed"
        );

        Ok(Self {
            path,
            token: token.to_string(),
        })
    }

    /// Remove the runtime token file only when it still contains this service's current token.
    fn cleanup_if_current(&self) {
        info!(
            event = "admin_token_file.cleanup_started",
            path = %self.path.display(),
            "admin token file cleanup started"
        );
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
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    event = "admin_token_file.cleanup_not_found",
                    path = %self.path.display(),
                    "admin token file already absent during cleanup"
                );
                return;
            }
            Err(source) => {
                warn!(
                    event = "admin_token_file.cleanup_failed",
                    path = %self.path.display(),
                    stage = "read_current_token",
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
                stage = "remove_current_token_file",
                error = %source,
                "failed to remove admin token file"
            );
            return;
        }
        info!(
            event = "admin_token_file.cleanup_completed",
            path = %self.path.display(),
            "admin token file cleanup completed"
        );
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
        return Ok(ServiceProcessRole::Service(StartupReporter::Pipe {
            stream,
            started_at: Instant::now(),
        }));
    }

    if foreground {
        println!("data-store bootstrap mode=foreground");
        return Ok(ServiceProcessRole::Service(StartupReporter::Stdout {
            active_progress_chars: 0,
            started_at: Instant::now(),
        }));
    }

    enter_background_process()
}

#[cfg(unix)]
/// Spawn a freshly exec'd detached service child while the parent relays startup status.
fn enter_background_process() -> Result<ServiceProcessRole, ApiError> {
    let started_at = Instant::now();
    let parent_pid = std::process::id();
    info!(
        event = "startup.background_spawn_started",
        parent_pid, "background service spawn started"
    );
    let (parent_stream, child_stream) = match UnixStream::pair() {
        Ok(streams) => streams,
        Err(source) => {
            error!(
                event = "startup.background_spawn_failed",
                parent_pid,
                stage = "create_startup_pipe",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to create background startup status pipe"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed to create startup status pipe: {source}"),
            });
        }
    };
    let startup_fd = child_stream.as_raw_fd();
    if let Err(source) = clear_close_on_exec(startup_fd) {
        error!(
            event = "startup.background_spawn_failed",
            parent_pid,
            stage = "clear_close_on_exec",
            error = %source,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "failed to prepare background startup status pipe"
        );
        return Err(source);
    }
    let executable = match env::current_exe() {
        Ok(executable) => executable,
        Err(source) => {
            error!(
                event = "startup.background_spawn_failed",
                parent_pid,
                stage = "resolve_current_executable",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to resolve executable for background spawn"
            );
            return Err(ApiError::InternalIo {
                message: format!(
                    "failed to resolve current executable for background spawn: {source}"
                ),
            });
        }
    };
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
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(source) => {
            error!(
                event = "startup.background_spawn_failed",
                parent_pid,
                stage = "spawn_child",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to spawn background service process"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed to spawn background service process: {source}"),
            });
        }
    };
    let child_pid = child.id();
    drop(child_stream);
    println!(
        "data-store bootstrap mode=background parent_pid={} child_pid={}",
        parent_pid, child_pid
    );
    info!(
        event = "startup.background_spawned",
        parent_pid,
        child_pid,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "background service process spawned"
    );
    let relay_outcome = relay_startup_status(parent_stream)?;
    info!(
        event = "startup.background_relay_completed",
        parent_pid,
        child_pid,
        startup_failed = relay_outcome.startup_failed,
        last_status_line = relay_outcome.last_status_line.as_deref().unwrap_or("none"),
        last_progress_line = relay_outcome
            .last_progress_line
            .as_deref()
            .unwrap_or("none"),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "background startup relay completed"
    );
    if relay_outcome.startup_failed {
        match child.wait() {
            Ok(status) => {
                info!(
                    event = "startup.background_child_exit_observed",
                    parent_pid,
                    child_pid,
                    exit_status = %status,
                    success = status.success(),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "background service child exit observed after startup failure"
                );
            }
            Err(source) => {
                warn!(
                    event = "startup.background_child_wait_failed",
                    parent_pid,
                    child_pid,
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to wait for background service child after startup failure"
                );
            }
        }
        error!(
            event = "startup.background_startup_failed",
            parent_pid,
            child_pid,
            last_status_line = relay_outcome.last_status_line.as_deref().unwrap_or("none"),
            last_progress_line = relay_outcome
                .last_progress_line
                .as_deref()
                .unwrap_or("none"),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "background service failed during startup"
        );
        return Err(ApiError::InternalIo {
            message: "background service failed during startup".to_string(),
        });
    }
    let child_status = match child.try_wait() {
        Ok(status) => status,
        Err(source) => {
            error!(
                event = "startup.background_child_status_failed",
                parent_pid,
                child_pid,
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to inspect background service status"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed to inspect background service status: {source}"),
            });
        }
    };
    if let Some(status) = child_status {
        info!(
            event = "startup.background_child_exit_observed",
            parent_pid,
            child_pid,
            exit_status = %status,
            success = status.success(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "background service child exit observed during startup handoff"
        );
        if !status.success() {
            error!(
                event = "startup.background_startup_failed",
                parent_pid,
                child_pid,
                exit_status = %status,
                last_status_line = relay_outcome
                    .last_status_line
                    .as_deref()
                    .unwrap_or("none"),
                last_progress_line = relay_outcome
                    .last_progress_line
                    .as_deref()
                    .unwrap_or("none"),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "background service exited during startup"
            );
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
fn relay_startup_status(stream: UnixStream) -> Result<StartupRelayOutcome, ApiError> {
    let started_at = Instant::now();
    info!(event = "startup.relay_started", "startup relay started");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let mut startup_failed = false;
    let mut active_progress_chars = 0_usize;
    let mut last_status_line = None;
    let mut last_progress_line = None;
    loop {
        line.clear();
        let bytes_read = match reader.read_line(&mut line) {
            Ok(bytes_read) => bytes_read,
            Err(source) => {
                error!(
                    event = "startup.relay_read_failed",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to read background startup status"
                );
                return Err(ApiError::InternalIo {
                    message: format!("failed to read background startup status: {source}"),
                });
            }
        };
        if bytes_read == 0 {
            break;
        }
        if let Some(progress) = line.strip_prefix(STARTUP_PROGRESS_PREFIX) {
            let progress = progress.trim_end_matches(['\r', '\n']);
            last_progress_line = Some(sanitize_startup_line(progress));
            info!(
                event = "startup.relay_progress_line",
                message = last_progress_line.as_deref().unwrap_or("none"),
                stage = startup_message_stage(progress),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup progress relayed from background child"
            );
            active_progress_chars = write_active_terminal_line(progress, active_progress_chars)?;
            continue;
        }
        if active_progress_chars > 0 {
            println!();
            active_progress_chars = 0;
        }
        let status = line.trim_end_matches(['\r', '\n']);
        last_status_line = Some(sanitize_startup_line(status));
        if status.starts_with("admin_shutdown_token=") {
            info!(
                event = "startup.relay_admin_token_line",
                token_present = true,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup admin token line relayed from background child"
            );
        } else {
            info!(
                event = "startup.relay_status_line",
                message = last_status_line.as_deref().unwrap_or("none"),
                stage = startup_message_stage(status),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup status relayed from background child"
            );
        }
        if line.contains("data-store startup fatal=")
            || line.contains("data-store startup http=bind_failed")
        {
            startup_failed = true;
            warn!(
                event = "startup.relay_failure_detected",
                message = last_status_line.as_deref().unwrap_or("none"),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup relay detected background child startup failure"
            );
        }
        print!("{line}");
        std::io::stdout()
            .flush()
            .map_err(|source| ApiError::InternalIo {
                message: format!("failed to flush startup status stdout: {source}"),
            })?;
    }
    if active_progress_chars > 0 {
        println!();
    }
    info!(
        event = "startup.relay_eof",
        startup_failed,
        last_status_line = last_status_line.as_deref().unwrap_or("none"),
        last_progress_line = last_progress_line.as_deref().unwrap_or("none"),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "startup relay reached EOF"
    );

    Ok(StartupRelayOutcome {
        startup_failed,
        last_status_line,
        last_progress_line,
    })
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

/// Write one terminal active line and pad over remnants from the previous active line.
fn write_active_terminal_line(message: &str, previous_chars: usize) -> Result<usize, ApiError> {
    let message_chars = message.chars().count();
    let padding = " ".repeat(previous_chars.saturating_sub(message_chars));
    print!("\r{message}{padding}");
    std::io::stdout()
        .flush()
        .map_err(|source| ApiError::InternalIo {
            message: format!("failed to flush active terminal line stdout: {source}"),
        })?;

    Ok(message_chars)
}

/// Extract a compact startup stage label from the existing operator-facing startup text.
fn startup_message_stage(message: &str) -> &str {
    let trimmed = message.trim();
    let startup_fields = trimmed
        .strip_prefix("data-store startup ")
        .or_else(|| trimmed.strip_prefix("data-store bootstrap "))
        .unwrap_or(trimmed);
    let first_field = startup_fields
        .split_whitespace()
        .next()
        .unwrap_or("unknown");
    first_field
        .split_once('=')
        .map(|(key, _)| key)
        .unwrap_or(first_field)
}

/// Return a log-safe version of one startup line without exposing startup-scoped secrets.
fn sanitize_startup_line(line: &str) -> String {
    if line.starts_with("admin_shutdown_token=") {
        return "admin_shutdown_token=<redacted>".to_string();
    }

    line.to_string()
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
async fn wait_for_shutdown_signal(signal: Arc<ShutdownSignal>) {
    info!(
        event = "shutdown.wait_started",
        stage = "shutdown_signal_waiting",
        "waiting for shutdown signal"
    );
    // Temporary Phase 1 adapter: the blocking condvar wait runs on Tokio's
    // blocking pool so it does not pin an async worker thread. This shim is
    // removed together with the Axum server in Phase 4.
    match tokio::task::spawn_blocking(move || signal.wait()).await {
        Ok(()) => {
            info!(
                event = "shutdown.signal_received",
                stage = "shutdown_signal_received",
                "shutdown signal received"
            );
        }
        Err(source) => {
            warn!(
                event = "shutdown.signal_wait_failed",
                stage = "shutdown_signal_waiting",
                error = %source,
                "shutdown signal wait task failed before signal"
            );
        }
    }
}

/// Encode bytes as lowercase hexadecimal without adding another dependency.
fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for value in bytes {
        output.push_str(&format!("{value:02x}"));
    }

    output
}
