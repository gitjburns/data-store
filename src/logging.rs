use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use tracing_subscriber::{
    filter::LevelFilter,
    fmt::{MakeWriter, format::FmtSpan},
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

use crate::{
    config::{LoggingConfig, LoggingLevel},
    error::ApiError,
};

#[derive(Debug, Clone)]
pub struct LoggingRuntime {
    /// Absolute path actually used for the file sink after config-relative resolution.
    pub resolved_file_path: PathBuf,
}

#[derive(Debug, Clone)]
struct SharedLogWriter {
    /// Shared append-only handle used by tracing's per-event writer guards.
    file: Arc<Mutex<File>>,
}

#[derive(Debug)]
struct SharedLogGuard {
    /// One event-scoped writer guard around the process-wide log file.
    file: Arc<Mutex<File>>,
}

/// Initialize the service log file and install the global tracing subscriber.
pub fn init_file_logging(
    config: &LoggingConfig,
    config_root: &Path,
) -> Result<LoggingRuntime, ApiError> {
    let resolved_file_path = config.resolved_file_path(config_root);
    // The service is intended to run unattended, so a missing first-run log
    // directory is created before any long-lived work starts.
    if let Some(parent) = resolved_file_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to create log directory {}: {source}",
                parent.display()
            ),
        })?;
    }
    // Append mode preserves previous service runs while avoiding a separate
    // rotation policy in this first operational-hardening slice.
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&resolved_file_path)
        .map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to open log file {}: {source}",
                resolved_file_path.display()
            ),
        })?;
    let writer = SharedLogWriter {
        file: Arc::new(Mutex::new(file)),
    };
    let filter = config.level.to_level_filter();
    // The existing formatter includes inherited work-context fields alongside
    // events. Context spans are metadata, not additional start/close log records.
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_thread_ids(false)
        .with_span_events(FmtSpan::NONE)
        .with_writer(writer)
        .with_filter(filter);

    tracing_subscriber::registry()
        .with(layer)
        .try_init()
        .map_err(|source| ApiError::InternalIo {
            message: format!("failed to initialize service logging: {source}"),
        })?;

    Ok(LoggingRuntime { resolved_file_path })
}

impl LoggingLevel {
    /// Convert config-backed log level values into tracing subscriber filters.
    fn to_level_filter(self) -> LevelFilter {
        match self {
            Self::Trace => LevelFilter::TRACE,
            Self::Debug => LevelFilter::DEBUG,
            Self::Info => LevelFilter::INFO,
            Self::Warn => LevelFilter::WARN,
            Self::Error => LevelFilter::ERROR,
        }
    }
}

impl<'writer> MakeWriter<'writer> for SharedLogWriter {
    type Writer = SharedLogGuard;

    /// Return a shared file writer for one tracing event.
    fn make_writer(&'writer self) -> Self::Writer {
        SharedLogGuard {
            file: Arc::clone(&self.file),
        }
    }
}

impl Write for SharedLogGuard {
    /// Write one tracing buffer through the shared append-only log file.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("log file lock is poisoned"))?;

        file.write(buf)
    }

    /// Flush the shared log file so completed events are visible to service operators.
    fn flush(&mut self) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("log file lock is poisoned"))?;

        file.flush()
    }
}
