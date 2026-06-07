use std::{
    fs,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::mpsc,
    task::{self, JoinHandle},
    time::sleep,
};
use tracing::{error, info};

use crate::{
    config::DoclingConfig,
    docling_activity::{
        DoclingActivityReport, format_docling_activity_message, inspect_docling_activity,
    },
    error::ApiError,
    source::ResolvedSource,
};

static CONVERSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const MAX_DIAGNOSTIC_CHARS: usize = 16_000;
const CHILD_OUTPUT_READ_CHUNK_BYTES: usize = 8_192;
const DOCLING_WAIT_POLL_MILLIS: u64 = 250;
const POST_100_FIRST_FEEDBACK_SECONDS: u64 = 1;
const POST_100_FEEDBACK_CADENCE_SECONDS: u64 = 1;
const POST_100_SAMPLE_SECONDS: u64 = 0;

#[derive(Debug, Clone)]
pub struct ResolvedDoclingOptions {
    pub pdf_backend: String,
    pub ocr_mode: String,
    pub device: String,
    pub num_threads: u32,
    pub page_batch_size: u32,
    pub document_timeout_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct DoclingConversionResult {
    pub source: ResolvedSource,
    pub options: ResolvedDoclingOptions,
    pub output_dir: PathBuf,
    pub markdown_path: PathBuf,
    pub markdown: String,
    pub args: Vec<String>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct DoclingProgressUpdate {
    pub message: String,
    pub percentage: Option<u64>,
}

struct DoclingRunOutput {
    status: ExitStatus,
    stdout: String,
    stderr: String,
    timed_out: bool,
}

#[derive(Debug, Clone, Default)]
struct DoclingProgressState {
    latest_message: Option<String>,
    latest_percentage: Option<u64>,
    latest_progress_at: Option<Instant>,
    completed_reported_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct DoclingProgressSnapshot {
    latest_message: Option<String>,
    latest_percentage: Option<u64>,
    latest_progress_age: Option<Duration>,
    completed_reported_for: Option<Duration>,
}

/// Convert one resolved PDF source to markdown using only service-configured Docling options.
///
/// Diagnostics are bounded but preserved so conversion failures remain explicit and inspectable.
pub async fn convert_source_to_markdown(
    config: &DoclingConfig,
    index_root: &Path,
    source: ResolvedSource,
    progress_sender: Option<mpsc::Sender<DoclingProgressUpdate>>,
) -> Result<DoclingConversionResult, ApiError> {
    let options = resolve_docling_options(config)?;
    let output_dir = create_conversion_output_dir(index_root)?;
    let args = build_docling_args(&output_dir, &source.absolute_path, &options);
    let output = run_docling(
        config,
        &args,
        index_root,
        &output_dir,
        &source,
        progress_sender,
    )
    .await?;
    let stdout = truncate_diagnostic_text(&output.stdout);
    let stderr = truncate_diagnostic_text(&output.stderr);

    if output.timed_out || !output.status.success() {
        let timeout_context = if output.timed_out {
            format!(" after timeoutSeconds={}", options.document_timeout_seconds)
        } else {
            String::new()
        };
        return Err(ApiError::DoclingConversion {
            message: format!(
                "Docling conversion failed{} for {} with status {}; args={}; stderr={}; stdout={}",
                timeout_context,
                source.absolute_path.display(),
                format_exit_status(output.status.code(), output.status.signal()),
                args.join(" "),
                stderr,
                stdout
            ),
        });
    }

    let markdown_path = find_markdown_artifact(&output_dir, &source.absolute_path)?;
    let markdown = read_and_normalize_markdown(&markdown_path)?;

    Ok(DoclingConversionResult {
        source,
        options,
        output_dir,
        markdown_path,
        markdown,
        args,
        stdout,
        stderr,
    })
}

/// Resolve service-configured Docling options for one conversion attempt.
fn resolve_docling_options(config: &DoclingConfig) -> Result<ResolvedDoclingOptions, ApiError> {
    let pdf_backend = config.pdf_backend.trim().to_string();
    let ocr_mode = config.ocr_mode.trim().to_string();
    let device = config.device.trim().to_string();
    let num_threads = config.num_threads;
    let page_batch_size = config.page_batch_size;
    let document_timeout_seconds = config.document_timeout_seconds;

    if !matches!(ocr_mode.as_str(), "auto" | "on" | "off") {
        return Err(ApiError::DoclingConversion {
            message: "docling.ocr_mode must be one of auto, on, or off".to_string(),
        });
    }

    Ok(ResolvedDoclingOptions {
        pdf_backend,
        ocr_mode,
        device,
        num_threads,
        page_batch_size,
        document_timeout_seconds,
    })
}

/// Create one unique service-owned directory for a Docling conversion attempt.
fn create_conversion_output_dir(index_root: &Path) -> Result<PathBuf, ApiError> {
    let root = index_root.join("docling-conversions");
    fs::create_dir_all(&root).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to create Docling conversion root at {}: {source}",
            root.display()
        ),
    })?;

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| ApiError::InternalIo {
            message: format!("system clock is before UNIX epoch: {source}"),
        })?
        .as_millis();
    let sequence = CONVERSION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let output_dir = root.join(format!("conversion-{timestamp_ms}-{sequence}"));

    fs::create_dir(&output_dir).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to create Docling conversion directory at {}: {source}",
            output_dir.display()
        ),
    })?;

    Ok(output_dir)
}

/// Build Docling CLI arguments without shell interpolation.
fn build_docling_args(
    output_dir: &Path,
    source_path: &Path,
    options: &ResolvedDoclingOptions,
) -> Vec<String> {
    let mut args = vec![
        "--to".to_string(),
        "md".to_string(),
        "--output".to_string(),
        output_dir.display().to_string(),
        "--image-export-mode".to_string(),
        "placeholder".to_string(),
        "--pdf-backend".to_string(),
        options.pdf_backend.clone(),
        "--document-timeout".to_string(),
        options.document_timeout_seconds.to_string(),
        "--device".to_string(),
        options.device.clone(),
        "--num-threads".to_string(),
        options.num_threads.to_string(),
    ];

    if options.ocr_mode == "on" {
        args.push("--ocr".to_string());
    }
    if options.ocr_mode == "off" {
        args.push("--no-ocr".to_string());
    }
    args.push("--page-batch-size".to_string());
    args.push(options.page_batch_size.to_string());

    args.push(source_path.display().to_string());
    args
}

/// Run the configured Docling executable directly with no stdin and no shell interpretation.
///
/// The caller owns diagnostic truncation so success and failure paths preserve the same output shape.
async fn run_docling(
    config: &DoclingConfig,
    args: &[String],
    index_root: &Path,
    output_dir: &Path,
    source: &ResolvedSource,
    progress_sender: Option<mpsc::Sender<DoclingProgressUpdate>>,
) -> Result<DoclingRunOutput, ApiError> {
    let started = Instant::now();
    info!(
        event = "docling.process.starting",
        executable_path = %config.docling_path.display(),
        python_path = %config.python_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        absolute_source = %source.absolute_path.display(),
        output_dir = %output_dir.display(),
        working_dir = %index_root.display(),
        timeout_seconds = config.document_timeout_seconds,
        args_count = args.len(),
        elapsed_ms = 0_u64,
        "Docling process starting"
    );
    let mut child = match Command::new(&config.docling_path)
        .args(args)
        .current_dir(index_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(io_error) => {
            error!(
                event = "docling.process.spawn_failed",
                executable_path = %config.docling_path.display(),
                python_path = %config.python_path.display(),
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                absolute_source = %source.absolute_path.display(),
                output_dir = %output_dir.display(),
                working_dir = %index_root.display(),
                timeout_seconds = config.document_timeout_seconds,
                args_count = args.len(),
                error = %io_error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling process spawn failed"
            );
            return Err(ApiError::DoclingUnavailable {
                message: format!(
                    "Docling CLI failed to start at {}; configured python_path is {}: {io_error}",
                    config.docling_path.display(),
                    config.python_path.display()
                ),
            });
        }
    };
    let process_id = child.id();
    info!(
        event = "docling.process.spawned",
        executable_path = %config.docling_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        process_id = ?process_id,
        timeout_seconds = config.document_timeout_seconds,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling process spawned"
    );
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            error!(
                event = "docling.process.pipe_unavailable",
                executable_path = %config.docling_path.display(),
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                output_dir = %output_dir.display(),
                process_id = ?process_id,
                pipe = "stdout",
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling child stdout pipe unavailable"
            );
            return Err(ApiError::InternalIo {
                message: "Docling child stdout pipe was not available".to_string(),
            });
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            error!(
                event = "docling.process.pipe_unavailable",
                executable_path = %config.docling_path.display(),
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                output_dir = %output_dir.display(),
                process_id = ?process_id,
                pipe = "stderr",
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling child stderr pipe unavailable"
            );
            return Err(ApiError::InternalIo {
                message: "Docling child stderr pipe was not available".to_string(),
            });
        }
    };
    let source_requested = source.requested.clone();
    let relative_source = source.relative_path.display().to_string();
    let output_dir_for_log = output_dir.display().to_string();
    let progress_state = Arc::new(Mutex::new(DoclingProgressState::default()));
    info!(
        event = "docling.child_output_reader.spawned",
        task_purpose = "read_docling_child_output",
        pipe = "stdout",
        process_id = ?process_id,
        source_requested = %source_requested,
        relative_source = %relative_source,
        output_dir = %output_dir_for_log,
        "Docling child output reader task spawned"
    );
    let stdout_reader = tokio::spawn(read_child_output(
        stdout,
        "stdout",
        process_id,
        source_requested.clone(),
        relative_source.clone(),
        output_dir_for_log.clone(),
        None,
        None,
    ));
    info!(
        event = "docling.child_output_reader.spawned",
        task_purpose = "read_docling_child_output",
        pipe = "stderr",
        process_id = ?process_id,
        source_requested = %source_requested,
        relative_source = %relative_source,
        output_dir = %output_dir_for_log,
        "Docling child output reader task spawned"
    );
    let stderr_reader = tokio::spawn(read_child_output(
        stderr,
        "stderr",
        process_id,
        source_requested,
        relative_source,
        output_dir_for_log,
        progress_sender.clone(),
        Some(progress_state.clone()),
    ));
    let expected_markdown_path = expected_markdown_artifact_path(output_dir, &source.absolute_path);
    let (status, timed_out) = wait_for_docling_process(
        &mut child,
        config,
        output_dir,
        source,
        process_id,
        started,
        progress_state,
        progress_sender,
        expected_markdown_path,
    )
    .await?;
    info!(
        event = "docling.process.wait_completed",
        executable_path = %config.docling_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        process_id = ?process_id,
        timed_out,
        exit_code = ?status.code(),
        signal = ?status.signal(),
        timeout_seconds = config.document_timeout_seconds,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling process wait completed"
    );
    let stdout = join_child_output(stdout_reader, "stdout", process_id, started).await;
    let stderr = join_child_output(stderr_reader, "stderr", process_id, started).await;
    let stdout = stdout?;
    let stderr = stderr?;
    let stdout_diagnostic = truncate_diagnostic_text(&stdout);
    let stderr_diagnostic = truncate_diagnostic_text(&stderr);
    if timed_out || !status.success() {
        error!(
            event = "docling.process.failed",
            executable_path = %config.docling_path.display(),
            source_requested = %source.requested,
            relative_source = %source.relative_path.display(),
            output_dir = %output_dir.display(),
            process_id = ?process_id,
            timed_out,
            exit_code = ?status.code(),
            signal = ?status.signal(),
            timeout_seconds = config.document_timeout_seconds,
            stdout_chars = stdout.chars().count(),
            stderr_chars = stderr.chars().count(),
            stdout = %stdout_diagnostic,
            stderr = %stderr_diagnostic,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Docling process failed"
        );
    } else {
        info!(
            event = "docling.process.completed",
            executable_path = %config.docling_path.display(),
            source_requested = %source.requested,
            relative_source = %source.relative_path.display(),
            output_dir = %output_dir.display(),
            process_id = ?process_id,
            timed_out,
            exit_code = ?status.code(),
            signal = ?status.signal(),
            timeout_seconds = config.document_timeout_seconds,
            stdout_chars = stdout.chars().count(),
            stderr_chars = stderr.chars().count(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Docling process completed"
        );
    }

    Ok(DoclingRunOutput {
        status,
        stdout,
        stderr,
        timed_out,
    })
}

/// Wait for Docling while emitting post-100% activity feedback at a bounded cadence.
async fn wait_for_docling_process(
    child: &mut tokio::process::Child,
    config: &DoclingConfig,
    output_dir: &Path,
    source: &ResolvedSource,
    process_id: Option<u32>,
    started: Instant,
    progress_state: Arc<Mutex<DoclingProgressState>>,
    progress_sender: Option<mpsc::Sender<DoclingProgressUpdate>>,
    expected_markdown_path: PathBuf,
) -> Result<(ExitStatus, bool), ApiError> {
    let timeout_duration = Duration::from_secs(config.document_timeout_seconds);
    let mut last_feedback_at = None;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok((status, false)),
            Ok(None) => {}
            Err(wait_error) => {
                error!(
                    event = "docling.process.wait_failed",
                    executable_path = %config.docling_path.display(),
                    source_requested = %source.requested,
                    relative_source = %source.relative_path.display(),
                    output_dir = %output_dir.display(),
                    process_id = ?process_id,
                    timeout_seconds = config.document_timeout_seconds,
                    error = %wait_error,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Docling process wait failed"
                );
                return Err(ApiError::InternalIo {
                    message: format!("failed while waiting for Docling CLI: {wait_error}"),
                });
            }
        }

        if started.elapsed() >= timeout_duration {
            return timeout_docling_process(child, config, output_dir, source, process_id, started)
                .await;
        }

        let now = Instant::now();
        let snapshot = snapshot_docling_progress(&progress_state, now);
        if should_emit_post_100_feedback(&snapshot, last_feedback_at, now) {
            last_feedback_at = Some(now);
            emit_post_100_docling_feedback(
                process_id,
                output_dir,
                source,
                started,
                timeout_duration,
                &snapshot,
                progress_sender.as_ref(),
                expected_markdown_path.clone(),
            )
            .await;
        }

        let poll_duration = Duration::from_millis(DOCLING_WAIT_POLL_MILLIS)
            .min(timeout_duration.saturating_sub(started.elapsed()));
        sleep(poll_duration).await;
    }
}

/// Kill a Docling child after the configured document timeout has been reached.
async fn timeout_docling_process(
    child: &mut tokio::process::Child,
    config: &DoclingConfig,
    output_dir: &Path,
    source: &ResolvedSource,
    process_id: Option<u32>,
    started: Instant,
) -> Result<(ExitStatus, bool), ApiError> {
    error!(
        event = "docling.process.timeout_reached",
        executable_path = %config.docling_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        process_id = ?process_id,
        timeout_seconds = config.document_timeout_seconds,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling process timeout reached"
    );
    let kill_result = child.start_kill();
    let status = match child.wait().await {
        Ok(status) => status,
        Err(io_error) => {
            error!(
                event = "docling.process.timeout_wait_failed",
                executable_path = %config.docling_path.display(),
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                output_dir = %output_dir.display(),
                process_id = ?process_id,
                timeout_seconds = config.document_timeout_seconds,
                kill_requested = kill_result.is_ok(),
                error = %io_error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling process wait after timeout failed"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed while waiting for timed-out Docling CLI: {io_error}"),
            });
        }
    };
    if let Err(kill_error) = kill_result {
        error!(
            event = "docling.process.kill_failed",
            executable_path = %config.docling_path.display(),
            source_requested = %source.requested,
            relative_source = %source.relative_path.display(),
            output_dir = %output_dir.display(),
            process_id = ?process_id,
            exit_code = ?status.code(),
            signal = ?status.signal(),
            timeout_seconds = config.document_timeout_seconds,
            error = %kill_error,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Docling process kill after timeout failed"
        );
        return Err(ApiError::InternalIo {
            message: format!("failed to kill timed-out Docling CLI: {kill_error}"),
        });
    }

    Ok((status, true))
}

/// Emit one synthetic post-100% progress update with process and artifact metrics.
async fn emit_post_100_docling_feedback(
    process_id: Option<u32>,
    output_dir: &Path,
    source: &ResolvedSource,
    started: Instant,
    timeout_duration: Duration,
    progress_snapshot: &DoclingProgressSnapshot,
    progress_sender: Option<&mpsc::Sender<DoclingProgressUpdate>>,
    expected_markdown_path: PathBuf,
) {
    let Some(process_id) = process_id else {
        info!(
            event = "docling.post_100_feedback.skipped",
            source_requested = %source.requested,
            relative_source = %source.relative_path.display(),
            output_dir = %output_dir.display(),
            reason = "missing_process_id",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Docling post-100 feedback skipped"
        );
        return;
    };
    let timeout_remaining = timeout_duration.saturating_sub(started.elapsed());
    let latest_progress_message = progress_snapshot
        .latest_message
        .as_deref()
        .map(truncate_progress_message_for_log)
        .unwrap_or_else(|| "none".to_string());
    info!(
        event = "docling.post_100_feedback.started",
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        process_id,
        latest_percentage = ?progress_snapshot.latest_percentage,
        latest_progress_message = %latest_progress_message,
        latest_progress_age_ms = ?progress_snapshot.latest_progress_age.map(|age| age.as_millis() as u64),
        completed_reported_ms = ?progress_snapshot.completed_reported_for.map(|age| age.as_millis() as u64),
        timeout_remaining_ms = timeout_remaining.as_millis() as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling post-100 feedback inspection started"
    );

    let output_dir_for_inspection = output_dir.to_path_buf();
    let sample_duration = Duration::from_secs(POST_100_SAMPLE_SECONDS);
    let inspection = task::spawn_blocking(move || {
        inspect_docling_activity(
            process_id,
            &output_dir_for_inspection,
            &expected_markdown_path,
            sample_duration,
        )
    })
    .await;
    let report = match inspection {
        Ok(report) => report,
        Err(join_error) => {
            error!(
                event = "docling.post_100_feedback.failed",
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                output_dir = %output_dir.display(),
                process_id,
                is_panic = join_error.is_panic(),
                is_cancelled = join_error.is_cancelled(),
                error = %join_error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling post-100 feedback inspection task failed"
            );
            return;
        }
    };
    log_post_100_report(
        &report,
        process_id,
        output_dir,
        source,
        started,
        timeout_remaining,
    );
    let message = format_docling_activity_message(&report, started.elapsed(), timeout_remaining);

    if let Some(progress_sender) = progress_sender {
        if let Err(error) = progress_sender
            .send(DoclingProgressUpdate {
                message,
                percentage: None,
            })
            .await
        {
            error!(
                event = "docling.post_100_feedback.delivery_failed",
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                output_dir = %output_dir.display(),
                process_id,
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling post-100 feedback delivery failed"
            );
        }
    }
}

/// Log the compact inspection report without storing raw stack samples.
fn log_post_100_report(
    report: &DoclingActivityReport,
    process_id: u32,
    output_dir: &Path,
    source: &ResolvedSource,
    started: Instant,
    timeout_remaining: Duration,
) {
    info!(
        event = "docling.post_100_feedback.completed",
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        process_id,
        activity = report.activity_label,
        process_state = ?report.process.state,
        cpu_percent = ?report.process.cpu_percent,
        memory_percent = ?report.process.memory_percent,
        rss_bytes = ?report.process.rss_bytes,
        thread_count = ?report.process.thread_count,
        process_error = ?report.process.error,
        sample_tensor_frames = report.sample.tensor_frames,
        sample_openmp_wait_frames = report.sample.openmp_wait_frames,
        sample_pdf_frames = report.sample.pdf_frames,
        sample_ocr_image_frames = report.sample.ocr_image_frames,
        sample_file_io_frames = report.sample.file_io_frames,
        sample_blocked_wait_frames = report.sample.blocked_wait_frames,
        sample_unavailable_reason = ?report.sample.unavailable_reason,
        artifact_files = report.artifacts.file_count,
        artifact_markdown_files = report.artifacts.markdown_count,
        artifact_total_bytes = report.artifacts.total_bytes,
        expected_markdown_exists = report.artifacts.expected_markdown_exists,
        largest_file_name = ?report.artifacts.largest_file_name,
        largest_file_bytes = ?report.artifacts.largest_file_bytes,
        artifact_error = ?report.artifacts.error,
        timeout_remaining_ms = timeout_remaining.as_millis() as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling post-100 feedback inspection completed"
    );
}

/// Return a non-panicking snapshot of Docling stderr progress state.
fn snapshot_docling_progress(
    progress_state: &Arc<Mutex<DoclingProgressState>>,
    now: Instant,
) -> DoclingProgressSnapshot {
    let Ok(state) = progress_state.lock() else {
        return DoclingProgressSnapshot {
            latest_message: None,
            latest_percentage: None,
            latest_progress_age: None,
            completed_reported_for: None,
        };
    };

    DoclingProgressSnapshot {
        latest_message: state.latest_message.clone(),
        latest_percentage: state.latest_percentage,
        latest_progress_age: state
            .latest_progress_at
            .and_then(|timestamp| now.checked_duration_since(timestamp)),
        completed_reported_for: state
            .completed_reported_at
            .and_then(|timestamp| now.checked_duration_since(timestamp)),
    }
}

/// Decide whether enough time has passed to emit another post-100% feedback update.
fn should_emit_post_100_feedback(
    snapshot: &DoclingProgressSnapshot,
    last_feedback_at: Option<Instant>,
    now: Instant,
) -> bool {
    let Some(completed_reported_for) = snapshot.completed_reported_for else {
        return false;
    };
    if completed_reported_for < Duration::from_secs(POST_100_FIRST_FEEDBACK_SECONDS) {
        return false;
    }

    last_feedback_at
        .and_then(|last_feedback_at| now.checked_duration_since(last_feedback_at))
        .map(|age| age >= Duration::from_secs(POST_100_FEEDBACK_CADENCE_SECONDS))
        .unwrap_or(true)
}

/// Bound the latest Docling progress line before writing it to the service log.
fn truncate_progress_message_for_log(value: &str) -> String {
    const MAX_PROGRESS_LOG_CHARS: usize = 240;
    let mut truncated = value
        .chars()
        .take(MAX_PROGRESS_LOG_CHARS)
        .collect::<String>();
    if value.chars().count() > MAX_PROGRESS_LOG_CHARS {
        truncated.push_str("...");
    }

    truncated
}

/// Read one child-process pipe while preserving bounded diagnostics and optional progress.
async fn read_child_output<R>(
    mut reader: R,
    label: &'static str,
    process_id: Option<u32>,
    source_requested: String,
    relative_source: String,
    output_dir: String,
    progress_sender: Option<mpsc::Sender<DoclingProgressUpdate>>,
    progress_state: Option<Arc<Mutex<DoclingProgressState>>>,
) -> Result<String, ApiError>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let started = Instant::now();
    info!(
        event = "docling.child_output_reader.started",
        task_purpose = "read_docling_child_output",
        pipe = label,
        process_id = ?process_id,
        source_requested = %source_requested,
        relative_source = %relative_source,
        output_dir = %output_dir,
        elapsed_ms = 0_u64,
        "Docling child output reader started"
    );
    let mut output = String::new();
    let mut buffer = [0_u8; CHILD_OUTPUT_READ_CHUNK_BYTES];
    loop {
        let bytes_read = match reader.read(&mut buffer).await {
            Ok(bytes_read) => bytes_read,
            Err(source) => {
                error!(
                    event = "docling.child_output_reader.failed",
                    task_purpose = "read_docling_child_output",
                    pipe = label,
                    process_id = ?process_id,
                    source_requested = %source_requested,
                    relative_source = %relative_source,
                    output_dir = %output_dir,
                    stage = "pipe_reading",
                    output_chars = output.chars().count(),
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Docling child output reader failed"
                );
                return Err(ApiError::InternalIo {
                    message: format!("failed to read Docling child output: {source}"),
                });
            }
        };
        if bytes_read == 0 {
            info!(
                event = "docling.child_output_reader.completed",
                task_purpose = "read_docling_child_output",
                pipe = label,
                process_id = ?process_id,
                source_requested = %source_requested,
                relative_source = %relative_source,
                output_dir = %output_dir,
                output_chars = output.chars().count(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Docling child output reader completed"
            );
            return Ok(output);
        }

        let chunk = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
        output = append_bounded_diagnostic_text(&output, &chunk);
        if progress_sender.is_some() || progress_state.is_some() {
            if let Err(error) = emit_docling_progress_from_chunk(
                progress_sender.as_ref(),
                progress_state.as_ref(),
                &chunk,
            )
            .await
            {
                error!(
                    event = "docling.child_output_reader.failed",
                    task_purpose = "read_docling_child_output",
                    pipe = label,
                    process_id = ?process_id,
                    source_requested = %source_requested,
                    relative_source = %relative_source,
                    output_dir = %output_dir,
                    stage = "progress_delivery",
                    output_chars = output.chars().count(),
                    error_kind = error.error_kind(),
                    error = %error,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Docling child output reader failed"
                );
                return Err(error);
            }
        }
    }
}

/// Join one child-output reader task and label failures with the pipe name.
async fn join_child_output(
    handle: JoinHandle<Result<String, ApiError>>,
    label: &'static str,
    process_id: Option<u32>,
    process_started: Instant,
) -> Result<String, ApiError> {
    match handle.await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            error!(
                event = "docling.child_output_reader.task_failed",
                task_purpose = "read_docling_child_output",
                pipe = label,
                process_id = ?process_id,
                error_kind = error.error_kind(),
                error = %error,
                elapsed_ms = process_started.elapsed().as_millis() as u64,
                "Docling child output reader task returned an error"
            );
            Err(error)
        }
        Err(source) => {
            error!(
                event = "docling.child_output_reader.join_failed",
                task_purpose = "read_docling_child_output",
                pipe = label,
                process_id = ?process_id,
                is_panic = source.is_panic(),
                is_cancelled = source.is_cancelled(),
                error = %source,
                elapsed_ms = process_started.elapsed().as_millis() as u64,
                "Docling child output reader task join failed"
            );
            Err(ApiError::InternalIo {
                message: format!("Docling {label} reader task failed: {source}"),
            })
        }
    }
}

/// Emit and record parsed Docling progress from one stderr chunk.
async fn emit_docling_progress_from_chunk(
    sender: Option<&mpsc::Sender<DoclingProgressUpdate>>,
    progress_state: Option<&Arc<Mutex<DoclingProgressState>>>,
    chunk: &str,
) -> Result<(), ApiError> {
    for line in chunk.split(['\r', '\n']) {
        if let Some(progress) = parse_docling_progress_line(line) {
            if let Some(progress_state) = progress_state {
                record_docling_progress(progress_state, &progress);
            }
            if let Some(sender) = sender {
                sender
                    .send(progress)
                    .await
                    .map_err(|_| ApiError::InternalIo {
                        message:
                            "operation response stream closed before Docling progress delivery"
                                .to_string(),
                    })?;
            }
        }
    }

    Ok(())
}

/// Record parsed progress for the process wait loop without blocking pipe reads.
fn record_docling_progress(
    progress_state: &Arc<Mutex<DoclingProgressState>>,
    progress: &DoclingProgressUpdate,
) {
    let now = Instant::now();
    let Ok(mut state) = progress_state.lock() else {
        return;
    };
    state.latest_message = Some(progress.message.clone());
    state.latest_percentage = progress.percentage;
    state.latest_progress_at = Some(now);
    if progress.percentage == Some(100) && state.completed_reported_at.is_none() {
        state.completed_reported_at = Some(now);
    }
}

/// Convert one Docling diagnostic line into user-visible progress when possible.
fn parse_docling_progress_line(line: &str) -> Option<DoclingProgressUpdate> {
    let message = strip_ansi_sequences(line).trim().to_string();
    if message.is_empty() {
        return None;
    }

    if let Some(percentage) = extract_percentage(&message) {
        return Some(DoclingProgressUpdate {
            message,
            percentage: Some(percentage),
        });
    }
    if contains_case_insensitive(&message, "processing")
        || contains_case_insensitive(&message, "converting")
        || contains_case_insensitive(&message, "saving")
    {
        return Some(DoclingProgressUpdate {
            message,
            percentage: None,
        });
    }

    None
}

/// Return whether one string contains a case-insensitive ASCII needle.
fn contains_case_insensitive(value: &str, needle: &str) -> bool {
    value.to_ascii_lowercase().contains(needle)
}

/// Extract a rounded percentage from one progress line.
fn extract_percentage(message: &str) -> Option<u64> {
    let percent_index = message.find('%')?;
    let prefix = message[..percent_index].trim_end();
    let start = prefix
        .rfind(|value: char| !(value.is_ascii_digit() || value == '.'))
        .map(|index| index + 1)
        .unwrap_or(0);
    let raw = &prefix[start..];
    let parsed = raw.parse::<f64>().ok()?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return None;
    }

    Some(parsed.round() as u64)
}

/// Remove common ANSI escape sequences from Docling terminal diagnostics.
fn strip_ansi_sequences(value: &str) -> String {
    let mut stripped = String::new();
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\x1b' {
            stripped.push(ch);
            continue;
        }
        if chars.peek() == Some(&'[') {
            let _ = chars.next();
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        }
    }

    stripped
}

/// Locate the markdown artifact Docling produced for one source file.
fn find_markdown_artifact(output_dir: &Path, source_path: &Path) -> Result<PathBuf, ApiError> {
    let expected_path = expected_markdown_artifact_path(output_dir, source_path);
    if expected_path.is_file() {
        return Ok(expected_path);
    }

    let discovered = discover_markdown_files(output_dir)?;
    if discovered.len() == 1 {
        return Ok(discovered[0].clone());
    }

    let discovered_text = if discovered.is_empty() {
        "none".to_string()
    } else {
        discovered
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };

    Err(ApiError::DoclingConversion {
        message: format!(
            "Docling completed but no unique markdown artifact was found; expected={}; discovered={}",
            expected_path.display(),
            discovered_text
        ),
    })
}

/// Build Docling's expected markdown artifact path for one source path.
fn expected_markdown_artifact_path(output_dir: &Path, source_path: &Path) -> PathBuf {
    output_dir.join(format!(
        "{}.md",
        source_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("output")
    ))
}

/// Recursively discover markdown files under the Docling output directory.
fn discover_markdown_files(output_dir: &Path) -> Result<Vec<PathBuf>, ApiError> {
    let mut results = Vec::new();
    collect_markdown_files(output_dir, &mut results)?;
    results.sort();
    Ok(results)
}

/// Add markdown files from one directory subtree to the accumulator.
fn collect_markdown_files(dir: &Path, results: &mut Vec<PathBuf>) -> Result<(), ApiError> {
    let entries = fs::read_dir(dir).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to read Docling output directory {}: {source}",
            dir.display()
        ),
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to inspect Docling output entry in {}: {source}",
                dir.display()
            ),
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to inspect Docling output path {}: {source}",
                path.display()
            ),
        })?;

        if file_type.is_dir() {
            collect_markdown_files(&path, results)?;
            continue;
        }
        if file_type.is_file()
            && path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
        {
            results.push(path);
        }
    }

    Ok(())
}

/// Read and normalize markdown text for later unit splitting.
fn read_and_normalize_markdown(markdown_path: &Path) -> Result<String, ApiError> {
    let raw = fs::read_to_string(markdown_path).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to read Docling markdown artifact at {}: {source}",
            markdown_path.display()
        ),
    })?;

    Ok(normalize_markdown(&raw))
}

/// Normalize Docling markdown line endings and excessive blank space.
fn normalize_markdown(markdown: &str) -> String {
    let normalized = markdown.replace("\r\n", "\n").replace('\r', "\n");
    let mut collapsed = String::new();
    let mut blank_count = 0;

    for line in normalized.lines() {
        if line.trim().is_empty() {
            blank_count += 1;
            if blank_count <= 2 {
                collapsed.push('\n');
            }
            continue;
        }

        blank_count = 0;
        collapsed.push_str(line.trim_end());
        collapsed.push('\n');
    }

    collapsed.trim().to_string()
}

/// Truncate diagnostic text so API errors remain readable.
fn truncate_diagnostic_text(value: &str) -> String {
    let mut truncated = value
        .trim()
        .chars()
        .take(MAX_DIAGNOSTIC_CHARS)
        .collect::<String>();
    if value.chars().count() > MAX_DIAGNOSTIC_CHARS {
        truncated.push_str("...");
    }
    truncated
}

/// Append one diagnostic chunk while keeping the most recent bounded text.
fn append_bounded_diagnostic_text(current: &str, chunk: &str) -> String {
    let next = format!("{current}{chunk}");
    if next.chars().count() <= MAX_DIAGNOSTIC_CHARS {
        return next;
    }

    next.chars()
        .rev()
        .take(MAX_DIAGNOSTIC_CHARS)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

/// Format child-process exit status fields for diagnostics.
fn format_exit_status(exit_code: Option<i32>, signal: Option<i32>) -> String {
    match (exit_code, signal) {
        (Some(code), _) => format!("exitCode={code}"),
        (None, Some(signal)) => format!("signal={signal}"),
        (None, None) => "unknown".to_string(),
    }
}
