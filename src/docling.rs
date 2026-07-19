use std::{
    fs,
    io::Read,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use tracing::{error, info};

use crate::{
    config::DoclingConfig,
    docling_activity::{
        DoclingActivityReport, format_docling_activity_message, inspect_docling_activity,
    },
    error::ApiError,
    source::ResolvedSource,
    util::{MAX_DIAGNOSTIC_CHARS, panic_payload_message, truncate_diagnostic_text},
};

static CONVERSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const CHILD_OUTPUT_READ_CHUNK_BYTES: usize = 8_192;
const DOCLING_WAIT_POLL_MILLIS: u64 = 250;
const POST_100_FIRST_FEEDBACK_SECONDS: u64 = 1;
const POST_100_FEEDBACK_CADENCE_SECONDS: u64 = 1;
const POST_100_SAMPLE_SECONDS: u64 = 0;

/// Effective Docling CLI options for one conversion, resolved from
/// `[docling]` config. These are identity-bearing parser configuration
/// (D3): they are folded into `parserConfigHash`, so changing any value
/// yields a new parse identity.
#[derive(Debug, Clone)]
pub struct ResolvedDoclingOptions {
    pub pdf_backend: String,
    pub ocr_mode: String,
    pub device: String,
    pub num_threads: u32,
    pub page_batch_size: u32,
    pub document_timeout_seconds: u64,
}

/// Result of one Docling `--to json` conversion (decision D6): the raw
/// DoclingDocument JSON artifact plus bounded process diagnostics (args,
/// stdout, stderr) for operator-visible failure context.
// The PDF parser worker reads only the artifact and stream fields today, so
// the identity/context fields (source, options, output_dir, args) are allowed
// until a consumer (C5 dispatch diagnostics) reads them.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DoclingJsonConversionResult {
    pub source: ResolvedSource,
    pub options: ResolvedDoclingOptions,
    pub output_dir: PathBuf,
    pub json_path: PathBuf,
    /// Artifact text byte-for-byte as Docling wrote it. NO line normalization
    /// is applied: JSON is a structured payload where a line rewrite could
    /// silently alter string values.
    pub json_text: String,
    pub args: Vec<String>,
    pub stdout: String,
    pub stderr: String,
}

/// One progress line parsed from Docling stderr, forwarded to the
/// operation's progress channel; `percentage` is present only when the line
/// carried a parseable percent figure.
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

// Identity facts for one Docling launch, grouped so `run_docling` takes one
// coherent context instead of a long parameter list; all fields are borrows,
// so the struct is `Copy` and the body binds them like locals.
#[derive(Clone, Copy)]
struct DoclingLaunch<'a> {
    config: &'a DoclingConfig,
    args: &'a [String],
    index_root: &'a Path,
    output_dir: &'a Path,
    source: &'a ResolvedSource,
    output_format: &'a str,
}

// Keep stable Docling process facts together so wait-time logs and feedback use
// the same lifecycle identity without changing operation handles.
struct DoclingProcessContext<'a> {
    config: &'a DoclingConfig,
    output_dir: &'a Path,
    source: &'a ResolvedSource,
    /// `--to` output format of this conversion (`json`); process lifecycle
    /// logs carry it so the format is explicit per process.
    output_format: &'a str,
    process_id: u32,
    started: Instant,
}

// Reader threads need owned copies of diagnostic identity because their closures
// outlive the stack frame that spawned the Docling process.
struct DoclingChildOutputContext {
    label: &'static str,
    process_id: u32,
    source_requested: String,
    relative_source: String,
    output_dir: String,
}

/// Convert one resolved PDF source to DoclingDocument JSON (`--to json`,
/// decision D6) using only service-configured Docling options; the typed
/// candidate-unit source for the PDF parser worker.
///
/// `output_dir_override` lets the worker place Docling output inside its
/// bundle workspace (`parser_raw/`, spec §12.2 diagnostics); `None` keeps
/// the default service-owned conversion directory.
pub fn convert_source_to_document_json(
    config: &DoclingConfig,
    index_root: &Path,
    source: ResolvedSource,
    progress_sender: Option<SyncSender<DoclingProgressUpdate>>,
    output_dir_override: Option<&Path>,
) -> Result<DoclingJsonConversionResult, ApiError> {
    let options = resolve_docling_options(config)?;
    let output_dir = match output_dir_override {
        Some(dir) => {
            // The caller owns the override directory; create-if-missing
            // keeps the contract explicit instead of failing on a fresh
            // workspace directory.
            fs::create_dir_all(dir).map_err(|create_error| ApiError::InternalIo {
                message: format!(
                    "failed to create Docling output directory at {}: {create_error}",
                    dir.display()
                ),
            })?;
            dir.to_path_buf()
        }
        None => create_conversion_output_dir(index_root)?,
    };
    let (args, stdout, stderr) = execute_docling_conversion(
        config,
        index_root,
        &source,
        progress_sender,
        &output_dir,
        &options,
        "json",
    )?;
    let json_path = find_conversion_artifact(&output_dir, &source.absolute_path, "json")?;
    let json_text = read_raw_artifact(&json_path)?;

    Ok(DoclingJsonConversionResult {
        source,
        options,
        output_dir,
        json_path,
        json_text,
        args,
        stdout,
        stderr,
    })
}

/// Run one Docling conversion attempt for the configured output format,
/// mapping timeouts and non-zero exits to explicit conversion errors.
/// `output_format` is both the `--to` value and the artifact extension —
/// Docling names its artifact `{source_stem}.{format}` (this service uses
/// `json`).
fn execute_docling_conversion(
    config: &DoclingConfig,
    index_root: &Path,
    source: &ResolvedSource,
    progress_sender: Option<SyncSender<DoclingProgressUpdate>>,
    output_dir: &Path,
    options: &ResolvedDoclingOptions,
    output_format: &str,
) -> Result<(Vec<String>, String, String), ApiError> {
    let args = build_docling_args(output_dir, &source.absolute_path, options, output_format);
    let expected_artifact =
        expected_artifact_path(output_dir, &source.absolute_path, output_format);
    let output = run_docling(
        &DoclingLaunch {
            config,
            args: &args,
            index_root,
            output_dir,
            source,
            output_format,
        },
        progress_sender,
        expected_artifact,
    )?;
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

    Ok((args, stdout, stderr))
}

/// Resolve service-configured Docling options for one conversion attempt.
/// `pub(crate)` so the PDF parser worker can hash the exact effective
/// options into its `parserConfigHash` before starting a conversion.
pub(crate) fn resolve_docling_options(
    config: &DoclingConfig,
) -> Result<ResolvedDoclingOptions, ApiError> {
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

    let timestamp_ms = crate::primitives::current_time_ms()?;
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

/// Build Docling CLI arguments without shell interpolation. `output_format`
/// supplies the `--to` value; every other flag (including placeholder image
/// export) is fixed for this service's conversions.
fn build_docling_args(
    output_dir: &Path,
    source_path: &Path,
    options: &ResolvedDoclingOptions,
    output_format: &str,
) -> Vec<String> {
    let mut args = vec![
        "--to".to_string(),
        output_format.to_string(),
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
fn run_docling(
    launch: &DoclingLaunch<'_>,
    progress_sender: Option<SyncSender<DoclingProgressUpdate>>,
    expected_artifact_path: PathBuf,
) -> Result<DoclingRunOutput, ApiError> {
    let DoclingLaunch {
        config,
        args,
        index_root,
        output_dir,
        source,
        output_format,
    } = *launch;
    let started = Instant::now();
    info!(
        event = "docling.process.starting",
        executable_path = %config.docling_path.display(),
        python_path = %config.python_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        absolute_source = %source.absolute_path.display(),
        output_dir = %output_dir.display(),
        output_format,
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
                output_format,
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
        output_format,
        process_id,
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
                process_id,
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
                process_id,
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
    let stdout_context = DoclingChildOutputContext {
        label: "stdout",
        process_id,
        source_requested: source_requested.clone(),
        relative_source: relative_source.clone(),
        output_dir: output_dir_for_log.clone(),
    };
    info!(
        event = "docling.child_output_reader.spawned",
        task_purpose = "read_docling_child_output",
        pipe = stdout_context.label,
        process_id = stdout_context.process_id,
        source_requested = %stdout_context.source_requested,
        relative_source = %stdout_context.relative_source,
        output_dir = %stdout_context.output_dir,
        "Docling child output reader thread spawned"
    );
    let stdout_reader =
        thread::spawn(move || read_child_output(stdout, stdout_context, None, None));
    let stderr_context = DoclingChildOutputContext {
        label: "stderr",
        process_id,
        source_requested,
        relative_source,
        output_dir: output_dir_for_log,
    };
    info!(
        event = "docling.child_output_reader.spawned",
        task_purpose = "read_docling_child_output",
        pipe = stderr_context.label,
        process_id = stderr_context.process_id,
        source_requested = %stderr_context.source_requested,
        relative_source = %stderr_context.relative_source,
        output_dir = %stderr_context.output_dir,
        "Docling child output reader thread spawned"
    );
    let stderr_progress_sender = progress_sender.clone();
    let stderr_progress_state = progress_state.clone();
    let stderr_reader = thread::spawn(move || {
        read_child_output(
            stderr,
            stderr_context,
            stderr_progress_sender,
            Some(stderr_progress_state),
        )
    });
    let process_context = DoclingProcessContext {
        config,
        output_dir,
        source,
        output_format,
        process_id,
        started,
    };
    let (status, timed_out) = wait_for_docling_process(
        &mut child,
        &process_context,
        progress_state,
        progress_sender,
        expected_artifact_path,
    )?;
    info!(
        event = "docling.process.wait_completed",
        executable_path = %config.docling_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        output_format,
        process_id,
        timed_out,
        exit_code = ?status.code(),
        signal = ?status.signal(),
        timeout_seconds = config.document_timeout_seconds,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling process wait completed"
    );
    let stdout = join_child_output(stdout_reader, "stdout", process_id, started);
    let stderr = join_child_output(stderr_reader, "stderr", process_id, started);
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
            output_format,
            process_id,
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
            output_format,
            process_id,
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
fn wait_for_docling_process(
    child: &mut Child,
    context: &DoclingProcessContext<'_>,
    progress_state: Arc<Mutex<DoclingProgressState>>,
    mut progress_sender: Option<SyncSender<DoclingProgressUpdate>>,
    expected_artifact_path: PathBuf,
) -> Result<(ExitStatus, bool), ApiError> {
    let config = context.config;
    let output_dir = context.output_dir;
    let source = context.source;
    let output_format = context.output_format;
    let process_id = context.process_id;
    let started = context.started;
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
                    output_format,
                    process_id,
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
            return timeout_docling_process(child, context);
        }

        let now = Instant::now();
        let snapshot = snapshot_docling_progress(&progress_state, now);
        if should_emit_post_100_feedback(&snapshot, last_feedback_at, now) {
            last_feedback_at = Some(now);
            let consumer_disconnected = emit_post_100_docling_feedback(
                context,
                timeout_duration,
                &snapshot,
                progress_sender.as_ref(),
                &expected_artifact_path,
            );
            // A dead progress consumer never affects the conversion: stop
            // attempting delivery (feedback stays log-only) and keep
            // waiting for the process.
            if consumer_disconnected {
                progress_sender = None;
            }
        }

        let poll_duration = Duration::from_millis(DOCLING_WAIT_POLL_MILLIS)
            .min(timeout_duration.saturating_sub(started.elapsed()));
        thread::sleep(poll_duration);
    }
}

/// Kill a Docling child after the configured document timeout has been reached.
fn timeout_docling_process(
    child: &mut Child,
    context: &DoclingProcessContext<'_>,
) -> Result<(ExitStatus, bool), ApiError> {
    let config = context.config;
    let output_dir = context.output_dir;
    let source = context.source;
    let output_format = context.output_format;
    let process_id = context.process_id;
    let started = context.started;
    error!(
        event = "docling.process.timeout_reached",
        executable_path = %config.docling_path.display(),
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        output_dir = %output_dir.display(),
        output_format,
        process_id,
        timeout_seconds = config.document_timeout_seconds,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling process timeout reached"
    );
    let kill_result = child.kill();
    let status = match child.wait() {
        Ok(status) => status,
        Err(io_error) => {
            error!(
                event = "docling.process.timeout_wait_failed",
                executable_path = %config.docling_path.display(),
                source_requested = %source.requested,
                relative_source = %source.relative_path.display(),
                output_dir = %output_dir.display(),
                output_format,
                process_id,
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
            output_format,
            process_id,
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

/// Emit one synthetic post-100% progress update with process and artifact
/// metrics. Returns whether the progress consumer is gone so the wait loop
/// can stop attempting delivery; delivery is best-effort and never blocks
/// or fails the conversion.
fn emit_post_100_docling_feedback(
    context: &DoclingProcessContext<'_>,
    timeout_duration: Duration,
    progress_snapshot: &DoclingProgressSnapshot,
    progress_sender: Option<&SyncSender<DoclingProgressUpdate>>,
    expected_artifact_path: &Path,
) -> bool {
    let output_dir = context.output_dir;
    let source = context.source;
    let process_id = context.process_id;
    let started = context.started;
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

    let sample_duration = Duration::from_secs(POST_100_SAMPLE_SECONDS);
    let report = inspect_docling_activity(
        process_id,
        output_dir,
        expected_artifact_path,
        sample_duration,
    );
    log_post_100_report(&report, context, timeout_remaining);
    let message = format_docling_activity_message(&report, started.elapsed(), timeout_remaining);

    // try_send keeps feedback delivery decoupled from the conversion: a
    // full channel means the consumer is behind, and this synthetic update
    // is droppable (the same facts were just written to the service log
    // above); a disconnected consumer is reported to the wait loop so it
    // stops sending. Neither case blocks nor fails the parse.
    if let Some(progress_sender) = progress_sender {
        match progress_sender.try_send(DoclingProgressUpdate {
            message,
            percentage: None,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {
                info!(
                    event = "docling.post_100_feedback.consumer_disconnected",
                    source_requested = %source.requested,
                    relative_source = %source.relative_path.display(),
                    output_dir = %output_dir.display(),
                    process_id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Docling progress consumer disconnected; feedback continues log-only"
                );
                return true;
            }
        }
    }
    false
}

/// Log the compact inspection report without storing raw stack samples.
fn log_post_100_report(
    report: &DoclingActivityReport,
    context: &DoclingProcessContext<'_>,
    timeout_remaining: Duration,
) {
    let output_dir = context.output_dir;
    let source = context.source;
    let process_id = context.process_id;
    let started = context.started;
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
        expected_artifact_exists = report.artifacts.expected_artifact_exists,
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
fn read_child_output<R>(
    mut reader: R,
    context: DoclingChildOutputContext,
    mut progress_sender: Option<SyncSender<DoclingProgressUpdate>>,
    progress_state: Option<Arc<Mutex<DoclingProgressState>>>,
) -> Result<String, ApiError>
where
    R: Read,
{
    let DoclingChildOutputContext {
        label,
        process_id,
        source_requested,
        relative_source,
        output_dir,
    } = context;
    let started = Instant::now();
    info!(
        event = "docling.child_output_reader.started",
        task_purpose = "read_docling_child_output",
        pipe = label,
        process_id,
        source_requested = %source_requested,
        relative_source = %relative_source,
        output_dir = %output_dir,
        elapsed_ms = 0_u64,
        "Docling child output reader started"
    );
    let mut output = String::new();
    let mut buffer = [0_u8; CHILD_OUTPUT_READ_CHUNK_BYTES];
    loop {
        let bytes_read = match reader.read(&mut buffer) {
            Ok(bytes_read) => bytes_read,
            Err(source) => {
                error!(
                    event = "docling.child_output_reader.failed",
                    task_purpose = "read_docling_child_output",
                    pipe = label,
                    process_id,
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
                process_id,
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
            let consumer_disconnected = emit_docling_progress_from_chunk(
                progress_sender.as_ref(),
                progress_state.as_ref(),
                &chunk,
            );
            // Progress delivery is decoupled from the conversion: a
            // disconnected consumer stops further delivery attempts but
            // never fails the reader or the parse. Logged once here, not
            // per chunk; progress state recording continues regardless.
            if consumer_disconnected {
                progress_sender = None;
                info!(
                    event = "docling.child_output_reader.progress_consumer_disconnected",
                    task_purpose = "read_docling_child_output",
                    pipe = label,
                    process_id,
                    source_requested = %source_requested,
                    relative_source = %relative_source,
                    output_dir = %output_dir,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Docling progress consumer disconnected; conversion continues without progress delivery"
                );
            }
        }
    }
}

/// Join one child-output reader thread and label failures with the pipe name.
fn join_child_output(
    handle: JoinHandle<Result<String, ApiError>>,
    label: &'static str,
    process_id: u32,
    process_started: Instant,
) -> Result<String, ApiError> {
    match handle.join() {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            error!(
                event = "docling.child_output_reader.task_failed",
                task_purpose = "read_docling_child_output",
                pipe = label,
                process_id,
                error_kind = error.error_kind(),
                error = %error,
                elapsed_ms = process_started.elapsed().as_millis() as u64,
                "Docling child output reader thread returned an error"
            );
            Err(error)
        }
        // std thread joins fail only on panic; there is no cancellation state.
        Err(panic_payload) => {
            let panic_message = panic_payload_message(panic_payload.as_ref());
            error!(
                event = "docling.child_output_reader.join_failed",
                task_purpose = "read_docling_child_output",
                pipe = label,
                process_id,
                is_panic = true,
                panic_message = %panic_message,
                elapsed_ms = process_started.elapsed().as_millis() as u64,
                "Docling child output reader thread join failed"
            );
            Err(ApiError::InternalIo {
                message: format!("Docling {label} reader thread panicked: {panic_message}"),
            })
        }
    }
}

/// Emit and record parsed Docling progress from one stderr chunk. Returns
/// whether the progress consumer has disconnected.
///
/// Delivery is strictly best-effort so a dead or slow consumer can never
/// block the pipe reader or kill an otherwise healthy conversion:
/// `try_send` drops an update when the channel is full (progress is
/// monotone feedback; a missed update is superseded by the next one), and
/// a disconnected receiver is reported to the caller so it stops sending.
fn emit_docling_progress_from_chunk(
    sender: Option<&SyncSender<DoclingProgressUpdate>>,
    progress_state: Option<&Arc<Mutex<DoclingProgressState>>>,
    chunk: &str,
) -> bool {
    let mut consumer_disconnected = false;
    for line in chunk.split(['\r', '\n']) {
        if let Some(progress) = parse_docling_progress_line(line) {
            if let Some(progress_state) = progress_state {
                record_docling_progress(progress_state, &progress);
            }
            if let Some(sender) = sender
                && !consumer_disconnected
            {
                match sender.try_send(progress) {
                    Ok(()) | Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => consumer_disconnected = true,
                }
            }
        }
    }

    consumer_disconnected
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

/// Locate the artifact with one extension that Docling produced for one
/// source file: the expected `{source_stem}.{extension}` name first, then a
/// recursive single-match fallback for layouts where Docling nests output.
fn find_conversion_artifact(
    output_dir: &Path,
    source_path: &Path,
    extension: &str,
) -> Result<PathBuf, ApiError> {
    let expected_path = expected_artifact_path(output_dir, source_path, extension);
    if expected_path.is_file() {
        return Ok(expected_path);
    }

    let discovered = discover_artifact_files(output_dir, extension)?;
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
            "Docling completed but no unique .{extension} artifact was found; expected={}; discovered={}",
            expected_path.display(),
            discovered_text
        ),
    })
}

/// Build Docling's expected artifact path for one source path and extension.
fn expected_artifact_path(output_dir: &Path, source_path: &Path, extension: &str) -> PathBuf {
    output_dir.join(format!(
        "{}.{extension}",
        source_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("output")
    ))
}

/// Recursively discover files with one extension under the Docling output
/// directory.
fn discover_artifact_files(output_dir: &Path, extension: &str) -> Result<Vec<PathBuf>, ApiError> {
    let mut results = Vec::new();
    collect_artifact_files(output_dir, extension, &mut results)?;
    results.sort();
    Ok(results)
}

/// Add files with one extension from one directory subtree to the
/// accumulator.
fn collect_artifact_files(
    dir: &Path,
    extension: &str,
    results: &mut Vec<PathBuf>,
) -> Result<(), ApiError> {
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
            collect_artifact_files(&path, extension, results)?;
            continue;
        }
        if file_type.is_file()
            && path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case(extension))
        {
            results.push(path);
        }
    }

    Ok(())
}

/// Read one Docling artifact verbatim. No normalization by design:
/// DoclingDocument JSON is a structured payload that must stay byte-for-byte
/// as written (a line rewrite could alter string values), and the raw file
/// doubles as preserved diagnostic evidence in the parser bundle.
fn read_raw_artifact(path: &Path) -> Result<String, ApiError> {
    fs::read_to_string(path).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to read Docling artifact at {}: {source}",
            path.display()
        ),
    })
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
