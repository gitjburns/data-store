use std::{
    fs,
    io::Read,
    os::{fd::OwnedFd, unix::net::UnixStream},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::limits::{DiagnosticLimits, ParsingLimits};

#[derive(Debug, Clone)]
pub struct DoclingActivityReport {
    pub activity_label: &'static str,
    pub process: ProcessMetrics,
    pub sample: SampleCounters,
    pub artifacts: ArtifactMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct ProcessMetrics {
    pub state: Option<String>,
    pub cpu_percent: Option<f64>,
    pub memory_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub thread_count: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SampleCounters {
    pub tensor_frames: usize,
    pub openmp_wait_frames: usize,
    pub pdf_frames: usize,
    pub ocr_image_frames: usize,
    pub file_io_frames: usize,
    pub blocked_wait_frames: usize,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ArtifactMetrics {
    pub file_count: u64,
    pub markdown_count: u64,
    pub total_bytes: u64,
    /// Whether the conversion's expected output artifact (format-dependent:
    /// `.md` or `.json`) exists yet in the output directory.
    pub expected_artifact_exists: bool,
    pub largest_file_name: Option<String>,
    pub largest_file_bytes: Option<u64>,
    pub error: Option<String>,
}

/// Inspect one still-running Docling process and reduce raw OS evidence to compact metrics.
pub fn inspect_docling_activity(
    process_id: u32,
    output_dir: &Path,
    expected_artifact_path: &Path,
    sample_duration: Duration,
    remaining_document_time: Duration,
    parsing: &ParsingLimits,
    diagnostics: &DiagnosticLimits,
) -> DoclingActivityReport {
    let inspection_started = Instant::now();
    let process = inspect_process_metrics(process_id, diagnostics);
    let sample = sample_process_activity(
        process_id,
        sample_duration,
        remaining_document_time.saturating_sub(inspection_started.elapsed()),
        parsing,
        diagnostics,
    );
    let artifacts = inspect_artifacts(output_dir, expected_artifact_path, diagnostics);
    let activity_label = classify_activity(&process, &sample);

    DoclingActivityReport {
        activity_label,
        process,
        sample,
        artifacts,
    }
}

/// Format the report as one terminal-friendly progress line.
pub fn format_docling_activity_message(
    report: &DoclingActivityReport,
    elapsed: Duration,
    _timeout_remaining: Duration,
) -> String {
    format!(
        "docling_converting: processing and generating markdown: cpu:{} mem:{} rss:{} thr:{} elapsed:{}",
        format_percent(report.process.cpu_percent),
        format_percent(report.process.memory_percent),
        format_bytes_option(report.process.rss_bytes),
        format_optional_u64(report.process.thread_count),
        format_duration(elapsed),
    )
}

/// Read cheap process metrics from the platform process table.
fn inspect_process_metrics(process_id: u32, diagnostics: &DiagnosticLimits) -> ProcessMetrics {
    let output = Command::new("ps")
        .args([
            "-p",
            &process_id.to_string(),
            "-o",
            "stat=,%cpu=,%mem=,rss=",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    let output = match output {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            return ProcessMetrics {
                error: Some(format!(
                    "ps failed: status={}; stderr={}",
                    output.status,
                    truncate_for_metric(&String::from_utf8_lossy(&output.stderr), diagnostics)
                )),
                thread_count: inspect_process_thread_count(process_id),
                ..ProcessMetrics::default()
            };
        }
        Err(error) => {
            return ProcessMetrics {
                error: Some(format!("ps failed: {error}")),
                thread_count: inspect_process_thread_count(process_id),
                ..ProcessMetrics::default()
            };
        }
    };
    parse_process_metrics(
        &String::from_utf8_lossy(&output.stdout),
        inspect_process_thread_count(process_id),
    )
}

/// Parse the whitespace-delimited ps output emitted by inspect_process_metrics.
fn parse_process_metrics(output: &str, thread_count: Option<u64>) -> ProcessMetrics {
    let fields = output.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 4 {
        return ProcessMetrics {
            error: Some("ps output did not include expected fields".to_string()),
            thread_count,
            ..ProcessMetrics::default()
        };
    }

    ProcessMetrics {
        state: Some(fields[0].to_string()),
        cpu_percent: fields.get(1).and_then(|value| value.parse::<f64>().ok()),
        memory_percent: fields.get(2).and_then(|value| value.parse::<f64>().ok()),
        rss_bytes: fields
            .get(3)
            .and_then(|value| value.parse::<u64>().ok())
            .map(|kilobytes| kilobytes.saturating_mul(1024)),
        thread_count,
        error: None,
    }
}

/// Count macOS process threads from ps -M output when direct ps fields are unavailable.
fn inspect_process_thread_count(process_id: u32) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-M", "-p", &process_id.to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let process_id_text = process_id.to_string();
    let count = String::from_utf8_lossy(&output.stdout)
        .lines()
        .skip(1)
        .filter(|line| {
            let mut fields = line.split_whitespace();
            fields
                .next()
                .map(|field| field == process_id_text)
                .unwrap_or(false)
                || fields
                    .next()
                    .map(|field| field == process_id_text)
                    .unwrap_or(false)
        })
        .count();

    u64::try_from(count).ok().filter(|count| *count > 0)
}

/// Run a bounded macOS sample command and count diagnostic frame categories.
fn sample_process_activity(
    process_id: u32,
    sample_duration: Duration,
    remaining_document_time: Duration,
    parsing: &ParsingLimits,
    diagnostics: &DiagnosticLimits,
) -> SampleCounters {
    if sample_duration.is_zero() {
        return SampleCounters::default();
    }

    let report = match run_bounded_sample(
        process_id,
        sample_duration,
        remaining_document_time,
        parsing,
        diagnostics,
    ) {
        Ok(report) => report,
        Err(error) => {
            return SampleCounters {
                unavailable_reason: Some(error),
                ..SampleCounters::default()
            };
        }
    };
    let call_graph = call_graph_text(&report).to_ascii_lowercase();

    SampleCounters {
        tensor_frames: count_any(
            &call_graph,
            &[
                "libtorch_cpu",
                "tensoriterator",
                "at::native",
                "convolution",
                "matmul",
                "softmax",
                "sigmoid",
                "add_kernel",
                "mul_kernel",
            ],
        ),
        openmp_wait_frames: count_any(
            &call_graph,
            &[
                "libomp",
                "__kmp",
                "kmp_flag",
                "fork_barrier",
                "join_barrier",
            ],
        ),
        pdf_frames: count_any(&call_graph, &["pdfium", "pdf_parsers", "pdf"]),
        ocr_image_frames: count_any(
            &call_graph,
            &["ocr", "easyocr", "tesseract", "_imaging", "opencv", "cv2"],
        ),
        file_io_frames: count_any(
            &call_graph,
            &[
                " read",
                " write",
                " open",
                " fsync",
                "pread",
                "pwrite",
                "getdirentries",
            ],
        ),
        blocked_wait_frames: count_any(
            &call_graph,
            &[
                "__psynch",
                "pthread_cond_wait",
                "semaphore_wait",
                "mach_msg",
                "kevent",
            ],
        ),
        unavailable_reason: None,
    }
}

/// Drain both sampler pipes while polling, without allowing telemetry to extend
/// the document deadline. Every return confirms exit or reports failed cleanup.
fn run_bounded_sample(
    process_id: u32,
    sample_duration: Duration,
    remaining_document_time: Duration,
    parsing: &ParsingLimits,
    diagnostics: &DiagnosticLimits,
) -> Result<String, String> {
    let started = Instant::now();
    let mut facts = SampleProcessFacts::default();
    tracing::info!(
        event = "docling.sample.started",
        executable = "sample",
        target_process_id = process_id,
        sample_seconds = sample_duration.as_secs(),
        document_remaining_ms = remaining_document_time.as_millis() as u64,
        output_limit_bytes = parsing.process_log_bytes,
        "native Docling activity sampling started"
    );
    let result = run_bounded_sample_inner(
        process_id,
        sample_duration,
        remaining_document_time.saturating_sub(started.elapsed()),
        parsing,
        &mut facts,
    );
    match &result {
        Ok(_) => {
            tracing::info!(event = "docling.sample.completed", target_process_id = process_id, process_id = ?facts.process_id,
            stdout_observed_bytes = facts.stdout_observed, stdout_retained_bytes = facts.stdout_retained,
            stderr_observed_bytes = facts.stderr_observed, stderr_retained_bytes = facts.stderr_retained,
            truncated = facts.truncated, timed_out = facts.timed_out, cleanup = facts.cleanup,
            elapsed_ms = started.elapsed().as_millis() as u64, "native activity sample completed")
        }
        Err(source) => {
            tracing::warn!(event = "docling.sample.failed", target_process_id = process_id, process_id = ?facts.process_id,
            stdout_observed_bytes = facts.stdout_observed, stdout_retained_bytes = facts.stdout_retained,
            stderr_observed_bytes = facts.stderr_observed, stderr_retained_bytes = facts.stderr_retained,
            truncated = facts.truncated, timed_out = facts.timed_out, cleanup = facts.cleanup, error = %crate::util::truncate_diagnostic_text(source, diagnostics),
            elapsed_ms = started.elapsed().as_millis() as u64, "native activity sample failed")
        }
    }
    result
}

/// Lifecycle facts survive early failures without retaining sampled process contents.
#[derive(Default)]
struct SampleProcessFacts {
    process_id: Option<u32>,
    stdout_observed: usize,
    stdout_retained: usize,
    stderr_observed: usize,
    stderr_retained: usize,
    truncated: bool,
    timed_out: bool,
    cleanup: &'static str,
}

/// The outer boundary logs every result; this body owns sockets, deadline, and reaping.
fn run_bounded_sample_inner(
    process_id: u32,
    sample_duration: Duration,
    remaining_document_time: Duration,
    parsing: &ParsingLimits,
    facts: &mut SampleProcessFacts,
) -> Result<String, String> {
    facts.cleanup = "not_started";
    let budget = sample_duration
        .saturating_add(Duration::from_millis(parsing.activity_sample_grace_ms))
        .min(remaining_document_time);
    if budget.is_zero() {
        facts.timed_out = true;
        return Err("sample unavailable: document deadline reached before collection".to_owned());
    }
    let deadline = Instant::now()
        .checked_add(budget)
        .ok_or_else(|| "sample timeout overflowed".to_owned())?;
    let (mut stdout, child_stdout) = SamplePipe::new("stdout")?;
    let (mut stderr, child_stderr) = SamplePipe::new("stderr")?;
    let mut child = Command::new("sample")
        .args([
            &process_id.to_string(),
            &sample_duration.as_secs().max(1).to_string(),
            "-file",
            "/dev/stdout",
        ])
        .stdout(child_stdout)
        .stderr(child_stderr)
        .spawn()
        .map_err(|error| format!("sample failed to start: {error}"))?;
    facts.process_id = Some(child.id());
    facts.cleanup = "pending";
    tracing::info!(
        event = "docling.sample.spawned",
        executable = "sample",
        target_process_id = process_id,
        process_id = child.id(),
        timeout_ms = budget.as_millis() as u64,
        "native activity sampler spawned"
    );
    let mut result = (|| {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(parsing.process_read_chunk_bytes)
            .map_err(|source| format!("allocate sampler output read buffer: {source}"))?;
        buffer.resize(parsing.process_read_chunk_bytes, 0);
        loop {
            stdout.drain(&mut buffer, parsing.process_log_bytes, deadline)?;
            stderr.drain(&mut buffer, parsing.process_log_bytes, deadline)?;
            match child
                .try_wait()
                .map_err(|source| format!("sample wait failed: {source}"))?
            {
                Some(status) => {
                    // Once the direct child exits, drain only until EOF or the
                    // shared deadline; an inherited open writer is explicit failure.
                    while !stdout.eof || !stderr.eof {
                        stdout.drain(&mut buffer, parsing.process_log_bytes, deadline)?;
                        stderr.drain(&mut buffer, parsing.process_log_bytes, deadline)?;
                        if !stdout.eof || !stderr.eof {
                            if Instant::now() >= deadline {
                                facts.timed_out = true;
                                return Err(
                                    "sample output incomplete at document/collector deadline"
                                        .to_owned(),
                                );
                            }
                            thread::sleep(
                                Duration::from_millis(parsing.activity_poll_ms)
                                    .min(deadline.saturating_duration_since(Instant::now())),
                            );
                        }
                    }
                    if !status.success() {
                        return Err(format!(
                            "sample exited with {status}; retained stdout={} bytes stderr={} bytes",
                            stdout.bytes.len(),
                            stderr.bytes.len()
                        ));
                    }
                    if stdout.observed > stdout.bytes.len() || stderr.observed > stderr.bytes.len()
                    {
                        return Err(format!(
                            "sample capture truncated: stdout={} retained/{} observed bytes, stderr={} retained/{} observed bytes",
                            stdout.bytes.len(),
                            stdout.observed,
                            stderr.bytes.len(),
                            stderr.observed
                        ));
                    }
                    return Ok(format!(
                        "{}\n{}",
                        String::from_utf8_lossy(&stdout.bytes),
                        String::from_utf8_lossy(&stderr.bytes)
                    ));
                }
                None if Instant::now() >= deadline => {
                    facts.timed_out = true;
                    return Err(format!(
                        "sample timed out; partial stdout={} bytes stderr={} bytes",
                        stdout.bytes.len(),
                        stderr.bytes.len()
                    ));
                }
                None => thread::sleep(
                    Duration::from_millis(parsing.activity_poll_ms)
                        .min(deadline.saturating_duration_since(Instant::now())),
                ),
            }
        }
    })();
    facts.stdout_observed = stdout.observed;
    facts.stdout_retained = stdout.bytes.len();
    facts.stderr_observed = stderr.observed;
    facts.stderr_retained = stderr.bytes.len();
    facts.truncated = stdout.observed > stdout.bytes.len() || stderr.observed > stderr.bytes.len();
    // No reader thread can outlive this function. Even a failed kill closes the
    // parent sockets immediately and reports that process exit is unconfirmed.
    let exited = match child.try_wait() {
        Ok(status) => status.is_some(),
        Err(source) => {
            result = Err(format!(
                "{}; sample cleanup poll failed: {source}",
                result
                    .as_ref()
                    .err()
                    .map(String::as_str)
                    .unwrap_or("sample collection finished")
            ));
            false
        }
    };
    if !exited {
        facts.cleanup = "termination_requested";
        if let Err(source) = child.kill() {
            match child.try_wait() {
                Ok(Some(_)) => {}
                observed => {
                    facts.cleanup = "exit_unconfirmed";
                    return Err(format!(
                        "{}; sample termination failed, exit unconfirmed: {source}; final poll={observed:?}",
                        result
                            .as_ref()
                            .err()
                            .map(String::as_str)
                            .unwrap_or("sample cleanup")
                    ));
                }
            }
        }
        child.wait().map_err(|source| {
            facts.cleanup = "reap_failed";
            format!(
                "{}; sample reap failed: {source}",
                result
                    .as_ref()
                    .err()
                    .map(String::as_str)
                    .unwrap_or("sample collection finished")
            )
        })?;
        facts.cleanup = "reaped_after_termination";
    } else {
        facts.cleanup = "exit_confirmed";
    }
    result
}

/// Parent-owned nonblocking streams keep bounded capture independent of child writes.
struct SamplePipe {
    stream: UnixStream,
    label: &'static str,
    bytes: Vec<u8>,
    observed: usize,
    eof: bool,
}

impl SamplePipe {
    /// Configure the reader before spawn while leaving child writes blocking.
    fn new(label: &'static str) -> Result<(Self, Stdio), String> {
        let (read, write) = UnixStream::pair()
            .map_err(|source| format!("create sample {label} socket: {source}"))?;
        read.set_nonblocking(true)
            .map_err(|source| format!("configure sample {label} socket: {source}"))?;
        Ok((
            Self {
                stream: read,
                label,
                bytes: Vec::new(),
                observed: 0,
                eof: false,
            },
            Stdio::from(OwnedFd::from(write)),
        ))
    }

    /// Bound work per turn and keep draining discarded bytes so output cannot stall sampling.
    fn drain(&mut self, buffer: &mut [u8], cap: usize, deadline: Instant) -> Result<(), String> {
        for _ in 0..cap.div_ceil(buffer.len()) {
            if self.eof || Instant::now() >= deadline {
                break;
            }
            match self.stream.read(buffer) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(count) => {
                    self.observed = self.observed.saturating_add(count);
                    let retain = count.min(cap.saturating_sub(self.bytes.len()));
                    self.bytes.extend_from_slice(&buffer[..retain]);
                }
                Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(source) => return Err(format!("read sample {}: {source}", self.label)),
            }
        }
        Ok(())
    }
}

/// Restrict stack classification to the call graph so loaded libraries do not dominate counts.
fn call_graph_text(report: &str) -> &str {
    let after_call_graph = report
        .split_once("Call graph:")
        .map(|(_, tail)| tail)
        .unwrap_or(report);
    after_call_graph
        .split_once("Binary Images:")
        .map(|(head, _)| head)
        .unwrap_or(after_call_graph)
}

/// Count the total number of marker occurrences in a sample report.
fn count_any(value: &str, markers: &[&str]) -> usize {
    markers
        .iter()
        .map(|marker| value.matches(marker).count())
        .sum()
}

/// Inspect output artifacts without reading produced file contents.
fn inspect_artifacts(
    output_dir: &Path,
    expected_artifact_path: &Path,
    diagnostics: &DiagnosticLimits,
) -> ArtifactMetrics {
    let mut metrics = ArtifactMetrics {
        expected_artifact_exists: expected_artifact_path.is_file(),
        ..ArtifactMetrics::default()
    };
    if let Err(error) = visit_artifact_dir(output_dir, &mut metrics, diagnostics) {
        metrics.error = Some(error);
    }

    metrics
}

/// Recursively visit artifact files and aggregate only metadata.
fn visit_artifact_dir(
    dir: &Path,
    metrics: &mut ArtifactMetrics,
    diagnostics: &DiagnosticLimits,
) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|error| format!("failed to read artifact dir {}: {error}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("failed to read artifact entry: {error}"))?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|error| {
            format!(
                "failed to read artifact metadata {}: {error}",
                path.display()
            )
        })?;
        if metadata.is_dir() {
            visit_artifact_dir(&path, metrics, diagnostics)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        let file_size = metadata.len();
        metrics.file_count = metrics.file_count.saturating_add(1);
        metrics.total_bytes = metrics.total_bytes.saturating_add(file_size);
        if path.extension().and_then(|value| value.to_str()) == Some("md") {
            metrics.markdown_count = metrics.markdown_count.saturating_add(1);
        }
        if metrics
            .largest_file_bytes
            .map(|largest| file_size > largest)
            .unwrap_or(true)
        {
            metrics.largest_file_bytes = Some(file_size);
            metrics.largest_file_name =
                path.file_name()
                    .and_then(|value| value.to_str())
                    .map(|file_name| {
                        file_name
                            .chars()
                            .take(diagnostics.activity_process_name_chars)
                            .collect()
                    });
        }
    }

    Ok(())
}

/// Classify current activity from sampled frames and cheap process facts.
fn classify_activity(process: &ProcessMetrics, sample: &SampleCounters) -> &'static str {
    if sample.tensor_frames > 0 {
        return "PyTorch CPU model work";
    }
    if sample.pdf_frames > 0 {
        return "PDF parsing";
    }
    if sample.ocr_image_frames > 0 {
        return "OCR/image processing";
    }
    if sample.file_io_frames > sample.blocked_wait_frames && sample.file_io_frames > 0 {
        return "file I/O";
    }
    if sample.openmp_wait_frames > 0 && sample.blocked_wait_frames > 0 {
        return "OpenMP synchronization";
    }
    if sample.blocked_wait_frames > 0 {
        return "waiting or blocked";
    }
    if process.cpu_percent.map(|cpu| cpu >= 50.0).unwrap_or(false) {
        return "active CPU work";
    }

    "unknown activity"
}

/// Format an optional percentage value for a compact progress line.
fn format_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}%"))
        .unwrap_or_else(|| "?".to_string())
}

/// Format an optional integer value for a compact progress line.
fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "?".to_string())
}

/// Format an optional byte count for a compact progress line.
fn format_bytes_option(value: Option<u64>) -> String {
    value.map(format_bytes).unwrap_or_else(|| "?".to_string())
}

/// Format bytes using binary units while staying terse.
fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes_f64 = bytes as f64;

    if bytes_f64 >= GIB {
        format!("{:.1}GiB", bytes_f64 / GIB)
    } else if bytes_f64 >= MIB {
        format!("{:.1}MiB", bytes_f64 / MIB)
    } else if bytes_f64 >= KIB {
        format!("{:.1}KiB", bytes_f64 / KIB)
    } else {
        format!("{bytes}B")
    }
}

/// Format a duration for a progress line without excessive precision.
fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!("{:02}:{:02}", seconds / 60, seconds % 60)
}

/// Bound diagnostic command errors included in metrics.
fn truncate_for_metric(value: &str, diagnostics: &DiagnosticLimits) -> String {
    let mut truncated = value
        .trim()
        .chars()
        .take(diagnostics.activity_error_chars)
        .collect::<String>();
    if value.chars().count() > diagnostics.activity_error_chars {
        truncated.push_str("...");
    }
    truncated
}
