use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const SAMPLE_TIMEOUT_GRACE_SECONDS: u64 = 15;

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
    pub expected_markdown_exists: bool,
    pub largest_file_name: Option<String>,
    pub largest_file_bytes: Option<u64>,
    pub error: Option<String>,
}

/// Inspect one still-running Docling process and reduce raw OS evidence to compact metrics.
pub fn inspect_docling_activity(
    process_id: u32,
    output_dir: &Path,
    expected_markdown_path: &Path,
    sample_duration: Duration,
) -> DoclingActivityReport {
    let process = inspect_process_metrics(process_id);
    let sample = sample_process_activity(process_id, sample_duration);
    let artifacts = inspect_artifacts(output_dir, expected_markdown_path);
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
        "Docling:100% cpu:{} mem:{} rss:{} thr:{} st:{} t={} omp={} pdf={} ocr={} io={} blk={} out={} files={} {}",
        format_percent(report.process.cpu_percent),
        format_percent(report.process.memory_percent),
        format_bytes_option(report.process.rss_bytes),
        format_optional_u64(report.process.thread_count),
        report.process.state.as_deref().unwrap_or("?"),
        report.sample.tensor_frames,
        report.sample.openmp_wait_frames,
        report.sample.pdf_frames,
        report.sample.ocr_image_frames,
        report.sample.file_io_frames,
        report.sample.blocked_wait_frames,
        format_bytes(report.artifacts.total_bytes),
        report.artifacts.file_count,
        format_duration(elapsed),
    )
}

/// Read cheap process metrics from the platform process table.
fn inspect_process_metrics(process_id: u32) -> ProcessMetrics {
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
                    truncate_for_metric(&String::from_utf8_lossy(&output.stderr))
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
fn sample_process_activity(process_id: u32, sample_duration: Duration) -> SampleCounters {
    let report = match run_bounded_sample(process_id, sample_duration) {
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

/// Execute sample with a timeout around the command itself, not the Docling process.
fn run_bounded_sample(process_id: u32, sample_duration: Duration) -> Result<String, String> {
    let mut child = Command::new("sample")
        .args([
            &process_id.to_string(),
            &sample_duration.as_secs().max(1).to_string(),
            "-file",
            "/dev/stdout",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("sample failed to start: {error}"))?;
    let deadline = Instant::now()
        .checked_add(sample_duration + Duration::from_secs(SAMPLE_TIMEOUT_GRACE_SECONDS))
        .ok_or_else(|| "sample timeout overflowed".to_string())?;

    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| format!("sample output read failed: {error}"))?;
                if !output.status.success() {
                    return Err(format!(
                        "sample exited with {}; stderr={}",
                        output.status,
                        truncate_for_metric(&String::from_utf8_lossy(&output.stderr))
                    ));
                }
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Ok(format!("{stdout}\n{stderr}"));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("sample timed out".to_string());
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("sample wait failed: {error}"));
            }
        }
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
fn inspect_artifacts(output_dir: &Path, expected_markdown_path: &Path) -> ArtifactMetrics {
    let mut metrics = ArtifactMetrics {
        expected_markdown_exists: expected_markdown_path.is_file(),
        ..ArtifactMetrics::default()
    };
    if let Err(error) = visit_artifact_dir(output_dir, &mut metrics) {
        metrics.error = Some(error);
    }

    metrics
}

/// Recursively visit artifact files and aggregate only metadata.
fn visit_artifact_dir(dir: &Path, metrics: &mut ArtifactMetrics) -> Result<(), String> {
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
            visit_artifact_dir(&path, metrics)?;
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
                        let mut value = file_name.to_string();
                        value.truncate(80);
                        value
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
    if seconds >= 3600 {
        format!("{}h{}m", seconds / 3600, (seconds % 3600) / 60)
    } else if seconds >= 60 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

/// Bound diagnostic command errors included in metrics.
fn truncate_for_metric(value: &str) -> String {
    const MAX_ERROR_CHARS: usize = 240;
    let mut truncated = value
        .trim()
        .chars()
        .take(MAX_ERROR_CHARS)
        .collect::<String>();
    if value.chars().count() > MAX_ERROR_CHARS {
        truncated.push_str("...");
    }
    truncated
}
