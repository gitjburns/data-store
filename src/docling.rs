use std::{
    fs,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::process::Command;

use crate::{config::DoclingConfig, error::ApiError, source::ResolvedSource};

static CONVERSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const MAX_DIAGNOSTIC_CHARS: usize = 16_000;

#[derive(Debug, Clone)]
pub struct ResolvedDoclingOptions {
    pub pdf_backend: String,
    pub ocr_mode: String,
    pub page_batch_size: Option<u32>,
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

/// Convert one resolved PDF source to markdown using only service-configured Docling options.
///
/// Diagnostics are bounded but preserved so conversion failures remain explicit and inspectable.
pub async fn convert_source_to_markdown(
    config: &DoclingConfig,
    index_root: &Path,
    source: ResolvedSource,
) -> Result<DoclingConversionResult, ApiError> {
    let options = resolve_docling_options(config)?;
    let output_dir = create_conversion_output_dir(index_root)?;
    let args = build_docling_args(&output_dir, &source.absolute_path, &options);
    let output = run_docling(config, &args, index_root).await?;
    let stdout = truncate_diagnostic_text(&String::from_utf8_lossy(&output.stdout));
    let stderr = truncate_diagnostic_text(&String::from_utf8_lossy(&output.stderr));

    if !output.status.success() {
        return Err(ApiError::DoclingConversion {
            message: format!(
                "Docling conversion failed for {} with status {}; args={}; stderr={}; stdout={}",
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
    let pdf_backend = config.default_pdf_backend.trim().to_string();
    let ocr_mode = config.default_ocr_mode.trim().to_string();
    let page_batch_size = config.page_batch_size;

    if !matches!(ocr_mode.as_str(), "auto" | "on" | "off") {
        return Err(ApiError::DoclingConversion {
            message: "docling.default_ocr_mode must be one of auto, on, or off".to_string(),
        });
    }

    Ok(ResolvedDoclingOptions {
        pdf_backend,
        ocr_mode,
        page_batch_size,
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
    ];

    if options.ocr_mode == "on" {
        args.push("--ocr".to_string());
    }
    if options.ocr_mode == "off" {
        args.push("--no-ocr".to_string());
    }
    if let Some(page_batch_size) = options.page_batch_size {
        args.push("--page-batch-size".to_string());
        args.push(page_batch_size.to_string());
    }

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
) -> Result<std::process::Output, ApiError> {
    Command::new(&config.docling_path)
        .args(args)
        .current_dir(index_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|source| ApiError::DoclingUnavailable {
            message: format!(
                "Docling CLI failed to start at {}; configured python_path is {}: {source}",
                config.docling_path.display(),
                config.python_path.display()
            ),
        })
}

/// Locate the markdown artifact Docling produced for one source file.
fn find_markdown_artifact(output_dir: &Path, source_path: &Path) -> Result<PathBuf, ApiError> {
    let expected_path = output_dir.join(format!(
        "{}.md",
        source_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("output")
    ));
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

/// Format child-process exit status fields for diagnostics.
fn format_exit_status(exit_code: Option<i32>, signal: Option<i32>) -> String {
    match (exit_code, signal) {
        (Some(code), _) => format!("exitCode={code}"),
        (None, Some(signal)) => format!("signal={signal}"),
        (None, None) => "unknown".to_string(),
    }
}
