//! Offline native-PDF evaluation. Outputs belong to a new scratch directory;
//! this binary does not initialize the service, open its database, or run OCR.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[path = "../parse/mupdf_cleanup.rs"]
mod mupdf_cleanup;
#[path = "../parse/native_pdf.rs"]
mod native_pdf;

use native_pdf::{BlockKind, EXTRACTION_FLAGS, ExtractedPdf};

const USAGE: &str = "pdf-extract-diagnostic <input.pdf> <new-output-directory>\n\
Both paths must be inside this repository. The output parent must already exist.\n\
Writes raw.json, extracted.md, cleaned.md, mupdf_cleanup.json, and report.json.\n\
No OCR, heading inference, font heuristics, database writes, or production routing.\n\
The cleaned preview uses the production text cleaner; raw.json retains all native blocks.";
const DEPENDENCY_LOCK: &str = include_str!("../../Cargo.lock");

/// Stable input and exclusive scratch output chosen explicitly by the operator.
struct Options {
    input: PathBuf,
    output: PathBuf,
}

/// A running report survives an interrupted evaluation without claiming success.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Running,
    Succeeded,
    Failed,
}

/// Identify the last reached boundary when extraction or output publication fails.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    InspectInput,
    Extract,
    Render,
    WriteArtifacts,
    Complete,
}

/// Measured extraction facts; image-only and empty pages are visible separately
/// from text-bearing pages without guessing whether any page needs OCR.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Metrics {
    pages: usize,
    text_blocks: usize,
    image_blocks: usize,
    other_blocks: usize,
    text_characters: usize,
    pages_without_text: Vec<u64>,
    extracted_markdown_bytes: usize,
    cleaned_markdown_bytes: usize,
}

/// Durable scratch evidence of parameters, measured times, and terminal outcome.
/// The compiled lock hash identifies the exact dependency resolution being measured.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report<'a> {
    backend: &'static str,
    extraction_flags: String,
    dependency_lock_sha256: String,
    input: &'a Path,
    output: &'a Path,
    started_unix_ms: u128,
    status: Status,
    stage: Stage,
    elapsed_ms: u128,
    input_bytes: Option<u64>,
    extraction_ms: Option<u128>,
    rendering_ms: Option<u128>,
    metrics: Option<Metrics>,
    error: Option<String>,
}

/// Run one explicit evaluation and preserve a terminal report even on normal
/// failure. A pre-existing output directory is rejected before extraction begins.
fn main() -> Result<()> {
    let Some(options) = parse_options()? else {
        return Ok(());
    };
    fs::create_dir(&options.output)
        .with_context(|| format!("create new scratch directory {}", options.output.display()))?;
    let started = Instant::now();
    let mut report = Report {
        backend: "mupdf",
        extraction_flags: format!("{EXTRACTION_FLAGS:?}"),
        dependency_lock_sha256: format!("{:x}", Sha256::digest(DEPENDENCY_LOCK.as_bytes())),
        input: &options.input,
        output: &options.output,
        started_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        status: Status::Running,
        stage: Stage::InspectInput,
        elapsed_ms: 0,
        input_bytes: None,
        extraction_ms: None,
        rendering_ms: None,
        metrics: None,
        error: None,
    };
    let report_path = options.output.join("report.json");
    write_json(&report_path, &report)?;
    eprintln!("native PDF evaluation started: {}", options.input.display());
    let outcome = evaluate(&options, &mut report, started);
    report.elapsed_ms = started.elapsed().as_millis();
    match &outcome {
        Ok(()) => {
            report.status = Status::Succeeded;
            report.stage = Stage::Complete;
        }
        Err(error) => {
            report.status = Status::Failed;
            report.error = Some(format!("{error:#}"));
        }
    }
    if let Err(report_error) = write_json(&report_path, &report) {
        if let Err(original_error) = outcome {
            bail!(
                "evaluation failed: {original_error:#}; terminal report also failed: {report_error:#}"
            );
        }
        return Err(report_error);
    }
    match outcome {
        Ok(()) => {
            eprintln!(
                "native PDF evaluation completed in {} ms; report: {}",
                report.elapsed_ms,
                report_path.display()
            );
            Ok(())
        }
        Err(error) => {
            eprintln!(
                "native PDF evaluation failed in {} ms; report: {}",
                report.elapsed_ms,
                report_path.display()
            );
            Err(error)
        }
    }
}

/// Require explicit repository-local paths and resolve existing parents before
/// creating scratch output, so symlinks cannot redirect writes outside the workspace.
fn parse_options() -> Result<Option<Options>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        println!("{USAGE}");
        return Ok(None);
    }
    if args.len() != 2 {
        bail!("{USAGE}");
    }
    let root = fs::canonicalize(env!("CARGO_MANIFEST_DIR"))?;
    let input = fs::canonicalize(PathBuf::from(&args[0])).context("resolve input PDF path")?;
    if !input.starts_with(&root) || !input.is_file() {
        bail!("input must be a file inside {}", root.display());
    }
    let requested_output = std::env::current_dir()?.join(&args[1]);
    let name = requested_output
        .file_name()
        .context("output must name a new directory")?;
    let parent = fs::canonicalize(
        requested_output
            .parent()
            .context("output needs an existing parent")?,
    )
    .context("resolve output parent directory")?;
    if !parent.starts_with(&root) {
        bail!("output must be inside {}", root.display());
    }
    let output = parent.join(name);
    Ok(Some(Options { input, output }))
}

/// Separate native extraction time from Markdown cleanup and artifact I/O. Input
/// metadata is checked again because this diagnostic expects a stable comparison PDF.
fn evaluate(options: &Options, report: &mut Report<'_>, started: Instant) -> Result<()> {
    let before = fs::metadata(&options.input).context("inspect input PDF")?;
    let modified_before = before.modified().context("read input modification time")?;
    report.input_bytes = Some(before.len());
    report.stage = Stage::Extract;
    checkpoint(options, report, started)?;
    let extraction_started = Instant::now();
    let document = native_pdf::extract_pdf(&options.input)?;
    report.extraction_ms = Some(extraction_started.elapsed().as_millis());
    let after = fs::metadata(&options.input).context("recheck input PDF")?;
    if before.len() != after.len() || modified_before != after.modified()? {
        bail!(
            "input PDF changed during extraction; discard this comparison and retry with a stable file"
        );
    }
    report.stage = Stage::WriteArtifacts;
    checkpoint(options, report, started)?;
    write_json(&options.output.join("raw.json"), &document)?;
    report.stage = Stage::Render;
    checkpoint(options, report, started)?;
    let rendering_started = Instant::now();
    let cleaned_document = mupdf_cleanup::clean_document(&document)?;
    let (extracted, cleaned, metrics) = render_markdown(&document, &cleaned_document);
    report.rendering_ms = Some(rendering_started.elapsed().as_millis());
    report.metrics = Some(metrics);
    report.stage = Stage::WriteArtifacts;
    checkpoint(options, report, started)?;
    write_json(
        &options.output.join(mupdf_cleanup::REPORT_FILE_NAME),
        &cleaned_document.report,
    )?;
    fs::write(options.output.join("extracted.md"), extracted).context("write extracted.md")?;
    fs::write(options.output.join("cleaned.md"), cleaned).context("write cleaned.md")?;
    Ok(())
}

/// Publish the last reached boundary before long-running work so an interrupted
/// native call leaves an honest running report with a known extraction stage.
fn checkpoint(options: &Options, report: &mut Report<'_>, started: Instant) -> Result<()> {
    report.elapsed_ms = started.elapsed().as_millis();
    write_json(&options.output.join("report.json"), report)
}

/// Keep the raw baseline inspectable while rendering exactly the production
/// paragraphs in cleaned.md. Page notices belong only to the raw baseline.
fn render_markdown(
    document: &ExtractedPdf,
    cleaned_document: &mupdf_cleanup::CleanedDocument,
) -> (String, String, Metrics) {
    let mut extracted = String::new();
    let mut cleaned = String::new();
    for paragraph in &cleaned_document.paragraphs {
        cleaned.push_str(&paragraph.text);
        cleaned.push_str("\n\n");
    }
    let mut metrics = Metrics {
        pages: document.pages.len(),
        text_blocks: 0,
        image_blocks: 0,
        other_blocks: 0,
        text_characters: 0,
        pages_without_text: Vec::new(),
        extracted_markdown_bytes: 0,
        cleaned_markdown_bytes: 0,
    };
    for page in &document.pages {
        let marker = format!("<!-- PDF page {} -->\n\n", page.page_number);
        extracted.push_str(&marker);
        let mut has_text = false;
        for block in &page.blocks {
            match block.kind {
                BlockKind::Text => metrics.text_blocks += 1,
                BlockKind::Image => metrics.image_blocks += 1,
                BlockKind::Struct | BlockKind::Vector | BlockKind::Grid => {
                    metrics.other_blocks += 1
                }
            }
            let text = block
                .lines
                .iter()
                .map(|line| {
                    line.spans
                        .iter()
                        .map(|span| span.text.as_str())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
                .join("\n");
            metrics.text_characters += block
                .lines
                .iter()
                .flat_map(|line| &line.spans)
                .map(|span| span.text.chars().count())
                .sum::<usize>();
            if text.trim().is_empty() {
                continue;
            }
            has_text = true;
            extracted.push_str(&text);
            extracted.push_str("\n\n");
        }
        if !has_text {
            metrics.pages_without_text.push(page.page_number);
            let notice = "[No embedded text extracted on this page; OCR was not performed.]\n\n";
            extracted.push_str(notice);
        }
    }
    metrics.extracted_markdown_bytes = extracted.len();
    metrics.cleaned_markdown_bytes = cleaned.len();
    (extracted, cleaned, metrics)
}

/// Publish scratch JSON through a flushed sibling temporary file, preserving the
/// last complete report if interrupted during a checkpoint. Both paths are owned
/// by this evaluation's newly created directory.
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    let file =
        File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .with_context(|| format!("serialize {}", path.display()))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("finish {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    drop(writer);
    fs::rename(&temporary, path).with_context(|| format!("publish {}", path.display()))
}
