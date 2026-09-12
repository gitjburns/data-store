//! MuPDF production producer. Only the child enters native code; the parent owns
//! its deadline, bounded process diagnostics, and the untrusted parser bundle.
//! Script-based cleanup precedes canonical mapping, with raw extraction and repair
//! traces retained separately from the cleaned paragraphs.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    os::{fd::OwnedFd, unix::net::UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use tracing::{debug, error, info, warn};

use crate::{
    canonical,
    error::ApiError,
    model::{
        ContentType, CoordinateSystem, FigureBody, FigureType, Locator, PageBboxLocator, PageBody,
        ParseMetrics, ParseWarningSeverity, ParserCapabilityProfile, TextBlockBody,
        UnitRelationshipType,
    },
    primitives::utc_now,
    util::{LogContext, truncate_diagnostic_text, truncate_persisted_detail},
};

use super::{
    bundle::{
        BUNDLE_STREAM_LOG_CAP_BYTES, BundleIdentity, BundleWriter, CandidateContentUnit,
        CandidateUnitRelationship, CandidateWarning, ParserExecutionStatus, ParserResult,
        parse_staging_root,
    },
    mupdf_cleanup::{self, CleanedDocument, CleanupReport},
    native_pdf::{self, BlockKind, EXTRACTION_FLAGS, ExtractedPdf},
};

const WORKER_FLAG: &str = "--internal-mupdf-worker";
const PARSER_NAME: &str = "mupdf_pdf";
/// Version the canonical mapping independently from the native library dependency.
const PARSER_VERSION: &str = "2";
const RAW_FILE_NAME: &str = "mupdf.json";
const DEPENDENCY_LOCK: &str = include_str!("../../Cargo.lock");
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Dispatch the private two-path child protocol before config, HTTP, or model startup.
/// The scheduler supplies an exclusive output path inside its owned bundle workspace.
pub(crate) fn run_internal_command() -> Option<Result<()>> {
    let mut args = env::args_os().skip(1);
    if args.next().as_deref() != Some(OsStr::new(WORKER_FLAG)) {
        return None;
    }
    Some((|| {
        let input = args
            .next()
            .context("MuPDF worker requires an input PDF path")?;
        let output = args
            .next()
            .context("MuPDF worker requires an output JSON path")?;
        ensure!(
            args.next().is_none(),
            "MuPDF worker accepts exactly two paths"
        );
        run_child(Path::new(&input), Path::new(&output))
    })())
}

/// Write native output only after complete extraction. A partial JSON file from a
/// killed writer is retained as failed-run evidence and is never mapped by the parent.
fn run_child(input: &Path, output: &Path) -> Result<()> {
    let document = native_pdf::extract_pdf(input)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .with_context(|| format!("create MuPDF raw output {}", output.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &document)
        .with_context(|| format!("serialize MuPDF raw output {}", output.display()))?;
    writer
        .flush()
        .with_context(|| format!("flush MuPDF raw output {}", output.display()))
}

/// Bind repeat-parse guards and bundle validation to the same extraction and mapping.
pub(crate) fn capability_profile(
    document_timeout_seconds: u64,
) -> Result<ParserCapabilityProfile, ApiError> {
    let parser_config_hash = canonical::canonical_sha256_hex(&serde_json::json!({
        "documentTimeoutSeconds": document_timeout_seconds,
        "extractionFlags": format!("{EXTRACTION_FLAGS:?}"),
        "dependencyLockSha256": canonical::sha256_hex_bytes(DEPENDENCY_LOCK.as_bytes()),
        "mappingVersion": PARSER_VERSION,
        "cleanupVersion": mupdf_cleanup::CLEANUP_VERSION,
    }))?;
    let mut profile = ParserCapabilityProfile {
        parser_name: PARSER_NAME.to_owned(),
        parser_version: PARSER_VERSION.to_owned(),
        parser_config_hash,
        emits_content_types: vec![
            ContentType::Page,
            ContentType::TextBlock,
            ContentType::Figure,
        ],
        emits_relationship_types: vec![
            UnitRelationshipType::PhysicallyContains,
            UnitRelationshipType::AppearsOn,
            UnitRelationshipType::Precedes,
        ],
        emits_locator_kinds: vec!["page_bbox".to_owned()],
        emits_body_fields: None,
        profile_hash: String::new(),
    };
    profile.profile_hash =
        canonical::canonical_sha256_hex_without_field(&profile, canonical::PROFILE_HASH_JSON_KEY)?;
    Ok(profile)
}

/// Preserve the observed process result separately from the subsequent JSON/mapping verdict.
struct ProcessOutput {
    status: ExitStatus,
    timed_out: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Reap the owned child on every exit, including unwinding before normal wait completion.
struct OwnedChild {
    child: Child,
    reaped: bool,
}

impl OwnedChild {
    /// Poll only the owned process; the deadline covers native extraction and raw output.
    fn wait(
        &mut self,
        timeout: Duration,
        stdout: &mut OutputCapture,
        stderr: &mut OutputCapture,
    ) -> Result<(ExitStatus, bool)> {
        let started = Instant::now();
        loop {
            stdout.drain_available()?;
            stderr.drain_available()?;
            match self.child.try_wait().context("poll MuPDF child")? {
                Some(status) => {
                    self.reaped = true;
                    return Ok((status, false));
                }
                None if started.elapsed() >= timeout => {
                    warn!(
                        event = "mupdf.process.timeout",
                        process_id = self.child.id(),
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "MuPDF document deadline reached; stopping owned child"
                    );
                    return self.stop().map(|status| (status, true));
                }
                None => thread::sleep(PROCESS_POLL_INTERVAL),
            }
        }
    }

    /// Confirm exit after a kill, including the race where the child exited just before it.
    fn stop(&mut self) -> Result<ExitStatus> {
        match self.child.try_wait() {
            Ok(Some(status)) => {
                self.reaped = true;
                return Ok(status);
            }
            Ok(None) => {}
            Err(source) => {
                // A broken polling call is precisely when cleanup is needed;
                // it must not prevent the independent kill attempt below.
                warn!(event = "mupdf.process.cleanup_poll_failed", process_id = self.child.id(),
                    error = %source, "MuPDF poll failed; attempting termination anyway");
            }
        }
        if let Err(source) = self.child.kill() {
            if let Ok(Some(status)) = self.child.try_wait() {
                self.reaped = true;
                return Ok(status);
            }
            // Do not block in wait when termination itself could not be confirmed.
            return Err(source).context("terminate MuPDF child; exit remains unconfirmed");
        }
        let status = self.child.wait().context("reap terminated MuPDF child")?;
        self.reaped = true;
        info!(event = "mupdf.process.reaped", process_id = self.child.id(), status = %status,
            "terminated MuPDF child reaped");
        Ok(status)
    }
}

impl Drop for OwnedChild {
    /// An exceptional parent exit must not abandon native work or conceal cleanup failure.
    fn drop(&mut self) {
        if !self.reaped
            && let Err(source) = self.stop()
        {
            error!(event = "mupdf.process.cleanup_failed", process_id = self.child.id(),
                error = %format!("{source:#}"), "MuPDF child cleanup failed; exit is unconfirmed");
        }
    }
}

/// Parent-owned, nonblocking capture requires no reader thread that could outlive
/// child termination. Only the parent's socket endpoint is made nonblocking.
struct OutputCapture {
    stream: UnixStream,
    label: &'static str,
    captured: Vec<u8>,
    observed_bytes: usize,
    eof: bool,
}

impl OutputCapture {
    /// Prepare both endpoints before spawning native work, preserving blocking child writes.
    fn new(label: &'static str) -> Result<(Self, Stdio)> {
        let (reader, writer) =
            UnixStream::pair().with_context(|| format!("create MuPDF {label} capture socket"))?;
        reader
            .set_nonblocking(true)
            .with_context(|| format!("make MuPDF {label} capture nonblocking"))?;
        Ok((
            Self {
                stream: reader,
                label,
                captured: Vec::new(),
                observed_bytes: 0,
                eof: false,
            },
            Stdio::from(OwnedFd::from(writer)),
        ))
    }

    /// Limit each drain turn so even continuous output cannot starve the document deadline.
    /// Bytes beyond the retained allowance are consumed, keeping the child unblocked.
    fn drain_available(&mut self) -> Result<()> {
        let mut buffer = [0_u8; 8192];
        for _ in 0..(BUNDLE_STREAM_LOG_CAP_BYTES / buffer.len()) {
            if self.eof {
                break;
            }
            let count = match self.stream.read(&mut buffer) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(count) => count,
                Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(source) => {
                    return Err(source).with_context(|| format!("read MuPDF {}", self.label));
                }
            };
            self.observed_bytes += count;
            let retained =
                count.min(BUNDLE_STREAM_LOG_CAP_BYTES.saturating_sub(self.captured.len()));
            self.captured.extend_from_slice(&buffer[..retained]);
        }
        Ok(())
    }

    /// Drain the finite buffer after confirmed child exit; never wait for an unclosed writer.
    fn finish(mut self) -> Result<Vec<u8>> {
        while !self.eof {
            let before = self.observed_bytes;
            self.drain_available()?;
            ensure!(
                self.eof || self.observed_bytes > before,
                "MuPDF {} stayed open after child exit; capture is incomplete",
                self.label
            );
        }
        let truncated = self.observed_bytes > self.captured.len();
        debug!(
            event = "mupdf.output.captured",
            stream = self.label,
            observed_bytes = self.observed_bytes,
            retained_bytes = self.captured.len(),
            truncated,
            "MuPDF output capture completed"
        );
        if truncated {
            self.captured
                .extend_from_slice(b"\n[MuPDF process output truncated]\n");
        }
        Ok(self.captured)
    }
}

/// Run the same executable in extraction-only mode and poll its output synchronously.
/// Parent-owned nonblocking reads remain bounded even if process cleanup fails.
fn extract_in_child(input: &Path, output: &Path, timeout_seconds: u64) -> Result<ProcessOutput> {
    let started = Instant::now();
    let executable = env::current_exe().context("resolve MuPDF worker executable")?;
    info!(event = "mupdf.process.starting", executable_path = %executable.display(),
        source_path = %input.display(), output_path = %output.display(), timeout_seconds,
        "MuPDF extraction child starting");
    let (mut stdout, child_stdout) = OutputCapture::new("stdout")?;
    let (mut stderr, child_stderr) = OutputCapture::new("stderr")?;
    let child = Command::new(&executable)
        .arg(WORKER_FLAG)
        .arg(input)
        .arg(output)
        .stdin(Stdio::null())
        .stdout(child_stdout)
        .stderr(child_stderr)
        .spawn()
        .with_context(|| format!("spawn MuPDF worker {}", executable.display()))?;
    let mut child = OwnedChild {
        child,
        reaped: false,
    };
    let process_id = child.child.id();
    info!(
        event = "mupdf.process.spawned",
        process_id, "MuPDF extraction child spawned"
    );
    let (status, timed_out) = child.wait(
        Duration::from_secs(timeout_seconds),
        &mut stdout,
        &mut stderr,
    )?;
    info!(event = "mupdf.process.wait_completed", process_id, status = %status, timed_out,
        elapsed_ms = started.elapsed().as_millis() as u64, "MuPDF child exit confirmed");
    let stdout = stdout.finish()?;
    let stderr = stderr.finish()?;
    info!(event = "mupdf.process.completed", process_id, status = %status, timed_out,
        stdout_bytes = stdout.len(), stderr_bytes = stderr.len(),
        elapsed_ms = started.elapsed().as_millis() as u64, "MuPDF child process completed");
    Ok(ProcessOutput {
        status,
        timed_out,
        stdout,
        stderr,
    })
}

/// Stage producer successes and failures alike; only staging-infrastructure errors escape.
pub(crate) fn run_pdf_parse(
    timeout_seconds: u64,
    index_root: &Path,
    source: &Path,
    source_id: &str,
    source_hash: &str,
) -> Result<PathBuf, ApiError> {
    let started = Instant::now();
    let context = LogContext::new("pdf_parser", &crate::util::diagnostic_id("mupdf"));
    let _entered = context.enter();
    info!(event = "parse.mupdf_worker.started", parser_name = PARSER_NAME,
        parser_version = PARSER_VERSION, source_id, source_path = %source.display(), timeout_seconds,
        "MuPDF parse worker starting");
    let outcome = stage_parse(timeout_seconds, index_root, source, source_id, source_hash);
    match &outcome {
        Ok(bundle) => info!(event = "parse.mupdf_worker.completed", source_id,
            bundle_dir = %bundle.display(), elapsed_ms = started.elapsed().as_millis() as u64,
            "MuPDF parser outcome staged for canonical import"),
        Err(source) => error!(event = "parse.mupdf_worker.staging_failed", source_id,
            error = %source, elapsed_ms = started.elapsed().as_millis() as u64,
            "MuPDF staging infrastructure failed"),
    }
    outcome
}

/// Own the bundle separately from producer work so failed extraction/mapping remains inspectable.
fn stage_parse(
    timeout_seconds: u64,
    index_root: &Path,
    source: &Path,
    source_id: &str,
    source_hash: &str,
) -> Result<PathBuf, ApiError> {
    let started = Instant::now();
    let started_at = utc_now()?;
    let profile = capability_profile(timeout_seconds)?;
    let mut writer = BundleWriter::create(
        &parse_staging_root(index_root),
        BundleIdentity {
            parser_name: profile.parser_name,
            parser_version: profile.parser_version,
            parser_config_hash: profile.parser_config_hash,
            capability_profile_hash: profile.profile_hash,
            source_id: source_id.to_owned(),
            source_hash: source_hash.to_owned(),
        },
    )?;
    let raw_dir = writer.parser_raw_dir()?;
    let raw_path = raw_dir.join(RAW_FILE_NAME);
    let mut process = None;
    let mut cleanup_report = None;
    let mapped = (|| -> Result<MappedDocument> {
        process = Some(extract_in_child(source, &raw_path, timeout_seconds)?);
        let output = process.as_ref().context("MuPDF process outcome missing")?;
        if output.timed_out || !output.status.success() {
            bail!(
                "MuPDF extraction failed: status={}, timed_out={}, timeout_seconds={}; stdout={}; stderr={}",
                output.status,
                output.timed_out,
                timeout_seconds,
                truncate_diagnostic_text(&String::from_utf8_lossy(&output.stdout)),
                truncate_diagnostic_text(&String::from_utf8_lossy(&output.stderr))
            );
        }
        info!(event = "parse.mupdf_worker.raw_read_started", raw_path = %raw_path.display(),
            "reading completed MuPDF extraction");
        let file = File::open(&raw_path)
            .with_context(|| format!("open MuPDF output {}", raw_path.display()))?;
        let document: ExtractedPdf = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("decode MuPDF output {}", raw_path.display()))?;
        let cleanup_started = Instant::now();
        info!(
            event = "parse.mupdf_worker.cleanup_started",
            source_id,
            cleanup_version = mupdf_cleanup::CLEANUP_VERSION,
            "MuPDF text cleanup starting"
        );
        let cleaned = mupdf_cleanup::clean_document(&document).context("clean MuPDF text")?;
        info!(
            event = "parse.mupdf_worker.cleanup_completed",
            source_id,
            input_text_blocks = cleaned.report.input_text_blocks,
            output_paragraphs = cleaned.report.output_paragraphs,
            joined_blocks = cleaned.report.joined_blocks,
            removed_lines = cleaned.report.removed_lines.len(),
            dropped_paragraphs = cleaned.report.dropped_paragraphs,
            repair_passes = cleaned.report.repair_passes,
            elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
            "MuPDF cleanup calculated; staging report and canonical candidates next"
        );
        info!(
            event = "parse.mupdf_worker.mapping_started",
            page_count = document.pages.len(),
            "MuPDF raw output received; mapping canonical candidates"
        );
        let mapped = map_document(&document, &cleaned).context("map MuPDF canonical candidates");
        // Keep successful cleanup evidence even if the following mapping rejects
        // geometry or another canonical claim from the native output.
        cleanup_report = Some(cleaned.report);
        mapped
    })();
    if let Some(report) = cleanup_report {
        write_cleanup_report(&raw_dir.join(mupdf_cleanup::REPORT_FILE_NAME), &report)?;
    }
    let (status, failure, metrics) = match mapped {
        Ok(mapped) => {
            for unit in &mapped.units {
                writer.append_candidate_unit(unit)?;
            }
            for relationship in &mapped.relationships {
                writer.append_candidate_relationship(relationship)?;
            }
            for warning in &mapped.warnings {
                debug!(
                    event = "parse.mupdf_worker.finding",
                    code = warning.code,
                    detail = warning.message,
                    "MuPDF parse finding"
                );
                writer.append_warning(warning)?;
            }
            info!(
                event = "parse.mupdf_worker.mapping_completed",
                source_id,
                unit_count = mapped.units.len(),
                relationship_count = mapped.relationships.len(),
                warning_count = mapped.warnings.len(),
                pages_without_text = mapped.pages_without_text,
                unsupported_blocks = mapped.unsupported_blocks,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "MuPDF cleaned candidates staged with original source locators"
            );
            (ParserExecutionStatus::Succeeded, None, mapped.metrics)
        }
        Err(source) => {
            let detail = format!("{source:#}");
            error!(event = "parse.mupdf_worker.failed", source_id, error = %detail,
                elapsed_ms = started.elapsed().as_millis() as u64, "MuPDF producer failed");
            (
                ParserExecutionStatus::Failed,
                Some(truncate_persisted_detail(&detail)),
                empty_metrics(),
            )
        }
    };
    let result = ParserResult {
        status,
        error: failure,
        started_at,
        completed_at: utc_now()?,
        elapsed_ms: started.elapsed().as_millis() as u64,
        tool_identity: BTreeMap::from([
            ("backend".to_owned(), "mupdf".to_owned()),
            (
                "dependency_lock_sha256".to_owned(),
                canonical::sha256_hex_bytes(DEPENDENCY_LOCK.as_bytes()),
            ),
            (
                "extraction_flags".to_owned(),
                format!("{EXTRACTION_FLAGS:?}"),
            ),
        ]),
    };
    let (stdout, stderr) = process
        .as_ref()
        .map(|output| (output.stdout.as_slice(), output.stderr.as_slice()))
        .unwrap_or((&[], &[]));
    writer.finish(&result, &metrics, stdout, stderr)
}

/// A report write is staging infrastructure, not a producer verdict. Never promote
/// successful cleaned candidates without the raw-to-cleaned transformation evidence.
fn write_cleanup_report(path: &Path, report: &CleanupReport) -> Result<(), ApiError> {
    let write = || -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create cleanup report {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, report).context("serialize MuPDF cleanup report")?;
        writer.flush().context("flush MuPDF cleanup report")
    };
    write().map_err(|source| ApiError::InternalIo {
        message: format!("{source:#}"),
    })?;
    debug!(event = "parse.mupdf_worker.cleanup_report_staged", path = %path.display(),
        "MuPDF cleanup report staged for bundle archival");
    Ok(())
}

/// A failed producer has no complete canonical counts; absence is not measured zero.
fn empty_metrics() -> ParseMetrics {
    ParseMetrics {
        unit_count: None,
        relationship_count: None,
        page_count: None,
        table_count: None,
        figure_count: None,
        ocr_region_count: None,
        annotation_count: None,
        projection_count: None,
    }
}

/// Mapping products remain untrusted until the existing canonical importer validates them.
struct MappedDocument {
    units: Vec<CandidateContentUnit>,
    relationships: Vec<CandidateUnitRelationship>,
    warnings: Vec<CandidateWarning>,
    metrics: ParseMetrics,
    pages_without_text: usize,
    unsupported_blocks: usize,
}

/// Anchor each cleaned paragraph at its first native block while retaining every
/// contributing page locator. Physical page containers and images keep native order.
fn map_document(document: &ExtractedPdf, cleaned: &CleanedDocument) -> Result<MappedDocument> {
    let mut mapped = MappedDocument {
        units: Vec::new(),
        relationships: Vec::new(),
        warnings: Vec::new(),
        metrics: empty_metrics(),
        pages_without_text: 0,
        unsupported_blocks: 0,
    };
    let mut paragraphs = BTreeMap::new();
    for paragraph in &cleaned.paragraphs {
        let first = paragraph
            .sources
            .first()
            .context("cleaned paragraph has no source lines")?;
        ensure!(
            paragraphs
                .insert((first.page_number, first.block_index), paragraph)
                .is_none(),
            "cleaned paragraphs have duplicate native anchors"
        );
    }
    let mut previous_page: Option<String> = None;
    let mut figures = 0;
    for (page_index, page) in document.pages.iter().enumerate() {
        ensure!(
            page.page_number == page_index as u64 + 1,
            "MuPDF page numbers are not consecutive at index {page_index}"
        );
        let bounds = checked_bounds(page.bounds)?;
        let width = bounds[2] - bounds[0];
        let height = bounds[3] - bounds[1];
        ensure!(
            width > 0.0 && height > 0.0,
            "MuPDF page {} has empty bounds",
            page.page_number
        );
        let page_id = format!("#/pages/{}", page.page_number);
        push_unit(
            &mut mapped.units,
            &page_id,
            ContentType::Page,
            &PageBody {
                page_number: page.page_number,
                width,
                height,
                rotation: None,
                rendered_image_uri: None,
            },
            None,
            Vec::new(),
        )?;
        if let Some(previous) = previous_page.as_deref() {
            push_relationship(
                &mut mapped.relationships,
                previous,
                &page_id,
                UnitRelationshipType::Precedes,
            );
        }
        previous_page = Some(page_id.clone());
        let mut previous_block: Option<String> = None;
        // Cleanup may remove all text or move a continuation into an earlier
        // paragraph. The no-embedded-text warning still describes extraction.
        let has_text = page
            .blocks
            .iter()
            .filter(|block| matches!(block.kind, BlockKind::Text))
            .flat_map(|block| &block.lines)
            .flat_map(|line| &line.spans)
            .any(|span| !span.text.trim().is_empty());
        for (block_index, block) in page.blocks.iter().enumerate() {
            let locator = page_locator(page.page_number, bounds, checked_bounds(block.bounds)?);
            let local_id = format!("{page_id}/blocks/{block_index}");
            let mut appears_on = BTreeSet::from([page.page_number]);
            match block.kind {
                BlockKind::Text => {
                    let Some(paragraph) = paragraphs.remove(&(page.page_number, block_index))
                    else {
                        continue;
                    };
                    let mut locators = Vec::with_capacity(paragraph.sources.len());
                    for source in &paragraph.sources {
                        let page_index = usize::try_from(
                            source
                                .page_number
                                .checked_sub(1)
                                .context("cleaned source page number is zero")?,
                        )?;
                        let source_page = document
                            .pages
                            .get(page_index)
                            .context("cleaned paragraph references a missing page")?;
                        locators.push(page_locator(
                            source.page_number,
                            checked_bounds(source_page.bounds)?,
                            checked_bounds(source.bounds)?,
                        ));
                        appears_on.insert(source.page_number);
                    }
                    push_unit(
                        &mut mapped.units,
                        &local_id,
                        ContentType::TextBlock,
                        &TextBlockBody {
                            text: paragraph.text.clone(),
                            normalized_text: None,
                            block_role: None,
                            language: None,
                        },
                        Some(&page_id),
                        locators,
                    )?;
                }
                BlockKind::Image => {
                    figures += 1;
                    push_unit(
                        &mut mapped.units,
                        &local_id,
                        ContentType::Figure,
                        &FigureBody {
                            image_uri: None,
                            caption: None,
                            alt_text: None,
                            figure_type: Some(FigureType::Unknown),
                            ocr_text: None,
                        },
                        Some(&page_id),
                        vec![locator],
                    )?;
                }
                BlockKind::Struct | BlockKind::Vector | BlockKind::Grid => {
                    // The raw artifact retains these categories and bounds. A warning
                    // makes their omission from the canonical emission surface explicit.
                    mapped.unsupported_blocks += 1;
                    mapped.warnings.push(CandidateWarning {
                        code: "mupdf_unsupported_block".to_owned(),
                        message: format!("page {} block {block_index}: native {:?} block retained only in raw output", page.page_number, block.kind),
                        severity: ParseWarningSeverity::Warning,
                        locator: Some(locator),
                        unit_local_id: Some(page_id.clone()),
                    });
                    continue;
                }
            }
            push_relationship(
                &mut mapped.relationships,
                &page_id,
                &local_id,
                UnitRelationshipType::PhysicallyContains,
            );
            for page_number in appears_on {
                push_relationship(
                    &mut mapped.relationships,
                    &local_id,
                    &format!("#/pages/{page_number}"),
                    UnitRelationshipType::AppearsOn,
                );
            }
            if let Some(previous) = previous_block.as_deref() {
                push_relationship(
                    &mut mapped.relationships,
                    previous,
                    &local_id,
                    UnitRelationshipType::Precedes,
                );
            }
            previous_block = Some(local_id);
        }
        if !has_text {
            mapped.pages_without_text += 1;
            mapped.warnings.push(CandidateWarning {
                code: "mupdf_page_without_text".to_owned(),
                message: format!(
                    "page {} has no embedded text; MuPDF OCR is not enabled",
                    page.page_number
                ),
                severity: ParseWarningSeverity::Warning,
                locator: Some(page_locator(page.page_number, bounds, bounds)),
                unit_local_id: Some(page_id),
            });
        }
    }
    ensure!(
        paragraphs.is_empty(),
        "cleaned paragraph anchors were not mapped"
    );
    mapped.metrics = ParseMetrics {
        unit_count: Some(mapped.units.len() as u64),
        relationship_count: Some(mapped.relationships.len() as u64),
        page_count: Some(document.pages.len() as u64),
        table_count: Some(0),
        figure_count: Some(figures),
        ocr_region_count: Some(0),
        annotation_count: None,
        projection_count: None,
    };
    Ok(mapped)
}

/// Reject malformed worker geometry before deriving canonical source locators.
fn checked_bounds(bounds: [f32; 4]) -> Result<[f64; 4]> {
    ensure!(
        bounds.iter().all(|value| value.is_finite()),
        "MuPDF returned non-finite bounds"
    );
    ensure!(
        bounds[2] >= bounds[0] && bounds[3] >= bounds[1],
        "MuPDF returned inverted bounds"
    );
    Ok(bounds.map(f64::from))
}

/// Translate MuPDF's top-left page space to bottom-left PDF points. Subtract the
/// native page origin as well as flipping Y; nonzero page bounds must not shift citations.
fn page_locator(page_number: u64, page: [f64; 4], block: [f64; 4]) -> Locator {
    Locator::PageBbox(PageBboxLocator {
        page_number,
        bbox: [
            block[0] - page[0],
            page[3] - block[3],
            block[2] - page[0],
            page[3] - block[1],
        ],
        coordinate_system: Some(CoordinateSystem::PdfPoints),
    })
}

/// Give every candidate a stable sequence identity without changing its typed body.
fn push_unit(
    units: &mut Vec<CandidateContentUnit>,
    local_id: &str,
    content_type: ContentType,
    body: &impl Serialize,
    parent: Option<&str>,
    locators: Vec<Locator>,
) -> Result<()> {
    units.push(CandidateContentUnit {
        local_id: local_id.to_owned(),
        content_type,
        body: serde_json::to_value(body)
            .with_context(|| format!("serialize MuPDF candidate {local_id}"))?,
        parent_local_id: parent.map(str::to_owned),
        sequence_index: units.len() as u64,
        locators,
    });
    Ok(())
}

/// Record containment, placement, and sibling order as distinct canonical relationships.
fn push_relationship(
    relationships: &mut Vec<CandidateUnitRelationship>,
    from: &str,
    to: &str,
    relationship_type: UnitRelationshipType,
) {
    relationships.push(CandidateUnitRelationship {
        from_local_id: from.to_owned(),
        to_local_id: to.to_owned(),
        relationship_type,
        relationship_role: None,
        sequence_index: relationships.len() as u64,
    });
}
