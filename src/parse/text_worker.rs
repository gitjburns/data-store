//! Plain-text parser worker (spec §36 MVP): splits UTF-8 text sources into
//! typed candidate text blocks with char-range locators, staged as a parser
//! output bundle. Implemented by work package C4d.
//!
//! Trust boundary (spec §12.1): like every parser worker, this is an
//! untrusted producer. Its only write is a staged bundle under the parse
//! staging root via `crate::parse::bundle::BundleWriter`; nothing here is
//! canonical until the importer (C4b) validates and imports it. Canonical
//! IDs are never assigned here — candidate records carry parser-local IDs.
//!
//! Outcome model: a parse that fails because of the SOURCE (unreadable
//! file, invalid UTF-8, structural ceiling exceeded) is a recorded outcome —
//! the worker seals a failure bundle (spec §12.2: failure bundles are
//! preserved for diagnostics) and returns `Ok`. `Err` is reserved for
//! worker-side workspace faults (staging I/O), where no meaningful bundle
//! exists to record.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use tracing::{error, info, warn};

use crate::canonical;
use crate::error::ApiError;
use crate::model::{
    CharRangeLocator, ContentType, Locator, ParseMetrics, ParserCapabilityProfile, TextBlockBody,
    TextBlockRole, UnitRelationshipType,
};
use crate::parse::bundle::{
    BundleIdentity, BundleWriter, CandidateContentUnit, CandidateUnitRelationship,
    ParserExecutionStatus, ParserResult, parse_staging_root,
};
use crate::primitives::utc_now;
use crate::util::truncate_persisted_detail;

/// Parser identity name recorded in every manifest and capability profile
/// this worker emits.
pub(crate) const PLAIN_TEXT_PARSER_NAME: &str = "plain_text";

/// Parser implementation version. Bump when splitting or emission behavior
/// changes in a way the importer or activation can observe (a version bump
/// creates net-new canonical graphs, spec §12 rule 3).
pub(crate) const PLAIN_TEXT_PARSER_VERSION: &str = "1";

/// Structural ceiling on candidate text blocks from one source (spec §12.1
/// rule 5: parser execution has explicit resource bounds; §13.1: exceeded
/// limits are a hard fault). A source splitting into more paragraphs than
/// this fails the parse as a recorded outcome instead of staging an
/// unbounded bundle. Same ceiling family the importer enforces at §13.1
/// validation.
pub(crate) const MAX_CANDIDATE_TEXT_BLOCKS: u64 = 999_999;

/// Wire name of the char-range locator kind (spec §17), as declared in the
/// capability profile's `emitsLocatorKinds`. Must stay in sync with the
/// serde `kind` tag of `Locator::CharRange`.
const LOCATOR_KIND_CHAR_RANGE: &str = "char_range";

/// Canonical hash of this parser's effective configuration. The parser has
/// no tunable settings today, but the split policy is still versioned
/// configuration: hashing `{"paragraphSplit":"blank_line"}` means a future
/// policy change produces a different `parserConfigHash` instead of
/// silently reusing the old identity.
fn plain_text_config_hash() -> Result<String, ApiError> {
    canonical::canonical_sha256_hex(&serde_json::json!({ "paragraphSplit": "blank_line" }))
}

/// Build this parser's statically declared capability profile (spec §12.4)
/// with its hash computed over every field except the hash itself.
///
/// Declared surface: atomic `text_block` units only (no pages or sections —
/// plain text has neither), chained by `precedes` relationships, located by
/// `char_range` offsets into the source text.
pub(crate) fn plain_text_capability_profile() -> Result<ParserCapabilityProfile, ApiError> {
    let mut profile = ParserCapabilityProfile {
        parser_name: PLAIN_TEXT_PARSER_NAME.to_string(),
        parser_version: PLAIN_TEXT_PARSER_VERSION.to_string(),
        parser_config_hash: plain_text_config_hash()?,
        emits_content_types: vec![ContentType::TextBlock],
        emits_relationship_types: vec![UnitRelationshipType::Precedes],
        emits_locator_kinds: vec![LOCATOR_KIND_CHAR_RANGE.to_string()],
        emits_body_fields: None,
        profile_hash: String::new(),
    };
    profile.profile_hash = capability_profile_hash_of(&profile)?;
    Ok(profile)
}

/// Canonical hash over a capability profile's fields excluding
/// `profile_hash` itself (the shared self-hash pattern in
/// `crate::canonical`); the model struct stays the single source of the
/// hashed shape.
fn capability_profile_hash_of(profile: &ParserCapabilityProfile) -> Result<String, ApiError> {
    canonical::canonical_sha256_hex_without_field(profile, canonical::PROFILE_HASH_JSON_KEY)
}

/// Facts one parse produced, carried from the inner worker to the terminal
/// boundary log: the staged bundle directory plus the counts the log
/// reports. Byte/char counts live here (and in the log) because
/// `ParseMetrics` has no fields for them.
struct TextParseOutcome {
    bundle_dir: PathBuf,
    status: ParserExecutionStatus,
    /// Bounded failure detail when `status` is `Failed`; already safe to
    /// log and persist.
    failure_detail: Option<String>,
    unit_count: u64,
    relationship_count: u64,
    byte_count: u64,
    char_count: u64,
}

/// Run one plain-text parse over `source_absolute_path` and stage the
/// resulting parser output bundle under `{index_root}/fabric/staging/parse`;
/// returns the promoted bundle directory.
///
/// `Ok` covers both parse outcomes: a succeeded bundle full of candidate
/// text blocks, or a sealed failure bundle recording why the source could
/// not be parsed (unreadable, invalid UTF-8, ceiling exceeded). `Err` means
/// the staging workspace itself faulted and no bundle exists.
///
/// This function owns the worker's diagnostic boundary: it logs start,
/// terminal success with counts, recorded parse failure, and workspace
/// fault, each with elapsed time.
pub(crate) fn run_text_parse(
    index_root: &Path,
    source_absolute_path: &Path,
    source_id: &str,
    source_hash: &str,
) -> Result<PathBuf, ApiError> {
    let started = Instant::now();
    info!(
        event = "parse.text_worker.parse_started",
        source_id,
        source_path = %source_absolute_path.display(),
        "plain-text parse starting"
    );

    match run_text_parse_inner(
        index_root,
        source_absolute_path,
        source_id,
        source_hash,
        started,
    ) {
        Ok(outcome) => {
            let elapsed_ms = started.elapsed().as_millis() as u64;
            match outcome.status {
                ParserExecutionStatus::Succeeded => info!(
                    event = "parse.text_worker.parse_succeeded",
                    source_id,
                    bundle_dir = %outcome.bundle_dir.display(),
                    unit_count = outcome.unit_count,
                    relationship_count = outcome.relationship_count,
                    byte_count = outcome.byte_count,
                    char_count = outcome.char_count,
                    elapsed_ms,
                    "plain-text parse succeeded; bundle staged"
                ),
                // A failed parse is an expected untrusted-input outcome
                // (warn), recorded durably in the staged failure bundle.
                ParserExecutionStatus::Failed => warn!(
                    event = "parse.text_worker.parse_failed_recorded",
                    source_id,
                    bundle_dir = %outcome.bundle_dir.display(),
                    detail = outcome.failure_detail.as_deref().unwrap_or(""),
                    byte_count = outcome.byte_count,
                    elapsed_ms,
                    "plain-text parse failed; failure bundle staged"
                ),
            }
            Ok(outcome.bundle_dir)
        }
        Err(source) => {
            error!(
                event = "parse.text_worker.worker_faulted",
                source_id,
                source_path = %source_absolute_path.display(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "plain-text parse worker faulted; no bundle staged"
            );
            Err(source)
        }
    }
}

/// Parse steps behind the logging wrapper `run_text_parse`: open the bundle,
/// read and decode the source, split paragraphs, stream candidate units and
/// precedes relationships, and seal the bundle with outcome and metrics.
fn run_text_parse_inner(
    index_root: &Path,
    source_absolute_path: &Path,
    source_id: &str,
    source_hash: &str,
    started: Instant,
) -> Result<TextParseOutcome, ApiError> {
    let started_at = utc_now()?;

    // Identity first: even a bundle that records a failed parse carries the
    // full parser identity claims (the writer folds them into the manifest).
    let profile = plain_text_capability_profile()?;
    let identity = BundleIdentity {
        parser_name: profile.parser_name.clone(),
        parser_version: profile.parser_version.clone(),
        parser_config_hash: profile.parser_config_hash.clone(),
        capability_profile_hash: profile.profile_hash.clone(),
        source_id: source_id.to_string(),
        source_hash: source_hash.to_string(),
    };
    let mut writer = BundleWriter::create(&parse_staging_root(index_root), identity)?;

    // Source read/decode failures are parse outcomes, not worker faults:
    // seal a failure bundle and return Ok.
    let bytes = match fs::read(source_absolute_path) {
        Ok(bytes) => bytes,
        Err(source) => {
            return finish_failed(
                writer,
                started_at,
                started,
                format!(
                    "failed to read source file at {}: {source}",
                    source_absolute_path.display()
                ),
                0,
            );
        }
    };
    let byte_count = bytes.len() as u64;
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(source) => {
            return finish_failed(
                writer,
                started_at,
                started,
                format!("source is not valid UTF-8: {}", source.utf8_error()),
                byte_count,
            );
        }
    };
    let char_count = text.chars().count() as u64;

    // The source already sits fully in memory, so splitting before the
    // ceiling check is bounded by a small multiple of the source size; the
    // ceiling bounds the candidate record count, not transient memory.
    let paragraphs = split_paragraphs(&text);
    if paragraphs.len() as u64 > MAX_CANDIDATE_TEXT_BLOCKS {
        return finish_failed(
            writer,
            started_at,
            started,
            format!(
                "source splits into {} paragraphs, exceeding the ceiling of \
                 {MAX_CANDIDATE_TEXT_BLOCKS} candidate text blocks",
                paragraphs.len()
            ),
            byte_count,
        );
    }

    // Emit one text_block per paragraph in reading order, chained by
    // precedes relationships (no pages or sections exist in plain text, so
    // units have no parent and the chain is the whole structure).
    let unit_count = paragraphs.len() as u64;
    for (index, paragraph) in paragraphs.into_iter().enumerate() {
        let body = TextBlockBody {
            text: paragraph.text,
            normalized_text: None,
            block_role: Some(TextBlockRole::Paragraph),
            language: None,
        };
        // Serialization of our own typed body is a worker-side invariant;
        // failure here is a fault, not a parse outcome.
        let body = serde_json::to_value(&body).map_err(|source| ApiError::InternalIo {
            message: format!("failed to serialize text block body: {source}"),
        })?;
        writer.append_candidate_unit(&CandidateContentUnit {
            local_id: text_block_local_id(index),
            content_type: ContentType::TextBlock,
            body,
            parent_local_id: None,
            sequence_index: index as u64,
            locators: vec![Locator::CharRange(CharRangeLocator {
                start: paragraph.char_start,
                end: paragraph.char_end,
            })],
        })?;
        if index > 0 {
            writer.append_candidate_relationship(&CandidateUnitRelationship {
                from_local_id: text_block_local_id(index - 1),
                to_local_id: text_block_local_id(index),
                relationship_type: UnitRelationshipType::Precedes,
                relationship_role: None,
                sequence_index: (index - 1) as u64,
            })?;
        }
    }
    let relationship_count = unit_count.saturating_sub(1);

    let parser_result = ParserResult {
        status: ParserExecutionStatus::Succeeded,
        error: None,
        started_at,
        completed_at: utc_now()?,
        elapsed_ms: started.elapsed().as_millis() as u64,
        // In-process parser: no external tool ran, so no identity facts.
        tool_identity: BTreeMap::new(),
    };
    // No external process ran, so both captured log streams are empty; the
    // parser preserves no raw output (the source itself is the raw input).
    let bundle_dir = writer.finish(
        &parser_result,
        &text_parse_metrics(unit_count, relationship_count),
        &[],
        &[],
    )?;
    Ok(TextParseOutcome {
        bundle_dir,
        status: ParserExecutionStatus::Succeeded,
        failure_detail: None,
        unit_count,
        relationship_count,
        byte_count,
        char_count,
    })
}

/// Seal the open bundle as a recorded parse failure (spec §12.2: failure
/// bundles are preserved for diagnostics) and return the outcome for the
/// terminal log. `Err` from here means the seal itself faulted — the
/// workspace failure supersedes the parse failure it was trying to record.
fn finish_failed(
    writer: BundleWriter,
    started_at: String,
    started: Instant,
    detail: String,
    byte_count: u64,
) -> Result<TextParseOutcome, ApiError> {
    // Bound once here so the identical detail is safe both to persist in
    // the bundle and to emit in the boundary log.
    let detail = truncate_persisted_detail(&detail);
    let parser_result = ParserResult {
        status: ParserExecutionStatus::Failed,
        error: Some(detail.clone()),
        started_at,
        completed_at: utc_now()?,
        elapsed_ms: started.elapsed().as_millis() as u64,
        tool_identity: BTreeMap::new(),
    };
    let bundle_dir = writer.finish(&parser_result, &text_parse_metrics(0, 0), &[], &[])?;
    Ok(TextParseOutcome {
        bundle_dir,
        status: ParserExecutionStatus::Failed,
        failure_detail: Some(detail),
        unit_count: 0,
        relationship_count: 0,
        byte_count,
        // Unknown or meaningless on failure (the source may not even be
        // valid UTF-8); the failure log reports bytes only.
        char_count: 0,
    })
}

/// Parser-local ID for the text block at `index`. Deterministic from the
/// sequence index; replaced by a canonical parse-scoped ID at import.
fn text_block_local_id(index: usize) -> String {
    format!("text_block-{index}")
}

/// The metrics this worker measures: candidate unit and relationship
/// counts. `ParseMetrics` has no byte/char-count fields, so those facts are
/// reported in the boundary logs instead of the persisted metrics.
fn text_parse_metrics(unit_count: u64, relationship_count: u64) -> ParseMetrics {
    ParseMetrics {
        unit_count: Some(unit_count),
        relationship_count: Some(relationship_count),
        page_count: None,
        table_count: None,
        figure_count: None,
        ocr_region_count: None,
        annotation_count: None,
        projection_count: None,
    }
}

/// One paragraph carved out of the source text. Invariant: `text` is the
/// exact source slice covered by `[char_start, char_end)`, so the locator
/// and the body text can never disagree.
struct ParagraphSpan {
    /// Char offset (not byte offset) of the paragraph's first character in
    /// the source text — the locator kind is `char_range` (§17).
    char_start: u64,
    /// Char offset one past the paragraph's last character (exclusive).
    char_end: u64,
    /// Exact source slice of the paragraph, internal line breaks preserved.
    text: String,
}

/// Split source text into paragraphs on blank-line boundaries: a paragraph
/// is a maximal run of lines containing any non-whitespace character,
/// separated by one or more empty or whitespace-only lines. A paragraph
/// spans from the start of its first line to the end of its last line,
/// excluding the trailing line terminator; lines are `\n`-separated, so a
/// CRLF source keeps its `\r` characters inside the exact slice (locator
/// offsets stay honest against the original text). An empty or
/// whitespace-only source yields no paragraphs.
fn split_paragraphs(text: &str) -> Vec<ParagraphSpan> {
    let mut paragraphs = Vec::new();
    // Parallel offsets: chars feed the locator, bytes feed &str slicing.
    let mut char_offset: u64 = 0;
    let mut byte_offset: usize = 0;
    // Start offsets of the open paragraph run, if one is open.
    let mut open: Option<(u64, usize)> = None;
    // End offsets (char, byte) just past the last non-blank line seen in
    // the open run; only meaningful while `open` is Some.
    let mut run_end: (u64, usize) = (0, 0);

    for line in text.split('\n') {
        let line_chars = line.chars().count() as u64;
        let line_bytes = line.len();
        if line.trim().is_empty() {
            // Blank line: close the open run, if any.
            if let Some((char_start, byte_start)) = open.take() {
                paragraphs.push(ParagraphSpan {
                    char_start,
                    char_end: run_end.0,
                    text: text[byte_start..run_end.1].to_string(),
                });
            }
        } else {
            if open.is_none() {
                open = Some((char_offset, byte_offset));
            }
            run_end = (char_offset + line_chars, byte_offset + line_bytes);
        }
        // Advance past the line and its `\n` separator. After the final
        // segment (which has no separator) the offsets overshoot by one,
        // but they are never read again.
        char_offset += line_chars + 1;
        byte_offset += line_bytes + 1;
    }
    // Close a run left open at end-of-text (source without trailing blank
    // line).
    if let Some((char_start, byte_start)) = open {
        paragraphs.push(ParagraphSpan {
            char_start,
            char_end: run_end.0,
            text: text[byte_start..run_end.1].to_string(),
        });
    }
    paragraphs
}
