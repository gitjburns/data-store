//! Shared, versioned cleanup of candidate text before canonical hashing.
//! Workers retain pre-cleanup records and a transformation report in parser_raw;
//! source locators continue to describe the original extraction, not string offsets
//! into the cleaned body. Structural references are rebuilt after unit merges.

mod prose;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use tracing::{error, info};

use crate::error::ApiError;
use crate::model::{ContentType, CoordinateSystem, Locator, TextBlockRole, UnitRelationshipType};
use crate::parse::bundle::{CandidateContentUnit, CandidateUnitRelationship};

/// Bump when any cleanup rule changes; both workers include this in parser identity.
pub(crate) const CLEANUP_VERSION: &str = "2";
const PRE_CLEANUP_FILE: &str = "pre_cleanup.json";
const REPORT_FILE: &str = "cleanup.json";
// PDF coordinates are points. This tolerance only compares column alignment;
// paragraph continuity also requires page-boundary, parent, and language evidence.
const COLUMN_ALIGNMENT_TOLERANCE: f64 = 2.0;

/// PDFs carry geometric evidence for reflow; plain text does not.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupKind {
    Pdf,
    PlainText,
}

/// Durable selection/normalization facts for the original extraction artifacts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CleanupReport {
    version: &'static str,
    input_units: usize,
    output_units: usize,
    changed_text_units: usize,
    protected_units: usize,
    removed_furniture: BTreeSet<String>,
    merged_paragraphs: BTreeMap<String, String>,
}

impl CleanupReport {
    /// Keep worker warnings attached to the surviving paragraph; furniture-only
    /// findings remain document warnings and remain available in raw extraction.
    pub(crate) fn remap_local_id<'a>(&'a self, id: &'a str) -> Option<&'a str> {
        if self.removed_furniture.contains(id) {
            return None;
        }
        Some(
            self.merged_paragraphs
                .get(id)
                .map(String::as_str)
                .unwrap_or(id),
        )
    }
}

/// Preserve the exact candidate records before cleanup, then record transformations
/// beside them. This is a worker staging boundary; canonical writes remain in importer.
pub(crate) fn stage_cleanup(
    raw_dir: &Path,
    units: &mut Vec<CandidateContentUnit>,
    relationships: &mut Vec<CandidateUnitRelationship>,
    kind: CleanupKind,
    source_id: &str,
) -> Result<CleanupReport, ApiError> {
    let started = Instant::now();
    info!(
        event = "parse.cleanup.started",
        source_id,
        version = CLEANUP_VERSION,
        units = units.len(),
        "candidate cleanup started"
    );
    let outcome = (|| {
        // The two named fields preserve both membership and the original
        // graph, including local IDs that merges or furniture removal will retire.
        #[derive(Serialize)]
        struct OriginalCandidates<'a> {
            units: &'a [CandidateContentUnit],
            relationships: &'a [CandidateUnitRelationship],
        }
        write_json(
            &raw_dir.join(PRE_CLEANUP_FILE),
            &OriginalCandidates {
                units,
                relationships,
            },
        )?;
        let report = clean_units(units, relationships, kind)?;
        write_json(&raw_dir.join(REPORT_FILE), &report)?;
        Ok(report)
    })();
    match &outcome {
        Ok(report) => info!(
            event = "parse.cleanup.completed",
            source_id,
            version = CLEANUP_VERSION,
            input_units = report.input_units,
            output_units = report.output_units,
            changed_text_units = report.changed_text_units,
            protected_units = report.protected_units,
            removed_furniture = report.removed_furniture.len(),
            merged_paragraphs = report.merged_paragraphs.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "candidate cleanup staged"
        ),
        Err(source) => error!(event = "parse.cleanup.failed", source_id, error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64, "candidate cleanup failed"),
    }
    outcome
}

/// Flush each staged artifact explicitly so a delayed I/O error cannot produce a
/// successful bundle whose raw record was only partially written.
fn write_json(path: &Path, value: &impl Serialize) -> Result<(), ApiError> {
    let file = File::create(path).map_err(|source| io_error(path, source))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, value).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to write cleanup artifact {}: {source}",
            path.display()
        ),
    })?;
    writer
        .write_all(b"\n")
        .map_err(|source| io_error(path, source))?;
    writer.flush().map_err(|source| io_error(path, source))
}

/// Preserve the artifact path and OS error at the staging boundary.
fn io_error(path: &Path, source: std::io::Error) -> ApiError {
    ApiError::InternalIo {
        message: format!("cleanup artifact {}: {source}", path.display()),
    }
}

/// Transform only known prose and leaf furniture. Keeping the first local ID of
/// a merged paragraph makes every retired ID traceable through the report.
fn clean_units(
    units: &mut Vec<CandidateContentUnit>,
    relationships: &mut Vec<CandidateUnitRelationship>,
    kind: CleanupKind,
) -> Result<CleanupReport, ApiError> {
    let mut report = CleanupReport {
        version: CLEANUP_VERSION,
        input_units: units.len(),
        output_units: 0,
        changed_text_units: 0,
        protected_units: 0,
        removed_furniture: BTreeSet::new(),
        merged_paragraphs: BTreeMap::new(),
    };
    let parents: BTreeSet<String> = units
        .iter()
        .filter_map(|unit| unit.parent_local_id.clone())
        .collect();
    let page_ends = page_prose_extents(units)?;
    let mut cleaned: Vec<CandidateContentUnit> = Vec::with_capacity(units.len());
    for mut unit in std::mem::take(units) {
        let role = text_role(&unit)?;
        let leaf = !parents.contains(&unit.local_id);
        if leaf && matches!(role, Some(TextBlockRole::Header | TextBlockRole::Footer)) {
            report.removed_furniture.insert(unit.local_id);
            continue;
        }
        if leaf && role == Some(TextBlockRole::Paragraph) && !prose::is_protected(text_of(&unit)) {
            if let Some(text) = unit.body.get("text").and_then(serde_json::Value::as_str) {
                let normalized = prose::clean_prose(text, kind == CleanupKind::Pdf);
                if normalized != text {
                    report.changed_text_units += 1;
                    unit.body["text"] = serde_json::Value::String(normalized);
                }
            }
            if kind == CleanupKind::Pdf
                && let Some(previous) = cleaned.last_mut()
                && !parents.contains(&previous.local_id)
                && !prose::is_protected(text_of(previous))
                && can_join_pages(previous, &unit, &page_ends, &report)?
            {
                let left = text_of(previous).trim_end();
                let right = text_of(&unit).trim_start();
                let joined = if let Some(prefix) = left.strip_suffix('\u{00ad}') {
                    format!("{prefix}{right}")
                } else if left.ends_with('-') {
                    // A hard hyphen may belong to a real compound. Preserve it;
                    // geometry establishes the join, not the spelling of the word.
                    format!("{left}{right}")
                } else {
                    format!("{left} {right}")
                };
                previous.body["text"] = serde_json::Value::String(joined);
                previous.locators.extend(unit.locators);
                report
                    .merged_paragraphs
                    .insert(unit.local_id, previous.local_id.clone());
                continue;
            }
        } else {
            report.protected_units += 1;
        }
        cleaned.push(unit);
    }
    for (index, unit) in cleaned.iter_mut().enumerate() {
        unit.sequence_index = index as u64;
    }
    report.output_units = cleaned.len();
    rebuild_relationships(&cleaned, relationships, &report);
    *units = cleaned;
    Ok(report)
}

/// Use the parser's typed role instead of guessing that arbitrary numbers or
/// low-word-count lines are garbage. Invalid candidate roles fail before import.
fn text_role(unit: &CandidateContentUnit) -> Result<Option<TextBlockRole>, ApiError> {
    if unit.content_type != ContentType::TextBlock {
        return Ok(None);
    }
    match unit.body.get("blockRole") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|source| ApiError::InternalIo {
                message: format!("cleanup cannot decode role of {}: {source}", unit.local_id),
            }),
    }
}

/// Read prose text without synthesizing content for non-text structures.
fn text_of(unit: &CandidateContentUnit) -> &str {
    unit.body
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

/// First and last prose blocks on a page provide page-boundary evidence without
/// deleting everything inside a fixed margin band.
struct PageProseExtent {
    first: String,
    last: String,
}

/// Capture original page endpoints before filtering so joins do not invent
/// adjacency merely because another block was removed.
fn page_prose_extents(
    units: &[CandidateContentUnit],
) -> Result<BTreeMap<u64, PageProseExtent>, ApiError> {
    let mut extents: BTreeMap<u64, PageProseExtent> = BTreeMap::new();
    for unit in units {
        if text_role(unit)? != Some(TextBlockRole::Paragraph) {
            continue;
        }
        for locator in &unit.locators {
            if let Locator::PageBbox(page) = locator {
                extents
                    .entry(page.page_number)
                    .and_modify(|extent| extent.last = unit.local_id.clone())
                    .or_insert_with(|| PageProseExtent {
                        first: unit.local_id.clone(),
                        last: unit.local_id.clone(),
                    });
            }
        }
    }
    Ok(extents)
}

/// Merge only an unindented lowercase continuation across consecutive pages of
/// the same logical paragraph stream. Missing geometry or punctuation is not proof.
fn can_join_pages(
    previous: &CandidateContentUnit,
    next: &CandidateContentUnit,
    extents: &BTreeMap<u64, PageProseExtent>,
    report: &CleanupReport,
) -> Result<bool, ApiError> {
    if text_role(previous)? != Some(TextBlockRole::Paragraph)
        || previous.parent_local_id.is_none()
        || previous.parent_local_id != next.parent_local_id
    {
        return Ok(false);
    }
    let left = text_of(previous).trim_end();
    let right = text_of(next).trim_start();
    if !right.chars().next().is_some_and(char::is_lowercase)
        || !left
            .chars()
            .last()
            .is_some_and(|c| c.is_alphabetic() || c == '-' || c == '\u{00ad}')
    {
        return Ok(false);
    }
    let before = previous
        .locators
        .iter()
        .filter_map(|l| match l {
            Locator::PageBbox(p) => Some(p),
            _ => None,
        })
        .next_back();
    let after = next.locators.iter().find_map(|l| match l {
        Locator::PageBbox(p) => Some(p),
        _ => None,
    });
    let (Some(before), Some(after)) = (before, after) else {
        return Ok(false);
    };
    if before.coordinate_system != Some(CoordinateSystem::PdfPoints)
        || after.coordinate_system != Some(CoordinateSystem::PdfPoints)
        || before.page_number.checked_add(1) != Some(after.page_number)
        || (before.bbox[0] - after.bbox[0]).abs() > COLUMN_ALIGNMENT_TOLERANCE
    {
        return Ok(false);
    }
    // A paragraph can span more than two pages: the previous page's original
    // endpoint may already have been merged into the surviving first local ID.
    Ok(extents
        .get(&before.page_number)
        .is_some_and(|e| report.remap_local_id(&e.last) == Some(previous.local_id.as_str()))
        && extents
            .get(&after.page_number)
            .is_some_and(|e| e.first == next.local_id))
}

/// Retire references to furniture, redirect merged endpoints, and reconstruct
/// sibling reading order. Non-ordering relationship roles and types are preserved.
fn rebuild_relationships(
    units: &[CandidateContentUnit],
    relationships: &mut Vec<CandidateUnitRelationship>,
    report: &CleanupReport,
) {
    let mut rewritten = Vec::with_capacity(relationships.len());
    let mut seen = BTreeSet::new();
    for mut edge in std::mem::take(relationships) {
        if matches!(
            edge.relationship_type,
            UnitRelationshipType::Precedes | UnitRelationshipType::Follows
        ) {
            continue;
        }
        let (Some(from), Some(to)) = (
            report.remap_local_id(&edge.from_local_id),
            report.remap_local_id(&edge.to_local_id),
        ) else {
            continue;
        };
        if from == to {
            continue;
        }
        let key = (
            from.to_string(),
            to.to_string(),
            edge.relationship_type.wire_name(),
            edge.relationship_role.clone(),
        );
        if !seen.insert(key) {
            continue;
        }
        edge.from_local_id = from.to_string();
        edge.to_local_id = to.to_string();
        rewritten.push(edge);
    }
    let mut siblings: BTreeMap<Option<&str>, Vec<&str>> = BTreeMap::new();
    for unit in units {
        siblings
            .entry(unit.parent_local_id.as_deref())
            .or_default()
            .push(&unit.local_id);
    }
    for group in siblings.values() {
        for pair in group.windows(2) {
            rewritten.push(CandidateUnitRelationship {
                from_local_id: pair[0].to_string(),
                to_local_id: pair[1].to_string(),
                relationship_type: UnitRelationshipType::Precedes,
                relationship_role: None,
                sequence_index: 0,
            });
        }
    }
    for (index, edge) in rewritten.iter_mut().enumerate() {
        edge.sequence_index = index as u64;
    }
    *relationships = rewritten;
}
