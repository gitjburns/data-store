//! Shared, versioned cleanup of candidate text before canonical hashing.
//! Workers retain pre-cleanup records and a transformation report in parser_raw;
//! source locators continue to describe the original extraction, not string offsets
//! into the cleaned body. Structural references are rebuilt after cleanup so
//! sibling reading order is reconstructed from the surviving units.

mod prose;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use tracing::{error, info};

use crate::error::ApiError;
use crate::model::{ContentType, TextBlockRole, UnitRelationshipType};
use crate::parse::bundle::{CandidateContentUnit, CandidateUnitRelationship};

/// Bump when any cleanup rule changes; the plain-text worker includes this in
/// parser identity.
pub(crate) const CLEANUP_VERSION: &str = "2";
const PRE_CLEANUP_FILE: &str = "pre_cleanup.json";
const REPORT_FILE: &str = "cleanup.json";

/// Plain text carries no geometric evidence, so cleanup never merges units.
/// Phase 4 adds the EPUB variant; the enum stays so workers name their format.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupKind {
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
    merged_paragraphs: BTreeMap<String, String>,
}

impl CleanupReport {
    /// Redirect a local ID to the surviving paragraph it was merged into, or
    /// return it unchanged. Cleanup never removes units (SPEC-epub §2.2 has
    /// no furniture roles), so every ID survives.
    pub(crate) fn remap_local_id<'a>(&'a self, id: &'a str) -> &'a str {
        self.merged_paragraphs
            .get(id)
            .map(String::as_str)
            .unwrap_or(id)
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
        // graph before any text normalization or relationship rebuild.
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
        // Every format shares the prose pass; only PlainText exists until Phase 4.
        let report = match kind {
            CleanupKind::PlainText => clean_units(units, relationships)?,
        };
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

/// Transform only known leaf prose; every unit survives. `merged_paragraphs`
/// stays in the report contract although no current format merges units.
fn clean_units(
    units: &mut Vec<CandidateContentUnit>,
    relationships: &mut Vec<CandidateUnitRelationship>,
) -> Result<CleanupReport, ApiError> {
    let mut report = CleanupReport {
        version: CLEANUP_VERSION,
        input_units: units.len(),
        output_units: 0,
        changed_text_units: 0,
        protected_units: 0,
        merged_paragraphs: BTreeMap::new(),
    };
    let parents: BTreeSet<String> = units
        .iter()
        .filter_map(|unit| unit.parent_local_id.clone())
        .collect();
    let mut cleaned: Vec<CandidateContentUnit> = Vec::with_capacity(units.len());
    for mut unit in std::mem::take(units) {
        let role = text_role(&unit)?;
        let leaf = !parents.contains(&unit.local_id);
        if leaf && role == Some(TextBlockRole::Paragraph) && !prose::is_protected(text_of(&unit)) {
            if let Some(text) = unit.body.get("text").and_then(serde_json::Value::as_str) {
                let normalized = prose::clean_prose(text);
                if normalized != text {
                    report.changed_text_units += 1;
                    unit.body["text"] = serde_json::Value::String(normalized);
                }
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
    match unit.body.get("role") {
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

/// Redirect merged endpoints and reconstruct sibling reading order.
/// Non-ordering relationship roles and types are preserved.
fn rebuild_relationships(
    units: &[CandidateContentUnit],
    relationships: &mut Vec<CandidateUnitRelationship>,
    report: &CleanupReport,
) {
    let mut rewritten = Vec::with_capacity(relationships.len());
    let mut seen = BTreeSet::new();
    for mut edge in std::mem::take(relationships) {
        // Reading order is rebuilt below from the surviving siblings, so the
        // original `precedes` chain is dropped rather than remapped.
        if edge.relationship_type == UnitRelationshipType::Precedes {
            continue;
        }
        let from = report.remap_local_id(&edge.from_local_id);
        let to = report.remap_local_id(&edge.to_local_id);
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
