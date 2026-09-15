//! Lossless canonical evidence for the final ranked passages.
//!
//! Passage construction and ranking own selection. Assembly resolves exactly
//! those selected units in the caller's captured SQLite snapshot, preserving
//! passage rank and canonical reading order with first-inclusion deduplication.
//! It never expands the result or reapplies presentation token limits.

use std::collections::BTreeSet;
use std::time::Instant;

use crate::sqlite::Connection;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use tracing::{error, info};

use crate::error::ApiError;
use crate::model::unit::ContentType;
use crate::model::{Locator, SemanticAnnotation};
use crate::primitives::utc_now;

use crate::assembly::model::{
    AppliedAssemblyRule, AssemblyPolicy, AssemblyReason, ContextAssemblyTrace, EvidencePack,
    EvidenceUnit,
};
use crate::assembly::operators::selected_relationships;
use crate::assembly::policy::{CapturedParseRef, SELECTED_PASSAGE_RULE_ID};

/// Borrowed final passage membership keeps assembly independent of query DTOs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PassageEvidence<'a> {
    /// Representative ranked unit, which alone receives the passage score.
    pub(crate) anchor_unit_id: &'a str,
    /// Captured active parse shared by every contributing canonical unit.
    pub(crate) parse_id: &'a str,
    /// Canonical units actually used in the passage, in reading order.
    pub(crate) unit_ids: &'a [String],
    /// Final passage reranker score.
    pub(crate) score: f64,
}

/// The R6 evidence-shaping flags the request resolved (defaults applied by
/// C8d-1's request layer): whether to attach source locators, selected-unit
/// relationships, and the intersecting annotations. Plain bools, so C8d-2 hands
/// them over without `assembly/` depending on the request DTO.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EvidenceOptions {
    /// Attach each unit's `locators` (spec `includeSourceLocators`).
    pub(crate) include_source_locators: bool,
    /// Populate `EvidencePack.relationships` with links between selected units
    /// (spec `includeRelationships`).
    pub(crate) include_relationships: bool,
    /// Populate `EvidencePack.annotations` for the selected units
    /// (spec `includeAnnotations`).
    pub(crate) include_annotations: bool,
}

/// Resolve final passage membership without adding context or truncating raw bodies.
///
/// The caller owns passage count and displayed-text limits. Assembly's separate
/// raw safety ceilings fail visibly so the result and its audit pack cannot
/// disagree. The token counter is CPU-only and must not acquire the model gate.
// Explicit boundary inputs keep assembly independent of query execution types.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_evidence_pack(
    conn: &Connection,
    captured: &[CapturedParseRef<'_>],
    policy: &AssemblyPolicy,
    passages: &[PassageEvidence<'_>],
    options: EvidenceOptions,
    count_tokens: impl Fn(&str) -> Result<usize, ApiError>,
    query_id: &str,
    query_text: &str,
) -> Result<EvidencePack, ApiError> {
    let started_at = Instant::now();
    info!(
        event = "assembly.build.started",
        query_id,
        passage_count = passages.len(),
        captured_parse_count = captured.len(),
        policy_id = policy.id.as_str(),
        policy_version = policy.version.as_str(),
        policy_hash = policy.policy_hash.as_str(),
        "assembly evidence-pack build started"
    );

    // This closure keeps every fallible assembly operation under one diagnostic
    // boundary, including annotation/relationship reads and timestamp generation.
    let mut stage = "passage_inclusion";
    let assembled = (|| {
        let mut state = SelectionState {
            selected: Vec::new(),
            selected_set: BTreeSet::new(),
            tokens_used: 0,
            applied_rules: Vec::new(),
        };
        for passage in passages {
            include_selected_passage(
                conn,
                captured,
                policy,
                passage,
                &options,
                &count_tokens,
                &mut state,
            )
            .inspect_err(|source| {
                error!(event = "assembly.passage.failed", query_id,
                    parse_id = passage.parse_id, unit_id = passage.anchor_unit_id,
                    stage = "passage_inclusion", error = %source,
                    error_chain = %crate::util::error_chain(source, &conn.limits().diagnostics),
                    "selected passage could not be assembled");
            })?;
        }
        let selected_unit_ids: Vec<String> = state
            .selected
            .iter()
            .map(|unit| unit.unit_id.clone())
            .collect();
        let relationships = if options.include_relationships {
            stage = "relationship_reading";
            Some(selected_relationships(
                conn,
                &state
                    .selected
                    .iter()
                    .map(|unit| (unit.parse_id.as_str(), unit.unit_id.as_str()))
                    .collect::<Vec<_>>(),
            )?)
        } else {
            None
        };
        let annotations = if options.include_annotations {
            stage = "annotation_reading";
            Some(collect_annotations(conn, captured, &selected_unit_ids)?)
        } else {
            None
        };
        stage = "evidence_pack_construction";
        let pack = EvidencePack {
            query_id: query_id.to_string(),
            query_text: query_text.to_string(),
            evidence_units: state
                .selected
                .iter()
                .map(|unit| render_evidence_unit(unit, &options))
                .collect(),
            relationships,
            annotations,
            assembly_trace: ContextAssemblyTrace {
                assembly_policy_id: policy.id.clone(),
                assembly_policy_version: policy.version.clone(),
                assembly_policy_hash: policy.policy_hash.clone(),
                input_hit_ids: passages
                    .iter()
                    .map(|passage| passage.anchor_unit_id.to_string())
                    .collect(),
                applied_rules: state.applied_rules,
                selected_unit_ids,
                rejected_hit_ids: None,
                rejected_unit_ids: None,
                budget: policy.budgets.clone(),
            },
            created_at: utc_now()?,
        };
        Ok::<_, ApiError>((pack, state.tokens_used))
    })();

    match assembled {
        Ok((pack, tokens_used)) => {
            info!(
                event = "assembly.build.completed",
                query_id,
                passage_count = passages.len(),
                selected_unit_count = pack.evidence_units.len(),
                applied_rule_count = pack.assembly_trace.applied_rules.len(),
                tokens_used,
                max_tokens = policy.budgets.max_tokens,
                max_evidence_units = policy.budgets.max_evidence_units,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "assembly evidence-pack build completed"
            );
            Ok(pack)
        }
        Err(source) => {
            error!(
                event = "assembly.build.failed",
                stage,
                query_id,
                passage_count = passages.len(),
                error = %source,
                error_chain = %crate::util::error_chain(&source, &conn.limits().diagnostics),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "assembly evidence-pack build failed"
            );
            Err(source)
        }
    }
}

/// First inclusion fixes raw-unit order; later passage anchors may add scores.
struct SelectionState {
    selected: Vec<SelectedUnit>,
    selected_set: BTreeSet<String>,
    tokens_used: usize,
    applied_rules: Vec<AppliedAssemblyRule>,
}

/// One unit chosen for the pack, fully resolved at selection time. Its content
/// is read ONCE (parse-scoped, §14) when the unit is first selected — so token
/// charging (R10) and the final pack render both use the same resolved body and
/// no second read occurs. The `parse_id` is fixed at selection and never
/// re-derived (§14 scoping is settled here).
struct SelectedUnit {
    unit_id: String,
    parse_id: String,
    source_id: String,
    content_type: ContentType,
    /// Canonical §18 body payload, parsed from `body_json`.
    body: Value,
    /// Extracted text projection (also the `count_tokens` input), `None` for
    /// container/structural types.
    text_projection: Option<String>,
    /// Parsed `locators_json`, resolved only when `includeSourceLocators`.
    locators: Option<Vec<Locator>>,
    /// Inclusion reasons in application order; surfaced on `EvidenceUnit.reasons`.
    reasons: Vec<AssemblyReason>,
    /// Present only for anchor units (the rerank score); `None` for neighbors.
    score: Option<f64>,
}

/// Retain each contributing unit exactly once and attribute it to its passage.
/// Missing or out-of-snapshot members are errors, never partial evidence.
fn include_selected_passage(
    conn: &Connection,
    captured: &[CapturedParseRef<'_>],
    policy: &AssemblyPolicy,
    passage: &PassageEvidence<'_>,
    options: &EvidenceOptions,
    count_tokens: &impl Fn(&str) -> Result<usize, ApiError>,
    state: &mut SelectionState,
) -> Result<(), ApiError> {
    let source_id = captured
        .iter()
        .find(|parse| parse.parse_id == passage.parse_id)
        .map(|parse| parse.source_id)
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "selected passage {} uses uncaptured parse {}",
                passage.anchor_unit_id, passage.parse_id
            ),
        })?;
    if !passage
        .unit_ids
        .iter()
        .any(|id| id == passage.anchor_unit_id)
    {
        return Err(ApiError::StorageOperation {
            message: format!(
                "selected passage {} does not contain its representative anchor",
                passage.anchor_unit_id
            ),
        });
    }
    let mut added_unit_ids = Vec::new();
    for unit_id in passage.unit_ids {
        let score = (unit_id == passage.anchor_unit_id).then_some(passage.score);
        if state.selected_set.contains(unit_id) {
            if let Some(existing) = state
                .selected
                .iter_mut()
                .find(|unit| unit.unit_id == *unit_id)
            {
                if existing.parse_id != passage.parse_id {
                    return Err(ApiError::StorageOperation {
                        message: format!(
                            "selected unit {unit_id} belongs to conflicting passage parses"
                        ),
                    });
                }
                // A constituent promoted to a later passage's representative gains
                // that score; a previously ranked representative keeps its score.
                if existing.score.is_none() {
                    existing.score = score;
                }
            }
            continue;
        }
        if state.selected.len() >= policy.budgets.max_evidence_units as usize {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "raw evidence unit safety limit {} exceeded at unit {unit_id} of parse {}",
                    policy.budgets.max_evidence_units, passage.parse_id
                ),
            });
        }
        let content =
            read_unit_content(conn, passage.parse_id, unit_id, options)?.ok_or_else(|| {
                ApiError::StorageOperation {
                    message: format!(
                        "selected evidence unit {unit_id} missing from captured parse {}",
                        passage.parse_id
                    ),
                }
            })?;
        if content.source_id != source_id {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "selected evidence unit {unit_id} of parse {} belongs to source {}, expected {source_id}",
                    passage.parse_id, content.source_id
                ),
            });
        }
        let unit_tokens = match &content.text_projection {
            Some(text) => count_tokens(text)?,
            None => 0,
        };
        // Presentation may bound a long unit's text; raw evidence preserves its
        // complete canonical body. Exceeding this separate ceiling fails the query.
        let total_tokens = state
            .tokens_used
            .checked_add(unit_tokens)
            .filter(|total| *total <= policy.budgets.max_tokens as usize)
            .ok_or_else(|| ApiError::StorageOperation {
                message: format!(
                    "raw evidence token safety limit {} exceeded at unit {unit_id} of parse {}",
                    policy.budgets.max_tokens, passage.parse_id
                ),
            })?;
        state.tokens_used = total_tokens;
        state.selected_set.insert(unit_id.clone());
        state.selected.push(SelectedUnit {
            unit_id: unit_id.clone(),
            parse_id: passage.parse_id.to_string(),
            source_id: content.source_id,
            content_type: content.content_type,
            body: content.body,
            text_projection: content.text_projection,
            locators: content.locators,
            reasons: vec![AssemblyReason::SelectedPassage],
            score,
        });
        added_unit_ids.push(unit_id.clone());
    }
    // Keep an entry for every final passage, even if every member was already
    // retained, so rank-order passage attribution remains visible in the trace.
    state.applied_rules.push(AppliedAssemblyRule {
        rule_id: SELECTED_PASSAGE_RULE_ID.to_string(),
        anchor_unit_id: Some(passage.anchor_unit_id.to_string()),
        anchor_hit_id: Some(passage.anchor_unit_id.to_string()),
        added_unit_ids,
        reason: AssemblyReason::SelectedPassage,
    });
    Ok(())
}

/// A unit's resolved canonical content, read once at selection time.
struct ResolvedContent {
    source_id: String,
    content_type: ContentType,
    body: Value,
    text_projection: Option<String>,
    locators: Option<Vec<Locator>>,
}

/// Read canonical content from the captured snapshot. A missing row is an invalid
/// selected member, which the caller reports as an error. Decode locators only
/// when requested for the raw evidence surface.
fn read_unit_content(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
    options: &EvidenceOptions,
) -> Result<Option<ResolvedContent>, ApiError> {
    let body_limit = conn.limits().resources.max_source_body_bytes;
    let cell_limit = conn.limits().resources.max_json_cell_bytes;
    let row = conn
        .query_row(
            SELECT_EVIDENCE_UNIT_SQL,
            params![parse_id, unit_id, body_limit, cell_limit],
            |row| {
                Ok(EvidenceUnitRow {
                    source_id: row.get::<_, String>(0)?,
                    content_type: row.get::<_, String>(1)?,
                    body_json: row.get::<_, Option<String>>(2)?,
                    locators_json: row.get::<_, Option<String>>(3)?,
                    locators_oversized: row.get::<_, bool>(4)?,
                })
            },
        )
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read evidence unit {unit_id} of parse {parse_id}: {source}"
            ),
        })?;

    let Some(row) = row else { return Ok(None) };
    let body_json = row.body_json.ok_or_else(|| ApiError::StorageOperation {
        message: format!("resource limit: body of evidence unit {unit_id} in parse {parse_id} exceeds resources.max_source_body_bytes {body_limit}"),
    })?;
    // A NULL locator column is valid. The separate SQL flag identifies only an
    // oversized present cell; its bytes never enter Rust and are never treated as absent.
    if row.locators_oversized {
        return Err(ApiError::StorageOperation {
            message: format!(
                "resource limit: locators of evidence unit {unit_id} in parse {parse_id} exceed resources.max_json_cell_bytes {cell_limit}"
            ),
        });
    }

    // Preserve unit identity when reporting a corrupt stored content type.
    let content_type: ContentType = serde_json::from_value(Value::String(row.content_type.clone()))
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted content unit {unit_id} type {:?} is not a known variant: {source}",
                row.content_type
            ),
        })?;
    let body: Value =
        serde_json::from_str(&body_json).map_err(|source| ApiError::StorageOperation {
            message: format!("persisted body of content unit {unit_id} is unparseable: {source}"),
        })?;

    // First reader of `locators_json`: a canonical `Vec<model::Locator>`, NULL →
    // `None`. Parsed only when the request set `includeSourceLocators`.
    let locators = if options.include_source_locators {
        parse_locators(&row.locators_json, unit_id)?
    } else {
        None
    };

    let text_projection = evidence_text(content_type, &body);

    Ok(Some(ResolvedContent {
        source_id: row.source_id,
        content_type,
        body,
        text_projection,
        locators,
    }))
}

/// Render a fully-resolved selected unit into its `EvidenceUnit`. Pure move/clone
/// of already-resolved fields (no I/O): `includeSourceLocators` gates whether the
/// resolved locators are surfaced; the score/reasons come from selection state.
fn render_evidence_unit(sel: &SelectedUnit, options: &EvidenceOptions) -> EvidenceUnit {
    EvidenceUnit {
        unit_id: sel.unit_id.clone(),
        source_id: sel.source_id.clone(),
        parse_id: sel.parse_id.clone(),
        content_type: sel.content_type,
        body: sel.body.clone(),
        text_projection: sel.text_projection.clone(),
        // Locators were resolved only when the option is set; gate again so a
        // later option change cannot leak a stale resolution.
        locators: if options.include_source_locators {
            sel.locators.clone()
        } else {
            None
        },
        score: sel.score,
        reasons: reasons_to_strings(&sel.reasons),
    }
}

/// One evidence-unit content row read parse-scoped from `content_units`.
struct EvidenceUnitRow {
    source_id: String,
    content_type: String,
    body_json: Option<String>,
    locators_json: Option<String>,
    locators_oversized: bool,
}

/// Ordered SELECT of one unit's evidence fields, parse-scoped (§14) and keyed on
/// the unit id. `locators_json` is nullable (a unit may carry no locators).
/// Byte guards run before text copying; the explicit locator flag preserves NULL semantics.
const SELECT_EVIDENCE_UNIT_SQL: &str = "
SELECT source_id, content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?3 THEN body_json END,
       CASE WHEN length(CAST(locators_json AS BLOB)) <= ?4 THEN locators_json END,
       COALESCE(length(CAST(locators_json AS BLOB)) > ?4, 0)
FROM content_units
WHERE parse_id = ?1 AND id = ?2";

/// Deserialize `content_units.locators_json` into `Option<Vec<Locator>>`. A NULL
/// column (no locators) yields `None`; a present-but-unparseable blob fails
/// loudly with the unit identity rather than being dropped silently.
fn parse_locators(
    locators_json: &Option<String>,
    unit_id: &str,
) -> Result<Option<Vec<Locator>>, ApiError> {
    match locators_json {
        None => Ok(None),
        Some(json) => {
            let locators: Vec<Locator> =
                serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "persisted locators of content unit {unit_id} are unparseable: {source}"
                    ),
                })?;
            Ok(Some(locators))
        }
    }
}

/// Collect the fresh annotations of the captured sources whose `target_unit_ids`
/// intersect the pack's selected units. Reads via
/// `crate::annotations::store::fresh_for_active_parse` (active-parse-scoped by
/// that function's own subselect, §21 rule 1), deduped by annotation id across
/// sources, in first-seen order (R13). Empty when nothing intersects.
fn collect_annotations(
    conn: &Connection,
    captured: &[CapturedParseRef<'_>],
    selected_unit_ids: &[String],
) -> Result<Vec<SemanticAnnotation>, ApiError> {
    let selected: BTreeSet<&str> = selected_unit_ids.iter().map(String::as_str).collect();
    let mut seen_sources: BTreeSet<&str> = BTreeSet::new();
    let mut seen_annotations: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();

    for parse in captured {
        // One read per distinct source: `fresh_for_active_parse` keys on source
        // and resolves the active parse internally, so two captured parses of one
        // source (impossible under the active-parse invariant, but cheap to guard)
        // would otherwise double-read.
        if !seen_sources.insert(parse.source_id) {
            continue;
        }
        let annotations = crate::annotations::store::fresh_for_active_parse(conn, parse.source_id)?;
        for annotation in annotations {
            let intersects = annotation
                .target_unit_ids
                .iter()
                .any(|target| selected.contains(target.as_str()));
            if intersects && seen_annotations.insert(annotation.id.clone()) {
                out.push(annotation);
            }
        }
    }
    Ok(out)
}

/// Render the inclusion reasons of a selected unit into the human-readable
/// `reasons` strings (§26 `EvidenceUnit.reasons`), using each reason's wire name.
/// `None` when the unit accrued no reasons (never happens for a selected unit,
/// which always carries at least its adding rule's reason).
fn reasons_to_strings(reasons: &[AssemblyReason]) -> Option<Vec<String>> {
    if reasons.is_empty() {
        None
    } else {
        Some(
            reasons
                .iter()
                .map(|r| reason_wire_name(r).to_string())
                .collect(),
        )
    }
}

/// Wire name of an assembly reason, matching the model enum's
/// `rename_all = "snake_case"`. Exhaustive so a new reason is a compile error.
fn reason_wire_name(reason: &AssemblyReason) -> &'static str {
    match reason {
        AssemblyReason::SelectedPassage => "selected_passage",
        AssemblyReason::Anchor => "anchor",
        AssemblyReason::RequiredCompletion => "required_completion",
        AssemblyReason::StructuralContext => "structural_context",
        AssemblyReason::ExplicitReference => "explicit_reference",
        AssemblyReason::LocalContinuity => "local_continuity",
    }
}

/// Extract the evidence-bearing text a unit contributes, returning `None` for
/// container/structural types that carry no direct text.
///
/// Shared by passage construction so displayed content and retained evidence
/// use the same canonical text fields. This is one of three synchronized
/// readers of the SPEC-epub §2.1 evidence-bearing types (`text` for
/// text_block, caption, table_cell; `code` for code_block; nothing else, no
/// normalized-text fallback): `projections::chunk`'s member derivation (the
/// fine grain every higher grain inherits) and `projections::view`'s
/// `render_document` must select the same fields when content types evolve.
/// The importer's `text_projection_hash` follows the same rule (§2.2).
pub(crate) fn evidence_text(content_type: ContentType, body: &Value) -> Option<String> {
    match content_type {
        ContentType::TextBlock | ContentType::Caption | ContentType::TableCell => body
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::CodeBlock => body
            .get("code")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::Document
        | ContentType::Page
        | ContentType::TextSection
        | ContentType::List
        | ContentType::ListItem
        | ContentType::Aside
        | ContentType::Table
        | ContentType::TableRow
        | ContentType::Figure => None,
    }
}
