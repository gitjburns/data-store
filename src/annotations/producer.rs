//! CAb: the shared producer contract — input purity, invocation planning,
//! §20 provenance assembly, and the per-kind dispatch shared by the entity,
//! relation, and summary producers.
//!
//! INPUT PURITY (binding cluster contract). A producer's prompt content is
//! EXACTLY the ordered text of its target units — nothing else. No corpus
//! context, no neighboring units, no source or parse metadata, no headings
//! synthesized from structure. This purity is what makes every producer
//! memoization-eligible (spec §21.3): the invocation's ordered input unit set
//! fully determines the model input, so identical input sets reuse identical
//! results. `invoke` therefore assembles the user content from the target
//! texts alone.
//!
//! Identity chain (ruling 4). For one invocation, the ordered input unit set
//! IS the resulting annotations' `targetUnitIds` IS the basis stage 3 hashes
//! into the §21.2 memo key. This module exposes that set (`Invocation.targets`)
//! and the producer identity (`identity_hash`); stage 3 computes the key.

use rusqlite::Connection;
use serde_json::json;
use tracing::warn;

use crate::annotations::{entity, llm_client::AnnotatorClient, relation, summary};
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::model::{
    self, ContentType, ContentUnit, ProducerType, ProvenanceInputRef, ProvenanceObjectType,
    SemanticAnnotationType,
};

/// Ordered SELECT of a parse's content units in reading order. Producer input
/// purity depends on this exact ordering: the sequence the units come back in
/// is the sequence their text is joined in, and thus the sequence recorded as
/// `targetUnitIds`. `sequence_index` is nullable in the schema; NULLs sort
/// last under SQLite ordering, and a tiebreak on `id` keeps the order total
/// and deterministic across runs. No deleted-row filter: content_units rows
/// are hard-deleted by hot cleanup (§31.2), never soft-deleted — the table
/// has no deleted_at column.
const SELECT_PARSE_UNITS_SQL: &str = "
SELECT id, content_type, primary_parent_id, body_json
FROM content_units
WHERE parse_id = ?1
ORDER BY sequence_index IS NULL, sequence_index, id";

/// One target unit contributing its text to a producer invocation. `unit_id`
/// becomes a `targetUnitIds` entry; `text` is the pure input text (ruling 2).
#[derive(Debug, Clone)]
pub(crate) struct InvocationTarget {
    pub(crate) unit_id: String,
    pub(crate) text: String,
}

/// What a single invocation covers. `split_index` distinguishes the
/// deterministic pieces of an oversized group/document (0 when the unit set
/// fit under `max_input_chars` and was not split).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InvocationKind {
    /// Entity/relation scope: the evidence-bearing descendants of one
    /// `text_section` (or a leading synthetic group; see `build_invocation_plan`).
    SectionGroup {
        section_unit_id: String,
        split_index: u32,
    },
    /// Summary scope: the whole document's evidence-bearing units in order.
    Document { split_index: u32 },
}

/// One planned producer call. `targets` is ordered; its unit ids are exactly
/// the resulting annotations' `targetUnitIds` and the stage-3 memo-key basis
/// (ruling 4). The producer sends only the joined target texts (input purity).
#[derive(Debug, Clone)]
pub(crate) struct Invocation {
    pub(crate) kind: InvocationKind,
    pub(crate) targets: Vec<InvocationTarget>,
}

/// One annotation produced from an invocation, before the stage-3 worker wraps
/// it with a target-unit-id list, provenance, and freshness. `body` is the
/// per-type camelCase JSON body; `confidence` is the optional model-reported
/// confidence (already range-validated by the producer parser).
#[derive(Debug, Clone)]
pub(crate) struct ProducedAnnotation {
    pub(crate) body: serde_json::Value,
    pub(crate) confidence: Option<f64>,
}

/// The three MVP producers. Each maps to one annotation type, a stable
/// producer name/version, its prompt, and its strict output parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProducerKind {
    Entity,
    Relation,
    Summary,
}

impl ProducerKind {
    /// The annotation type this producer emits.
    pub(crate) fn annotation_type(self) -> SemanticAnnotationType {
        match self {
            Self::Entity => SemanticAnnotationType::Entity,
            Self::Relation => SemanticAnnotationType::Relation,
            Self::Summary => SemanticAnnotationType::Summary,
        }
    }

    /// Stable producer name recorded in provenance and hashed into the
    /// producer identity. Changing it is a producer-identity change.
    pub(crate) fn producer_name(self) -> &'static str {
        match self {
            Self::Entity => "annotator_entity",
            Self::Relation => "annotator_relation",
            Self::Summary => "annotator_summary",
        }
    }

    /// Producer version. Bumped when the producer's output contract or prompt
    /// semantics change in a way that must invalidate prior memo reuse.
    pub(crate) fn producer_version(self) -> &'static str {
        "1"
    }

    /// This producer's system prompt (a named external-language constant per
    /// PRINCIPLES; the text lives in the per-producer module).
    pub(crate) fn prompt(self) -> &'static str {
        match self {
            Self::Entity => entity::SYSTEM_PROMPT,
            Self::Relation => relation::SYSTEM_PROMPT,
            Self::Summary => summary::SYSTEM_PROMPT,
        }
    }

    /// Canonical §16.2 hash over the producer's identity:
    /// {producerName, producerVersion, modelName, promptHash, endpoint}.
    ///
    /// This is the producer-configuration identity that feeds both the §21.2
    /// memo key and the provenance `configHash`. Changing the model, the
    /// endpoint, the prompt, or the input cap changes this hash, which
    /// invalidates memo reuse — old annotations are no longer considered
    /// equivalent to what this producer would now emit.
    ///
    /// `maxInputChars` is part of the identity because the input cap changes
    /// what a producer's prompt can contain: it drives the deterministic
    /// split/truncation of oversized section groups and documents, so the same
    /// target units under a different cap produce a different model input.
    /// It is therefore producer configuration, and a change to it must
    /// invalidate memo reuse just like a prompt or model change.
    pub(crate) fn identity_hash(self, config: &AnnotatorModelConfig) -> Result<String, ApiError> {
        let prompt_hash = self.prompt_hash()?;
        let identity = json!({
            "producerName": self.producer_name(),
            "producerVersion": self.producer_version(),
            "modelName": config.model,
            "promptHash": prompt_hash,
            "endpoint": config.endpoint,
            "maxInputChars": config.max_input_chars,
        });
        crate::canonical::canonical_sha256_hex(&identity)
    }

    /// Canonical content hash of the prompt text, recorded as provenance
    /// `promptHash` and folded into the producer identity.
    fn prompt_hash(self) -> Result<String, ApiError> {
        crate::canonical::canonical_sha256_hex(&serde_json::Value::String(
            self.prompt().to_string(),
        ))
    }

    /// Parse this producer's raw model output into zero or more produced
    /// annotations. An empty result is valid (e.g. no entities found); a
    /// malformed response is an error carrying bounded diagnostics.
    fn parse_output(self, raw: &str) -> Result<Vec<ProducedAnnotation>, ApiError> {
        match self {
            Self::Entity => entity::parse_output(raw),
            Self::Relation => relation::parse_output(raw),
            Self::Summary => summary::parse_output(raw),
        }
    }

    /// A compact purpose label for the client's request logs.
    fn request_purpose(self) -> &'static str {
        self.producer_name()
    }
}

/// Read `parse_id`'s content units and group them into the invocation plan
/// used by ALL three producers (entity/relation consume the section groups,
/// summary consumes the single document composite; `invoke` selects which).
///
/// Section grouping. Each `text_section` unit owns the evidence-bearing units
/// contained beneath it, where containment is the `primary_parent_id` chain
/// (transitive: a unit belongs to the nearest ancestor section). Units that
/// precede the first section — or every evidence-bearing unit in a sectionless
/// document — form a single leading synthetic group keyed by the FIRST such
/// unit's id, so those units are still covered without inventing a fake
/// section unit.
///
/// Evidence-bearing text extraction (empty-text units are skipped, so an
/// empty group produces no invocation):
///   - text_block  -> body.text
///   - table_cell  -> body.text, else body.normalizedText
///   - caption     -> body.text
///   - code_block  -> body.code
///
/// Splitting. A group whose joined text would exceed `max_input_chars` is
/// split deterministically into consecutive runs of WHOLE units, each run
/// under the cap, numbered by ascending `split_index`. A single unit larger
/// than the cap cannot be split across units, so its text is explicitly
/// truncated at the cap (with a warn log) rather than silently dropped or sent
/// oversized — truncation is explicit per PRINCIPLES. The Document invocation
/// covers all evidence-bearing units in sequence order and splits the same way.
pub(crate) fn build_invocation_plan(
    conn: &Connection,
    parse_id: &str,
    max_input_chars: usize,
) -> Result<Vec<Invocation>, ApiError> {
    let units = read_parse_units(conn, parse_id)?;

    // Resolve each evidence-bearing unit to its owning section id via the
    // primary_parent_id chain, in one pass over the ordered units. `None`
    // means the unit precedes any section (leading synthetic group).
    let section_ids: std::collections::HashSet<&str> = units
        .iter()
        .filter(|unit| unit.content_type == ContentType::TextSection)
        .map(|unit| unit.id.as_str())
        .collect();
    let parent_of: std::collections::HashMap<&str, &str> = units
        .iter()
        .filter_map(|unit| {
            unit.primary_parent_id
                .as_deref()
                .map(|parent| (unit.id.as_str(), parent))
        })
        .collect();

    // Section groups keyed by section id, in first-seen order; plus the
    // leading synthetic group for pre-section / sectionless units.
    let mut section_order: Vec<String> = Vec::new();
    let mut section_targets: std::collections::HashMap<String, Vec<InvocationTarget>> =
        std::collections::HashMap::new();
    let mut leading: Vec<InvocationTarget> = Vec::new();
    // Document composite: every evidence-bearing target in reading order.
    let mut document: Vec<InvocationTarget> = Vec::new();

    for unit in &units {
        let Some(text) = evidence_text(unit) else {
            continue;
        };
        if text.trim().is_empty() {
            // Empty-text units carry no evidence; skipping them keeps input
            // pure and avoids blank invocation inputs.
            continue;
        }
        let target = InvocationTarget {
            unit_id: unit.id.clone(),
            text,
        };
        document.push(target.clone());

        match owning_section(unit.id.as_str(), &parent_of, &section_ids) {
            Some(section_id) => {
                let key = section_id.to_string();
                section_targets
                    .entry(key.clone())
                    .or_insert_with(|| {
                        section_order.push(key.clone());
                        Vec::new()
                    })
                    .push(target);
            }
            None => leading.push(target),
        }
    }

    let mut invocations = Vec::new();

    // The leading synthetic group is keyed by its first unit's id (there is no
    // section unit to name it), and is emitted before the section groups so
    // the plan follows reading order.
    if let Some(first) = leading.first() {
        let group_key = first.unit_id.clone();
        push_section_splits(&mut invocations, &group_key, leading, max_input_chars);
    }
    for section_id in section_order {
        let targets = section_targets.remove(&section_id).unwrap_or_default();
        push_section_splits(&mut invocations, &section_id, targets, max_input_chars);
    }

    // The single Document composite over all evidence-bearing units, split the
    // same way. Emitted after section groups; `invoke` routes summary here.
    push_document_splits(&mut invocations, document, max_input_chars);

    Ok(invocations)
}

/// Assemble §20 provenance for a planned (about-to-run) invocation of `kind`.
/// `producerType` is Model; `modelName` and `endpoint`-derived `configHash`
/// come from config; `promptHash` is the prompt's content hash; `inputRefs`
/// are ContentUnit references for the ordered targets. The memoization fields
/// and confidence are left None here — they are filled by the stage-3 worker
/// on completion (confidence) and on memo reuse (memoized/memoizedFrom).
pub(crate) fn planned_provenance(
    kind: ProducerKind,
    config: &AnnotatorModelConfig,
    targets: &[InvocationTarget],
) -> Result<model::Provenance, ApiError> {
    let input_refs = targets
        .iter()
        .map(|target| ProvenanceInputRef {
            object_type: ProvenanceObjectType::ContentUnit,
            id: target.unit_id.clone(),
        })
        .collect::<Vec<_>>();

    Ok(model::Provenance {
        producer_type: ProducerType::Model,
        producer_name: kind.producer_name().to_string(),
        producer_version: Some(kind.producer_version().to_string()),
        config_hash: Some(kind.identity_hash(config)?),
        model_name: Some(config.model.clone()),
        model_version: None,
        prompt_hash: Some(kind.prompt_hash()?),
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(input_refs),
    })
}

/// Run one producer against one invocation: assemble the pure user content
/// (the ordered target texts joined by blank lines — INPUT PURITY, nothing
/// else), call the shared client, and dispatch strict parsing to the
/// producer's module.
///
/// Kind/invocation compatibility: Entity and Relation consume `SectionGroup`
/// invocations; Summary consumes `Document` invocations. A mismatch is a
/// programming error in the stage-3 routing, surfaced loudly rather than
/// silently producing wrong-scoped annotations.
pub(crate) fn invoke(
    kind: ProducerKind,
    client: &AnnotatorClient,
    invocation: &Invocation,
) -> Result<Vec<ProducedAnnotation>, ApiError> {
    match (kind, &invocation.kind) {
        (ProducerKind::Entity | ProducerKind::Relation, InvocationKind::SectionGroup { .. }) => {}
        (ProducerKind::Summary, InvocationKind::Document { .. }) => {}
        (kind, other) => {
            return Err(ApiError::AnnotationProducer {
                message: format!(
                    "producer {} cannot consume invocation kind {other:?}",
                    kind.producer_name()
                ),
            });
        }
    }

    // Input purity: the user content is exactly the ordered target texts,
    // joined by a blank line, with no other tokens. Blank-line joining keeps
    // unit boundaries legible to the model without adding structural metadata.
    let user_content = invocation
        .targets
        .iter()
        .map(|target| target.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");

    let raw = client.complete(kind.request_purpose(), kind.prompt(), &user_content)?;
    kind.parse_output(&raw)
}

/// Choose which invocations a producer consumes. Exposed so the stage-3 worker
/// filters the shared plan without re-encoding the routing rule: Entity and
/// Relation take section groups, Summary takes the document composite.
pub(crate) fn invocation_matches_kind(kind: ProducerKind, invocation: &Invocation) -> bool {
    matches!(
        (kind, &invocation.kind),
        (
            ProducerKind::Entity | ProducerKind::Relation,
            InvocationKind::SectionGroup { .. }
        ) | (ProducerKind::Summary, InvocationKind::Document { .. })
    )
}

/// Resolve a unit's owning section by walking the `primary_parent_id` chain up
/// to the nearest ancestor that is a `text_section`. Returns `None` when no
/// ancestor is a section (the unit precedes any section, or the document has
/// none), which routes the unit into the leading synthetic group. The walk is
/// bounded by the parent-map size, so a cyclic/self-referential chain
/// terminates instead of looping.
fn owning_section<'units>(
    unit_id: &'units str,
    parent_of: &std::collections::HashMap<&'units str, &'units str>,
    section_ids: &std::collections::HashSet<&'units str>,
) -> Option<&'units str> {
    let mut current = unit_id;
    let mut steps = 0usize;
    let max_steps = parent_of.len();
    while let Some(&parent) = parent_of.get(current) {
        if section_ids.contains(parent) {
            return Some(parent);
        }
        current = parent;
        steps += 1;
        if steps > max_steps {
            // Parent chain is cyclic or malformed; stop rather than loop. The
            // unit falls into the leading synthetic group, which is a visible,
            // covered outcome, not a silent drop.
            return None;
        }
    }
    None
}

/// Extract the evidence-bearing text for the content types the producers read,
/// returning `None` for every other type (pages, sections, tables, rows,
/// figures, image regions carry no direct producer text). The extracted text
/// is the pure producer input (ruling 2); no normalization or metadata is
/// mixed in beyond selecting the body's text-bearing field.
///
/// MUST STAY IN STEP (four sites): this is one of four arm-for-arm mirrors of the
/// per-`ContentType` evidence-text extraction. The others are
/// `crate::query::rerank::evidence_text` (`src/query/rerank.rs`),
/// `crate::projections::multivector::evidence_text` (`src/projections/multivector.rs`),
/// and `crate::assembly::evidence::evidence_text` (`src/assembly/evidence.rs`).
/// All four select the same field per type — including the `TableCell` fallback
/// to `normalizedText` — so a content type gaining or losing a text-bearing field
/// must change ALL FOUR together. This copy spells `TextBlock` and `Caption` as
/// two arms while the others group them, but both resolve to the body `text`
/// field identically: that is a cosmetic difference, not a divergence in the text
/// resolved.
fn evidence_text(unit: &ContentUnit) -> Option<String> {
    match unit.content_type {
        ContentType::TextBlock => unit
            .body
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::Caption => unit
            .body
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::TableCell => unit
            .body
            .get("text")
            .and_then(|value| value.as_str())
            .or_else(|| {
                unit.body
                    .get("normalizedText")
                    .and_then(|value| value.as_str())
            })
            .map(str::to_string),
        ContentType::CodeBlock => unit
            .body
            .get("code")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::Page
        | ContentType::TextSection
        | ContentType::Table
        | ContentType::TableRow
        | ContentType::Figure
        | ContentType::ImageRegion => None,
    }
}

/// Split one section group's ordered targets into `SectionGroup` invocations,
/// each under `max_input_chars`, and push them onto the plan.
fn push_section_splits(
    invocations: &mut Vec<Invocation>,
    section_unit_id: &str,
    targets: Vec<InvocationTarget>,
    max_input_chars: usize,
) {
    for (split_index, run) in split_targets(targets, max_input_chars, section_unit_id)
        .into_iter()
        .enumerate()
    {
        invocations.push(Invocation {
            kind: InvocationKind::SectionGroup {
                section_unit_id: section_unit_id.to_string(),
                split_index: split_index as u32,
            },
            targets: run,
        });
    }
}

/// Split the document composite into `Document` invocations, each under
/// `max_input_chars`, and push them onto the plan.
fn push_document_splits(
    invocations: &mut Vec<Invocation>,
    targets: Vec<InvocationTarget>,
    max_input_chars: usize,
) {
    for (split_index, run) in split_targets(targets, max_input_chars, "document")
        .into_iter()
        .enumerate()
    {
        invocations.push(Invocation {
            kind: InvocationKind::Document {
                split_index: split_index as u32,
            },
            targets: run,
        });
    }
}

/// Split ordered targets deterministically into consecutive runs of whole
/// units whose joined length stays under `max_input_chars`.
///
/// Joined length accounts for the `\n\n` separators `invoke` inserts, so the
/// budget matches what is actually sent. A single unit larger than the cap
/// cannot share a run with any other unit and cannot be split across units, so
/// its text is truncated at the cap (explicit, warn-logged) and it forms its
/// own run — never dropped and never sent oversized. `group_label` is a log
/// facet only. Returns an empty vec for empty input (no invocation planned).
fn split_targets(
    targets: Vec<InvocationTarget>,
    max_input_chars: usize,
    group_label: &str,
) -> Vec<Vec<InvocationTarget>> {
    let mut runs: Vec<Vec<InvocationTarget>> = Vec::new();
    let mut current: Vec<InvocationTarget> = Vec::new();
    let mut current_chars = 0usize;
    let separator_chars = 2; // "\n\n" between consecutive targets in a run.

    for mut target in targets {
        let mut unit_chars = target.text.chars().count();

        // A single oversized unit is explicitly truncated at the cap; it can
        // never fit otherwise, and silent oversized sends or drops are both
        // forbidden (PRINCIPLES: truncation must be explicit).
        if unit_chars > max_input_chars {
            warn!(
                event = "annotator_plan.unit_truncated",
                group = group_label,
                unit_id = %target.unit_id,
                unit_chars,
                max_input_chars,
                "annotation producer input unit exceeds max_input_chars; truncating at cap"
            );
            target.text = target.text.chars().take(max_input_chars).collect();
            unit_chars = target.text.chars().count();
        }

        let added = if current.is_empty() {
            unit_chars
        } else {
            current_chars + separator_chars + unit_chars
        };
        if !current.is_empty() && added > max_input_chars {
            // Current run is full; start a fresh run with this unit.
            runs.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current_chars = if current.is_empty() {
            unit_chars
        } else {
            current_chars + separator_chars + unit_chars
        };
        current.push(target);
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

/// Read one parse's content units in reading order (bounded by the parse's
/// unit count). SQL failures surface as `StorageOperation` with the parse id,
/// mirroring the annotation store's read discipline.
fn read_parse_units(conn: &Connection, parse_id: &str) -> Result<Vec<ContentUnit>, ApiError> {
    let mut statement =
        conn.prepare(SELECT_PARSE_UNITS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare parse-units query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(rusqlite::params![parse_id], |row| {
            Ok(PlanUnitRow {
                id: row.get(0)?,
                content_type: row.get(1)?,
                primary_parent_id: row.get(2)?,
                body_json: row.get(3)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query units for parse {parse_id}: {source}"),
        })?;

    let mut units = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read unit row for parse {parse_id}: {source}"),
        })?;
        units.push(plan_unit(row, parse_id)?);
    }
    Ok(units)
}

/// The columns of `content_units` this planner reads. Only reading-order,
/// containment, type, and body are needed; the full `ContentUnit` envelope is
/// reconstructed with placeholders for the fields planning never inspects.
struct PlanUnitRow {
    id: String,
    content_type: String,
    primary_parent_id: Option<String>,
    body_json: String,
}

/// Re-type one persisted unit row into the subset of `ContentUnit` this
/// planner needs. `content_type` re-types through the model enum (a value
/// outside the schema CHECK set fails loudly), and `body_json` parses back to
/// JSON so `evidence_text` can select its text field. Fields the planner never
/// reads are filled with inert placeholders; this row view is internal to
/// planning and never persisted or re-serialized.
fn plan_unit(row: PlanUnitRow, parse_id: &str) -> Result<ContentUnit, ApiError> {
    let content_type: ContentType = serde_json::from_value(serde_json::Value::String(
        row.content_type.clone(),
    ))
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "persisted content unit {} type {:?} is not a known variant: {source}",
            row.id, row.content_type
        ),
    })?;
    let body: serde_json::Value =
        serde_json::from_str(&row.body_json).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted body of content unit {} is unparseable: {source}",
                row.id
            ),
        })?;

    Ok(ContentUnit {
        id: row.id,
        source_id: String::new(),
        parse_id: parse_id.to_string(),
        content_type,
        body_hash: String::new(),
        text_hash: None,
        structure_hash: None,
        primary_parent_id: row.primary_parent_id,
        sequence_index: None,
        locators: None,
        body,
        created_at: String::new(),
        deleted_at: None,
    })
}

// --- Shared strict output-parsing helpers ---------------------------------
//
// The three producers all: tolerate a surrounding Markdown code fence, then
// strictly deserialize a bare JSON object, then validate non-empty strings and
// optional confidence bounds. These helpers live here (rather than in a new
// module) because the CAb scope is fixed to these five files; they are
// `pub(crate)` so entity/relation/summary share one implementation instead of
// duplicating the rule three times.

/// Bounded diagnostic length for a malformed model response embedded in an
/// error. The full output is never included (DIAGNOSTICS forbidden-data rules
/// treat full model output as off-limits); this cap bounds even the excerpt.
const MALFORMED_EXCERPT_CHARS: usize = 500;

/// Strip a single optional surrounding Markdown code fence from a model
/// response and trim surrounding whitespace.
///
/// Deliberate, commented lenience: models frequently wrap bare-JSON responses
/// in ```` ```json ... ``` ```` despite the prompt forbidding it. Tolerating
/// exactly one wrapping fence keeps otherwise-valid responses usable without
/// weakening the strict JSON parse that follows. Anything other than a clean
/// single fence is left untouched, so the strict parse still rejects genuinely
/// malformed output.
pub(crate) fn strip_optional_code_fence(raw: &str) -> &str {
    let trimmed = raw.trim();
    let Some(without_open) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    // Drop an optional language tag on the opening fence line (e.g. ```json).
    let after_first_line = match without_open.find('\n') {
        Some(newline) => &without_open[newline + 1..],
        // A fence with no newline is not a well-formed block; leave the trimmed
        // text for the strict parser to reject.
        None => return trimmed,
    };
    match after_first_line.rfind("```") {
        Some(close) => after_first_line[..close].trim(),
        None => trimmed,
    }
}

/// Strictly deserialize a producer response, mapping a parse failure to
/// `ApiError::AnnotationProducer` with a bounded excerpt of the ORIGINAL raw
/// output (never the full text). `producer` labels which producer failed.
pub(crate) fn strict_from_str<T: serde::de::DeserializeOwned>(
    json_text: &str,
    producer: &str,
    raw: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(json_text).map_err(|source| ApiError::AnnotationProducer {
        message: format!(
            "{producer} producer returned malformed output: {source}; output_excerpt={}",
            malformed_excerpt(raw)
        ),
    })
}

/// Reject an empty (or whitespace-only) required string field. Empty required
/// strings are a malformed result, not a valid annotation.
pub(crate) fn validate_non_empty(value: &str, field: &str, raw: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::AnnotationProducer {
            message: format!(
                "annotation producer returned empty {field}; output_excerpt={}",
                malformed_excerpt(raw)
            ),
        });
    }
    Ok(())
}

/// Validate an optional confidence lies within [0,1]. An out-of-range value is
/// an error, never clamped: clamping would silently fabricate a calibrated
/// value the model did not report (accuracy principle). A non-finite value is
/// equally rejected.
pub(crate) fn validate_confidence(
    confidence: Option<f64>,
    field: &str,
    raw: &str,
) -> Result<(), ApiError> {
    let Some(value) = confidence else {
        return Ok(());
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(ApiError::AnnotationProducer {
            message: format!(
                "annotation producer returned {field} {value} outside [0,1]; output_excerpt={}",
                malformed_excerpt(raw)
            ),
        });
    }
    Ok(())
}

/// Bounded, escaped single-line excerpt of a malformed model response for
/// diagnostics. Never the full output; control characters are escaped so the
/// excerpt cannot corrupt a log line.
fn malformed_excerpt(raw: &str) -> String {
    let escaped = raw
        .trim()
        .chars()
        .flat_map(|character| character.escape_default())
        .take(MALFORMED_EXCERPT_CHARS)
        .collect::<String>();
    if raw.trim().chars().count() > MALFORMED_EXCERPT_CHARS {
        return format!("{escaped}...");
    }
    escaped
}
