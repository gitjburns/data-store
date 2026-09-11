//! Bounded annotation excerpts, single-goal producer chains, and source lineage.
//!
//! Each invocation consumes one exact source fragment. Later requests may use
//! that fragment and earlier outputs from the same chain; they never borrow
//! unrelated corpus context. The final output set commits atomically through
//! the existing worker. Interrupted chains restart as a whole.
//!
//! Source-unit hashes, fragment offsets, and exact text hashes identify coverage;
//! the ordered stage contracts additionally identify reusable producer output.

use rusqlite::Connection;
use serde_json::json;
use tracing::debug;

use crate::annotations::{
    chains, excerpt,
    llm_client::{AnnotatorClient, ENABLE_THINKING, MAX_COMPLETION_TOKENS},
    stages::Stage,
};
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::maintenance::AnnotationCancelReason;
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

/// Exact input slice of an immutable canonical unit. Offsets are Unicode scalar
/// positions, end-exclusive; they distinguish fragments even when text repeats.
#[derive(Debug, Clone)]
pub(crate) struct InvocationTarget {
    pub(crate) unit_id: String,
    pub(crate) text: String,
    pub(crate) start_char: usize,
    pub(crate) end_char: usize,
}

impl InvocationTarget {
    /// Use the same exact slice descriptor for memo identity and recorded lineage.
    pub(crate) fn text_range(&self) -> model::provenance::ProvenanceTextRange {
        model::provenance::ProvenanceTextRange {
            start_char: self.start_char,
            end_char: self.end_char,
            text_hash: crate::canonical::sha256_hex_bytes(self.text.as_bytes()),
        }
    }
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
    /// Summary scope: one excerpt in the document's reading order.
    Document { split_index: u32 },
}

/// One atomic producer chain. The planner supplies exactly one source fragment;
/// all outputs retain its unit reference and precise range in their provenance.
#[derive(Debug, Clone)]
pub(crate) struct Invocation {
    pub(crate) kind: InvocationKind,
    pub(crate) targets: Vec<InvocationTarget>,
}

impl Invocation {
    /// Correlate an invocation's preparation, HTTP call, and persistence without
    /// logging its text. The inherited source context belongs to the caller.
    pub(crate) fn log_context(&self) -> crate::util::LogContext {
        let context = crate::util::LogContext::new(
            "annotation_invocation",
            &crate::util::diagnostic_id("invocation"),
        );
        context.record("target_units", self.targets.len() as u64);
        if let Some(target) = self.targets.first() {
            context.record("unit_id", target.unit_id.as_str());
            context.record("excerpt_start_char", target.start_char as u64);
            context.record("excerpt_end_char", target.end_char as u64);
            context.record("excerpt_chars", target.text.chars().count() as u64);
        }
        match &self.kind {
            InvocationKind::SectionGroup {
                section_unit_id,
                split_index,
            } => {
                context.record("section_id", section_unit_id.as_str());
                context.record("split_index", *split_index);
            }
            InvocationKind::Document { split_index } => {
                // Document summaries have no section ID; do not invent one.
                context.record("split_index", *split_index);
            }
        }
        context
    }
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

/// Preserve the failed boundary for independent execution/output retry budgets.
/// Only malformed output changes sampling; cancellation consumes neither budget.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvocationFailure {
    /// Operator cancellation is neither rejected model output nor a call fault.
    #[error("annotation invocation cancelled: {0:?}")]
    Cancelled(AnnotationCancelReason),
    /// The endpoint did not return a usable completion envelope.
    #[error(transparent)]
    Call(ApiError),
    /// A received completion failed the producer's annotation contract.
    #[error(transparent)]
    InvalidOutput(ApiError),
    /// Local invocation routing violated the producer contract.
    #[error(transparent)]
    Internal(ApiError),
}

impl InvocationFailure {
    /// Supply stable diagnostic labels without parsing provider error text.
    pub(crate) fn class(&self) -> &'static str {
        match self {
            Self::Cancelled(_) => "cancelled",
            Self::Call(_) => "call_failure",
            Self::InvalidOutput(_) => "invalid_output",
            Self::Internal(_) => "internal_error",
        }
    }

    /// Preserve actual failures for persistence; cancellation must never mint a failed row.
    pub(crate) fn error(&self) -> Option<&ApiError> {
        match self {
            Self::Cancelled(_) => None,
            Self::Call(error) | Self::InvalidOutput(error) | Self::Internal(error) => Some(error),
        }
    }
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

    /// Version 2 identifies excerpt-scoped, single-goal chains and their receipts.
    pub(crate) fn producer_version(self) -> &'static str {
        "2"
    }

    /// Ordered single-goal contracts contributing to this producer's identity.
    pub(crate) fn stages(self) -> &'static [Stage] {
        match self {
            Self::Entity => &[Stage::EntityNames, Stage::EntityTypes],
            Self::Relation => &[Stage::Statements, Stage::Relations, Stage::Evidence],
            Self::Summary => &[Stage::Summary],
        }
    }

    /// Hash every generation-affecting contract, including schemas and output limits.
    /// Naming policy is intentionally not composed into these single-goal prompts.
    pub(crate) fn identity_hash(self, config: &AnnotatorModelConfig) -> Result<String, ApiError> {
        let schemas = self
            .stages()
            .iter()
            .map(|stage| stage.schema())
            .collect::<Result<Vec<_>, _>>()?;
        crate::canonical::canonical_sha256_hex(&json!({
            "producerName": self.producer_name(),
            "producerVersion": self.producer_version(),
            "modelName": config.model,
            "promptHash": self.prompt_hash()?,
            "outputSchemas": schemas,
            "endpoint": config.endpoint,
            "maxInputChars": config.max_input_chars,
            "maxCompletionTokens": MAX_COMPLETION_TOKENS,
            "enableThinking": ENABLE_THINKING,
        }))
    }

    /// Hash the ordered stage names and exact prompts, rather than one composite task.
    fn prompt_hash(self) -> Result<String, ApiError> {
        let prompts = self
            .stages()
            .iter()
            .map(|stage| (stage.name(), stage.prompt()))
            .collect::<Vec<_>>();
        crate::canonical::canonical_sha256_hex_of(&prompts)
    }
}

/// Plan one bounded fragment per invocation for all three producer kinds.
/// Section ownership groups discovery/dry-run work; it never enlarges a request.
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
/// Large units are partitioned losslessly before section/document enumeration.
/// Entity/relation and summary plans share exactly the same fragment boundaries.
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
        for fragment in excerpt::split_text(&text, max_input_chars)? {
            let target = InvocationTarget {
                unit_id: unit.id.clone(),
                text: fragment.text,
                start_char: fragment.start_char,
                end_char: fragment.end_char,
            };
            // Both producer routes own their fragment until worker discovery consumes it.
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
    }

    let mut invocations = Vec::new();

    // The leading synthetic group is keyed by its first unit's id (there is no
    // section unit to name it), and is emitted before the section groups so
    // the plan follows reading order.
    if let Some(first) = leading.first() {
        let group_key = first.unit_id.clone();
        push_section_splits(&mut invocations, &group_key, leading)?;
    }
    for section_id in section_order {
        let targets = section_targets.remove(&section_id).unwrap_or_default();
        push_section_splits(&mut invocations, &section_id, targets)?;
    }

    // Summary remains excerpt-scoped; no request reconstructs the large document.
    push_document_splits(&mut invocations, document)?;

    debug!(
        event = "annotator_plan.completed",
        parse_id,
        invocations = invocations.len(),
        max_input_chars,
        "bounded annotation excerpt plan prepared"
    );

    Ok(invocations)
}

/// Record the chain identity and exact source slices before work starts. Empty
/// results retain this same coverage, so a no-entities result still covers its excerpt.
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
            text_range: Some(target.text_range()),
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
        // Planned provenance predates the call: the effective sampling
        // temperature is stamped at completion (`completed_provenance`), and
        // memo reuses honestly keep None (no call ran).
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(input_refs),
    })
}

/// Run an excerpt's dependent request chain before returning any persisted outputs.
///
/// Kind/invocation compatibility: Entity and Relation consume `SectionGroup`
/// invocations; Summary consumes `Document` invocations. A mismatch is a
/// programming error in the stage-3 routing, surfaced loudly rather than
/// silently producing wrong-scoped annotations.
///
/// `temperature` is the per-call sampling temperature, passed through to the
/// client verbatim: the base `PRODUCER_TEMPERATURE` for first attempts, or a
/// retry-escalated value from the worker's ladder. It is deliberately NOT part
/// of producer identity (the prompt is unchanged); the effective value is
/// recorded in the completed row's provenance and the call logs instead.
pub(crate) fn invoke(
    kind: ProducerKind,
    client: &AnnotatorClient,
    invocation: &Invocation,
    temperature: f64,
) -> Result<Vec<ProducedAnnotation>, InvocationFailure> {
    if let Some(reason) = client.cancellation().reason() {
        return Err(InvocationFailure::Cancelled(reason));
    }
    match (kind, &invocation.kind) {
        (ProducerKind::Entity | ProducerKind::Relation, InvocationKind::SectionGroup { .. }) => {}
        (ProducerKind::Summary, InvocationKind::Document { .. }) => {}
        (kind, other) => {
            return Err(InvocationFailure::Internal(ApiError::AnnotationProducer {
                message: format!(
                    "producer {} cannot consume invocation kind {other:?}",
                    kind.producer_name()
                ),
            }));
        }
    }

    // Never accidentally reassemble a large section into one request. The
    // planner, memo key, and provenance all describe this single exact fragment.
    let [target] = invocation.targets.as_slice() else {
        return Err(InvocationFailure::Internal(ApiError::AnnotationProducer {
            message: "annotation invocation must contain exactly one source excerpt".to_string(),
        }));
    };
    if target.text.trim().is_empty() {
        // A lossless partition may retain a whitespace-only fragment. Mark its
        // coverage through the normal empty-result path without spending inference.
        return Ok(Vec::new());
    }
    chains::run(kind, client, &target.text, temperature)
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
/// Keep field selection aligned with `projections::multivector::evidence_text`
/// and `assembly::evidence::evidence_text`, including TableCell's normalizedText
/// fallback. Query passages use the assembly extractor, so these three readers
/// must agree on canonical text when a content type changes.
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

/// Enumerate already-bounded fragments without recombining neighboring units.
fn push_section_splits(
    invocations: &mut Vec<Invocation>,
    section_unit_id: &str,
    targets: Vec<InvocationTarget>,
) -> Result<(), ApiError> {
    for (split_index, target) in targets.into_iter().enumerate() {
        invocations.push(Invocation {
            kind: InvocationKind::SectionGroup {
                section_unit_id: section_unit_id.to_string(),
                split_index: checked_split_index(split_index)?,
            },
            targets: vec![target],
        });
    }
    Ok(())
}

/// Give each summary the same bounded excerpt used by entity/relation producers.
fn push_document_splits(
    invocations: &mut Vec<Invocation>,
    targets: Vec<InvocationTarget>,
) -> Result<(), ApiError> {
    for (split_index, target) in targets.into_iter().enumerate() {
        invocations.push(Invocation {
            kind: InvocationKind::Document {
                split_index: checked_split_index(split_index)?,
            },
            targets: vec![target],
        });
    }
    Ok(())
}

/// Keep diagnostic invocation indexes lossless even for pathological input sizes.
fn checked_split_index(index: usize) -> Result<u32, ApiError> {
    u32::try_from(index).map_err(|_| ApiError::AnnotationProducer {
        message: format!("annotation excerpt count exceeds the supported index range: {index}"),
    })
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
// Shared shape checks keep stage-specific parsing and diagnostic behavior aligned.

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

/// Preserve the parser's error and output size without copying the response body
/// into durable diagnostics. The owning chain supplies the single-goal stage name.
pub(crate) fn strict_from_str<T: serde::de::DeserializeOwned>(
    json_text: &str,
    producer: &str,
    raw: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(json_text).map_err(|source| ApiError::AnnotationProducer {
        message: format!(
            "{producer} producer returned malformed output: {source}; output_chars={}",
            raw.chars().count()
        ),
    })
}

/// Match source text using only letters and digits, deliberately ignoring word
/// boundaries because extraction can join or split words. This is a forgiving
/// comparison only: source and model text remain unchanged. Punctuation-only
/// output cannot satisfy the check through an empty substring.
pub(crate) fn source_text_matches(source: &str, selected: &str) -> bool {
    let letters_and_digits = |text: &str| {
        text.chars()
            .filter(|character| character.is_alphanumeric())
            .collect::<String>()
    };
    let selected = letters_and_digits(selected);
    !selected.is_empty() && letters_and_digits(source).contains(&selected)
}

/// Reject an empty (or whitespace-only) required string field. Empty required
/// strings are a malformed result, not a valid annotation.
pub(crate) fn validate_non_empty(value: &str, field: &str, raw: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::AnnotationProducer {
            message: format!(
                "annotation producer returned empty {field}; output_chars={}",
                raw.chars().count()
            ),
        });
    }
    Ok(())
}
