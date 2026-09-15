//! Annotation excerpts over the active parse's context windows, single-goal
//! producer chains, and source lineage.
//!
//! Each invocation consumes one excerpt: a run of consecutive context windows
//! read from the section-dense artifact. The model receives the excerpt's
//! canonical text as its only source text, with the section path as separate
//! context. Later requests in a chain may use that text and earlier outputs;
//! they never borrow unrelated corpus context. The final output set commits
//! atomically through the existing worker. Interrupted chains restart as a whole.
//!
//! Source-unit hashes, fragment offsets, and exact text hashes identify
//! coverage; the ordered stage contracts additionally identify reusable
//! producer output. Each produced item is attributed to the excerpt fragments
//! whose text supports it (`attribute`).

use crate::sqlite::Connection;
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::annotations::{
    chains,
    llm_client::{AnnotatorClient, ENABLE_THINKING},
    stages::Stage,
};
use crate::artifact_store::ArtifactStore;
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::limits::IndexingLimits;
use crate::maintenance::AnnotationCancelReason;
use crate::model::{
    self, ProducerType, ProvenanceInputRef, ProvenanceObjectType, SemanticAnnotationType,
};
use crate::projections::{
    chunk,
    section_dense::{self, SectionDenseWindow},
};
use crate::types::AnnotationProgressCount;

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

/// Unit ids of a target list in first-occurrence order without duplicates: a
/// unit split across two fragments of one excerpt is one target unit. This is
/// the rule `chunk::ordered_unit_ids` applies to fragments, restated over
/// targets so a row's `targetUnitIds` never lists a unit twice.
pub(crate) fn target_unit_ids(targets: &[InvocationTarget]) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for target in targets {
        if !ids.iter().any(|id| id == &target.unit_id) {
            ids.push(target.unit_id.clone());
        }
    }
    ids
}

/// What a single invocation covers: one excerpt, the `index`-th run of up to
/// `indexing.excerpt_windows` consecutive context windows in the active
/// parse's window order. All three producers consume every excerpt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InvocationKind {
    Excerpt { index: u32 },
}

/// One atomic producer chain over one excerpt. `targets` lists the excerpt's
/// fragments in reading order, each with its text sliced from the window
/// canonical text. `canonical_text` is the window canonical texts joined by one
/// blank line and is the only source text the model sees. `section_path` is
/// the first window's path; it reaches the model as separate context and is
/// never part of any range, hash, or attribution.
#[derive(Debug, Clone)]
pub(crate) struct Invocation {
    pub(crate) kind: InvocationKind,
    pub(crate) targets: Vec<InvocationTarget>,
    pub(crate) section_path: Vec<String>,
    pub(crate) canonical_text: String,
}

impl Invocation {
    /// Correlate an invocation's preparation, HTTP call, and persistence without
    /// logging its text. The inherited source context belongs to the caller.
    pub(crate) fn log_context(&self) -> crate::util::LogContext {
        let context = crate::util::LogContext::new(
            "annotation_invocation",
            &crate::util::diagnostic_id("invocation"),
        );
        let InvocationKind::Excerpt { index } = &self.kind;
        context.record("excerpt_index", *index);
        context.record("target_units", self.targets.len() as u64);
        context.record("excerpt_chars", self.canonical_text.chars().count() as u64);
        if let Some(target) = self.targets.first() {
            context.record("unit_id", target.unit_id.as_str());
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

    /// Version 3 identifies context-window excerpts with the section path as
    /// separate prompt context and per-item fragment attribution.
    pub(crate) fn producer_version(self) -> &'static str {
        "3"
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
    /// Excerpt sizing (`excerpt_windows` context windows of `context_max_tokens`)
    /// shapes every passage, so grain changes change this identity.
    pub(crate) fn identity_hash(
        self,
        config: &AnnotatorModelConfig,
        indexing: &IndexingLimits,
    ) -> Result<String, ApiError> {
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
            "excerptWindows": indexing.excerpt_windows,
            "contextMaxTokens": indexing.context_max_tokens,
            "maxCompletionTokens": config.max_completion_tokens,
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

/// Plan one invocation per excerpt for all three producer kinds: the active
/// parse's published context windows, in window order, grouped into runs of
/// `indexing.excerpt_windows` (the final run may be shorter). Every producer
/// plan shares exactly the same excerpt boundaries.
pub(crate) fn build_invocation_plan(
    conn: &Connection,
    store: &ArtifactStore,
    parse_id: &str,
    dense_dimension: usize,
    indexing: &IndexingLimits,
) -> Result<Vec<Invocation>, ApiError> {
    invocation_plan(conn, store, parse_id, dense_dimension, indexing, true)
}

/// Measure the dispatch plan without adding a second preparation log during
/// the worker's inventory pass. Errors still reach the owning source boundary.
pub(super) fn measure_invocation_plan(
    conn: &Connection,
    store: &ArtifactStore,
    parse_id: &str,
    dense_dimension: usize,
    indexing: &IndexingLimits,
) -> Result<Vec<Invocation>, ApiError> {
    invocation_plan(conn, store, parse_id, dense_dimension, indexing, false)
}

/// Keep measurement and dispatch on identical excerpt boundaries. Only dispatch
/// emits the existing preparation record; health measurement adds no log entry.
fn invocation_plan(
    conn: &Connection,
    store: &ArtifactStore,
    parse_id: &str,
    dense_dimension: usize,
    indexing: &IndexingLimits,
    log_preparation: bool,
) -> Result<Vec<Invocation>, ApiError> {
    let Some(reference) =
        section_dense::load_section_dense_reference(conn, store, parse_id, dense_dimension)?
    else {
        // Context windows are built by the scheduler's content-derived
        // projection build before activation, so an active parse without a
        // section-dense reference is a parse whose build was skipped or is a
        // dry-run parse. The plan is empty and discovery visibly waits instead
        // of annotating nothing silently; the WARN names the waiting parse.
        warn!(
            event = "annotator_plan.windows_unpublished",
            parse_id, "annotation plan is empty: the parse has no published context windows yet"
        );
        return Ok(Vec::new());
    };
    // Startup validation keeps `excerpt_windows` positive; a zero here would
    // never close a run, so it is rejected rather than trusted.
    let run_length = usize::try_from(indexing.excerpt_windows)
        .ok()
        .filter(|length| *length > 0)
        .ok_or_else(|| ApiError::AnnotationProducer {
            message: format!(
                "indexing.excerpt_windows must be positive, got {}",
                indexing.excerpt_windows
            ),
        })?;

    let mut invocations: Vec<Invocation> = Vec::new();
    let mut open = ExcerptRun::default();
    section_dense::visit_section_dense(conn, store, &reference, |window| {
        open.push(window)?;
        if open.window_count == run_length {
            let index = checked_excerpt_index(invocations.len())?;
            invocations.push(std::mem::take(&mut open).into_invocation(index));
        }
        Ok(())
    })?;
    if open.window_count > 0 {
        // The final run may be shorter than `excerpt_windows`.
        let index = checked_excerpt_index(invocations.len())?;
        invocations.push(open.into_invocation(index));
    }

    if log_preparation {
        debug!(
            event = "annotator_plan.completed",
            parse_id,
            invocations = invocations.len(),
            excerpt_windows = indexing.excerpt_windows,
            "annotation excerpt plan prepared from published context windows"
        );
    }

    Ok(invocations)
}

/// An open run of consecutive context windows being packed into one excerpt.
#[derive(Default)]
struct ExcerptRun {
    window_count: usize,
    section_path: Vec<String>,
    canonical_text: String,
    targets: Vec<InvocationTarget>,
}

impl ExcerptRun {
    /// Append one window: its fragments become targets and its canonical text
    /// joins after one blank line, the same join the grains below use. The
    /// first window's section path names the run. The window is borrowed from
    /// the artifact stream, so its path and text are copied into the run.
    fn push(&mut self, window: &SectionDenseWindow) -> Result<(), ApiError> {
        if self.window_count == 0 {
            self.section_path = window.section_path.clone();
            self.canonical_text = window.targeting_text.clone();
        } else {
            self.canonical_text = chunk::join_text(&self.canonical_text, &window.targeting_text);
        }
        self.targets.extend(fragment_targets(window)?);
        self.window_count += 1;
        Ok(())
    }

    /// Close the run as the excerpt at `index`.
    fn into_invocation(self, index: u32) -> Invocation {
        Invocation {
            kind: InvocationKind::Excerpt { index },
            targets: self.targets,
            section_path: self.section_path,
            canonical_text: self.canonical_text,
        }
    }
}

/// Slice each fragment's text out of the window canonical text. The canonical
/// text is member texts joined by one blank line, with the cells of one table
/// row joined by one tab (`projections::chunk`), so after a fragment's exact
/// length the next characters are that tab, that blank line, or the end of the
/// text. Any other layout means the fragments and the text disagree, and the
/// plan fails loudly rather than attributing wrong text to a unit.
fn fragment_targets(window: &SectionDenseWindow) -> Result<Vec<InvocationTarget>, ApiError> {
    let layout_failure = |detail: &str| ApiError::AnnotationProducer {
        message: format!(
            "context window {} fragments do not lay out over its canonical text: {detail}",
            window.window_id
        ),
    };
    let chars: Vec<char> = window.targeting_text.chars().collect();
    let mut cursor = 0usize;
    let mut targets = Vec::with_capacity(window.fragments.len());
    for (position, fragment) in window.fragments.iter().enumerate() {
        let length = fragment
            .end_char
            .checked_sub(fragment.start_char)
            .filter(|length| *length > 0)
            .ok_or_else(|| layout_failure("empty or reversed fragment range"))?;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| layout_failure("fragment range overflows"))?;
        let text: String = chars
            .get(cursor..end)
            .ok_or_else(|| layout_failure("fragment extends beyond the canonical text"))?
            .iter()
            .collect();
        targets.push(InvocationTarget {
            unit_id: fragment.unit_id.clone(),
            text,
            start_char: fragment.start_char,
            end_char: fragment.end_char,
        });
        cursor = end;
        if position + 1 == window.fragments.len() {
            break;
        }
        if chars.get(cursor) == Some(&'\t') {
            cursor += 1;
        } else if chars.get(cursor..cursor + 2) == Some(&['\n', '\n']) {
            cursor += 2;
        } else {
            return Err(layout_failure(
                "fragment is not followed by a member separator",
            ));
        }
    }
    if cursor != chars.len() {
        return Err(layout_failure("fragments do not cover the canonical text"));
    }
    Ok(targets)
}

/// Keep diagnostic excerpt indexes lossless even for pathological input sizes.
fn checked_excerpt_index(index: usize) -> Result<u32, ApiError> {
    u32::try_from(index).map_err(|_| ApiError::AnnotationProducer {
        message: format!("annotation excerpt count exceeds the supported index range: {index}"),
    })
}

/// Record the chain identity and exact source slices before work starts. Empty
/// results retain this same coverage, so a no-entities result still covers its excerpt.
pub(crate) fn planned_provenance(
    kind: ProducerKind,
    config: &AnnotatorModelConfig,
    indexing: &IndexingLimits,
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
        config_hash: Some(kind.identity_hash(config, indexing)?),
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
/// The passage is the excerpt's canonical text exactly as the planner bounded
/// it (at most `excerpt_windows` context windows); nothing here reassembles a
/// larger source. The section path travels separately as prompt context so it
/// never enters the source text the stages quote from and match against.
///
/// `temperature` is the per-call sampling temperature, passed through to the
/// client verbatim: the base `PRODUCER_TEMPERATURE` for first attempts, or a
/// retry-escalated value from the worker's ladder. It is deliberately NOT part
/// of producer identity (the prompt is unchanged); the effective value is
/// recorded in the completed row's provenance and the call logs instead.
/// `progress` is committed document coverage at wave dispatch; model stages
/// cannot advance it because the worker persists results only after the wave.
pub(crate) fn invoke(
    kind: ProducerKind,
    client: &AnnotatorClient,
    invocation: &Invocation,
    temperature: f64,
    progress: Option<AnnotationProgressCount>,
) -> Result<Vec<ProducedAnnotation>, InvocationFailure> {
    if let Some(reason) = client.cancellation().reason() {
        return Err(InvocationFailure::Cancelled(reason));
    }
    if invocation.canonical_text.trim().is_empty() {
        // Windows are never whitespace-only by construction; if one ever is,
        // mark its coverage through the empty-result path without spending inference.
        return Ok(Vec::new());
    }
    // The section line is a one-line context header (stages.rs promises the
    // model "a line starting Section:"); heading text may carry newlines from
    // `<br/>`, so every whitespace run collapses to one space here.
    let section = (!invocation.section_path.is_empty()).then(|| {
        invocation
            .section_path
            .iter()
            .map(|part| part.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join(" / ")
    });
    chains::run(
        kind,
        client,
        &invocation.canonical_text,
        section.as_deref(),
        temperature,
        progress,
    )
}

/// Choose which invocations a producer consumes. Every producer takes every
/// excerpt; the function remains the single routing point the worker and the
/// dry run filter through, so a future kind split changes only this rule.
pub(crate) fn invocation_matches_kind(_kind: ProducerKind, invocation: &Invocation) -> bool {
    matches!(invocation.kind, InvocationKind::Excerpt { .. })
}

/// Attribute one produced item to the excerpt fragments whose text supports it:
/// an entity keeps the fragments matching its `name`, a relation keeps those
/// matching any of its `evidenceQuotes`, and a summary keeps every fragment.
/// Matching uses `source_text_matches` over each fragment's text alone, so a
/// quote that spans two fragments matches neither. When nothing matches, the
/// item keeps every fragment: the excerpt is still its true input, and coverage
/// must never narrow to nothing. The returned targets are owned because they
/// become the row's request and outlive this call.
pub(crate) fn attribute(
    kind: ProducerKind,
    body: &Value,
    targets: &[InvocationTarget],
) -> Vec<InvocationTarget> {
    let quotes: Vec<&str> = match kind {
        ProducerKind::Entity => body
            .get("name")
            .and_then(Value::as_str)
            .into_iter()
            .collect(),
        ProducerKind::Relation => body
            .get("evidenceQuotes")
            .and_then(Value::as_array)
            .map(|quotes| quotes.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default(),
        ProducerKind::Summary => return targets.to_vec(),
    };
    let matched: Vec<InvocationTarget> = targets
        .iter()
        .filter(|target| {
            quotes
                .iter()
                .any(|quote| source_text_matches(&target.text, quote))
        })
        .cloned()
        .collect();
    if matched.is_empty() {
        // Attribution fallback: no fragment individually supports the item, so
        // the whole excerpt stays its recorded input rather than an empty set.
        return targets.to_vec();
    }
    matched
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

/// Locate cleaned model text within its source using separate allowances for
/// repaired output characters and omitted source characters. Unicode lowercasing
/// and alphanumeric filtering normalize comparisons without trusting word
/// boundaries; neither input is rewritten and a match proves no semantics.
/// Punctuation-only output cannot satisfy the check through an empty substring.
pub(crate) fn source_text_matches(source: &str, selected: &str) -> bool {
    // Lowercase before filtering because Unicode case mappings can expand into
    // multiple characters, including combining marks that comparison ignores.
    let normalize = |text: &str| {
        text.chars()
            .flat_map(char::to_lowercase)
            .filter(|character| character.is_alphanumeric())
            .collect::<String>()
    };
    let selected = normalize(selected);
    if selected.is_empty() {
        return false;
    }
    let source = normalize(source);
    if source.contains(&selected) {
        return true;
    }
    let selected: Vec<char> = selected.chars().collect();
    let source: Vec<char> = source.chars().collect();
    let selected_length = selected.len();
    // Allow four repairs for short excerpts and 15% for longer ones, with a
    // 25% ceiling protecting very short strings. Source omissions have their
    // own budget so removing furniture does not spend the spelling allowance.
    let repair_limit = (selected_length / 4).min(4.max(selected_length * 3 / 20));
    let omission_limit = 32.max(selected_length);
    if selected_length - repair_limit > source.len() {
        return false;
    }

    // At output prefix i, row[e][j] holds the fewest source omissions for an
    // alignment ending at source prefix j with exactly e inserted/substituted
    // output characters. Keeping each repair count avoids discarding an
    // alternative alignment that alone satisfies both independent budgets.
    let unreachable = omission_limit + 1;
    let columns = source.len() + 1;
    let mut previous = vec![vec![unreachable; columns]; repair_limit + 1];
    let mut current = vec![vec![unreachable; columns]; repair_limit + 1];
    // An empty output can start anywhere: source text outside the matched
    // interval is free, while omissions inside it count against the budget.
    previous[0].fill(0);
    for (selected_index, selected_character) in selected.iter().enumerate() {
        let prefix_length = selected_index + 1;
        for row in &mut current {
            row.fill(unreachable);
        }
        if prefix_length <= repair_limit {
            current[prefix_length][0] = 0;
        }
        for (repairs, row) in current
            .iter_mut()
            .enumerate()
            .take(prefix_length.min(repair_limit) + 1)
        {
            for (source_index, source_character) in source.iter().enumerate() {
                let column = source_index + 1;
                let mut omissions = row[column - 1] + 1;
                if selected_character == source_character {
                    omissions = omissions.min(previous[repairs][column - 1]);
                } else if repairs > 0 {
                    omissions = omissions.min(previous[repairs - 1][column - 1]);
                }
                if repairs > 0 {
                    omissions = omissions.min(previous[repairs - 1][column]);
                }
                row[column] = omissions.min(unreachable);
            }
        }
        std::mem::swap(&mut previous, &mut current);
    }
    // Any source endpoint may finish the match; trailing source text is free.
    previous
        .iter()
        .any(|row| row.iter().any(|omissions| *omissions <= omission_limit))
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
