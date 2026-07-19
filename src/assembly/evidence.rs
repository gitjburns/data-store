//! Assembly evidence-pack construction (§26, §27). Owned by package C8b.
//!
//! `build_evidence_pack` is the assembly boundary: it takes the reranked anchor
//! units (in rank order) and the captured active parses, applies the sealed
//! `AssemblyPolicy` (§25) rule-by-rule to expand each anchor into its structural
//! neighborhood via C8a's seven graph operators, and emits an `EvidencePack`
//! (§26) plus the auditable `ContextAssemblyTrace` (§27).
//!
//! DECOUPLING (R11): every function is SYNCHRONOUS and takes
//! `conn: &rusqlite::Connection` (the caller-owned per-query read transaction,
//! DP1) plus explicit inputs — no async/http/tokio, no self-opened connection,
//! and the parameter types are borrowed primitives (unit ids, scores,
//! source_id/parse_id pairs), never the query pipeline's `execute.rs` types, so
//! `assembly/` never imports from `execute.rs` (no dependency cycle).
//!
//! DETERMINISM (R13): pack order is anchors in rank order; each anchor's
//! rule-added units follow their anchor in rule-application order, then unit id
//! ascending within one rule application; the first inclusion of any unit wins
//! and later re-inclusions are dropped. Given the same inputs the pack, the
//! trace, and every id list are byte-identical.
//!
//! §26 STRUCTURAL INVARIANT: `evidenceUnits` holds ONLY canonical ContentUnits
//! from active parses. This is enforced STRUCTURALLY, not by a post-filter:
//! every unit is resolved with `WHERE parse_id = ?1 AND id = ?2` against a
//! `parse_id` drawn from the captured active-parse set (an anchor's parse, or —
//! for operator-added units — the same parse the operator traversed within), so
//! a unit id can never resolve outside a captured active parse.
//!
//! DIAGNOSTICS: this function owns the assembly diagnostic boundary — it logs
//! start (query_id, anchor count, policy id/version/hash) and success (selected
//! and rejected counts, budget consumed) with `elapsed_ms`, and on error surfaces
//! local context (unit_id/parse_id) via the typed `ApiError` the operators and
//! resolvers already attach. C8a's R14 dependency check is invoked at the start
//! and its single aggregate per-query warn line fires there; this module adds no
//! per-parse or per-parse dependency logging of its own. Compact safe
//! identifiers only — no unit bodies, no text, no token counts in logs.

use std::collections::BTreeSet;
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use tracing::info;

use crate::error::ApiError;
use crate::model::unit::ContentType;
use crate::model::{Locator, SemanticAnnotation, UnitRelationship};
use crate::primitives::utc_now;

use crate::assembly::model::{
    AppliedAssemblyRule, AssemblyBudget, AssemblyCondition, AssemblyHitTypeCondition,
    AssemblyOperator, AssemblyPolicy, AssemblyReason, AssemblyRule, ContextAssemblyTrace,
    EvidencePack, EvidenceUnit,
};
use crate::assembly::operators::{
    OperatorOutput, include_anchor, include_caption_pair, include_continuation_chain,
    include_explicit_references, include_heading_path, include_parent_container,
    include_text_neighbors,
};
use crate::assembly::policy::{CapturedParseRef, unmet_dependencies};

/// One reranked anchor the assembler seats and expands. Borrowed primitives so
/// C8d-2 hands over the reranked units without `assembly/` importing its
/// `RerankerCandidateScore`/capture types (R11): `unit_id` + its originating
/// `parse_id` (from C8d-2's parse-of-unit map) + the rerank `score`. Anchors are
/// passed in RANK ORDER (best first); that order is the pack's outer ordering.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Anchor<'a> {
    /// The reranked unit id (also the anchor hit id: §26 hits are unit-grained).
    pub(crate) unit_id: &'a str,
    /// The active parse the anchor unit belongs to (§14 parse scoping); every
    /// operator expansion from this anchor stays within this parse.
    pub(crate) parse_id: &'a str,
    /// The final reranker score, surfaced on the anchor's `EvidenceUnit.score`.
    pub(crate) score: f64,
}

/// The R6 evidence-shaping flags the request resolved (defaults applied by
/// C8d-1's request layer): whether to attach source locators, the traversed
/// relationships, and the intersecting annotations. Plain bools, so C8d-2 hands
/// them over without `assembly/` depending on the request DTO.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EvidenceOptions {
    /// Attach each unit's `locators` (spec `includeSourceLocators`).
    pub(crate) include_source_locators: bool,
    /// Populate `EvidencePack.relationships` with the traversed edges
    /// (spec `includeRelationships`).
    pub(crate) include_relationships: bool,
    /// Populate `EvidencePack.annotations` for the selected units
    /// (spec `includeAnnotations`).
    pub(crate) include_annotations: bool,
}

/// Build the §26 `EvidencePack` from the reranked anchors and the sealed policy.
///
/// Applies the policy's rules to each anchor in rank order, expanding via C8a's
/// operators under the policy budgets (`max_evidence_units`, `max_tokens` via the
/// caller's `count_tokens` closure, `max_expansion_depth`), resolving each
/// selected unit's canonical content parse-scoped (§14), and recording every
/// inclusion/rejection in the trace (§27). No absolute quality thresholds (R5):
/// selection is policy-driven and budget-bounded only.
///
/// `count_tokens` (R10) is a CPU-only tokenizer closure supplied by C8d-2
/// (wrapping the same ColBERT tokenizer the chunker uses); it MUST NOT acquire
/// the model-call gate — assembly never touches the gate, which protects
/// accelerator work only. Assembly is pure graph/text CPU work.
// The eight-parameter contract is the assembly boundary C8d-2 calls: the read
// connection, the captured parses, the sealed policy, the anchors, the evidence
// options, the token closure, and the two query identifiers. Bundling any subset
// would only hide the boundary's real inputs, so the arity is kept explicit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_evidence_pack(
    conn: &Connection,
    captured: &[CapturedParseRef<'_>],
    policy: &AssemblyPolicy,
    anchors: &[Anchor<'_>],
    options: EvidenceOptions,
    count_tokens: impl Fn(&str) -> Result<usize, ApiError>,
    query_id: &str,
    query_text: &str,
) -> Result<EvidencePack, ApiError> {
    let started_at = Instant::now();
    info!(
        event = "assembly.build.started",
        query_id,
        anchor_count = anchors.len(),
        captured_parse_count = captured.len(),
        policy_id = policy.id.as_str(),
        policy_version = policy.version.as_str(),
        policy_hash = policy.policy_hash.as_str(),
        "assembly evidence-pack build started"
    );

    // R14: invoke C8a's dependency check at assembly start and let its single
    // aggregate per-query warn line fire. Inert required relationship types are
    // warned, never fatal (§25.1); a real storage/decode fault propagates.
    let _unmet = unmet_dependencies(conn, policy, captured, query_id)?;

    // Running selection state, threaded through every anchor/rule application so
    // the budget and first-inclusion-wins dedupe span the whole pack (not per
    // anchor). `selected` preserves pack order; `selected_set` is the O(1)
    // membership test backing first-inclusion-wins.
    let mut state = SelectionState {
        selected: Vec::new(),
        selected_set: BTreeSet::new(),
        tokens_used: 0,
        applied_rules: Vec::new(),
        rejected_unit_ids: Vec::new(),
        traversed_edges: Vec::new(),
    };

    // Every anchor is an input hit; recorded in rank order for the trace (§27).
    let input_hit_ids: Vec<String> = anchors.iter().map(|a| a.unit_id.to_string()).collect();

    for anchor in anchors {
        apply_anchor(conn, policy, anchor, &options, &count_tokens, &mut state)?;
    }

    // Every selected unit was already resolved parse-scoped (§14) at selection
    // time (its content, text projection, and locators are held on `SelectedUnit`),
    // so pack assembly is a pure render in pack order — no second content read.
    // §26's "only canonical units from active parses" holds structurally because
    // selection resolved each unit keyed on a captured `parse_id`.
    let mut evidence_units = Vec::with_capacity(state.selected.len());
    let mut selected_unit_ids = Vec::with_capacity(state.selected.len());
    for sel in &state.selected {
        selected_unit_ids.push(sel.unit_id.clone());
        evidence_units.push(render_evidence_unit(sel, &options));
    }

    // Relationships: ONLY the edges the operators actually traversed (R5/§26),
    // deduped by edge id, present only when the request asked. Never all edges of
    // the touched units.
    let relationships = if options.include_relationships {
        Some(dedupe_edges(state.traversed_edges))
    } else {
        None
    };

    // Annotations: fresh annotations of the captured sources whose targets
    // intersect the selected units, present only when the request asked.
    let annotations = if options.include_annotations {
        Some(collect_annotations(conn, captured, &selected_unit_ids)?)
    } else {
        None
    };

    let trace = ContextAssemblyTrace {
        assembly_policy_id: policy.id.clone(),
        assembly_policy_version: policy.version.clone(),
        assembly_policy_hash: policy.policy_hash.clone(),
        input_hit_ids,
        applied_rules: state.applied_rules,
        selected_unit_ids,
        // No hits are rejected pre-selection in v1 (every anchor is seated by the
        // anchor rule); reserved for future when-conditions that reject a hit.
        rejected_hit_ids: None,
        rejected_unit_ids: if state.rejected_unit_ids.is_empty() {
            None
        } else {
            Some(state.rejected_unit_ids.clone())
        },
        budget: policy.budgets.clone(),
    };

    let pack = EvidencePack {
        query_id: query_id.to_string(),
        query_text: query_text.to_string(),
        evidence_units,
        relationships,
        annotations,
        assembly_trace: trace,
        // Assembly time; a timestamp primitive, never a literal (§16.2).
        created_at: utc_now()?,
    };

    info!(
        event = "assembly.build.completed",
        query_id,
        anchor_count = anchors.len(),
        selected_unit_count = pack.evidence_units.len(),
        rejected_unit_count = pack
            .assembly_trace
            .rejected_unit_ids
            .as_ref()
            .map_or(0, Vec::len),
        applied_rule_count = pack.assembly_trace.applied_rules.len(),
        tokens_used = state.tokens_used,
        max_tokens = policy.budgets.max_tokens,
        max_evidence_units = policy.budgets.max_evidence_units,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "assembly evidence-pack build completed"
    );

    Ok(pack)
}

/// Running assembly selection state, threaded across all anchors so the budget
/// and first-inclusion-wins dedupe span the WHOLE pack. `selected` is pack order;
/// `selected_set` mirrors its membership for O(1) dedupe. Kept as one struct so
/// the per-anchor/per-rule helpers mutate one owner rather than many out-params.
struct SelectionState {
    /// Selected units in pack order (R13): anchor, then that anchor's rule-added
    /// units, then the next anchor.
    selected: Vec<SelectedUnit>,
    /// Membership mirror of `selected` for first-inclusion-wins dedupe.
    selected_set: BTreeSet<String>,
    /// Total tokens consumed by selected units' text projections (R10 budget).
    tokens_used: u32,
    /// Trace attribution: one entry per rule application that added ≥1 unit.
    applied_rules: Vec<AppliedAssemblyRule>,
    /// Units dropped after selection (over budget, or vanished at resolution).
    rejected_unit_ids: Vec<String>,
    /// Every edge the operators traversed, for `EvidencePack.relationships`.
    traversed_edges: Vec<UnitRelationship>,
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

/// Apply every matching policy rule to one anchor, in policy rule order. The
/// anchor is seated first (by the `include_anchor` operator inside its rule),
/// then each rule whose `when` matches expands it. Budget checks live in
/// `try_select`, so a rule that would overflow simply adds nothing further.
///
/// `max_expansion_depth` (§25 budget) is honored STRUCTURALLY: rule application
/// runs ONE hop out from the anchor and never re-anchors on an operator-added
/// unit, so no anchor's neighborhood expands past a single rule-application hop.
/// (The v1 policy sets depth 1, matching this; operators that walk a chain —
/// heading path, continuation — are C8a's own internally bounded walks, not
/// re-anchoring here.) A future depth > 1 would re-anchor added units up to that
/// bound; v1 does not, so the field is enforced by the non-recursive structure
/// rather than a counter.
fn apply_anchor(
    conn: &Connection,
    policy: &AssemblyPolicy,
    anchor: &Anchor<'_>,
    options: &EvidenceOptions,
    count_tokens: &impl Fn(&str) -> Result<usize, ApiError>,
    state: &mut SelectionState,
) -> Result<(), ApiError> {
    // The anchor's content type gates content-typed `when` conditions; resolved
    // once per anchor. A missing anchor unit yields `None` and simply matches no
    // content-typed rule (the anchor rule's `when` is unconditional, so the
    // anchor is still seated and later dropped at resolution if truly absent).
    let anchor_content_type = anchor_content_type(conn, anchor)?;

    for rule in &policy.rules {
        if !condition_matches(&rule.when, anchor_content_type) {
            continue;
        }
        apply_rule(
            conn,
            &policy.budgets,
            rule,
            anchor,
            options,
            count_tokens,
            state,
        )?;
    }
    Ok(())
}

/// Apply one matched rule to one anchor: run each of its operators, collect the
/// added unit ids (dedupe-first-wins, budget-bounded), and record ONE
/// `AppliedAssemblyRule` trace entry when the rule added ≥1 unit. Units added by
/// a rule are ordered unit-id ascending within the rule (R13), after the anchor.
fn apply_rule(
    conn: &Connection,
    budget: &AssemblyBudget,
    rule: &AssemblyRule,
    anchor: &Anchor<'_>,
    options: &EvidenceOptions,
    count_tokens: &impl Fn(&str) -> Result<usize, ApiError>,
    state: &mut SelectionState,
) -> Result<(), ApiError> {
    // Gather every operator's output for this rule first, so the rule's added
    // units can be ordered deterministically (unit-id ascending, R13) before
    // selection regardless of the order operators emit them.
    let mut candidate_ids: Vec<String> = Vec::new();
    let mut is_anchor_rule = false;
    for op in &rule.apply {
        let output = run_operator(conn, op.operator, anchor)?;
        if matches!(op.operator, AssemblyOperator::IncludeAnchor) {
            is_anchor_rule = true;
        }
        // Record traversed edges for `EvidencePack.relationships` (deduped later).
        state.traversed_edges.extend(output.traversed_edges);
        candidate_ids.extend(output.added_unit_ids);
    }

    // Anchor units keep operator (rank-driven) order — there is exactly one, the
    // anchor itself. Neighbor units added by an expansion rule sort unit-id
    // ascending for a deterministic within-rule order (R13).
    if !is_anchor_rule {
        candidate_ids.sort();
        candidate_ids.dedup();
    }

    let mut added_unit_ids = Vec::new();
    for unit_id in candidate_ids {
        let score = if is_anchor_rule {
            Some(anchor.score)
        } else {
            None
        };
        if try_select(
            conn,
            budget,
            &unit_id,
            anchor.parse_id,
            rule.reason,
            score,
            options,
            count_tokens,
            state,
        )? {
            added_unit_ids.push(unit_id);
        }
    }

    // One trace entry per rule application that added ≥1 unit (§27). The anchor
    // ids are the same unit for v1 (unit-grained hits): `anchorUnitId` and
    // `anchorHitId` both carry the anchor unit id.
    if !added_unit_ids.is_empty() {
        state.applied_rules.push(AppliedAssemblyRule {
            rule_id: rule.id.clone(),
            anchor_unit_id: Some(anchor.unit_id.to_string()),
            anchor_hit_id: Some(anchor.unit_id.to_string()),
            added_unit_ids,
            reason: rule.reason,
        });
    }
    Ok(())
}

/// Dispatch one operator to C8a's implementation for the given anchor. Every
/// operator is parse-scoped on `anchor.parse_id` (§14); `include_anchor` is the
/// only edge-free operator. Centralizes the operator vocabulary so a new operator
/// is a compile error here until wired.
fn run_operator(
    conn: &Connection,
    operator: AssemblyOperator,
    anchor: &Anchor<'_>,
) -> Result<OperatorOutput, ApiError> {
    match operator {
        AssemblyOperator::IncludeAnchor => Ok(include_anchor(anchor.unit_id)),
        AssemblyOperator::IncludeParentContainer => {
            include_parent_container(conn, anchor.parse_id, anchor.unit_id)
        }
        AssemblyOperator::IncludeHeadingPath => {
            include_heading_path(conn, anchor.parse_id, anchor.unit_id)
        }
        AssemblyOperator::IncludeCaptionPair => {
            include_caption_pair(conn, anchor.parse_id, anchor.unit_id)
        }
        AssemblyOperator::IncludeExplicitReferences => {
            include_explicit_references(conn, anchor.parse_id, anchor.unit_id)
        }
        AssemblyOperator::IncludeContinuationChain => {
            include_continuation_chain(conn, anchor.parse_id, anchor.unit_id)
        }
        AssemblyOperator::IncludeTextNeighbors => {
            include_text_neighbors(conn, anchor.parse_id, anchor.unit_id)
        }
    }
}

/// Try to add one unit to the pack under the budgets and first-inclusion-wins
/// dedupe. Returns `true` when the unit was newly added.
///
/// Order of guards is load-bearing (R13 determinism + R10 budget): (1) an
/// already-selected unit is a no-op but STILL attributes its reason (so the trace
/// records every rule that would have added it); (2) the `max_evidence_units`
/// count budget; (3) the `max_tokens` budget, computed from the unit's text
/// projection via the caller's `count_tokens` closure (CPU-only, never the gate).
/// A budget rejection records the unit id in `rejected_unit_ids` once.
#[allow(clippy::too_many_arguments)]
fn try_select(
    conn: &Connection,
    budget: &AssemblyBudget,
    unit_id: &str,
    parse_id: &str,
    reason: AssemblyReason,
    score: Option<f64>,
    options: &EvidenceOptions,
    count_tokens: &impl Fn(&str) -> Result<usize, ApiError>,
    state: &mut SelectionState,
) -> Result<bool, ApiError> {
    // (1) First-inclusion-wins: a unit already selected is not re-added, but the
    // later rule's reason is still attributed so the trace records every rule that
    // would have added it. An anchor score arriving after a neighbor inclusion
    // upgrades the recorded score (the unit is promoted to anchor evidence).
    if state.selected_set.contains(unit_id) {
        if let Some(existing) = state.selected.iter_mut().find(|u| u.unit_id == unit_id) {
            if !existing.reasons.contains(&reason) {
                existing.reasons.push(reason);
            }
            if existing.score.is_none() {
                existing.score = score;
            }
        }
        return Ok(false);
    }

    // (2) Count budget: `max_evidence_units` bounds the whole pack. Checked
    // BEFORE reading content so an over-count unit costs no I/O; recorded rejected.
    if state.selected.len() as u32 >= budget.max_evidence_units {
        if !state.rejected_unit_ids.iter().any(|u| u == unit_id) {
            state.rejected_unit_ids.push(unit_id.to_string());
        }
        return Ok(false);
    }

    // Resolve the unit's canonical content once, parse-scoped (§14). A unit with
    // no row (hard-deleted between selection and read: hot cleanup deletes on
    // archive) is rejected, not fatal — it simply cannot enter the pack.
    let Some(content) = read_unit_content(conn, parse_id, unit_id, options)? else {
        if !state.rejected_unit_ids.iter().any(|u| u == unit_id) {
            state.rejected_unit_ids.push(unit_id.to_string());
        }
        return Ok(false);
    };

    // (3) Token budget (R10): charge the unit's text projection via the caller's
    // CPU-only `count_tokens` closure (never the model-call gate). A unit whose
    // inclusion would exceed `max_tokens` is rejected; a text-free unit costs 0
    // tokens. u32 saturating add guards the (unreachable) overflow.
    let unit_tokens: u32 = match &content.text_projection {
        Some(text) => count_tokens(text)? as u32,
        None => 0,
    };
    if state.tokens_used.saturating_add(unit_tokens) > budget.max_tokens {
        if !state.rejected_unit_ids.iter().any(|u| u == unit_id) {
            state.rejected_unit_ids.push(unit_id.to_string());
        }
        return Ok(false);
    }

    // Selected: charge tokens, record membership, and seat the fully-resolved unit
    // in pack order (R13).
    state.tokens_used = state.tokens_used.saturating_add(unit_tokens);
    state.selected_set.insert(unit_id.to_string());
    state.selected.push(SelectedUnit {
        unit_id: unit_id.to_string(),
        parse_id: parse_id.to_string(),
        source_id: content.source_id,
        content_type: content.content_type,
        body: content.body,
        text_projection: content.text_projection,
        locators: content.locators,
        reasons: vec![reason],
        score,
    });
    Ok(true)
}

/// A unit's resolved canonical content, read once at selection time.
struct ResolvedContent {
    source_id: String,
    content_type: ContentType,
    body: Value,
    text_projection: Option<String>,
    locators: Option<Vec<Locator>>,
}

/// Read one unit's canonical content, parse-scoped (§14). Returns `None` when the
/// unit has no row (hard-deleted between selection and read). Mirrors
/// `crate::query::rerank::resolve_unit_content`'s parse-scoped read and loud
/// content-type re-typing, extended to also read `locators_json`. Locators are
/// parsed only when `includeSourceLocators` so an unwanted blob costs no decode.
fn read_unit_content(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
    options: &EvidenceOptions,
) -> Result<Option<ResolvedContent>, ApiError> {
    let row = conn
        .query_row(
            SELECT_EVIDENCE_UNIT_SQL,
            params![parse_id, unit_id],
            |row| {
                Ok(EvidenceUnitRow {
                    source_id: row.get::<_, String>(0)?,
                    content_type: row.get::<_, String>(1)?,
                    body_json: row.get::<_, String>(2)?,
                    locators_json: row.get::<_, Option<String>>(3)?,
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

    // Re-type the persisted content_type through the model enum: a value outside
    // the schema CHECK set fails loudly with the unit identity (mirrors
    // `rerank.rs::resolve_unit_content`), never silently mis-read.
    let content_type: ContentType = serde_json::from_value(Value::String(row.content_type.clone()))
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted content unit {unit_id} type {:?} is not a known variant: {source}",
                row.content_type
            ),
        })?;
    let body: Value =
        serde_json::from_str(&row.body_json).map_err(|source| ApiError::StorageOperation {
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
    body_json: String,
    locators_json: Option<String>,
}

/// Ordered SELECT of one unit's evidence fields, parse-scoped (§14) and keyed on
/// the unit id. `locators_json` is nullable (a unit may carry no locators).
const SELECT_EVIDENCE_UNIT_SQL: &str = "
SELECT source_id, content_type, body_json, locators_json
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

/// Read the anchor unit's content type for `when`-condition matching, parse-
/// scoped (§14). `None` when the anchor unit has no row (an unconditional rule
/// still matches; content-typed rules do not).
fn anchor_content_type(
    conn: &Connection,
    anchor: &Anchor<'_>,
) -> Result<Option<ContentType>, ApiError> {
    let content_type: Option<String> = conn
        .query_row(
            "SELECT content_type FROM content_units WHERE parse_id = ?1 AND id = ?2",
            params![anchor.parse_id, anchor.unit_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read content type for anchor unit {} of parse {}: {source}",
                anchor.unit_id, anchor.parse_id
            ),
        })?;

    match content_type {
        None => Ok(None),
        Some(wire) => {
            let ct: ContentType =
                serde_json::from_value(Value::String(wire.clone())).map_err(|source| {
                    ApiError::StorageOperation {
                        message: format!(
                            "anchor content unit {} type {wire:?} is not a known variant: {source}",
                            anchor.unit_id
                        ),
                    }
                })?;
            Ok(Some(ct))
        }
    }
}

/// Whether a rule's `when` matches an anchor (§25 conjunction: all present fields
/// must match). v1 conditions constrain `hitType` and `contentType`; the
/// relationship-presence fields (`hasOutgoing/IncomingRelationships`) are matched
/// too so a future policy using them behaves. A field left `None` is a wildcard.
fn condition_matches(when: &AssemblyCondition, anchor_content_type: Option<ContentType>) -> bool {
    // hitType: v1 hits are unit-grained (`content_unit`); `any` and `content_unit`
    // match, other explicit hit types do not. A `None` hitType is a wildcard.
    if let Some(hit_type) = when.hit_type {
        match hit_type {
            AssemblyHitTypeCondition::Any | AssemblyHitTypeCondition::ContentUnit => {}
            AssemblyHitTypeCondition::Chunk
            | AssemblyHitTypeCondition::SemanticAnnotation
            | AssemblyHitTypeCondition::RetrievalProjection => return false,
        }
    }

    // contentType: the anchor's type must be one of the listed types. An anchor
    // with no resolvable type matches no content-typed condition.
    if let Some(allowed) = when.content_type.as_ref() {
        match anchor_content_type {
            Some(ct) if allowed.contains(&ct) => {}
            _ => return false,
        }
    }

    // Relationship-presence conditions are unused by v1 rules (all `None`); a
    // present condition here would require an edge-presence probe. v1 never sets
    // them, so treat a set condition as non-matching until a policy needs it,
    // rather than silently ignoring the constraint.
    if when.has_outgoing_relationships.is_some() || when.has_incoming_relationships.is_some() {
        return false;
    }

    true
}

/// Dedupe traversed edges by their canonical edge id, preserving first-seen
/// order (R13). The operators may traverse the same edge from two rules (e.g. a
/// parent container and a heading-path first hop); the pack lists each edge once.
fn dedupe_edges(edges: Vec<UnitRelationship>) -> Vec<UnitRelationship> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::with_capacity(edges.len());
    for edge in edges {
        if seen.insert(edge.id.clone()) {
            out.push(edge);
        }
    }
    out
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
/// MUST STAY IN STEP (four sites): this is one of four arm-for-arm mirrors of the
/// per-`ContentType` evidence-text extraction. The others are
/// `crate::query::rerank::evidence_text` (`src/query/rerank.rs`),
/// `crate::projections::multivector::evidence_text` (`src/projections/multivector.rs`),
/// and `crate::annotations::producer::evidence_text` (`src/annotations/producer.rs`).
/// All four select the same field per type — including the `TableCell` fallback
/// to `normalizedText` — so a content type gaining or losing a text-bearing field
/// must change ALL FOUR together, or the reranker, the embedded plane, the
/// assembled pack, and the annotation producers disagree on a unit's text. Every
/// copy is a module-private `fn` in another package's owned file, so importing one
/// would require widening its visibility from a file this package does not own AND
/// would couple `assembly/` to `query/`/`projections/`/`annotations/` (R11); a
/// local copy behind this link comment is the decoupled choice, at the cost of
/// this duplication.
fn evidence_text(content_type: ContentType, body: &Value) -> Option<String> {
    match content_type {
        ContentType::TextBlock | ContentType::Caption => body
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::TableCell => body
            .get("text")
            .and_then(|value| value.as_str())
            .or_else(|| body.get("normalizedText").and_then(|value| value.as_str()))
            .map(str::to_string),
        ContentType::CodeBlock => body
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
