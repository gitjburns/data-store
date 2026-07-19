//! Sealed MVP `AssemblyPolicy` document (§25) and its policy surface: the
//! versioned, self-hashed v1 policy that governs how reranked hits expand into
//! an `EvidencePack`, plus the R14 dependency check that surfaces when the
//! relationship types the policy's rules traverse are absent from a captured
//! parse. Owned by package C8a.
//!
//! The policy is DATA, not code: `active_policy()` reads a compile-sealed
//! document rather than a config field, so a future policy version changes
//! assembly behavior without touching any consumer — the same discipline the
//! §21.4 required-annotation-set policy (`crate::annotations::policy`) and the
//! §24.2 retrieval profile (`crate::query::profile`) use.
//!
//! The `assembly_policy.changed` SystemEvent variant EXISTS
//! (`crate::model::event::SystemEvent::AssemblyPolicyChanged`, wire
//! `"assembly_policy.changed"`) but is NOT minted in this cluster: like the
//! sealed §21.4 policy and §24.2 profile, this document is compile-sealed and
//! its identity is fixed by `id`/`version`/`policy_hash`, so there is no runtime
//! change to event. The variant is reserved for a future externally-authored
//! policy path (§35).

use std::sync::OnceLock;

use rusqlite::{Connection, OptionalExtension, params};
use tracing::{info, warn};

use crate::error::ApiError;
use crate::model::parse::ConformanceReport;
use crate::model::relationship::UnitRelationshipType;
use crate::model::unit::ContentType;

use crate::assembly::model::{
    AssemblyBudget, AssemblyCondition, AssemblyOperation, AssemblyOperator, AssemblyPolicy,
    AssemblyReason, AssemblyRule,
};

/// JSON field name the policy stores its self-hash under. Named beside the
/// producer so the `seal_mvp_policy` builder and the
/// `canonical_sha256_hex_without_field` call cannot drift onto differently
/// named hash fields (mirrors `crate::annotations::policy`).
const POLICY_HASH_JSON_KEY: &str = "policyHash";

/// Fixed authoring timestamp of the sealed v1 policy document. A versioned
/// document's identity (id + version + hash) must be stable across processes,
/// so `createdAt` is a FIXED string and never the runtime clock — otherwise the
/// self-hash would change every boot. Mirrors `seal_mvp_policy` in
/// `crate::annotations::policy` and `seal_mvp_profile` in `crate::query::profile`.
const POLICY_CREATED_AT: &str = "2026-07-15T00:00:00.000Z";

/// The active MVP `AssemblyPolicy` document (§25), lazily sealed once and
/// reused. Fallible because sealing computes the self-hash, which serializes the
/// document and can fail loudly on a serde rename; the crate's explicit-`Result`
/// policy forbids panicking in the `OnceLock` initializer, so the resolved
/// `Result` is stored and each caller re-observes any sealing failure.
/// Consumers (the C8b assembly builder) read the document here rather than by
/// construction, so a future policy version changes behavior without changing
/// them.
pub(crate) fn active_policy() -> Result<&'static AssemblyPolicy, ApiError> {
    static POLICY: OnceLock<Result<AssemblyPolicy, ApiError>> = OnceLock::new();
    // `ApiError` is not `Clone`, so a stored sealing failure is re-surfaced by
    // reconstructing an `InternalIo` from its rendered message. Sealing only
    // ever fails with `InternalIo` (canonicalization), so the rendered text is
    // faithful and the source context is preserved. Mirrors
    // `crate::annotations::policy::active_policy`.
    match POLICY.get_or_init(seal_mvp_policy) {
        Ok(policy) => Ok(policy),
        Err(source) => Err(ApiError::InternalIo {
            message: source.to_string(),
        }),
    }
}

/// The relationship types the v1 rules' operators genuinely traverse (§25
/// `requiresRelationshipTypes`). This is declared as what the RULES NEED, not
/// what any parser promises: `continues_on` and `references` appear here even
/// though NO parse worker emits them (`pdf_worker` emits containment/precedes/
/// appears_on/caption edges; `text_worker` emits only `precedes`). That absence
/// is DELIBERATE — the R14 dependency check (`unmet_dependencies`) exists to
/// surface those inert edge types loudly at assembly time (§25.1: "visibly
/// inert, never silently no-op"), so a later parser gaining `continues_on`
/// activates the continuation-chain rule with no code change.
///
/// The set is derived from the operators the v1 rules invoke:
/// - `include_parent_container` / `include_heading_path` read containment:
///   `contains`, `physically_contains`, `logically_contains`.
/// - `include_caption_pair` reads `has_caption` (figure/table → caption) and
///   `caption_of` (caption → target).
/// - `include_continuation_chain` reads `continues_on` (never emitted).
/// - `include_text_neighbors` reads `precedes`/`follows` in reading order.
/// - `references` is required because the policy externalizes the
///   `include_explicit_references` operator (implemented in `operators.rs`)
///   even though no v1 rule invokes it and no parser emits the edge.
const REQUIRED_RELATIONSHIP_TYPES: [UnitRelationshipType; 9] = [
    UnitRelationshipType::Contains,
    UnitRelationshipType::PhysicallyContains,
    UnitRelationshipType::LogicallyContains,
    UnitRelationshipType::HasCaption,
    UnitRelationshipType::CaptionOf,
    UnitRelationshipType::ContinuesOn,
    UnitRelationshipType::References,
    UnitRelationshipType::Precedes,
    UnitRelationshipType::Follows,
];

/// Build and seal the MVP v1 `AssemblyPolicy` (R5 approved content — the values
/// below must not drift). `createdAt` is the FIXED authoring timestamp
/// (`POLICY_CREATED_AT`), never runtime state, so the self-hash is reproducible
/// across processes. The document self-hashes over its canonical serialization
/// with the `policyHash` field excluded, exactly as declared profiles/policies
/// do (§16.2).
fn seal_mvp_policy() -> Result<AssemblyPolicy, ApiError> {
    let mut policy = AssemblyPolicy {
        id: "assembly-policy".to_string(),
        version: "1".to_string(),
        description: Some(
            "MVP v1 assembly policy (spec §25): anchor inclusion plus structural \
             context (parent container, heading path), required completion (caption \
             pair), and local continuity (continuation chain, ±1 text neighbors). No \
             absolute quality thresholds."
                .to_string(),
        ),
        budgets: AssemblyBudget {
            // 30 evidence units per pack (R5 approved).
            max_evidence_units: 30,
            // 30 units × the 512-token banked-unit cap = 15360 tokens (R5).
            max_tokens: 15_360,
            // v1 expands at most one hop from each anchor (R5).
            max_expansion_depth: 1,
            // No separate cap on explicitly-referenced units: v1 rules do not
            // invoke `include_explicit_references`, so nothing pulls referenced
            // units and a distinct ceiling would be inert. `None` keeps the
            // shared `max_evidence_units` budget authoritative.
            max_referenced_units: None,
        },
        rules: vec![
            anchor_rule(),
            structural_context_rule(),
            required_completion_rule(),
            continuation_chain_rule(),
            text_neighbors_rule(),
        ],
        requires_relationship_types: Some(REQUIRED_RELATIONSHIP_TYPES.to_vec()),
        // Fixed authoring timestamp of this versioned document; see fn comment.
        created_at: POLICY_CREATED_AT.to_string(),
        created_by: None,
        policy_hash: String::new(),
    };
    // Self-hash over the document minus its own hash field (§16.2); the shared
    // helper fails loudly if a serde rename ever drops the field.
    policy.policy_hash =
        crate::canonical::canonical_sha256_hex_without_field(&policy, POLICY_HASH_JSON_KEY)?;
    Ok(policy)
}

/// v1 anchor rule: include every reranked anchor hit itself, whatever its type.
/// An all-`None` `when` matches every anchor.
fn anchor_rule() -> AssemblyRule {
    AssemblyRule {
        id: "anchor".to_string(),
        description: Some("Include every reranked anchor hit.".to_string()),
        when: AssemblyCondition {
            hit_type: None,
            content_type: None,
            has_outgoing_relationships: None,
            has_incoming_relationships: None,
        },
        apply: vec![operation(AssemblyOperator::IncludeAnchor)],
        reason: AssemblyReason::Anchor,
    }
}

/// v1 structural-context rule: for text-bearing anchors, add the parent
/// container and the heading path above them so the evidence carries its
/// structural setting.
fn structural_context_rule() -> AssemblyRule {
    AssemblyRule {
        id: "structural-context".to_string(),
        description: Some(
            "Add parent container and heading path for text-bearing anchors.".to_string(),
        ),
        when: AssemblyCondition {
            hit_type: None,
            content_type: Some(vec![
                ContentType::TextBlock,
                ContentType::TableCell,
                ContentType::CodeBlock,
                ContentType::Caption,
            ]),
            has_outgoing_relationships: None,
            has_incoming_relationships: None,
        },
        apply: vec![
            operation(AssemblyOperator::IncludeParentContainer),
            operation(AssemblyOperator::IncludeHeadingPath),
        ],
        reason: AssemblyReason::StructuralContext,
    }
}

/// v1 required-completion rule: for figure/table anchors, pull in the paired
/// caption so the evidence is not a figure without its caption.
fn required_completion_rule() -> AssemblyRule {
    AssemblyRule {
        id: "required-completion".to_string(),
        description: Some("Pull the caption pair for figure/table anchors.".to_string()),
        when: AssemblyCondition {
            hit_type: None,
            content_type: Some(vec![ContentType::Figure, ContentType::Table]),
            has_outgoing_relationships: None,
            has_incoming_relationships: None,
        },
        apply: vec![operation(AssemblyOperator::IncludeCaptionPair)],
        reason: AssemblyReason::RequiredCompletion,
    }
}

/// v1 local-continuity rule (continuation): follow the `continues_on` chain from
/// any anchor. INERT under the MVP corpus — no parser emits `continues_on` — and
/// the R14 check warns on that; the rule activates unchanged once a parser does.
fn continuation_chain_rule() -> AssemblyRule {
    AssemblyRule {
        id: "continuation-chain".to_string(),
        description: Some("Follow the continuation chain from the anchor.".to_string()),
        when: AssemblyCondition {
            hit_type: None,
            content_type: None,
            has_outgoing_relationships: None,
            has_incoming_relationships: None,
        },
        apply: vec![operation(AssemblyOperator::IncludeContinuationChain)],
        reason: AssemblyReason::LocalContinuity,
    }
}

/// v1 local-continuity rule (neighbors): add the ±1 reading-order neighbors of a
/// text_block anchor so a retrieved sentence carries its immediate context.
fn text_neighbors_rule() -> AssemblyRule {
    AssemblyRule {
        id: "text-neighbors".to_string(),
        description: Some("Add the ±1 reading-order neighbors of a text_block anchor.".to_string()),
        when: AssemblyCondition {
            hit_type: None,
            content_type: Some(vec![ContentType::TextBlock]),
            has_outgoing_relationships: None,
            has_incoming_relationships: None,
        },
        apply: vec![operation(AssemblyOperator::IncludeTextNeighbors)],
        reason: AssemblyReason::LocalContinuity,
    }
}

/// One parameterless operator invocation. v1 operators take no `parameters`;
/// the depth/count bounds live in the budgets, not per-operation config.
fn operation(operator: AssemblyOperator) -> AssemblyOperation {
    AssemblyOperation {
        operator,
        parameters: None,
    }
}

/// Ordered SELECT of a parse run's stored conformance report (§12.5). Keyed on
/// the parse-run id (`parse_runs.id`); the column is nullable, so a run without
/// a report reads as `None`.
const SELECT_CONFORMANCE_REPORT_SQL: &str = "
SELECT conformance_report_json
FROM parse_runs
WHERE id = ?1";

/// Read the measured §12.5 conformance report for one active parse, parse-keyed.
/// Returns `None` when the row is absent or its `conformance_report_json` is
/// NULL (a parse that recorded no report). Surfaces read/decode failures with
/// the parse identity via `ApiError::StorageOperation` (mirrors
/// `crate::query::rerank::resolve_unit_content`).
///
/// The report's `relationship_type_counts` (a `BTreeMap<String, u64>` keyed by
/// relationship-type WIRE name) is what the R14 check intersects against the
/// policy's `requiresRelationshipTypes`. Private helper of `unmet_dependencies`,
/// which is the check's public entry point.
fn read_conformance_report(
    conn: &Connection,
    parse_id: &str,
) -> Result<Option<ConformanceReport>, ApiError> {
    let row = conn
        .query_row(SELECT_CONFORMANCE_REPORT_SQL, params![parse_id], |row| {
            row.get::<_, Option<String>>(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read conformance report for parse {parse_id}: {source}"),
        })?;

    // Outer `None` = no such parse row; inner `None` = row present but the
    // report column is NULL. Both mean "no report to check" for R14.
    let Some(Some(report_json)) = row else {
        return Ok(None);
    };

    let report: ConformanceReport =
        serde_json::from_str(&report_json).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted conformance report for parse {parse_id} is unparseable: {source}"
            ),
        })?;
    Ok(Some(report))
}

/// One captured active parse the R14 check inspects: its parse-run id plus the
/// source it belongs to. Mirrors the (source_id, parse_id) shape the C8b/C8d-2
/// capture layer already holds, so the caller passes borrowed primitives and
/// `assembly/` stays decoupled from the query pipeline's capture types (R11).
///
/// `source_id` is part of the C8b-facing contract: the R14 check keys only on
/// `parse_id`, but the evidence builder's annotation collection
/// (`evidence.rs::collect_annotations`) reads `source_id`, so both fields are
/// live.
pub(crate) struct CapturedParseRef<'a> {
    pub(crate) source_id: &'a str,
    pub(crate) parse_id: &'a str,
}

/// R14 dependency check (§25.1): for every relationship type the policy's rules
/// require, intersect it against each captured parse's measured
/// `relationship_type_counts` and return the unmet set — the required types that
/// are absent or zero-count in at least one captured parse, each mapped to the
/// number of parses missing it.
///
/// The check NEVER errors on an unmet dependency: an inert relationship type is
/// WARNED, not fatal (§25.1: "visibly inert, never silently no-op"). It only
/// returns `Err` when a conformance report cannot be read/decoded — a real
/// storage fault, distinct from an expected absence. `count == 0` and
/// "type absent from the map" are treated identically (both are unmet).
///
/// Diagnostics (DIAGNOSTICS-ONBOARDING.md): logs the check start, then emits
/// exactly ONE aggregate `warn` line for the whole query when anything is unmet,
/// summarizing each missing relationship type → count of affected parses. It
/// does NOT emit a warn per `(parse, type)`: per-parse lines over a ~120-source
/// corpus are prohibited noisy logging (AGENTS.md). Only compact safe
/// identifiers are logged (missing types' wire names + affected-parse counts);
/// no parse_ids are enumerated, no document contents, no vectors.
///
/// C8b invokes this at assembly start and lets the warn fire; the returned set
/// lets C8b record the inertness in the trace if it chooses.
pub(crate) fn unmet_dependencies(
    conn: &Connection,
    policy: &AssemblyPolicy,
    captured: &[CapturedParseRef<'_>],
    query_id: &str,
) -> Result<Vec<UnitRelationshipType>, ApiError> {
    info!(
        event = "assembly.dependency_check.started",
        query_id,
        captured_parse_count = captured.len(),
        "assembly R14 relationship-dependency check started"
    );

    // No declared requirements → nothing to check; the policy traverses no
    // edges the corpus must supply.
    let Some(required) = policy.requires_relationship_types.as_ref() else {
        return Ok(Vec::new());
    };

    // Wire name → affected-parse count, aggregated across all captured parses.
    // A required type is counted once per parse whose report lacks it (or has a
    // zero count); the map is the ONE aggregate warn line's payload.
    let mut affected_parses: std::collections::BTreeMap<&'static str, u64> =
        std::collections::BTreeMap::new();
    // Unmet types in the policy's declared order, deduped: `UnitRelationshipType`
    // is not `Ord` (no `BTreeSet`), so insertion-order dedupe over a `Vec` keeps
    // the result deterministic (R13) without touching the model enum.
    let mut unmet: Vec<UnitRelationshipType> = Vec::new();

    for parse in captured {
        // A parse with no report cannot satisfy any requirement; treat every
        // required type as unmet for it. This keeps the check conservative:
        // absence of measurement is surfaced, never assumed satisfied.
        let counts = read_conformance_report(conn, parse.parse_id)?
            .map(|report| report.relationship_type_counts);

        for required_type in required {
            let wire = required_type.wire_name();
            let present = counts
                .as_ref()
                .and_then(|map| map.get(wire))
                .is_some_and(|&count| count > 0);
            if !present {
                if !unmet.contains(required_type) {
                    unmet.push(*required_type);
                }
                *affected_parses.entry(wire).or_insert(0) += 1;
            }
        }
    }

    if !affected_parses.is_empty() {
        // ONE aggregate line per query (amendment): structured `unmet` field
        // maps each missing relationship type's wire name to how many captured
        // parses lack it. No per-parse lines; no parse_ids enumerated.
        warn!(
            event = "assembly.dependency_check.unmet",
            query_id,
            captured_parse_count = captured.len(),
            unmet = ?affected_parses,
            "assembly policy requires relationship types absent from captured parses; \
             the corresponding rules are visibly inert (§25.1)"
        );
    }

    Ok(unmet)
}
