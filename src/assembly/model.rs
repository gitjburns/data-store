//! Assembly wire types (§25 AssemblyPolicy, §26 EvidencePack/EvidenceUnit,
//! §27 ContextAssemblyTrace). Types only: this module transcribes the spec
//! shapes and reuses the canonical `crate::model` enums (`ContentType`,
//! `UnitRelationshipType`, `Locator`, `SemanticAnnotation`, `UnitRelationship`)
//! rather than redefining them, so a change to the canonical model propagates
//! here as a compile error. The policy's self-hash seal, the fixed authoring
//! timestamp, and the `policyHash` JSON-key constant are C8a's concern in
//! `policy.rs`; this module only declares the `policy_hash` field.
//!
//! Wire conventions mirror `crate::model` and `crate::query::model` (§16.2):
//! `camelCase` field names, `deny_unknown_fields` on every struct,
//! `skip_serializing_if = "Option::is_none"` on spec-optional (`?`) fields so
//! omission stays distinct from explicit null, and closed string sets as
//! `snake_case` Rust enums matched exhaustively at every contract point.

use serde::{Deserialize, Serialize};

use crate::model::relationship::{UnitRelationship, UnitRelationshipType};
use crate::model::unit::ContentType;
use crate::model::{Locator, SemanticAnnotation};

/// Maximum ranked passages accepted by the query and retained by assembly.
pub(crate) const MAX_QUERY_RESULTS: usize = 100;

/// Bound canonical membership independently of a passage's displayed token count.
pub(crate) const MAX_PASSAGE_UNITS: usize = 64;

/// Versioned, self-hashed contract for retaining canonical passage evidence.
/// The sealed policy records inclusion behavior and raw evidence safety bounds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssemblyPolicy {
    /// Stable policy identity (spec `id`).
    pub(crate) id: String,
    /// Monotonic policy version (spec `version`).
    pub(crate) version: String,
    /// Human-readable description of this policy (spec `description?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,

    /// Hard ceilings the assembler must not exceed (spec `budgets`).
    pub(crate) budgets: AssemblyBudget,
    /// Inclusion rules recorded for final passages (spec `rules`).
    pub(crate) rules: Vec<AssemblyRule>,

    /// Historical graph-expansion requirements; absent in the active policy,
    /// which retains selected members without requiring graph expansion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) requires_relationship_types: Option<Vec<UnitRelationshipType>>,

    /// Fixed authoring timestamp of this policy document (spec `createdAt`).
    /// Sealed policies use a fixed string, never the runtime clock, so the
    /// self-hash is reproducible.
    pub(crate) created_at: String,
    /// Who authored the policy (spec `createdBy?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_by: Option<String>,

    /// Self-hash over the canonical serialization with this field excluded
    /// (spec `policyHash`). Sealed and verified by `policy.rs` (C8a); this
    /// module only declares the field.
    pub(crate) policy_hash: String,
}

/// Raw canonical evidence safety ceilings. Exceeding a ceiling fails assembly;
/// these do not limit displayed passage text or the requested result count.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssemblyBudget {
    /// Maximum number of evidence units in a pack (spec `maxEvidenceUnits`).
    pub(crate) max_evidence_units: u32,
    /// Maximum total token budget across the pack's text projections
    /// (spec `maxTokens`).
    pub(crate) max_tokens: u32,
    /// Graph-expansion depth; zero for final-passage retention.
    pub(crate) max_expansion_depth: u32,
    /// Optional cap on explicitly-referenced units pulled in by the
    /// `include_explicit_references` operator (spec `maxReferencedUnits?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_referenced_units: Option<u32>,
}

/// Spec §25 `AssemblyPolicy.rules[]`. One inclusion rule: when its `when`
/// condition matches an anchor, its `apply` operations run and every unit they
/// add is attributed to `reason` in the trace (§27).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssemblyRule {
    /// Stable rule identity, recorded in the trace's applied-rule entries
    /// (spec `id`).
    pub(crate) id: String,
    /// Human-readable description of the rule (spec `description?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// The condition under which this rule fires for an anchor (spec `when`).
    pub(crate) when: AssemblyCondition,
    /// The operations to apply when the condition matches (spec `apply`).
    pub(crate) apply: Vec<AssemblyOperation>,
    /// The trace reason attributed to every unit this rule adds (spec `reason`).
    pub(crate) reason: AssemblyReason,
}

/// Spec §25/§27 assembly reason. Why a unit was included; the single source of
/// truth for both `AssemblyRule.reason` (§25) and `AppliedAssemblyRule.reason`
/// (§27), so the two contract points can never drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AssemblyReason {
    /// The canonical unit contributes to a final ranked passage.
    SelectedPassage,
    /// The unit is a reranked anchor hit.
    Anchor,
    /// The unit completes a required structure (e.g. a caption's figure).
    RequiredCompletion,
    /// The unit provides structural context (parent container, heading path).
    StructuralContext,
    /// The unit is reached by an explicit reference edge.
    ExplicitReference,
    /// The unit continues or neighbors the anchor in reading order.
    LocalContinuity,
}

/// Spec §25 `AssemblyRule.when`. The condition matched against an anchor hit;
/// all present fields must match (conjunction). Every field is optional, so an
/// all-`None` condition matches every anchor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssemblyCondition {
    /// The hit type the anchor must have (spec `hitType?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hit_type: Option<AssemblyHitTypeCondition>,
    /// The content types the anchor's unit must be one of (spec `contentType?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content_type: Option<Vec<ContentType>>,
    /// Relationship types the anchor must have at least one outgoing edge of
    /// (spec `hasOutgoingRelationships?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_outgoing_relationships: Option<Vec<UnitRelationshipType>>,
    /// Relationship types the anchor must have at least one incoming edge of
    /// (spec `hasIncomingRelationships?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_incoming_relationships: Option<Vec<UnitRelationshipType>>,
}

/// Spec §25 `AssemblyCondition.hitType`. The hit-type selector for a rule,
/// including the `any` wildcard. Distinct from `crate::query::model`'s
/// `RetrievalHitType` because it adds the wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AssemblyHitTypeCondition {
    /// Matches any hit type.
    Any,
    Chunk,
    ContentUnit,
    SemanticAnnotation,
    RetrievalProjection,
}

/// Serialized inclusion operation and any parameters captured in its policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssemblyOperation {
    /// The inclusion operation represented by this policy rule.
    pub(crate) operator: AssemblyOperator,
    /// Free-form operator parameters, shape defined per operator (spec
    /// `parameters?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parameters: Option<serde_json::Value>,
}

/// Serialized inclusion vocabulary. Earlier variants remain readable for
/// historical policies; the active policy retains only selected passages.
// The shared `Include` prefix mirrors the normative §25 operator vocabulary
// (include_anchor, include_parent_container, ...) and must not be renamed.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AssemblyOperator {
    /// Retain canonical members supplied by final passage selection.
    IncludeSelectedPassage,
    /// Include the anchor unit itself.
    IncludeAnchor,
    /// Include the anchor's parent container unit.
    IncludeParentContainer,
    /// Include the chain of containers up to the heading/root.
    IncludeHeadingPath,
    /// Include a figure/table's caption (or a caption's figure/table).
    IncludeCaptionPair,
    /// Include units reached by explicit reference edges.
    IncludeExplicitReferences,
    /// Include the anchor's continuation chain.
    IncludeContinuationChain,
    /// Include the anchor's ±1 reading-order neighbors.
    IncludeTextNeighbors,
}

/// Spec §26 `EvidencePack`. The assembled result of a query: the canonical
/// content units selected for the answer, optional relationships/annotations
/// among them, and the trace that makes selection auditable (§27).
///
/// Per-query freshness (`EvidenceFreshness`) is intentionally omitted: it is
/// deferred with the QueryExecutionRecord audit tier and is not part of the
/// MVP pack.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EvidencePack {
    /// The query this pack answers (spec `queryId`).
    pub(crate) query_id: String,
    /// The query text, echoed for downstream consumers (spec `queryText`).
    pub(crate) query_text: String,
    /// The selected canonical content units, in deterministic pack order
    /// (spec `evidenceUnits`).
    pub(crate) evidence_units: Vec<EvidenceUnit>,
    /// Canonical relationships whose endpoints are both selected units, present
    /// only when the request set `includeRelationships` (spec `relationships?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relationships: Option<Vec<UnitRelationship>>,
    /// Semantic annotations targeting the selected units, present only when the
    /// request set `includeAnnotations` (spec `annotations?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) annotations: Option<Vec<SemanticAnnotation>>,
    /// The assembly trace explaining every inclusion and rejection (spec
    /// `assemblyTrace`).
    pub(crate) assembly_trace: ContextAssemblyTrace,
    /// When this pack was assembled (spec `createdAt`).
    pub(crate) created_at: String,
}

/// Spec §26 `EvidencePack.evidenceUnits[]`. One canonical ContentUnit rendered
/// for evidence, carrying its body, an optional text projection, and optional
/// locators/score/reasons. Only canonical units from captured active parses
/// appear here (§26 structural invariant, enforced by resolving every unit
/// against a captured `parse_id`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EvidenceUnit {
    /// Canonical unit identity (spec `unitId`).
    pub(crate) unit_id: String,
    /// Source that owns the unit (spec `sourceId`).
    pub(crate) source_id: String,
    /// Active parse the unit belongs to (spec `parseId`).
    pub(crate) parse_id: String,
    /// The unit's content type (spec `contentType`).
    pub(crate) content_type: ContentType,
    /// The unit's canonical §18 body payload (spec `body`).
    pub(crate) body: serde_json::Value,
    /// Extracted plain text for ranking/answering, when the content type has
    /// one (spec `textProjection?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text_projection: Option<String>,
    /// Source locators for the unit, present when the request set
    /// `includeSourceLocators` and the unit has any (spec `locators?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) locators: Option<Vec<Locator>>,
    /// The unit's rerank score, present for anchor units (spec `score?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) score: Option<f64>,
    /// Human-readable inclusion reasons for debug surfaces (spec `reasons?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reasons: Option<Vec<String>>,
}

/// Spec §27 `ContextAssemblyTrace`. The auditable record of an assembly run:
/// the policy identity/hash used, the input hits, every applied rule, the
/// selected and rejected ids, and the budget in force. Makes each pack
/// reproducible and every inclusion attributable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContextAssemblyTrace {
    /// The policy id used for this assembly (spec `assemblyPolicyId`).
    pub(crate) assembly_policy_id: String,
    /// The policy version used (spec `assemblyPolicyVersion`).
    pub(crate) assembly_policy_version: String,
    /// The policy self-hash used (spec `assemblyPolicyHash`).
    pub(crate) assembly_policy_hash: String,
    /// The input hit ids fed to assembly, in rank order (spec `inputHitIds`).
    pub(crate) input_hit_ids: Vec<String>,
    /// One retention rule application per final passage, including fully
    /// overlapping passages whose units were already retained.
    pub(crate) applied_rules: Vec<AppliedAssemblyRule>,
    /// The unit ids selected into the pack, in pack order (spec
    /// `selectedUnitIds`).
    pub(crate) selected_unit_ids: Vec<String>,
    /// Hits rejected before selection, e.g. out of scope or over budget
    /// (spec `rejectedHitIds?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rejected_hit_ids: Option<Vec<String>>,
    /// Historical rejected members; absent in v2 because missing members and
    /// exceeded raw safety bounds fail assembly instead of dropping evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rejected_unit_ids: Option<Vec<String>>,
    /// The budget in force for this assembly (spec `budget`).
    pub(crate) budget: AssemblyBudget,
}

/// Spec §27 `ContextAssemblyTrace.appliedRules[]`. One rule application: which
/// rule fired against which anchor and which units it added, with the reason
/// attributed to those units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AppliedAssemblyRule {
    /// The rule that fired (spec `ruleId`).
    pub(crate) rule_id: String,
    /// The anchor unit the rule fired against, when unit-grained
    /// (spec `anchorUnitId?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) anchor_unit_id: Option<String>,
    /// The anchor hit the rule fired against, when hit-grained
    /// (spec `anchorHitId?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) anchor_hit_id: Option<String>,
    /// The unit ids this application added to the pack (spec `addedUnitIds`).
    pub(crate) added_unit_ids: Vec<String>,
    /// The reason attributed to the added units (spec `reason`).
    pub(crate) reason: AssemblyReason,
}
