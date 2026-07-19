//! Shared §24 retrieval-fabric contract types consumed across the C7 cluster:
//! the retrieval channel tag (§24.3), a ranked retrieval hit (§24.4), and a
//! resolved query scope (§24.2). Every channel produces `RetrievalHit`s over
//! one `RetrievalChannel`, the pipeline fuses and ranks them, and C8d resolves
//! them to canonical ContentUnits for EvidencePack construction.
//!
//! Wire conventions mirror `crate::model` (§16.2): `camelCase` field names,
//! `deny_unknown_fields` on structs, `skip_serializing_if` on spec-optional
//! (`?`) fields so omission stays distinct from explicit null, and closed
//! string enums as `snake_case` Rust enums matched exhaustively at every
//! contract point.

use serde::{Deserialize, Serialize};

/// Spec §24.3 `RetrievalChannel`. The candidate-generation channel a hit came
/// from. The spec's full string set is
/// `"lexical" | "learned_sparse" | "dense" | "multi_vector" | "graph" |
/// "semantic" | "temporal"`; the C7 MVP retrieves over exactly three channels
/// — dense, lexical, graph — so only those variants are defined here. This
/// keeps every match on the channel exhaustive with no phantom arms for
/// channels the MVP never emits (`multi_vector` channel deferred post-MVP,
/// 2026-07-15 rescope; the remaining spec channels are unimplemented). The
/// wire form is the spec's snake_case string literals, so a future channel is
/// added by name without a serde rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalChannel {
    /// Exact-cosine dense retrieval over the per-parse dense plane.
    Dense,
    /// FTS5/BM25 lexical retrieval over chunk targeting text.
    Lexical,
    /// Semantic-graph traversal from entity-name matches (D9).
    Graph,
}

/// Spec §24.4 `hitType`. What kind of artifact a hit targets. Closed set; the
/// C7 channels emit `chunk`/`content_unit`-grained hits, and annotation- and
/// projection-targeted hits round out the spec surface consumed at C8d.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalHitType {
    Chunk,
    ContentUnit,
    SemanticAnnotation,
    RetrievalProjection,
}

/// Spec §24.4 `RetrievalHit`. One ranked targeting/ranking artifact produced
/// by a channel and carried through fusion and rerank.
///
/// Invariant: hits are internal targeting/ranking artifacts, resolved to
/// canonical ContentUnits before EvidencePack construction. Scope filtering
/// (§6) is applied at candidate generation in every channel; ranked hits are
/// never post-filtered for scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetrievalHit {
    /// The kind of artifact this hit targets (§24.4 `hitType`).
    pub(crate) hit_type: RetrievalHitType,

    /// Identity of the targeted artifact (chunk id, unit id, annotation id, or
    /// projection id, per `hit_type`).
    pub(crate) hit_id: String,

    /// Source that owns the hit's parse.
    pub(crate) source_id: String,
    /// Active parse the hit was generated against (scope is enforced by
    /// confining candidate generation to the captured active parses).
    pub(crate) parse_id: String,
    /// Canonical ContentUnit ids this hit resolves to (chunk-grained hits are
    /// resolved to their owning unit before fusion).
    pub(crate) unit_ids: Vec<String>,

    /// Channel that generated this hit.
    pub(crate) channel: RetrievalChannel,
    /// Channel- or fusion-assigned score (higher is stronger).
    pub(crate) score: f64,
    /// Rank within the channel or fused result; absent until ranking assigns
    /// it (spec `rank?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rank: Option<u32>,

    /// The retrieval projection this hit matched, when the hit originates from
    /// a projection surface (spec `matchedProjectionId?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) matched_projection_id: Option<String>,
    /// The semantic annotation this hit matched, when applicable
    /// (spec `matchedAnnotationId?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) matched_annotation_id: Option<String>,

    /// Human-readable explanation of why the hit was produced, for debug/trace
    /// surfaces (spec `explanation?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) explanation: Option<String>,
}

/// Spec §24.2 `ResolvedScope.kind`. Whether a query targets all sources, an
/// explicit source set, or a governance-domain set. Default is `all` (§6
/// reservation 1: the default scope is all sources).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResolvedScopeKind {
    All,
    SourceSet,
    DomainSet,
}

/// Spec §24.2 `ResolvedScope`. The resolved scope the pipeline enforces at
/// candidate generation: it selects which sources' active parses a query may
/// scan. Scope is never applied as a post-filter over ranked hits (§38); it
/// bounds the captured active set the channels read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ResolvedScope {
    /// Which selection mode this scope uses (§24.2 `kind`).
    pub(crate) kind: ResolvedScopeKind,

    /// Governance domains in scope when `kind` is `domain_set`
    /// (spec `governanceDomains?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) governance_domains: Option<Vec<String>>,
    /// Explicit source ids in scope when `kind` is `source_set`
    /// (spec `sourceIds?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_ids: Option<Vec<String>>,
}
