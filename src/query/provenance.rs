//! Wire types shared by the service and CLI for observed retrieval attribution.
//! Membership describes this query's candidate lists, not a counterfactual
//! guarantee that a passage would disappear without one of the channels.

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

/// How a query matched a stored NORMALIZED entity name (D9 amendment, CA2-P2
/// 2026-07-19). Strength order is RULED: `Exact` is strongest, then `Acronym`,
/// then `TokenPrefix`. `#[derive(Ord)]` makes `Exact < Acronym < TokenPrefix`
/// (declaration order), i.e. "stronger first" — a plain ascending sort places
/// exact-derived matches before fuzzy ones, and acronym before token-prefix,
/// exactly as the ruled ordering key requires. This ordinal ranks WITHIN a D9
/// tier, strictly BELOW the tier discriminator and strictly ABOVE matched-name
/// length (ordering key = tier, class, matched-name length, unitId).
///
/// Never double-classify: a stored name a query matches EXACTLY is `Exact` and
/// is never also recorded as a fuzzy class (the fuzzy scan skips names already
/// matched exactly — see `fuzzy_matched_names`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MatchClass {
    /// Normalized query n-gram == stored name (existing behavior, always on).
    Exact,
    /// A normalized query token == the first-letter acronym of a stored name.
    Acronym,
    /// Each normalized query token is a prefix of the corresponding stored-name
    /// token, in order (a leading-subsequence prefix match).
    TokenPrefix,
}

/// Attribution for the retrieved units retained in one final passage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RetrievalProvenance {
    pub(crate) channels: Vec<RetrievalChannel>,
    pub(crate) annotation_contribution: AnnotationContribution,
    pub(crate) matched_units: Vec<UnitRetrievalMatch>,
    /// Surrounding passage units that were not admitted to the fused pool.
    pub(crate) context_unit_ids: Vec<String>,
}

/// Whether annotations supplied additional unit matches in this query's lists.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnnotationContribution {
    None,
    Overlap,
    AdditionalMatches,
}

/// Per-unit memberships preserve overlaps that a fused hit's single tag cannot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UnitRetrievalMatch {
    pub(crate) unit_id: String,
    pub(crate) channels: Vec<RetrievalChannel>,
    pub(crate) graph_matches: Vec<GraphMatch>,
    pub(crate) dense_matches: Vec<DenseRetrievalMatch>,
}

/// Distinguish fine passage discovery from section-guided nomination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DenseRepresentation {
    Passage,
    Section,
}

/// Exact dense artifact that nominated a unit; section evidence never extends
/// to neighboring units outside that window's canonical input mapping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DenseRetrievalMatch {
    pub(crate) representation: DenseRepresentation,
    pub(crate) chunk_id: Option<String>,
    pub(crate) section_window_id: Option<String>,
    pub(crate) section_id: Option<String>,
    pub(crate) section_path: Vec<String>,
}

/// A query-matched normalized name and the actual path that reached a unit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphMatch {
    pub(crate) matched_entity: String,
    pub(crate) match_class: MatchClass,
    #[serde(flatten)]
    pub(crate) reach: GraphReach,
}

/// Keep direct mentions distinct from relation support and far-entity mentions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum GraphReach {
    DirectMention,
    RelationSupport { relationship: GraphRelationship },
    RelatedEntityMention { relationship: GraphRelationship },
}

/// A stored normalized triple in subject-to-object direction, even for an
/// incoming traversal. Supporting units identify where the relation was asserted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphRelationship {
    pub(crate) subject: String,
    pub(crate) predicate: String,
    pub(crate) object: String,
    pub(crate) supporting_unit_ids: Vec<String>,
}
