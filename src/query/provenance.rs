//! Wire types shared by the service and CLI for observed retrieval attribution.
//! Membership describes this query's candidate lists, not a counterfactual
//! guarantee that a passage would disappear without one of the channels.

use serde::{Deserialize, Serialize};

/// One cited range of a unit's evidence text, Unicode scalar offsets, end
/// exclusive: the wire form of a grain fragment (PLAN-grains Section 2). It
/// is declared here rather than reusing `projections::chunk::Fragment` because
/// the CLI binary path-includes this module and has no `projections` tree.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceFragment {
    pub(crate) unit_id: String,
    pub(crate) start_char: usize,
    pub(crate) end_char: usize,
}

/// Discovery mechanism recorded for a hit. Graph and semantic share the final
/// annotation fusion contribution, while their attribution remains distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalChannel {
    /// Exact-cosine dense retrieval over the per-parse dense plane.
    Dense,
    /// FTS5/BM25 lexical retrieval over chunk targeting text.
    Lexical,
    /// Semantic-graph traversal from entity-name matches (D9).
    Graph,
    /// Dense matching of stored annotation meaning, grouped with graph for fusion.
    Semantic,
}

/// How a query matched a stored NORMALIZED entity name (D9 amendment, CA2-P2
/// 2026-07-19). Strength order is RULED: `Exact` is strongest, then `Acronym`,
/// then `TokenPrefix`, then embedding-derived `Semantic`. Declaration order
/// means "stronger first" — a plain ascending sort places
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
    /// An entity annotation was selected by its embedding, not by query spelling.
    Semantic,
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
    /// Exact annotation-derived candidates retained in this unit's final passages.
    #[serde(default)]
    pub(crate) annotation_matches: Vec<AnnotationMatch>,
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

/// Exact canonical text, independent of model tokenization or projection version:
/// the fragments (PLAN-grains Section 2 membership records, Unicode scalar
/// offsets, end exclusive) in reading order, and the hash of the exact UTF-8
/// text they compose. Invariant: the text is the fragment slices in order, and
/// the only characters between, before, or after them are whitespace (the tab
/// or blank-line join of the grain, or the remnant of one at a model-window
/// boundary). A displayed unit range is one single-fragment excerpt.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceExcerpt {
    pub(crate) fragments: Vec<SourceFragment>,
    pub(crate) text_hash: String,
}

/// Search representations retain their distinct roles while sharing one annotation vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnnotationRepresentation {
    Entity,
    Relation,
    Summary,
    Combined,
    Source,
}

impl AnnotationRepresentation {
    /// A shared human-readable type label for model input framing and client attribution.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Relation => "relationship",
            Self::Summary => "summary",
            Self::Combined => "combined annotations",
            Self::Source => "source excerpt",
        }
    }
}

/// Recorded model-input match; annotation bodies remain controlled by evidence options.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AnnotationMatch {
    pub(crate) projection_id: String,
    pub(crate) representation_id: String,
    pub(crate) representation: AnnotationRepresentation,
    pub(crate) annotation_ids: Vec<String>,
    pub(crate) excerpt: SourceExcerpt,
    /// False for historical annotations that identify whole units without extraction offsets.
    pub(crate) exact_annotation_range: bool,
}
