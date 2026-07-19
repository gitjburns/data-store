//! SemanticAnnotation model (spec §21): derived, parse-scoped semantic
//! artifacts with provenance. The CA cluster (MVP tier) wires these shapes:
//! the annotation store (`crate::annotations::store`, CAa) persists and reads
//! them, and the CA producers/worker build them. The `body` generic (`TBody`)
//! is carried as untyped JSON; typed annotation bodies remain deferred.

use serde::{Deserialize, Serialize};

use crate::model::provenance::Provenance;

/// Spec §21. A derived semantic artifact over one or more target units,
/// queryable only while its parse is active, rebuilt within the parse
/// lifecycle and never migrated across parser versions. The spec's `TBody`
/// generic is carried as untyped JSON; typed annotation bodies are not
/// defined in this revision's implemented tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SemanticAnnotation {
    pub(crate) id: String,

    pub(crate) source_id: String,
    pub(crate) parse_id: String,

    pub(crate) target_unit_ids: Vec<String>,

    pub(crate) annotation_type: SemanticAnnotationType,
    pub(crate) body: serde_json::Value,

    pub(crate) provenance: Provenance,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) confidence: Option<f64>,

    /// Makes post-activation annotation builds visible truth, never silent
    /// absence (§21 rule 3).
    pub(crate) freshness_status: AnnotationFreshnessStatus,

    pub(crate) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deleted_at: Option<String>,
}

/// Spec §21 `annotationType`. Closed set of semantic annotation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SemanticAnnotationType {
    Entity,
    Claim,
    Topic,
    Summary,
    Keyword,
    Classification,
    Relation,
    QuestionAnswer,
    TableInterpretation,
    FigureInterpretation,
}

/// Spec §21 `freshnessStatus`. Build state of an annotation relative to its
/// active parse. Distinct from RetrievalProjection freshness (§22), which
/// additionally allows `superseded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnnotationFreshnessStatus {
    Fresh,
    Stale,
    Building,
    Failed,
}
