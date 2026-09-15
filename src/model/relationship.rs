//! UnitRelationship graph model (SPEC-epub §2.4, superseding canonical §19).
//! Durable canonical relationships are structural only; semantic or
//! retrieval relationships belong in SemanticAnnotation or
//! RetrievalProjection state, and relationships never cross a source
//! boundary.

use serde::{Deserialize, Serialize};

use crate::model::provenance::Provenance;

/// Spec §19. One directed structural edge between two ContentUnits of the
/// same source and parse; the canonical structure is this graph, with
/// `ContentUnit.primaryParentId`/`sequenceIndex` as convenience fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct UnitRelationship {
    pub(crate) id: String,

    pub(crate) source_id: String,
    pub(crate) parse_id: String,

    pub(crate) from_unit_id: String,
    pub(crate) to_unit_id: String,

    pub(crate) relationship_type: UnitRelationshipType,
    /// SPEC-epub §2.4: only `references` carries a role, one of
    /// `RELATIONSHIP_ROLES_REFERENCES`; every other type leaves it absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relationship_role: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sequence_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) confidence: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provenance: Option<Provenance>,

    pub(crate) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deleted_at: Option<String>,
}

/// SPEC-epub §2.4 `relationshipType`. Closed set of structural relationship
/// types (section contains block, caption caption_of figure, unit appears_on
/// page, block references footnote, ...). Section resolution walks
/// `contains` upward to the nearest `text_section`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UnitRelationshipType {
    Contains,
    Precedes,
    AppearsOn,
    CaptionOf,
    HasCaption,
    References,
}

/// SPEC-epub §2.4 `relationshipRole` values permitted on a `references`
/// edge. No other relationship type carries a role.
// Emitted by the EPUB worker (Phase 4); no core consumer reads it by name
// yet, so the definition is dead code until that worker lands.
#[allow(dead_code)]
pub(crate) const RELATIONSHIP_ROLES_REFERENCES: [&str; 3] =
    ["footnote", "cross_reference", "index_locator"];

impl UnitRelationshipType {
    /// The SPEC-epub §2.4 wire name of this relationship type, for
    /// string-keyed records and persisted columns. Must stay in sync with
    /// the serde `rename_all = "snake_case"` names above; the exhaustive
    /// match makes a new variant a compile error here. Mirrors
    /// `ContentType::wire_name`.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Contains => "contains",
            Self::Precedes => "precedes",
            Self::AppearsOn => "appears_on",
            Self::CaptionOf => "caption_of",
            Self::HasCaption => "has_caption",
            Self::References => "references",
        }
    }
}
