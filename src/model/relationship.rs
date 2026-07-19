//! UnitRelationship graph model (spec §19). Durable canonical relationships
//! are structural only; semantic or retrieval relationships belong in
//! SemanticAnnotation or RetrievalProjection state, and relationships never
//! cross a source boundary.

// Consumed from C4 onward; remove when C4 wires it.
#![allow(dead_code)]

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

/// Spec §19 `relationshipType`. Closed set of structural relationship types
/// (page contains block, caption caption_of figure, block continues_on
/// block, unit appears_on page, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UnitRelationshipType {
    Contains,
    PhysicallyContains,
    LogicallyContains,
    Precedes,
    Follows,
    AppearsOn,
    CaptionOf,
    HasCaption,
    References,
    ContinuesOn,
    DerivedFrom,
}

impl UnitRelationshipType {
    /// The spec §19 wire name of this relationship type, for string-keyed
    /// records and persisted columns. Must stay in sync with the serde
    /// `rename_all = "snake_case"` names above; the exhaustive match makes a
    /// new variant a compile error here. Mirrors `ContentType::wire_name`.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Contains => "contains",
            Self::PhysicallyContains => "physically_contains",
            Self::LogicallyContains => "logically_contains",
            Self::Precedes => "precedes",
            Self::Follows => "follows",
            Self::AppearsOn => "appears_on",
            Self::CaptionOf => "caption_of",
            Self::HasCaption => "has_caption",
            Self::References => "references",
            Self::ContinuesOn => "continues_on",
            Self::DerivedFrom => "derived_from",
        }
    }
}
