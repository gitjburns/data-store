//! Canonical ContentUnit envelope and the closed ContentType enumeration
//! (spec §15, §15.1). The untyped `body` is validated against its
//! contentType by `crate::model::body::content_type_body_matches` (§15.2).

// Consumed from C4 onward; remove when C4 wires it.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::model::locator::Locator;

/// Spec §15. The canonical evidence unit of a parse. The spec's `TBody`
/// generic is carried here as untyped JSON; the §15.2 mapping to the ten
/// typed bodies (§18) is enforced at creation by
/// `content_type_body_matches`, a §13.1 hard-gate invariant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContentUnit {
    pub(crate) id: String,

    pub(crate) source_id: String,
    pub(crate) parse_id: String,

    pub(crate) content_type: ContentType,

    pub(crate) body_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) structure_hash: Option<String>,

    /// Convenience fields only; the canonical structure is the
    /// UnitRelationship graph (§19).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sequence_index: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) locators: Option<Vec<Locator>>,

    pub(crate) body: serde_json::Value,

    pub(crate) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deleted_at: Option<String>,
}

/// Spec §15.1. Closed set of content unit types: `page` is a physical page
/// container, `text_section` a logical section container, `text_block` the
/// atomic textual evidence unit; each type requires its specific §18 body
/// (§15.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContentType {
    Page,
    TextSection,
    TextBlock,
    Table,
    TableRow,
    TableCell,
    Figure,
    Caption,
    ImageRegion,
    CodeBlock,
}

impl ContentType {
    /// Spec §15.1 wire name of this content type, for error messages and
    /// string-keyed records. Must stay in sync with the serde
    /// `rename_all = "snake_case"` names above; the exhaustive match makes a
    /// new variant a compile error here.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Page => "page",
            Self::TextSection => "text_section",
            Self::TextBlock => "text_block",
            Self::Table => "table",
            Self::TableRow => "table_row",
            Self::TableCell => "table_cell",
            Self::Figure => "figure",
            Self::Caption => "caption",
            Self::ImageRegion => "image_region",
            Self::CodeBlock => "code_block",
        }
    }
}
