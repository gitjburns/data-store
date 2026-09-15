//! Canonical ContentUnit envelope and the closed ContentType enumeration
//! (SPEC-epub §2.1, superseding canonical §15, §15.1). The untyped `body` is
//! validated against its contentType by
//! `crate::model::body::content_type_body_matches` (§15.2).

use serde::{Deserialize, Serialize};

use crate::model::locator::Locator;

/// Spec §15. The canonical evidence unit of a parse. The spec's `TBody`
/// generic is carried here as untyped JSON; the §15.2 mapping to the
/// thirteen typed bodies (SPEC-epub §2.2) is enforced at creation by
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

/// SPEC-epub §2.1. Closed set of content unit types. `document` is the
/// single root unit of a parse; `page` is a print-page marker;
/// `text_section` is a logical container; `text_block` is the atomic
/// textual evidence unit; `list`, `list_item`, and `aside` are containers;
/// `table`, `table_row`, and `table_cell` are the tabular decomposition;
/// `figure` is a visual object; `caption` is an independent caption unit;
/// `code_block` is a code fragment. Evidence-bearing types (text feeds
/// chunking, ColBERT, annotation, passages) are `text_block`, `caption`,
/// `table_cell`, and `code_block` only. Each type requires its specific
/// SPEC-epub §2.2 body (§15.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContentType {
    Document,
    Page,
    TextSection,
    TextBlock,
    List,
    ListItem,
    Aside,
    Table,
    TableRow,
    TableCell,
    Figure,
    Caption,
    CodeBlock,
}

impl ContentType {
    /// SPEC-epub §2.1 wire name of this content type, for error messages and
    /// string-keyed records. Must stay in sync with the serde
    /// `rename_all = "snake_case"` names above; the exhaustive match makes a
    /// new variant a compile error here.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Page => "page",
            Self::TextSection => "text_section",
            Self::TextBlock => "text_block",
            Self::List => "list",
            Self::ListItem => "list_item",
            Self::Aside => "aside",
            Self::Table => "table",
            Self::TableRow => "table_row",
            Self::TableCell => "table_cell",
            Self::Figure => "figure",
            Self::Caption => "caption",
            Self::CodeBlock => "code_block",
        }
    }
}
