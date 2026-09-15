//! Typed ContentUnit bodies (SPEC-epub §2.2, superseding canonical §18) and
//! the contentType-to-body mapping validator (spec §15.2). Typed bodies
//! preserve source-derived structure. No body field may reference another
//! unit; pairing and containment are relationships only (§16.1 body-hash
//! rule).
//!
//! The closed sets `SectionKind`, `TextBlockRole`, `ListKind`, `AsideKind`,
//! and `TableRowRole` are defined once here; the EPUB worker imports them
//! and declares no parallel copies (SPEC-epub §13.2).

// Body structs exist to be validated by strict deserialization in
// `content_type_body_matches` and to be emitted by workers; the core never
// reads most of their fields by name, so the field-level dead-code lint is
// silenced for this module.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::model::unit::ContentType;

/// Spec §15.2 / §13.1 hard gate: verify that a ContentUnit's untyped JSON
/// `body` deserializes as exactly the typed body its `contentType` requires.
/// Every body struct carries `deny_unknown_fields`, so both missing required
/// fields and undeclared fields reject. The match is exhaustive over
/// `ContentType`, so a new content type cannot compile without a body
/// mapping. This is the validation hook the import path (C4b) calls.
pub(crate) fn content_type_body_matches(
    content_type: ContentType,
    body: &serde_json::Value,
) -> Result<(), ApiError> {
    let result = match content_type {
        ContentType::Document => typed_body_check::<DocumentBody>(body),
        ContentType::Page => typed_body_check::<PageBody>(body),
        ContentType::TextSection => typed_body_check::<TextSectionBody>(body),
        ContentType::TextBlock => typed_body_check::<TextBlockBody>(body),
        ContentType::List => typed_body_check::<ListBody>(body),
        ContentType::ListItem => typed_body_check::<ListItemBody>(body),
        ContentType::Aside => typed_body_check::<AsideBody>(body),
        ContentType::Table => typed_body_check::<TableBody>(body),
        ContentType::TableRow => typed_body_check::<TableRowBody>(body),
        ContentType::TableCell => typed_body_check::<TableCellBody>(body),
        ContentType::Figure => typed_body_check::<FigureBody>(body),
        ContentType::Caption => typed_body_check::<CaptionBody>(body),
        ContentType::CodeBlock => typed_body_check::<CodeBlockBody>(body),
    };
    // A mismatch is a demonstrable structural fault (§13.1): surface the
    // content type and the exact shape error so the failure record explains
    // itself, and reject the unit.
    result.map_err(|error| ApiError::BadRequest {
        message: format!(
            "content unit body does not match contentType \"{}\": {error}",
            content_type.wire_name()
        ),
    })
}

/// Attempt strict typed deserialization of `body` as `T`, discarding the
/// value: only conformance matters here, not the parsed result.
fn typed_body_check<T: serde::de::DeserializeOwned>(
    body: &serde_json::Value,
) -> Result<(), serde_json::Error> {
    T::deserialize(body).map(|_: T| ())
}

/// SPEC-epub §2.2 `DocumentBody`: source metadata carried by the single
/// root `document` unit of a parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DocumentBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) creators: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) publisher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) identifiers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
}

/// SPEC-epub §2.2 `PageBody`: a print-page marker for `page` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageBody {
    /// 1-based position among the parse's page markers.
    pub(crate) ordinal: u64,
    /// Printed folio as declared, e.g. "xiv", "218".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
}

/// SPEC-epub §2.2 `TextSectionBody`: the logical section container for
/// `text_section` units. A container, not a paragraph (§15.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TextSectionBody {
    pub(crate) kind: SectionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) heading_text: Option<String>,
    /// Depth in the section tree; children of `document` are level 1.
    pub(crate) heading_level: u64,
    /// Declared number as matched, e.g. "Chapter 1.", "3.2.1.".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
    /// Heading trail from level 1 to this section, inclusive.
    pub(crate) section_path: Vec<String>,
}

/// SPEC-epub §2.2 `SectionKind`: the closed set of `text_section` kinds.
/// `Unknown` is the kind for a section the worker could not classify;
/// `section_kind_coverage` (SPEC-epub §2.6) counts sections whose kind is
/// not `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SectionKind {
    Part,
    Chapter,
    Section,
    Preface,
    Foreword,
    Introduction,
    Prologue,
    Epilogue,
    Afterword,
    Conclusion,
    Appendix,
    Glossary,
    Bibliography,
    Index,
    Notes,
    Acknowledgments,
    Dedication,
    Epigraph,
    Titlepage,
    CopyrightPage,
    Cover,
    Toc,
    Colophon,
    Unknown,
}

/// SPEC-epub §2.2 `TextBlockBody`: the atomic textual evidence unit for
/// `text_block` units (§15.2). `role` is required.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TextBlockBody {
    pub(crate) text: String,
    pub(crate) role: TextBlockRole,
    /// Declared marker: footnote number, item number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
    /// BCP 47 tag from the nearest declared language.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
}

/// SPEC-epub §2.2 `TextBlockRole`: the closed set of `text_block` roles.
/// There are no furniture roles (`header`/`footer` were removed in v0.4);
/// prose for passage assembly is `paragraph`, `quote`, `definition`, and
/// `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TextBlockRole {
    Paragraph,
    Heading,
    Title,
    Subtitle,
    Term,
    Definition,
    Footnote,
    Quote,
    Attribution,
    Formula,
    Unknown,
}

/// SPEC-epub §2.2 `ListBody`: the list container for `list` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ListBody {
    pub(crate) kind: ListKind,
    /// Declared start for ordered lists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start: Option<u64>,
}

/// SPEC-epub §2.2 `ListBody.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ListKind {
    Ordered,
    Unordered,
    Definition,
}

/// SPEC-epub §2.2 `ListItemBody`: one item container of a list for
/// `list_item` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ListItemBody {
    /// 1-based position within the list.
    pub(crate) ordinal: u64,
    /// Declared marker text when the source renders one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
}

/// SPEC-epub §2.2 `AsideBody`: the aside container for `aside` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AsideBody {
    pub(crate) kind: AsideKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
}

/// SPEC-epub §2.2 `AsideBody.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AsideKind {
    Note,
    Tip,
    Warning,
    Caution,
    Important,
    Sidebar,
    Epigraph,
    Example,
    Unknown,
}

/// SPEC-epub §2.2 `TableBody`: the table container for `table` units. The
/// decomposed rows/cells are `contains` children, not body fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caption: Option<String>,
    pub(crate) row_count: u64,
    pub(crate) column_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<Vec<TableHeader>>,
}

/// SPEC-epub §2.2 `TableHeader`: one header cell declaration within
/// `TableBody`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableHeader {
    pub(crate) row_index: u64,
    pub(crate) column_index: u64,
    pub(crate) text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_span: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) column_span: Option<u64>,
}

/// SPEC-epub §2.2 `TableRowBody`: one row of a decomposed table for
/// `table_row` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableRowBody {
    pub(crate) row_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) role: Option<TableRowRole>,
}

/// SPEC-epub §2.2 `TableRowBody.role`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableRowRole {
    Header,
    Body,
    Footer,
}

/// SPEC-epub §2.2 `TableCellBody`: one cell of a decomposed table for
/// `table_cell` units; first-class when table retrieval matters (§15.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableCellBody {
    pub(crate) row_index: u64,
    pub(crate) column_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_span: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) column_span: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text: Option<String>,
}

/// SPEC-epub §2.2 `FigureBody`: a visual object for `figure` units.
/// `image_hash` is the artifact-store key (SPEC-epub §2.7); the store
/// resolves hash to blob, so bodies are never rewritten at import.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FigureBody {
    /// SHA-256 hex of the archived image bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image_media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) alt_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caption: Option<String>,
}

/// SPEC-epub §2.2 `CaptionBody`: an independent caption unit for `caption`
/// units. Pairing to figures/tables is expressed only through
/// `caption_of`/`has_caption` UnitRelationship edges, never in the body
/// (§16.1 body-hash rule).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CaptionBody {
    pub(crate) text: String,
    /// E.g. "Figure 1-1.", "Table 3-1.".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
}

/// SPEC-epub §2.2 `CodeBlockBody`: a code fragment for `code_block` units.
/// `code` is the evidence text (the `textHash` projection for this type).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CodeBlockBody {
    pub(crate) code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
    /// E.g. "Example 2-1.".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
}
