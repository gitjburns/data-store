//! Typed ContentUnit bodies (spec §18) and the contentType-to-body mapping
//! validator (spec §15.2). Typed bodies preserve source-derived structure;
//! Markdown renderings are derived views, never substitutes.

// Consumed from C4 onward; remove when C4 wires it.
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
        ContentType::Page => typed_body_check::<PageBody>(body),
        ContentType::TextSection => typed_body_check::<TextSectionBody>(body),
        ContentType::TextBlock => typed_body_check::<TextBlockBody>(body),
        ContentType::Table => typed_body_check::<TableBody>(body),
        ContentType::TableRow => typed_body_check::<TableRowBody>(body),
        ContentType::TableCell => typed_body_check::<TableCellBody>(body),
        ContentType::Figure => typed_body_check::<FigureBody>(body),
        ContentType::Caption => typed_body_check::<CaptionBody>(body),
        ContentType::ImageRegion => typed_body_check::<ImageRegionBody>(body),
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

/// Spec §18 `PageBody`: the physical page container for `page` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageBody {
    pub(crate) page_number: u64,
    pub(crate) width: f64,
    pub(crate) height: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rotation: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rendered_image_uri: Option<String>,
}

/// Spec §18 `TextSectionBody`: the logical section container for
/// `text_section` units. A container, not a paragraph (§15.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TextSectionBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) heading_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) heading_level: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) section_path: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_text: Option<String>,
}

/// Spec §18 `TextBlockBody`: the atomic textual evidence unit for
/// `text_block` units (§15.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TextBlockBody {
    pub(crate) text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) block_role: Option<TextBlockRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
}

/// Spec §18 `TextBlockBody.blockRole`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TextBlockRole {
    Paragraph,
    Heading,
    ListItem,
    Footnote,
    Header,
    Footer,
    Quote,
    Formula,
    Unknown,
}

/// Spec §18 `TableBody`: the table container for `table` units. Normalized
/// renderings are supplements to the decomposed rows/cells, not substitutes
/// (§15.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caption: Option<String>,
    pub(crate) row_count: u64,
    pub(crate) column_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<Vec<TableHeader>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_markdown: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_csv_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_html_uri: Option<String>,
}

/// Spec §18 `TableHeader`: one header cell declaration within `TableBody`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableHeader {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) column_index: Option<u64>,
    pub(crate) text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) span: Option<TableHeaderSpan>,
}

/// Spec §18 `TableHeader.span`: the inline `{ rowSpan?, columnSpan? }`
/// object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableHeaderSpan {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_span: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) column_span: Option<u64>,
}

/// Spec §18 `TableRowBody`: one row of a decomposed table for `table_row`
/// units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableRowBody {
    pub(crate) row_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) role: Option<TableRowRole>,
}

/// Spec §18 `TableRowBody.role`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableRowRole {
    Header,
    Body,
    Footer,
}

/// Spec §18 `TableCellBody`: one cell of a decomposed table for `table_cell`
/// units; first-class when table retrieval matters (§15.2).
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_text: Option<String>,
    /// Spec allows `string | number | boolean | null`; explicit null is a
    /// present value distinct from an absent field (§1.1), hence the custom
    /// deserializer instead of plain `Option` null-collapsing.
    #[serde(
        default,
        deserialize_with = "some_table_cell_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) value: Option<TableCellValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value_type: Option<TableCellValueType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) header_refs: Option<Vec<String>>,
}

/// Spec §18 `TableCellBody.value`: closed union of primitive cell values.
/// `Null` represents an explicitly null cell value, which the spec treats as
/// distinct from the `value` field being absent. The JSON shapes of the
/// variants are disjoint, so untagged matching is unambiguous.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum TableCellValue {
    String(String),
    Number(serde_json::Number),
    Boolean(bool),
    Null,
}

/// Deserialize `TableCellBody.value` so explicit JSON null becomes
/// `Some(TableCellValue::Null)`: plain `Option` would map null to `None`,
/// collapsing the spec's present-null / absent distinction (§1.1).
fn some_table_cell_value<'de, D>(deserializer: D) -> Result<Option<TableCellValue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    TableCellValue::deserialize(deserializer).map(Some)
}

/// Spec §18 `TableCellBody.valueType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableCellValueType {
    String,
    Number,
    Date,
    Boolean,
    Currency,
    Unknown,
}

/// Spec §18 `FigureBody`: a visual object for `figure` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FigureBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caption: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) alt_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) figure_type: Option<FigureType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ocr_text: Option<String>,
}

/// Spec §18 `FigureBody.figureType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FigureType {
    Chart,
    Diagram,
    Photo,
    Screenshot,
    Drawing,
    Unknown,
}

/// Spec §18 `CaptionBody`: an independent caption unit for `caption` units.
/// Post-import, pairing to figures/tables is authoritative via
/// caption_of/has_caption UnitRelationship edges; `captionForUnitIds` is a
/// spec-optional field this system's workers deliberately leave absent,
/// because the importer never remaps references embedded inside bodies and
/// bodyHash must stay purely content-derived (§16.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CaptionBody {
    pub(crate) text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caption_for_unit_ids: Option<Vec<String>>,
}

/// Spec §18 `ImageRegionBody`: a region inside an image for `image_region`
/// units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ImageRegionBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ocr_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) confidence: Option<f64>,
}

/// Spec §18 `CodeBlockBody`: a code fragment for `code_block` units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CodeBlockBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
    pub(crate) code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalized_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_line: Option<u64>,
}
