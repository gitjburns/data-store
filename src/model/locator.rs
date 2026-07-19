//! Locators (spec §17): the eight closed locator kinds mapping ContentUnits
//! back to source positions. Locators are durable canonical provenance;
//! retrieval projections must never be the only path back to evidence.

// Consumed from C4 onward; remove when C4 wires it.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// Spec §17. Closed union of the eight locator kinds, discriminated on the
/// wire by the `kind` field. Each variant's payload struct carries
/// `deny_unknown_fields`, so an unknown `kind` and an unknown payload field
/// both reject at deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Locator {
    PageBbox(PageBboxLocator),
    CharRange(CharRangeLocator),
    ByteRange(ByteRangeLocator),
    TimeRange(TimeRangeLocator),
    DomPath(DomPathLocator),
    XmlPath(XmlPathLocator),
    TableCell(TableCellLocator),
    RepoPath(RepoPathLocator),
}

/// Spec §17 `PageBBoxLocator` (kind `page_bbox`): a bounding box on a
/// physical page.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageBboxLocator {
    pub(crate) page_number: u64,
    /// `[x0, y0, x1, y1]` in the declared coordinate system.
    pub(crate) bbox: [f64; 4],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) coordinate_system: Option<CoordinateSystem>,
}

/// Spec §17 `PageBBoxLocator.coordinateSystem`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CoordinateSystem {
    PdfPoints,
    Pixels,
    Normalized,
}

/// Spec §17 `CharRangeLocator` (kind `char_range`): a character offset range.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CharRangeLocator {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

/// Spec §17 `ByteRangeLocator` (kind `byte_range`): a byte offset range.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ByteRangeLocator {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

/// Spec §17 `TimeRangeLocator` (kind `time_range`): a millisecond time range.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TimeRangeLocator {
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
}

/// Spec §17 `DomPathLocator` (kind `dom_path`): a path into an HTML DOM.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DomPathLocator {
    pub(crate) path: String,
}

/// Spec §17 `XmlPathLocator` (kind `xml_path`): a path into an XML document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct XmlPathLocator {
    pub(crate) path: String,
}

/// Spec §17 `TableCellLocator` (kind `table_cell`): a cell position within a
/// table, with optional spans.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TableCellLocator {
    pub(crate) row_index: u64,
    pub(crate) column_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_span: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) column_span: Option<u64>,
}

/// Spec §17 `RepoPathLocator` (kind `repo_path`): a file path in a
/// repository, optionally pinned to a line range and commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RepoPathLocator {
    pub(crate) path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) commit: Option<String>,
}
