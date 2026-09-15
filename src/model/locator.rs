//! Locators (SPEC-epub §2.3, superseding canonical §17): the two closed
//! locator kinds mapping ContentUnits back to source positions. Locators are
//! durable canonical provenance; retrieval projections must never be the
//! only path back to evidence.

use serde::{Deserialize, Serialize};

/// SPEC-epub §2.3. Closed union of the two locator kinds, discriminated on
/// the wire by the `kind` field. Each variant's payload struct carries
/// `deny_unknown_fields`, so an unknown `kind` and an unknown payload field
/// both reject at deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Locator {
    DomPath(DomPathLocator),
    CharRange(CharRangeLocator),
}

/// SPEC-epub §2.3 `DomPathLocator` (kind `dom_path`): an element path into
/// one content document of a package (EPUB), optionally narrowed to a range
/// of that element's child nodes.
// Constructed by the EPUB worker (Phase 4) and read only through serde
// until a core consumer inspects its fields by name; the derived impls do
// not count as reads for the dead-code lint.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DomPathLocator {
    /// Package-relative href of the content document.
    pub(crate) document: String,
    /// Element path (SPEC-epub §9.1).
    pub(crate) path: String,
    /// The element's `id` attribute when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) element_id: Option<String>,
    /// Inclusive 0-based child-node index range within the element,
    /// counting every node kind (elements, text, comments alike).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) node_range: Option<[u64; 2]>,
}

/// SPEC-epub §2.3 `CharRangeLocator` (kind `char_range`): a character
/// offset range; emitted by the plain-text worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CharRangeLocator {
    pub(crate) start: u64,
    pub(crate) end: u64,
}
