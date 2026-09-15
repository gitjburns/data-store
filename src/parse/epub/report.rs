//! Raw structure report (SPEC-epub §11.3): `parser_raw/epub_structure.json`,
//! diagnostic evidence only. Carries no content text beyond navigation
//! labels and headings; unresolved links are recorded by href, never by
//! link text, and section kind rules are keyed by unit id, never by heading.
//!
//! Facts arrive incrementally during the §11.2 walk in whatever order the
//! emitting code reaches them; the report keeps documents in first-mention
//! order (spine order in practice, since the walk is in spine order) and
//! every list in call order, so the file reads as the walk happened.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::Serialize;

use crate::error::ApiError;
use crate::model::body::DocumentBody;

/// File name of the report inside the bundle's `parser_raw` directory
/// (SPEC-epub §11.2 step 6).
const REPORT_FILE_NAME: &str = "epub_structure.json";

/// One spine `itemref` as read (§5.2): non-linear items are emitted in
/// place and recorded here rather than dropped.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SpineEntry {
    idref: String,
    href: String,
    linear: bool,
}

/// One navigation node in preorder. The tree is flattened; `depth` (root
/// nodes are 0) recovers the nesting, so the file stays a plain list that
/// can be scanned without recursion.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NavEntry {
    depth: u64,
    label: String,
    /// `NavNode::href`: the target as written (`href#fragment`); absent for
    /// an entry that declares no target (an EPUB 3 `span`).
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    /// `NavNode::resolved`: false only when `target` is present and reached
    /// no archive member (§5.4).
    resolved: bool,
    /// Local id of the `text_section` emitted for the node; absent only
    /// when the node was never emitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    unit_local_id: Option<String>,
}

/// One page marker (§7.6) with its parse-wide ordinal and label as known
/// after the `page-list` override.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PageMarkerEntry {
    ordinal: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
}

/// Which §7.2 kind rule classified one emitted `text_section`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SectionKindEntry {
    unit_local_id: String,
    rule: u8,
}

/// Everything §11.3 records per content document. A document entry is
/// created by whichever fact is recorded first, so per-block facts that
/// arrive before `document_summary` are never lost; the summary fills the
/// count fields in place.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DocumentReport {
    href: String,
    /// Units emitted, keyed by content-type wire name.
    units_by_type: BTreeMap<String, u64>,
    /// Whitespace-only blocks dropped (§7.5).
    dropped_blocks: u64,
    /// Elements walked as generic containers, keyed by local name (§7.5).
    unknown_elements: BTreeMap<String, u64>,
    page_markers: Vec<PageMarkerEntry>,
    /// Internal link hrefs that resolved to no unit (§10.4), in source
    /// order.
    unresolved_links: Vec<String>,
    /// Caption pairings counted per §7.9 rule number.
    caption_pairings_by_rule: BTreeMap<u8, u64>,
}

impl DocumentReport {
    /// An entry with no facts yet, for the first mention of `href`.
    fn empty(href: &str) -> Self {
        Self {
            href: href.to_string(),
            units_by_type: BTreeMap::new(),
            dropped_blocks: 0,
            unknown_elements: BTreeMap::new(),
            page_markers: Vec::new(),
            unresolved_links: Vec::new(),
            caption_pairings_by_rule: BTreeMap::new(),
        }
    }
}

/// The §11.3 report: package facts, spine and navigation summaries, and
/// per-document mapping facts, written once after the walk.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StructureReport {
    package_version: String,
    /// Metadata exactly as `package.rs` read it into the `document` body.
    metadata: DocumentBody,
    /// Manifest item counts keyed by media type.
    manifest_counts: BTreeMap<String, u64>,
    spine: Vec<SpineEntry>,
    /// Navigation tree in preorder; see `NavEntry::depth`.
    navigation: Vec<NavEntry>,
    /// Per-document facts in first-mention order.
    documents: Vec<DocumentReport>,
    /// Kind rule per emitted section, in emission order.
    section_kind_rules: Vec<SectionKindEntry>,
    /// Position of each document's entry in `documents`, so per-block
    /// recording is a map lookup rather than a scan. Not part of the file.
    #[serde(skip)]
    document_index: BTreeMap<String, usize>,
}

impl StructureReport {
    /// Start a report for one package; metadata is recorded as read.
    pub(crate) fn new(package_version: &str, metadata: &DocumentBody) -> Self {
        Self {
            package_version: package_version.to_string(),
            // The contract passes metadata by reference while the package
            // keeps its own copy for the `document` unit; the report needs
            // an owned snapshot to serialize after the walk.
            metadata: metadata.clone(),
            manifest_counts: BTreeMap::new(),
            spine: Vec::new(),
            navigation: Vec::new(),
            documents: Vec::new(),
            section_kind_rules: Vec::new(),
            document_index: BTreeMap::new(),
        }
    }

    /// Record manifest item counts by media type; replaces any earlier
    /// counts, since the manifest is read once.
    pub(crate) fn manifest_counts(&mut self, counts: BTreeMap<String, u64>) {
        self.manifest_counts = counts;
    }

    /// Record one spine item with its linear flag, in spine order.
    pub(crate) fn spine_item(&mut self, idref: &str, href: &str, linear: bool) {
        self.spine.push(SpineEntry {
            idref: idref.to_string(),
            href: href.to_string(),
            linear,
        });
    }

    /// Record one navigation node in preorder with its tree depth (root
    /// nodes are 0), resolution status, and emitted unit id. The caller
    /// visits the tree in preorder so the flattened list reconstructs it.
    pub(crate) fn nav_node(
        &mut self,
        depth: u64,
        label: &str,
        target: Option<&str>,
        resolved: bool,
        unit_local_id: Option<&str>,
    ) {
        self.navigation.push(NavEntry {
            depth,
            label: label.to_string(),
            target: target.map(str::to_string),
            resolved,
            unit_local_id: unit_local_id.map(str::to_string),
        });
    }

    /// Record one content document's unit counts by type, dropped blocks,
    /// and unknown elements by local name. Called once per document after
    /// its walk; it overwrites those three fields and leaves the per-block
    /// facts recorded earlier for the same document untouched.
    pub(crate) fn document_summary(
        &mut self,
        href: &str,
        units_by_type: BTreeMap<String, u64>,
        dropped_blocks: u64,
        unknown_elements: BTreeMap<String, u64>,
    ) {
        let document = self.document(href);
        document.units_by_type = units_by_type;
        document.dropped_blocks = dropped_blocks;
        document.unknown_elements = unknown_elements;
    }

    /// Record one unresolved internal link by its href as written (§10.4).
    pub(crate) fn unresolved_link(&mut self, document: &str, href: &str) {
        self.document(document)
            .unresolved_links
            .push(href.to_string());
    }

    /// Count one caption pairing under its §7.9 rule number.
    pub(crate) fn caption_pairing(&mut self, document: &str, rule: u8) {
        *self
            .document(document)
            .caption_pairings_by_rule
            .entry(rule)
            .or_insert(0) += 1;
    }

    /// Record which §7.2 kind rule fired for one emitted section.
    pub(crate) fn section_kind_rule(&mut self, unit_local_id: &str, rule: u8) {
        self.section_kind_rules.push(SectionKindEntry {
            unit_local_id: unit_local_id.to_string(),
            rule,
        });
    }

    /// Unresolved internal links recorded so far for `href`, read by the
    /// walk for the `epub.document.mapped` event after link resolution.
    pub(crate) fn unresolved_link_count(&self, href: &str) -> u64 {
        self.document_index
            .get(href)
            .and_then(|index| self.documents.get(*index))
            .map_or(0, |document| document.unresolved_links.len() as u64)
    }

    /// Record one page marker with its parse-wide ordinal and label.
    pub(crate) fn page_marker(&mut self, document: &str, ordinal: u64, label: Option<&str>) {
        self.document(document).page_markers.push(PageMarkerEntry {
            ordinal,
            label: label.map(str::to_string),
        });
    }

    /// Write `epub_structure.json` into the bundle's `parser_raw`
    /// directory. Any failure is a staging fault (`ApiError`), never a
    /// statement about the source.
    pub(crate) fn write(&self, raw_dir: &Path) -> Result<(), ApiError> {
        let path = raw_dir.join(REPORT_FILE_NAME);
        let bytes = serde_json::to_vec(self).map_err(|source| ApiError::InternalIo {
            message: format!("failed to serialize {REPORT_FILE_NAME}: {source}"),
        })?;
        fs::write(&path, bytes).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to write {REPORT_FILE_NAME} at {}: {source}",
                path.display()
            ),
        })
    }

    /// The entry for `href`, created on first mention so facts may arrive
    /// in any order relative to `document_summary`.
    fn document(&mut self, href: &str) -> &mut DocumentReport {
        let index = match self.document_index.get(href) {
            Some(index) => *index,
            None => {
                let index = self.documents.len();
                self.documents.push(DocumentReport::empty(href));
                self.document_index.insert(href.to_string(), index);
                index
            }
        };
        // Proven in bounds: every index in `document_index` was assigned
        // as `documents.len()` immediately before the matching push, and
        // `documents` is never truncated.
        &mut self.documents[index]
    }
}
