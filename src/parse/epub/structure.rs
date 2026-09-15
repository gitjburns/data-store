//! Section tree and block walk (SPEC-epub §7.1, §7.4, §7.5, §7.6, §7.11,
//! §11.2 passes one and two). `walk_spine` owns the section stack, the
//! current-section rule, page ordinals, `appears_on` assignment,
//! title/subtitle detection, the one-block lookahead, the `document` unit,
//! and every child walk; it calls `blocks.rs` to emit each container or text
//! block and finishes with `links::resolve_all`.
//!
//! Edge ownership, fixed by `blocks.rs`: every emitter there emits the
//! `contains` edge for the units it emits and chains `precedes` only for
//! rows under a table and cells under a row. This module emits `contains`
//! for its own units (`document`, sections, pages), chains `precedes` for
//! every other sibling group through one `last_child` map keyed by parent,
//! and assigns `appears_on` to every leaf (§7.6).
//!
//! Structure only (§1.3): section kinds come from `kinds::kind_from_semantic`
//! and heading-opened subsections; the only text comparison is §7.11, which
//! matches block text against the section's own label, heading, and
//! navigation label.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use tracing::debug;

use crate::error::ApiError;
use crate::limits::EpubLimits;
use crate::model::body::{PageBody, SectionKind, TextBlockRole, TextSectionBody};
use crate::model::locator::{DomPathLocator, Locator};
use crate::model::{ContentType, UnitRelationshipType};
use crate::parse::bundle::CandidateContentUnit;
use crate::parse::epub::archive::Archive;
use crate::parse::epub::blocks::{self, BlockContext, BlockSource, CaptionCandidate};
use crate::parse::epub::kinds;
use crate::parse::epub::links::{self, FootnoteIndex, HrefOutcome, LinkRecord, UnitIndex};
use crate::parse::epub::navigation::{NavNode, Navigation, NavigationSource};
use crate::parse::epub::package::Package;
use crate::parse::epub::report::StructureReport;
use crate::parse::epub::text::{self, TextContext};
use crate::parse::epub::xhtml;
use crate::parse::epub::{Emitter, EpubFailure, EpubStage, WorkerResult};

/// §11.5 warning codes raised by this module.
const WARNING_NAV_TARGET_UNRESOLVED: &str = "epub_nav_target_unresolved";
const WARNING_UNKNOWN_ELEMENT: &str = "epub_unknown_element";
const WARNING_EMPTY_BLOCKS_DROPPED: &str = "epub_empty_blocks_dropped";
const WARNING_PAGE_MARKER_UNLABELED: &str = "epub_page_marker_unlabeled";

/// Element path of the package document, the `document` unit's locator
/// (§7.1, §9.2).
const PACKAGE_DOCUMENT_PATH: &str = "/package[1]";

/// Manifest `properties` token of the EPUB 3 navigation document; a spine
/// item naming it is skipped wholesale (§7.5).
const NAV_PROPERTY: &str = "nav";

/// `h1`–`h6` in number order; the index is the `h` number minus one.
const HEADING_ELEMENTS: [&str; 6] = ["h1", "h2", "h3", "h4", "h5", "h6"];

/// `epub:type` token of a pseudo-heading (§7.5 heading rule).
const TITLE_EPUB_TYPE: &str = "title";

/// `epub:type` / `data-type` value of a note reference (§10.3).
const NOTEREF_TYPE: &str = "noteref";

/// `epub:type` tokens and `data-type` value of a footnote container that
/// emits no `aside` (§7.7 last paragraph).
const FOOTNOTE_TYPES: [&str; 3] = ["footnote", "endnote", "rearnote"];

/// Element names that carry §7.2 rule 1 and 2 semantics for a section
/// besides the target element itself: the nearest enclosing `section` or
/// `div`, and the document `body`.
const KIND_CARRIERS: [&str; 3] = ["section", "div", "body"];

/// Names walked as generic containers by §7.7 (no unit of their own).
const GENERIC_CONTAINERS: [&str; 9] = [
    "section", "article", "div", "header", "footer", "main", "center", "details", "summary",
];

/// Block-level names §7.5 and §7.7 account for; any other non-inline element
/// reaching the walk is counted under `epub_unknown_element` (§7.5).
const KNOWN_BLOCK_ELEMENTS: [&str; 33] = [
    "p",
    "section",
    "article",
    "div",
    "aside",
    "blockquote",
    "figure",
    "figcaption",
    "table",
    "caption",
    "thead",
    "tbody",
    "tfoot",
    "tr",
    "td",
    "th",
    "colgroup",
    "col",
    "ul",
    "ol",
    "dl",
    "li",
    "dt",
    "dd",
    "pre",
    "math",
    "svg",
    "header",
    "footer",
    "main",
    "details",
    "summary",
    "center",
];

/// Characters stripped between a title and its subtitle in the navigation
/// label (§7.11): `:`, `.`, and the dashes.
const SUBTITLE_SEPARATORS: [char; 4] = [':', '.', '-', '\u{2013}'];

/// Em dash, also a §7.11 separator; listed apart so the array above stays
/// readable.
const EM_DASH: char = '\u{2014}';

/// Pass one (§11.2 step 4, plan Section 2): parse every spine document and
/// index every `(member, id)` whose element is, or lies inside, a footnote
/// block by §7.10 rules 1 and 2, or lies in a document classified `notes` at
/// document granularity (a navigation node targeting it, or the navigation
/// section current at its start, has kind `notes`). The "current" section
/// here is the last target passed in reading order, tracked across
/// documents by the document-order position of each target element.
pub(crate) fn index_footnotes(
    archive: &mut Archive,
    package: &Package,
    navigation: &Navigation,
    limits: &EpubLimits,
) -> Result<FootnoteIndex, EpubFailure> {
    let nav = NavTree::flatten(navigation);
    let nav_member = nav_document_member(package, navigation);
    let mut index = FootnoteIndex::new();
    // Kind of the navigation section current across document boundaries.
    let mut current_kind: Option<SectionKind> = None;
    for item in &package.spine {
        if nav_member == Some(item.href.as_str()) {
            continue;
        }
        let source = load_document(archive, &item.href, limits)?;
        let document = xhtml::parse(&source, &item.href, limits)
            .map_err(|failure| failure.with_stage(EpubStage::Document(item.href.to_string())))?;
        let ids = xhtml::id_index(&document);
        let body = body_element(&document);

        // Every navigation node targeting this document: its kind and, when
        // its target element exists, the element's document-order position.
        let mut any_notes = false;
        let mut last_passed: Option<(usize, SectionKind)> = None;
        for entry in &nav.entries {
            let Some(target) = entry.node.target.as_ref().filter(|t| t.member == item.href) else {
                continue;
            };
            let element = match target.fragment.as_deref() {
                None => Some(body),
                Some(fragment) => ids.get(fragment).and_then(|id| document.get_node(*id)),
            };
            let (kind, _) = match element {
                Some(element) => section_kind_of(element, entry.node.kind_hint),
                None => hint_kind(entry.node.kind_hint),
            };
            any_notes |= kind == SectionKind::Notes;
            if let Some(element) = element {
                let position = element.id().get_usize();
                if last_passed.is_none_or(|(last, _)| position >= last) {
                    last_passed = Some((position, kind));
                }
            }
        }
        let document_is_notes = any_notes || current_kind == Some(SectionKind::Notes);

        for (id, node_id) in &ids {
            let Some(node) = document.get_node(*node_id) else {
                continue;
            };
            // Rule 3 is applied at document granularity here; rules 1 and 2
            // are the ancestor check with no section kind.
            if document_is_notes || blocks::is_footnote_block(node, SectionKind::Unknown) {
                index.insert(&item.href, id);
            }
        }
        if let Some((_, kind)) = last_passed {
            current_kind = Some(kind);
        }
    }
    Ok(index)
}

/// Pass two (§11.2 step 5): emit the `document` unit, walk spine documents
/// in order emitting sections, blocks, pages, images, and report facts, then
/// resolve links and log one `epub.document.mapped` event per document.
/// The events are deferred until after `links::resolve_all` because each
/// document's unresolved-link count is known only then.
pub(crate) fn walk_spine(
    archive: &mut Archive,
    package: &Package,
    navigation: &Navigation,
    footnotes: &FootnoteIndex,
    limits: &EpubLimits,
    emitter: &mut Emitter,
    report: &mut StructureReport,
) -> WorkerResult<()> {
    let mut manifest_counts: BTreeMap<String, u64> = BTreeMap::new();
    for item in package.manifest.values() {
        *manifest_counts
            .entry(item.media_type.to_string())
            .or_insert(0) += 1;
    }
    report.manifest_counts(manifest_counts);
    for item in &package.spine {
        report.spine_item(&item.idref, &item.href, item.linear);
    }

    // §7.1: the document unit is the first unit and has no parent.
    let document_id = stream_unit(
        emitter,
        ContentType::Document,
        &package.metadata,
        None,
        package_locator(&package.href),
    )?;

    let mut walk = Walk {
        emitter,
        report,
        nav: NavTree::flatten(navigation),
        nav_sections: Vec::new(),
        sections: Vec::new(),
        document_id,
        current: None,
        heading_stack: Vec::new(),
        last_child: BTreeMap::new(),
        page_current: None,
        page_ordinal: 0,
        after_title: None,
        links: Vec::new(),
        units: UnitIndex::new(),
        documents: Vec::new(),
    };
    walk.nav_sections = vec![None; walk.nav.entries.len()];

    let nav_member = nav_document_member(package, navigation);
    let walked: Vec<&str> = package
        .spine
        .iter()
        .map(|item| item.href.as_str())
        .filter(|href| nav_member != Some(*href))
        .collect();
    for (position, member) in walked.iter().enumerate() {
        walk_document(
            &mut walk,
            member,
            DocumentPosition {
                first: position == 0,
                last: position + 1 == walked.len(),
            },
            archive,
            package,
            navigation,
            footnotes,
            limits,
        )?;
    }
    // Only reachable when every spine item was the navigation document:
    // rule 1 still makes every navigation node a section, located on the
    // package document because no content document was walked.
    if walked.is_empty() {
        for idx in 0..walk.nav.entries.len() {
            if walk.nav_sections[idx].is_none() {
                walk.emit_placeholder(idx, package_locator(&package.href))?;
            }
        }
    }

    links::resolve_all(&walk.links, &walk.units, walk.emitter, walk.report)?;

    for (idx, entry) in walk.nav.entries.iter().enumerate() {
        let unit = walk.nav_sections[idx].map(|section| walk.sections[section].id.as_str());
        walk.report.nav_node(
            entry.depth,
            &entry.node.label,
            entry.node.href.as_deref(),
            entry.node.resolved,
            unit,
        );
    }
    for stats in &walk.documents {
        debug!(
            event = "epub.document.mapped",
            href = stats.href,
            unit_count = stats.unit_count,
            dropped_blocks = stats.dropped_blocks,
            unresolved_links = walk.report.unresolved_link_count(&stats.href),
            page_markers = stats.page_markers,
            "EPUB content document mapped"
        );
    }
    Ok(())
}

/// Where a document sits among the walked spine items.
#[derive(Clone, Copy)]
struct DocumentPosition {
    first: bool,
    last: bool,
}

/// Read, decode, and entity-rewrite one content document; every failure is
/// staged to that document.
fn load_document(
    archive: &mut Archive,
    member: &str,
    limits: &EpubLimits,
) -> Result<String, EpubFailure> {
    let stage = || EpubStage::Document(member.to_string());
    let bytes = archive.read(member)?.ok_or_else(|| {
        EpubFailure::new(
            stage(),
            format!("spine member {member} is not in the archive"),
        )
    })?;
    let decoded =
        xhtml::decode(&bytes, member, limits).map_err(|failure| failure.with_stage(stage()))?;
    Ok(xhtml::rewrite_entities(&decoded))
}

/// Normalized member name of the EPUB 3 navigation document when it is the
/// chosen source; a spine item naming it is skipped (§7.5).
fn nav_document_member<'p>(package: &'p Package, navigation: &Navigation) -> Option<&'p str> {
    if navigation.source != NavigationSource::Nav {
        return None;
    }
    package
        .manifest
        .values()
        .find(|item| {
            item.properties
                .iter()
                .any(|property| property == NAV_PROPERTY)
        })
        .map(|item| item.href.as_str())
}

/// The `body` element of a content document, else the document element
/// when the member has none (the walk still visits its children).
fn body_element<'a, 'input>(
    document: &'a roxmltree::Document<'input>,
) -> roxmltree::Node<'a, 'input> {
    document
        .descendants()
        .find(|node| node.is_element() && xhtml::local_name(*node) == "body")
        .unwrap_or_else(|| document.root_element())
}

/// Locator of the package document (§9.2 `document` rule), also used for
/// navigation sections emitted when no content document was walked.
fn package_locator(package_href: &str) -> Locator {
    Locator::DomPath(DomPathLocator {
        document: package_href.to_string(),
        path: PACKAGE_DOCUMENT_PATH.to_string(),
        element_id: None,
        node_range: None,
    })
}

/// Stream one unit owned by this module with its locator and, when it has a
/// parent, its `contains` edge. Serialization of a typed body this module
/// built is a worker invariant; its failure is a fault, not a parse outcome.
fn stream_unit<B: Serialize>(
    emitter: &mut Emitter,
    content_type: ContentType,
    body: &B,
    parent: Option<&str>,
    locator: Locator,
) -> WorkerResult<String> {
    let local_id = emitter.next_local_id(content_type);
    let body = serde_json::to_value(body).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to serialize {} body: {source}",
            content_type.wire_name()
        ),
    })?;
    emitter.unit(CandidateContentUnit {
        // The unit record owns one copy of the id; the caller receives the
        // other for its edges and bookkeeping.
        local_id: local_id.clone(),
        content_type,
        body,
        parent_local_id: parent.map(str::to_string),
        // Overwritten by the emitter with the stream position.
        sequence_index: 0,
        locators: vec![locator],
    })?;
    if let Some(parent) = parent {
        emitter.relationship(parent, &local_id, UnitRelationshipType::Contains, None)?;
    }
    Ok(local_id)
}

/// One navigation node in the flattened tree; `depth` is 0 for roots and
/// `children` are indexes into the same list, in navigation order.
struct NavEntry<'n> {
    node: &'n NavNode,
    parent: Option<usize>,
    depth: u64,
    children: Vec<usize>,
}

/// The navigation tree flattened in preorder, so a parent's index is always
/// below its children's.
struct NavTree<'n> {
    entries: Vec<NavEntry<'n>>,
    roots: Vec<usize>,
    /// Per entry: the target's member was walked and its fragment id was
    /// absent, learned only when that document is parsed. Such a node is
    /// §7.4 rule 4 from then on (see `resolvable`).
    fragment_missing: Vec<bool>,
}

impl<'n> NavTree<'n> {
    /// Flatten `navigation.toc` in preorder.
    fn flatten(navigation: &'n Navigation) -> Self {
        let mut tree = Self {
            entries: Vec::new(),
            roots: Vec::new(),
            fragment_missing: Vec::new(),
        };
        for node in &navigation.toc {
            let idx = tree.push(node, None, 0);
            tree.roots.push(idx);
        }
        tree.fragment_missing = vec![false; tree.entries.len()];
        tree
    }

    /// Append `node` and its subtree; returns the node's index. Recursion
    /// depth is the navigation depth, bounded by `max_element_depth`.
    fn push(&mut self, node: &'n NavNode, parent: Option<usize>, depth: u64) -> usize {
        let idx = self.entries.len();
        self.entries.push(NavEntry {
            node,
            parent,
            depth,
            children: Vec::new(),
        });
        for child in &node.children {
            let child_idx = self.push(child, Some(idx), depth + 1);
            self.entries[idx].children.push(child_idx);
        }
        idx
    }

    /// The sibling list `idx` belongs to: its parent's children, or the
    /// roots.
    fn siblings(&self, idx: usize) -> &[usize] {
        match self.entries[idx].parent {
            Some(parent) => &self.entries[parent].children,
            None => &self.roots,
        }
    }

    /// Whether the node has a target that reached an archive member and is
    /// not yet known to name a missing fragment; any other node is emitted
    /// by §7.4 rule 4 next to its previous sibling or under its parent,
    /// never at a passed target.
    fn resolvable(&self, idx: usize) -> bool {
        self.entries[idx].node.target.is_some() && !self.fragment_missing[idx]
    }

    /// The node emitted immediately before `idx` under §7.4 rule 4: its
    /// previous sibling, else its parent; `None` for a first root, whose
    /// position is the document unit's.
    fn rule4_anchor(&self, idx: usize) -> Option<usize> {
        let siblings = self.siblings(idx);
        let position = siblings.iter().position(|sibling| *sibling == idx)?;
        match position.checked_sub(1) {
            Some(previous) => Some(siblings[previous]),
            None => self.entries[idx].parent,
        }
    }
}

/// One emitted `text_section` and the facts later units need from it.
struct SectionInfo {
    id: String,
    kind: SectionKind,
    /// `headingLevel`: children of `document` are 1.
    level: u64,
    /// `sectionPath` of the section, the prefix of every child's path.
    path: Vec<String>,
    label: Option<String>,
    heading_text: Option<String>,
    /// The navigation label, for §7.11 title and subtitle detection.
    nav_label: Option<String>,
}

/// One open heading-derived subsection (§7.4 rule 5).
struct HeadingFrame {
    /// Index into `Walk::sections`.
    section: usize,
    /// The `h` number that opened it; a heading of a number at or below
    /// this closes it.
    number: u8,
    /// Document-order id of the `section` element whose first heading
    /// opened it; the frame closes when that element ends.
    scope: Option<usize>,
}

/// Facts for one document's `epub.document.mapped` event, logged after
/// link resolution.
struct DocumentStats {
    href: String,
    unit_count: u64,
    dropped_blocks: u64,
    page_markers: u64,
}

/// Parse-wide walk state: the section tree, the current-section rule, page
/// ordinals, sibling chaining, and the link and unit indexes consumed by
/// `links::resolve_all`.
struct Walk<'w, 'a, 'n> {
    emitter: &'w mut Emitter<'a>,
    report: &'w mut StructureReport,
    nav: NavTree<'n>,
    /// Per navigation entry, the index into `sections` once emitted.
    nav_sections: Vec<Option<usize>>,
    sections: Vec<SectionInfo>,
    document_id: String,
    /// The navigation or synthesized section current by §7.4 rule 2 (index
    /// into `sections`); heading-derived subsections nest under it.
    current: Option<usize>,
    heading_stack: Vec<HeadingFrame>,
    /// Last emitted child per parent id, for `precedes` chaining (§10.2).
    last_child: BTreeMap<String, String>,
    /// Local id of the last page marker passed anywhere in the parse.
    page_current: Option<String>,
    page_ordinal: u64,
    /// Text of the `title` block just emitted in the current section, until
    /// any other block follows (§7.11 subtitle rule).
    after_title: Option<String>,
    links: Vec<LinkRecord>,
    units: UnitIndex,
    documents: Vec<DocumentStats>,
}

impl Walk<'_, '_, '_> {
    /// The innermost section content belongs to: the top heading-derived
    /// subsection, else the current navigation or synthesized section.
    fn effective_section(&self) -> Option<usize> {
        self.heading_stack
            .last()
            .map(|frame| frame.section)
            .or(self.current)
    }

    /// Local id of the unit content is contained by right now: the
    /// effective section, else `document` (§7.4 rule 7).
    fn effective_parent(&self) -> &str {
        self.effective_section()
            .map_or(self.document_id.as_str(), |section| {
                self.sections[section].id.as_str()
            })
    }

    /// Kind of the effective section, `unknown` before any section.
    fn section_kind(&self) -> SectionKind {
        self.effective_section()
            .map_or(SectionKind::Unknown, |section| self.sections[section].kind)
    }

    /// Chain `child` after the previous child emitted under `parent` with
    /// `precedes` (§10.2) and remember it as the last one.
    fn chain(&mut self, parent: &str, child: &str) -> WorkerResult<()> {
        if let Some(previous) = self.last_child.get(parent) {
            self.emitter
                .relationship(previous, child, UnitRelationshipType::Precedes, None)?;
        }
        self.last_child
            .insert(parent.to_string(), child.to_string());
        Ok(())
    }

    /// Emit one `text_section` under `parent_section` (or `document`) and
    /// record it; returns its index into `sections`. `heading_text` is
    /// never empty: callers fall back to the navigation label or href.
    #[allow(clippy::too_many_arguments)] // every argument is one §7.2/§7.3 fact of the section; a struct would only rename them
    fn emit_section(
        &mut self,
        kind: SectionKind,
        rule: u8,
        label: Option<String>,
        heading_text: String,
        nav_label: Option<String>,
        parent_section: Option<usize>,
        locator: Locator,
    ) -> WorkerResult<usize> {
        let (parent_id, level, mut path) = match parent_section {
            // The parent keeps its own id and path; this section's body and
            // edge need their own copies.
            Some(parent) => (
                self.sections[parent].id.to_string(),
                self.sections[parent].level,
                self.sections[parent].path.to_vec(),
            ),
            None => (self.document_id.to_string(), 0, Vec::new()),
        };
        // §7.3: label and heading joined by one space when both exist.
        path.push(match &label {
            Some(label) => format!("{label} {heading_text}"),
            None => heading_text.to_string(),
        });
        let body = TextSectionBody {
            kind,
            heading_text: Some(heading_text),
            heading_level: level + 1,
            label,
            section_path: path,
        };
        let id = stream_unit(
            self.emitter,
            ContentType::TextSection,
            &body,
            Some(&parent_id),
            locator,
        )?;
        self.chain(&parent_id, &id)?;
        self.report.section_kind_rule(&id, rule);
        self.sections.push(SectionInfo {
            id,
            kind,
            level: level + 1,
            path: body.section_path,
            label: body.label,
            heading_text: body.heading_text,
            nav_label,
        });
        Ok(self.sections.len() - 1)
    }

    /// Emit the section for navigation entry `idx` (§7.4 rules 1, 3, 4):
    /// ancestors not yet emitted are emitted first as placeholders so the
    /// tree nests as in navigation, then the §7.4 rule 4 followers (target-
    /// less children and following siblings) are emitted at this position.
    fn emit_nav_entry(
        &mut self,
        idx: usize,
        heading: (Option<String>, String),
        kind: (SectionKind, u8),
        locator: Locator,
    ) -> WorkerResult<usize> {
        if let Some(parent) = self.nav.entries[idx].parent
            && self.nav_sections[parent].is_none()
        {
            // The placeholder shares the locator of the node that forced
            // it; its own target, if any, is registered when passed.
            self.emit_placeholder(parent, locator.clone())?;
        }
        let parent_section = self.nav.entries[idx]
            .parent
            .and_then(|parent| self.nav_sections[parent]);
        let nav_label = self.nav.entries[idx].node.label.to_string();
        let section = self.emit_section(
            kind.0,
            kind.1,
            heading.0,
            heading.1,
            Some(nav_label),
            parent_section,
            locator.clone(),
        )?;
        self.nav_sections[idx] = Some(section);
        self.emit_followers(idx, locator)?;
        Ok(section)
    }

    /// Emit navigation entry `idx` with `headingText` from its label and
    /// kind from its landmark or guide hint (§7.4 rule 4, and ancestors
    /// emitted on demand). Never warns: the caller decides that.
    fn emit_placeholder(&mut self, idx: usize, locator: Locator) -> WorkerResult<usize> {
        let node = self.nav.entries[idx].node;
        let heading = (None, node.label.to_string());
        self.emit_nav_entry(idx, heading, hint_kind(node.kind_hint), locator)
    }

    /// §7.4 rule 4 chain after `idx` is emitted: its leading target-less
    /// children are emitted at its position, then its target-less following
    /// siblings immediately after it; each chain stops at the first node
    /// with a target, which is emitted when that target is passed.
    fn emit_followers(&mut self, idx: usize, locator: Locator) -> WorkerResult<()> {
        for position in 0..self.nav.entries[idx].children.len() {
            let child = self.nav.entries[idx].children[position];
            if self.nav.resolvable(child) {
                break;
            }
            if self.nav_sections[child].is_none() {
                self.emit_placeholder(child, locator.clone())?;
            }
        }
        let siblings = self.nav.siblings(idx);
        let after: Vec<usize> = siblings
            .iter()
            .copied()
            .skip_while(|sibling| *sibling != idx)
            .skip(1)
            .collect();
        for sibling in after {
            if self.nav.resolvable(sibling) {
                break;
            }
            if self.nav_sections[sibling].is_none() {
                self.emit_placeholder(sibling, locator.clone())?;
            }
        }
        Ok(())
    }

    /// Emit one `page` unit (§7.6) contained by the effective section or
    /// `document`, with the next parse-wide ordinal; it becomes the current
    /// page. An unlabeled marker is warned per document.
    fn emit_page(
        &mut self,
        member: &str,
        label: Option<String>,
        locator: Locator,
    ) -> WorkerResult<String> {
        let ordinal = self.page_ordinal + 1;
        let parent = self.effective_parent().to_string();
        let body = PageBody { ordinal, label };
        if body.label.is_none() {
            // The warning aggregate owns its locator; the unit keeps this one.
            self.emitter
                .warning(WARNING_PAGE_MARKER_UNLABELED, member, Some(locator.clone()));
        }
        let id = stream_unit(
            self.emitter,
            ContentType::Page,
            &body,
            Some(&parent),
            locator,
        )?;
        self.chain(&parent, &id)?;
        self.report
            .page_marker(member, ordinal, body.label.as_deref());
        self.page_ordinal = ordinal;
        // The walk keeps the current page for `appears_on`; the caller gets
        // its own copy for the edges of the leaf being assigned.
        self.page_current = Some(id.clone());
        Ok(id)
    }
}

/// Section kind of an element by §7.2 rules 1 to 3: `epub:type` on the
/// element, its nearest enclosing `section`/`div`, or the `body` (rule 1);
/// `data-type` on the same elements (rule 2); the landmark or guide hint
/// (rule 3); else `unknown` under rule 4. Returns the kind and the rule.
/// The carrier set is exactly the spec's three elements: a more distant
/// `section`/`div` never supplies the kind when the nearest one has none.
fn section_kind_of(element: roxmltree::Node, hint: Option<SectionKind>) -> (SectionKind, u8) {
    let ancestors = || element.ancestors().skip(1).filter(|a| a.is_element());
    let nearest_container = ancestors().find(|a| {
        let name = xhtml::local_name(*a);
        KIND_CARRIERS.contains(&name) && name != "body"
    });
    let body = ancestors().find(|a| xhtml::local_name(*a) == "body");
    let carriers = || {
        std::iter::once(element)
            .chain(nearest_container)
            .chain(body)
    };
    if let Some(kind) =
        carriers().find_map(|n| xhtml::epub_type(n).and_then(kinds::kind_from_semantic))
    {
        return (kind, 1);
    }
    if let Some(kind) =
        carriers().find_map(|n| xhtml::data_type(n).and_then(kinds::kind_from_semantic))
    {
        return (kind, 2);
    }
    hint_kind(hint)
}

/// §7.2 rule 3 alone, for a section with no target element: the hint, else
/// `unknown` under rule 4.
fn hint_kind(hint: Option<SectionKind>) -> (SectionKind, u8) {
    hint.map_or((SectionKind::Unknown, 4), |kind| (kind, 3))
}

/// Document-order index of the first node after `node`'s subtree, so a
/// node `n` lies inside the subtree exactly when
/// `node.id() <= n.id() < subtree_end(node)`. Climbs to the nearest
/// following sibling of an ancestor; `usize::MAX` past the last node.
fn subtree_end(node: roxmltree::Node) -> usize {
    let mut cursor = node;
    loop {
        if let Some(sibling) = cursor.next_sibling() {
            return sibling.id().get_usize();
        }
        match cursor.parent() {
            Some(parent) => cursor = parent,
            None => return usize::MAX,
        }
    }
}

/// §7.5 heading test: `Some(Some(n))` for `h<n>`, `Some(None)` for a
/// pseudo-heading (`epub:type="title"` with no `h` element in its parent
/// container), `None` otherwise.
fn heading_number(node: roxmltree::Node) -> Option<Option<u8>> {
    if !node.is_element() {
        return None;
    }
    let name = xhtml::local_name(node);
    if let Some(position) = HEADING_ELEMENTS.iter().position(|h| *h == name) {
        return Some(Some(position as u8 + 1));
    }
    let titled = xhtml::epub_type(node).is_some_and(|value| has_token(value, TITLE_EPUB_TYPE));
    if !titled {
        return None;
    }
    let container_has_heading = node.parent_element().is_some_and(|parent| {
        parent
            .descendants()
            .skip(1)
            .any(|d| d.is_element() && HEADING_ELEMENTS.contains(&xhtml::local_name(d)))
    });
    (!container_has_heading).then_some(None)
}

/// Whether a space-separated token list contains `wanted`.
fn has_token(value: &str, wanted: &str) -> bool {
    value.split_ascii_whitespace().any(|token| token == wanted)
}

/// §7.7: an `aside`, `div`, or `section` typed `footnote`, `endnote`, or
/// `rearnote` emits no aside; its blocks are footnote blocks under the
/// enclosing parent.
fn is_footnote_container(node: roxmltree::Node) -> bool {
    if !matches!(xhtml::local_name(node), "aside" | "div" | "section") {
        return false;
    }
    let epub = xhtml::epub_type(node).unwrap_or_default();
    let data = xhtml::data_type(node).unwrap_or_default();
    FOOTNOTE_TYPES
        .iter()
        .any(|kind| has_token(epub, kind) || has_token(data, kind))
}

/// Whether any child element is block-level (not §7.5 inline).
fn has_block_child(node: roxmltree::Node) -> bool {
    node.children()
        .any(|child| child.is_element() && !xhtml::is_inline(child))
}

/// Whether a run of sibling nodes carries anything a block can be made of:
/// non-whitespace text or an element. Inside a `figure` an `img` does not
/// count, since its figure was already emitted (§7.9).
fn has_content(nodes: &[roxmltree::Node], in_figure: bool) -> bool {
    nodes.iter().any(|node| {
        if node.is_text() {
            return node
                .text()
                .is_some_and(|t| t.chars().any(|c| !c.is_whitespace()));
        }
        node.is_element() && !(in_figure && xhtml::local_name(*node) == "img")
    })
}

/// Whether the subtree holds an image element (`img` or `svg`).
fn holds_image(node: roxmltree::Node) -> bool {
    node.descendants()
        .any(|d| d.is_element() && matches!(xhtml::local_name(d), "img" | "svg"))
}

/// The image nodes `blocks::emit_figure` emits one figure for, in the same
/// order: every `img`/`svg` descendant of a `figure` outside its
/// `figcaption`, else the node itself.
fn figure_images<'t, 'input>(
    node: roxmltree::Node<'t, 'input>,
) -> Vec<roxmltree::Node<'t, 'input>> {
    if xhtml::local_name(node) != "figure" {
        return vec![node];
    }
    node.descendants()
        .filter(|d| d.is_element() && matches!(xhtml::local_name(*d), "img" | "svg"))
        .filter(|d| {
            !d.ancestors()
                .any(|a| a.is_element() && xhtml::local_name(a) == "figcaption")
        })
        .collect()
}

/// Every `img` inside the roots of a text-bearing block, each of which
/// yields a figure after the block (§7.9).
fn inline_images<'t, 'input>(
    roots: &[roxmltree::Node<'t, 'input>],
) -> Vec<roxmltree::Node<'t, 'input>> {
    roots
        .iter()
        .flat_map(|root| root.descendants())
        .filter(|d| d.is_element() && xhtml::local_name(*d) == "img")
        .collect()
}

/// First element child with local name `name`.
fn child_element<'t, 'input>(
    parent: roxmltree::Node<'t, 'input>,
    name: &str,
) -> Option<roxmltree::Node<'t, 'input>> {
    parent
        .children()
        .find(|c| c.is_element() && xhtml::local_name(*c) == name)
}

/// Rows `blocks::emit_table` emits for `table`: direct `tr` children plus
/// the `tr` children of `thead`/`tbody`/`tfoot`, mirroring its grid so the
/// per-document unit count covers rows without a returned row list.
fn table_row_count(table: roxmltree::Node) -> u64 {
    let mut count = 0u64;
    for child in table.children().filter(|c| c.is_element()) {
        match xhtml::local_name(child) {
            "tr" => count += 1,
            "thead" | "tbody" | "tfoot" => {
                count += child
                    .children()
                    .filter(|c| c.is_element() && xhtml::local_name(*c) == "tr")
                    .count() as u64;
            }
            _ => {}
        }
    }
    count
}

/// Whitespace-collapsed text of the document `<title>`, for §7.4 rule 6.
fn document_title(document: &roxmltree::Document) -> Option<String> {
    let title = document
        .descendants()
        .find(|node| node.is_element() && xhtml::local_name(*node) == "title")?;
    let mut out = String::new();
    let mut pending_space = false;
    for text in title
        .descendants()
        .filter(|n| n.is_text())
        .filter_map(|n| n.text())
    {
        for ch in text.chars() {
            if ch.is_whitespace() {
                pending_space = true;
            } else {
                if pending_space && !out.is_empty() {
                    out.push(' ');
                }
                pending_space = false;
                out.push(ch);
            }
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The §10.3 note-reference predicate for one document: an `a` carrying
/// `noteref` semantics, or whose internal href targets a pass-one footnote
/// block. Returned as `impl Fn` so it is higher-ranked over the node
/// lifetimes, as `TextContext::is_noteref` requires.
fn noteref_predicate<'p>(
    member: &'p str,
    footnotes: &'p FootnoteIndex,
) -> impl Fn(roxmltree::Node) -> bool + 'p {
    move |node| {
        if xhtml::epub_type(node).is_some_and(|v| has_token(v, NOTEREF_TYPE))
            || xhtml::data_type(node).is_some_and(|v| v.trim() == NOTEREF_TYPE)
        {
            return true;
        }
        node.attribute("href").is_some_and(|href| {
            matches!(
                links::resolve(member, href.trim()),
                HrefOutcome::Internal(target) if footnotes.is_footnote(&target)
            )
        })
    }
}

/// How a heading element is treated in the current walk context (§7.5).
#[derive(Clone, Copy, PartialEq, Eq)]
enum HeadingMode {
    /// Section context: it opens or names a section.
    Section,
    /// Inside an aside or figure: a `text_block` role `heading`.
    Block,
    /// Inside a list item, blockquote, or footnote container: an ordinary
    /// block with the container's role.
    Role,
}

/// The walk context for one container's children.
struct Frame {
    /// `None` in section context, where each block belongs to the section
    /// current when it is reached and navigation targets are passed; `Some`
    /// fixes the parent unit.
    container: Option<String>,
    role: TextBlockRole,
    heading: HeadingMode,
    /// Inside a `figure` whose images were already emitted.
    in_figure: bool,
}

impl Frame {
    /// The section-context frame the walk of a document starts with.
    fn section() -> Self {
        Self {
            container: None,
            role: TextBlockRole::Paragraph,
            heading: HeadingMode::Section,
            in_figure: false,
        }
    }

    /// A frame with a fixed parent.
    fn fixed(parent: String, role: TextBlockRole, heading: HeadingMode, in_figure: bool) -> Self {
        Self {
            container: Some(parent),
            role,
            heading,
            in_figure,
        }
    }
}

/// §7.5 category of one block-level element, tested in the listed order.
enum Category {
    Skipped,
    Ignored,
    /// A §7.6 rule 1 marker with its label.
    Marker(Option<String>),
    /// A heading with its `h` number, `None` for a pseudo-heading.
    Heading(Option<u8>),
    Aside,
    Blockquote,
    FootnoteContainer,
    List,
    Table,
    Figure,
    Pre,
    Formula,
    /// Walked without a unit; `section` closes heading subsections and an
    /// unlisted name is counted.
    Generic {
        section: bool,
        unknown: bool,
    },
}

/// A `section` element being walked, for §7.4 rule 5's element-scoped
/// closing of the subsection its first heading opened.
struct SectionElement {
    id: usize,
    heading_seen: bool,
}

/// Per-document facts for the §11.3 summary and the mapped event.
#[derive(Default)]
struct DocCounts {
    units_by_type: BTreeMap<String, u64>,
    dropped: u64,
    unknown: BTreeMap<String, u64>,
    page_markers: u64,
}

/// One content document's walk: the parsed tree, the extraction context,
/// the targets still to pass, and the headings consumed by §7.4 rules 3
/// and 6. Lifetimes: `'w` this walk's borrows, `'p`/`'a` the parse-wide
/// walk and its emitter, `'n` the navigation, `'d` the package-owned
/// member name and the text context, `'t`/`'input` the parsed tree and its
/// text.
struct DocWalk<'w, 'p, 'a, 'n, 'd, 't, 'input> {
    walk: &'w mut Walk<'p, 'a, 'n>,
    member: &'d str,
    document: &'t roxmltree::Document<'input>,
    body: roxmltree::Node<'t, 'input>,
    text: &'w TextContext<'d>,
    archive: &'w mut Archive,
    package: &'w Package,
    navigation: &'w Navigation,
    /// Targets not yet passed, sorted by descending document-order id so
    /// the next one to pass is last.
    pending_targets: Vec<(roxmltree::NodeId, usize)>,
    /// Heading elements consumed as a section's `headingText`.
    consumed_headings: BTreeSet<usize>,
    section_elements: Vec<SectionElement>,
    /// §7.9 rule 3 caption waiting for the first `code_block` under the
    /// aside being walked.
    pending_code_caption: Option<CaptionCandidate<'t, 'input>>,
    counts: DocCounts,
}

/// Build the `BlockContext` for one block call from a `DocWalk`'s disjoint
/// fields, leaving `archive` and `package` free for `emit_figure`.
macro_rules! block_ctx {
    ($doc:expr) => {
        BlockContext {
            document: $doc.member,
            section_kind: $doc.walk.section_kind(),
            text: $doc.text,
            emitter: &mut *$doc.walk.emitter,
            links: &mut $doc.walk.links,
            units: &mut $doc.walk.units,
            report: &mut *$doc.walk.report,
        }
    };
}

/// Walk one content document (§11.2 step 5): load and parse it, pass its
/// fragment-less targets, synthesize its section when §7.4 rule 6 or 8
/// applies, walk `body`, pass leftover targets, and record its summary.
#[allow(clippy::too_many_arguments)] // the contracted `walk_spine` inputs plus the parse-wide state and the item's position
fn walk_document(
    walk: &mut Walk,
    member: &str,
    position: DocumentPosition,
    archive: &mut Archive,
    package: &Package,
    navigation: &Navigation,
    footnotes: &FootnoteIndex,
    limits: &EpubLimits,
) -> WorkerResult<()> {
    let source = load_document(archive, member, limits)?;
    let document = xhtml::parse(&source, member, limits)
        .map_err(|failure| failure.with_stage(EpubStage::Document(member.to_string())))?;
    let ids = xhtml::id_index(&document);
    let body = body_element(&document);
    let predicate = noteref_predicate(member, footnotes);
    let text = TextContext {
        is_noteref: &predicate,
        package_language: package.metadata.language.as_deref(),
    };
    let mut doc = DocWalk {
        walk,
        member,
        document: &document,
        body,
        text: &text,
        archive,
        package,
        navigation,
        pending_targets: Vec::new(),
        consumed_headings: BTreeSet::new(),
        section_elements: Vec::new(),
        pending_code_caption: None,
        counts: DocCounts::default(),
    };
    doc.run(&ids, position)
}

impl<'t, 'input> DocWalk<'_, '_, '_, '_, '_, 't, 'input> {
    /// The document's walk from start to summary.
    fn run(
        &mut self,
        ids: &BTreeMap<String, roxmltree::NodeId>,
        position: DocumentPosition,
    ) -> WorkerResult<()> {
        // Scoped heading frames never survive their document (their
        // `section` element closed); drop any that did as a safety net.
        if let Some(scoped) = self
            .walk
            .heading_stack
            .iter()
            .position(|f| f.scope.is_some())
        {
            self.walk.heading_stack.truncate(scoped);
        }
        self.walk.after_title = None;

        if position.first {
            // §7.4 rule 4 for leading target-less roots: at the document
            // unit's position, before any content.
            for root_position in 0..self.walk.nav.roots.len() {
                let root = self.walk.nav.roots[root_position];
                if self.walk.nav.resolvable(root) {
                    break;
                }
                if self.walk.nav_sections[root].is_none() {
                    let locator = self.body_locator();
                    self.walk.emit_placeholder(root, locator)?;
                }
            }
        }

        // Targets in this document: fragment-less ones pass now, fragment
        // ones queue by position, missing fragments are §7.4 rule 4.
        let mut targets_document = false;
        let mut pass_now = Vec::new();
        for idx in 0..self.walk.nav.entries.len() {
            let Some(target) = self.walk.nav.entries[idx]
                .node
                .target
                .as_ref()
                .filter(|target| target.member == self.member)
            else {
                continue;
            };
            targets_document = true;
            match target.fragment.as_deref() {
                None => pass_now.push(idx),
                Some(fragment) => match ids.get(fragment) {
                    Some(node_id) => self.pending_targets.push((*node_id, idx)),
                    None => {
                        // §7.4 rule 4: the node now counts as target-less.
                        // It is emitted here only when the node it must
                        // follow (previous sibling, else parent) is already
                        // emitted, or it is a first root; otherwise
                        // `emit_followers` chains it when that node is
                        // emitted, keeping navigation order.
                        self.walk.nav.fragment_missing[idx] = true;
                        let anchor = self.walk.nav.rule4_anchor(idx);
                        let anchor_emitted =
                            anchor.is_none_or(|anchor| self.walk.nav_sections[anchor].is_some());
                        if anchor_emitted && self.walk.nav_sections[idx].is_none() {
                            let locator = self.body_locator();
                            self.walk.emit_placeholder(idx, locator)?;
                        }
                        self.walk.emitter.warning(
                            WARNING_NAV_TARGET_UNRESOLVED,
                            self.member,
                            Some(self.body_locator()),
                        );
                    }
                },
            }
        }
        self.pending_targets
            .sort_by_key(|(node_id, _)| std::cmp::Reverse(node_id.get_usize()));
        for idx in pass_now {
            let body = self.body;
            self.pass_target(idx, body)?;
        }

        // §7.4 rules 6 and 8.
        let synthesize = self.navigation.source == NavigationSource::None
            || (!targets_document && self.walk.current.is_none());
        if synthesize {
            self.synthesize_section()?;
        }

        // §10.4: an id in no unit resolves to the section current at its
        // position; every id starts at the document's opening section and
        // inner units override as they are emitted (outermost first).
        let parent = self.walk.effective_parent().to_string();
        for id in ids.keys() {
            self.walk.units.insert(self.member, id, &parent);
        }
        self.walk.units.insert_document(self.member, &parent);

        let body = self.body;
        self.walk_children(body, &Frame::section())?;

        // Targets never reached by a block (inside skipped content) pass at
        // the document's end.
        while let Some((node_id, idx)) = self.pending_targets.pop() {
            if let Some(element) = self.document.get_node(node_id) {
                self.pass_target(idx, element)?;
            }
        }

        if position.last {
            // Rule 1: every navigation node is a section. A node still not
            // emitted has a member that never appeared in the spine.
            for idx in 0..self.walk.nav.entries.len() {
                if self.walk.nav_sections[idx].is_some() {
                    continue;
                }
                let locator = self.body_locator();
                self.walk.emit_placeholder(idx, locator)?;
                if self.walk.nav.resolvable(idx) {
                    self.walk.emitter.warning(
                        WARNING_NAV_TARGET_UNRESOLVED,
                        self.member,
                        Some(self.body_locator()),
                    );
                }
            }
        }

        let counts = std::mem::take(&mut self.counts);
        let unit_count = counts.units_by_type.values().sum();
        self.walk.documents.push(DocumentStats {
            href: self.member.to_string(),
            unit_count,
            dropped_blocks: counts.dropped,
            page_markers: counts.page_markers,
        });
        self.walk.report.document_summary(
            self.member,
            counts.units_by_type,
            counts.dropped,
            counts.unknown,
        );
        Ok(())
    }

    /// Locator of this document's `body` (§9.2 sections rule, last case).
    fn body_locator(&self) -> Locator {
        xhtml::locator(self.member, self.body, None)
    }

    /// Count `n` units of one type emitted in this document.
    fn count(&mut self, content_type: ContentType, n: u64) {
        *self
            .counts
            .units_by_type
            .entry(content_type.wire_name().to_string())
            .or_insert(0) += n;
    }

    /// Local id content in `frame` is contained by right now.
    fn parent_of(&self, frame: &Frame) -> String {
        frame
            .container
            .as_deref()
            .unwrap_or_else(|| self.walk.effective_parent())
            .to_string()
    }

    /// §7.4 rule 6: one synthesized section for this document under
    /// `document`, kind from `body`, `headingText` from the first heading
    /// (consumed), else the `<title>` when it differs from the package
    /// title, else the href.
    fn synthesize_section(&mut self) -> WorkerResult<()> {
        self.walk.heading_stack.clear();
        let (kind, rule) = section_kind_of(self.body, None);
        let first_heading = self
            .body
            .descendants()
            .skip(1)
            .find(|node| heading_number(*node).is_some());
        let (label, heading_text) = match first_heading {
            Some(heading) => {
                self.consumed_headings.insert(heading.id().get_usize());
                let (label, text) = blocks::split_heading(heading, self.text);
                (label, non_empty(text))
            }
            None => (None, None),
        };
        let heading_text = heading_text
            .or_else(|| {
                document_title(self.document)
                    .filter(|title| self.package.metadata.title.as_deref() != Some(title.as_str()))
            })
            .unwrap_or_else(|| self.member.to_string());
        let locator = self.body_locator();
        let section =
            self.walk
                .emit_section(kind, rule, label, heading_text, None, None, locator)?;
        self.count(ContentType::TextSection, 1);
        self.walk.current = Some(section);
        Ok(())
    }

    /// §7.4 rule 2: pass every pending target whose element lies before
    /// `end` in document order (inside the block about to be handled, or in
    /// skipped content before it). Returns whether any target passed.
    fn pass_targets(&mut self, end: usize) -> WorkerResult<bool> {
        let mut passed = false;
        while let Some(&(node_id, idx)) = self.pending_targets.last() {
            if node_id.get_usize() >= end {
                break;
            }
            self.pending_targets.pop();
            if let Some(element) = self.document.get_node(node_id) {
                self.pass_target(idx, element)?;
                passed = true;
            }
        }
        Ok(passed)
    }

    /// Pass one navigation target (§7.4 rules 2 and 3): close every
    /// heading-derived subsection, emit the section at this position when
    /// not yet emitted, register the target for §10.4, and make it current.
    fn pass_target(
        &mut self,
        idx: usize,
        element: roxmltree::Node<'t, 'input>,
    ) -> WorkerResult<()> {
        self.walk.heading_stack.clear();
        self.walk.after_title = None;
        if self.walk.nav_sections[idx].is_none() {
            let heading = self.target_heading(idx, element);
            let kind = section_kind_of(element, self.walk.nav.entries[idx].node.kind_hint);
            let locator = xhtml::locator(self.member, element, None);
            let sections_before = self.walk.sections.len();
            self.walk.emit_nav_entry(idx, heading, kind, locator)?;
            self.count(
                ContentType::TextSection,
                (self.walk.sections.len() - sections_before) as u64,
            );
        }
        let Some(section) = self.walk.nav_sections[idx] else {
            return Ok(());
        };
        let section_id = self.walk.sections[section].id.to_string();
        match self.walk.nav.entries[idx]
            .node
            .target
            .as_ref()
            .and_then(|target| target.fragment.as_deref())
        {
            Some(fragment) => self.walk.units.pin(self.member, fragment, &section_id),
            None => self.walk.units.insert_document(self.member, &section_id),
        }
        self.walk.current = Some(section);
        Ok(())
    }

    /// §7.4 rule 3 `headingText` for a passed target: the target element
    /// when it is a heading, else the first heading after it before any
    /// text, else the navigation label. A heading used here is consumed so
    /// rule 5 does not open a subsection from it.
    fn target_heading(
        &mut self,
        idx: usize,
        element: roxmltree::Node<'t, 'input>,
    ) -> (Option<String>, String) {
        let label = self.walk.nav.entries[idx].node.label.as_str();
        let heading = if heading_number(element).is_some() {
            Some(element)
        } else {
            self.heading_after(element)
        };
        match heading {
            Some(heading) => {
                self.consumed_headings.insert(heading.id().get_usize());
                let (section_label, text) = blocks::split_heading(heading, self.text);
                (
                    section_label,
                    non_empty(text).unwrap_or_else(|| label.to_string()),
                )
            }
            None => (None, label.to_string()),
        }
    }

    /// The first heading after `from` in document order (its descendants
    /// first) reached before any non-whitespace text; skipped elements,
    /// page markers, and `svg` subtrees contribute neither.
    fn heading_after(
        &self,
        from: roxmltree::Node<'t, 'input>,
    ) -> Option<roxmltree::Node<'t, 'input>> {
        let mut skip_until: Option<usize> = None;
        for node in self.document.descendants().skip(from.id().get_usize() + 1) {
            if let Some(end) = skip_until {
                if node.id().get_usize() < end {
                    continue;
                }
                skip_until = None;
            }
            if node.is_element() {
                if xhtml::is_skipped(node)
                    || xhtml::local_name(node) == "svg"
                    || self.marker_label(node).is_some()
                {
                    skip_until = Some(subtree_end(node));
                    continue;
                }
                if heading_number(node).is_some() {
                    return Some(node);
                }
            } else if node.is_text()
                && node
                    .text()
                    .is_some_and(|t| t.chars().any(|c| !c.is_whitespace()))
            {
                return None;
            }
        }
        None
    }

    /// §7.6 marker test on one node of either rule: `Some(label)` when it
    /// is a marker.
    fn marker_label(&self, node: roxmltree::Node) -> Option<Option<String>> {
        blocks::page_marker(node, &self.navigation.page_labels, self.member)
            .or_else(|| blocks::dp_page_marker(node))
    }

    /// Locator of a marker: the element, or for `<?dp?>` its parent element
    /// with `nodeRange` at the instruction's child index (§9.2).
    fn marker_locator(&self, marker: roxmltree::Node) -> Locator {
        if !marker.is_pi() {
            return xhtml::locator(self.member, marker, None);
        }
        match marker.parent().filter(|p| p.is_element()) {
            Some(parent) => {
                let index = marker.prev_siblings().count().saturating_sub(1) as u64;
                xhtml::locator(self.member, parent, Some([index, index]))
            }
            None => xhtml::locator(self.member, self.document.root_element(), None),
        }
    }

    /// Emit the page for one marker node.
    fn emit_marker(&mut self, marker: roxmltree::Node) -> WorkerResult<String> {
        let label = self.marker_label(marker).flatten();
        let locator = self.marker_locator(marker);
        let id = self.walk.emit_page(self.member, label, locator)?;
        self.count(ContentType::Page, 1);
        self.counts.page_markers += 1;
        Ok(id)
    }

    /// Every marker in the subtrees of `roots`, in document order.
    fn markers_in(
        &self,
        roots: &[roxmltree::Node<'t, 'input>],
    ) -> Vec<roxmltree::Node<'t, 'input>> {
        roots
            .iter()
            .flat_map(|root| root.descendants())
            .filter(|node| self.marker_label(*node).is_some())
            .collect()
    }

    /// §7.6 `appears_on` for one leaf whose extent is the `roots`
    /// subtrees: the page current at its start, then one page per marker
    /// inside it (each emitted here, splitting the extent). `None` emits the
    /// inner markers without a leaf, for a dropped block.
    fn assign_leaf(
        &mut self,
        leaf: Option<&str>,
        roots: &[roxmltree::Node<'t, 'input>],
    ) -> WorkerResult<()> {
        let markers = self.markers_in(roots);
        self.assign_leaf_markers(leaf, &markers)
    }

    /// `assign_leaf` over an explicit marker list.
    fn assign_leaf_markers(
        &mut self,
        leaf: Option<&str>,
        markers: &[roxmltree::Node<'t, 'input>],
    ) -> WorkerResult<()> {
        let mut pages: Vec<String> = self.walk.page_current.iter().map(String::from).collect();
        for marker in markers {
            pages.push(self.emit_marker(*marker)?);
        }
        if let Some(leaf) = leaf {
            for page in &pages {
                self.walk.emitter.relationship(
                    leaf,
                    page,
                    UnitRelationshipType::AppearsOn,
                    None,
                )?;
            }
        }
        Ok(())
    }

    /// §7.5 classification of one block-level element, in the listed order.
    fn classify(&self, node: roxmltree::Node) -> Category {
        if xhtml::is_skipped(node) {
            return Category::Skipped;
        }
        if let Some(label) = blocks::page_marker(node, &self.navigation.page_labels, self.member) {
            return Category::Marker(label);
        }
        if let Some(number) = heading_number(node) {
            return Category::Heading(number);
        }
        if blocks::aside_kind(node).is_some() {
            return Category::Aside;
        }
        if is_footnote_container(node) {
            return Category::FootnoteContainer;
        }
        let name = xhtml::local_name(node);
        match name {
            "blockquote" => Category::Blockquote,
            "ul" | "ol" | "dl" => Category::List,
            "table" => Category::Table,
            "figure" | "img" | "svg" => Category::Figure,
            "pre" => Category::Pre,
            "math" => Category::Formula,
            "hr" | "wbr" => Category::Ignored,
            "section" => Category::Generic {
                section: true,
                unknown: false,
            },
            _ => Category::Generic {
                section: false,
                unknown: !KNOWN_BLOCK_ELEMENTS.contains(&name)
                    && !GENERIC_CONTAINERS.contains(&name),
            },
        }
    }

    /// Walk the element children of `container` (§7.5), splitting the
    /// child nodes into block-level elements and maximal runs of text and
    /// inline nodes (mixed content), each handled with its neighbours in
    /// view (the one-block lookahead of §11.2 step 5).
    fn walk_children(
        &mut self,
        container: roxmltree::Node<'t, 'input>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        let children: Vec<roxmltree::Node<'t, 'input>> = container.children().collect();
        self.walk_nodes(container, &children, frame)
    }

    /// `walk_children` over an explicit subset of `parent`'s child nodes.
    fn walk_nodes(
        &mut self,
        parent: roxmltree::Node<'t, 'input>,
        nodes: &[roxmltree::Node<'t, 'input>],
        frame: &Frame,
    ) -> WorkerResult<()> {
        let mut sources: Vec<BlockSource<'t, 'input>> = Vec::new();
        let mut run: Vec<roxmltree::Node<'t, 'input>> = Vec::new();
        for node in nodes {
            if node.is_element() && !xhtml::is_inline(*node) {
                if !run.is_empty() {
                    sources.push(run_source(parent, std::mem::take(&mut run)));
                }
                sources.push(BlockSource::Element(*node));
            } else {
                run.push(*node);
            }
        }
        if !run.is_empty() {
            sources.push(run_source(parent, run));
        }
        for index in 0..sources.len() {
            let before = index.checked_sub(1).and_then(|i| sources.get(i));
            let after = sources.get(index + 1);
            match &sources[index] {
                BlockSource::Element(node) => self.handle_element(*node, before, after, frame)?,
                run @ BlockSource::Run { .. } => self.handle_run(run, frame)?,
            }
        }
        Ok(())
    }

    /// One mixed-content run: nothing but whitespace and markers emits only
    /// the markers; otherwise it is one text-bearing block.
    fn handle_run(&mut self, source: &BlockSource<'t, 'input>, frame: &Frame) -> WorkerResult<()> {
        let BlockSource::Run { nodes, .. } = source else {
            return Ok(());
        };
        if !has_content(nodes, frame.in_figure) {
            return self.assign_leaf(None, nodes);
        }
        let passed = match (frame.container.is_none(), nodes.last()) {
            (true, Some(last)) => self.pass_targets(subtree_end(*last))?,
            _ => false,
        };
        let parent = self.parent_of(frame);
        self.emit_block(source, &parent, frame.role, frame, !passed)
    }

    /// Pass targets for a block about to be handled in section context.
    fn pass_for(&mut self, node: roxmltree::Node<'t, 'input>, frame: &Frame) -> WorkerResult<bool> {
        if frame.container.is_some() {
            return Ok(false);
        }
        self.pass_targets(subtree_end(node))
    }

    /// Dispatch one block-level element by its §7.5 category.
    fn handle_element(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        before: Option<&BlockSource<'t, 'input>>,
        after: Option<&BlockSource<'t, 'input>>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        match self.classify(node) {
            Category::Skipped | Category::Ignored => Ok(()),
            Category::Marker(label) => {
                self.walk.after_title = None;
                let locator = self.marker_locator(node);
                self.walk.emit_page(self.member, label, locator)?;
                self.count(ContentType::Page, 1);
                self.counts.page_markers += 1;
                Ok(())
            }
            Category::Heading(number) => {
                self.pass_for(node, frame)?;
                self.handle_heading(node, number, frame)
            }
            Category::Aside => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                self.handle_aside(node, frame)
            }
            Category::Blockquote => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                let inner = Frame::fixed(
                    self.parent_of(frame),
                    TextBlockRole::Quote,
                    HeadingMode::Role,
                    frame.in_figure,
                );
                self.walk_generic(node, &inner, false)
            }
            Category::FootnoteContainer => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                let inner = Frame::fixed(
                    self.parent_of(frame),
                    frame.role,
                    HeadingMode::Role,
                    frame.in_figure,
                );
                self.walk_generic(node, &inner, false)
            }
            Category::List => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                self.handle_list(node, frame)
            }
            Category::Table => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                self.handle_table(node, frame)
            }
            Category::Figure => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                self.handle_figure(node, before, after, frame)
            }
            Category::Pre => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                self.handle_pre(node, frame)
            }
            Category::Formula => {
                self.pass_for(node, frame)?;
                self.walk.after_title = None;
                let parent = self.parent_of(frame);
                let id = blocks::emit_formula(&mut block_ctx!(self), node, &parent)?;
                self.count(ContentType::TextBlock, 1);
                self.walk.chain(&parent, &id)?;
                self.assign_leaf(Some(&id), &[node])
            }
            Category::Generic { section, unknown } => {
                if unknown {
                    let name = xhtml::local_name(node);
                    *self.counts.unknown.entry(name.to_string()).or_insert(0) += 1;
                    self.walk.emitter.warning(
                        WARNING_UNKNOWN_ELEMENT,
                        self.member,
                        Some(xhtml::locator(self.member, node, None)),
                    );
                }
                self.walk_generic(node, frame, section)
            }
        }
    }

    /// A generic container (§7.7 "none" rows): with block children it is
    /// walked; otherwise it is itself the text-bearing block (§7.5). Its
    /// own id resolves to the current parent until an inner unit claims it.
    /// A `section` element closes the heading subsection its first heading
    /// opened when it ends (§7.4 rule 5).
    fn walk_generic(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        frame: &Frame,
        section: bool,
    ) -> WorkerResult<()> {
        if section {
            self.section_elements.push(SectionElement {
                id: node.id().get_usize(),
                heading_seen: false,
            });
        }
        if has_block_child(node) {
            if let Some(id) = node.attribute("id") {
                let parent = self.parent_of(frame);
                self.walk.units.insert(self.member, id, &parent);
            }
            self.walk_children(node, frame)?;
        } else {
            let passed = self.pass_for(node, frame)?;
            let parent = self.parent_of(frame);
            let source = BlockSource::Element(node);
            self.emit_block(&source, &parent, frame.role, frame, !passed)?;
        }
        if section {
            self.section_elements.pop();
            let id = node.id().get_usize();
            if let Some(index) = self
                .walk
                .heading_stack
                .iter()
                .position(|f| f.scope == Some(id))
            {
                self.walk.heading_stack.truncate(index);
            }
        }
        Ok(())
    }

    /// Emit one text-bearing block from `source` with `role` (§7.5, §7.7),
    /// applying §7.11 title/subtitle detection when `title_check` holds in
    /// section context, then the figures for images inside it (§7.9),
    /// `appears_on`, and sibling chaining. A block dropped as empty with no
    /// images and no markers inside counts under `epub_empty_blocks_dropped`
    /// and its ids resolve to the parent (§10.4).
    fn emit_block(
        &mut self,
        source: &BlockSource<'t, 'input>,
        parent: &str,
        role: TextBlockRole,
        frame: &Frame,
        title_check: bool,
    ) -> WorkerResult<()> {
        let roots: Vec<roxmltree::Node<'t, 'input>> = match source {
            BlockSource::Element(node) => vec![*node],
            BlockSource::Run { nodes, .. } => nodes.to_vec(),
        };
        let mut role = role;
        let mut title_text: Option<String> = None;
        if title_check
            && frame.container.is_none()
            && role == TextBlockRole::Paragraph
            && let Some((detected, text)) = self.title_role(source)
        {
            role = detected;
            title_text = Some(text);
        }
        let emitted = blocks::emit_text_block(&mut block_ctx!(self), source, parent, role)?;
        let images = if frame.in_figure {
            Vec::new()
        } else {
            inline_images(&roots)
        };
        match &emitted {
            Some(id) => {
                self.count(ContentType::TextBlock, 1);
                self.walk.chain(parent, id)?;
                self.assign_leaf(Some(id), &roots)?;
            }
            None => {
                let markers = self.markers_in(&roots);
                self.assign_leaf_markers(None, &markers)?;
                if images.is_empty() && markers.is_empty() {
                    self.counts.dropped += 1;
                    self.walk.emitter.warning(
                        WARNING_EMPTY_BLOCKS_DROPPED,
                        self.member,
                        Some(source_locator(self.member, source)),
                    );
                    for element in roots.iter().flat_map(|r| r.descendants()) {
                        if let Some(id) = element.attribute("id") {
                            self.walk.units.insert(self.member, id, parent);
                        }
                    }
                }
            }
        }
        // §7.11: the subtitle rule looks at the block right after a title.
        self.walk.after_title = match role {
            TextBlockRole::Title => title_text,
            _ => None,
        };
        let package = self.package;
        for image in images {
            let ids = blocks::emit_figure(
                &mut block_ctx!(self),
                image,
                parent,
                package,
                &mut *self.archive,
                None,
            )?;
            self.count(ContentType::Figure, ids.len() as u64);
            for id in &ids {
                self.walk.chain(parent, id)?;
                self.assign_leaf(Some(id), &[image])?;
            }
        }
        Ok(())
    }

    /// §7.11: `title` when the block's text equals the effective section's
    /// label, heading, or navigation label; `subtitle` when it follows a
    /// `title` block and equals the navigation label with the title and a
    /// leading separator removed. Returns the role and the block text.
    fn title_role(&self, source: &BlockSource<'t, 'input>) -> Option<(TextBlockRole, String)> {
        let section = self.walk.effective_section()?;
        let info = &self.walk.sections[section];
        let text = match source {
            BlockSource::Element(node) => text::extract(*node, self.text),
            BlockSource::Run { nodes, .. } => text::extract_run(nodes, self.text),
        };
        if text.is_empty() {
            return None;
        }
        let lower = text.to_lowercase();
        let is_title = [&info.label, &info.heading_text, &info.nav_label]
            .into_iter()
            .flatten()
            .any(|candidate| candidate.to_lowercase() == lower);
        if is_title {
            return Some((TextBlockRole::Title, text));
        }
        let (title, nav_label) = (
            self.walk.after_title.as_deref()?,
            info.nav_label.as_deref()?,
        );
        let nav_lower = nav_label.to_lowercase();
        let remainder = nav_lower.strip_prefix(&title.to_lowercase())?;
        let remainder = remainder
            .trim_start_matches(|c: char| {
                c.is_whitespace() || SUBTITLE_SEPARATORS.contains(&c) || c == EM_DASH
            })
            .trim();
        (!remainder.is_empty() && remainder == lower).then_some((TextBlockRole::Subtitle, text))
    }

    /// A heading element (§7.5 heading rule, §7.4 rule 5). In section
    /// context an `h` heading not consumed by rule 3 or 6 opens a
    /// subsection; every heading is then emitted as a `text_block` (role
    /// `heading`, or the container's role under `HeadingMode::Role`). The
    /// heading held as a pending §7.9 rule 3 caption is not emitted here.
    fn handle_heading(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        number: Option<u8>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        if self
            .pending_code_caption
            .as_ref()
            .and_then(|c| c.node)
            .is_some_and(|c| c.id() == node.id())
        {
            return Ok(());
        }
        let role = match frame.heading {
            HeadingMode::Section => {
                let first_in_section = self
                    .section_elements
                    .last_mut()
                    .map(|s| !std::mem::replace(&mut s.heading_seen, true));
                let consumed = self.consumed_headings.contains(&node.id().get_usize());
                if let Some(number) = number
                    && !consumed
                {
                    let scope = match first_in_section {
                        Some(true) => self.section_elements.last().map(|s| s.id),
                        _ => None,
                    };
                    self.open_subsection(node, number, scope)?;
                }
                TextBlockRole::Heading
            }
            HeadingMode::Block => TextBlockRole::Heading,
            HeadingMode::Role => frame.role,
        };
        let parent = self.parent_of(frame);
        let source = BlockSource::Element(node);
        self.emit_block(&source, &parent, role, frame, false)
    }

    /// §7.4 rule 5: open a heading-derived subsection under the effective
    /// section, closing open ones of number at or above `number` first.
    fn open_subsection(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        number: u8,
        scope: Option<usize>,
    ) -> WorkerResult<()> {
        while self
            .walk
            .heading_stack
            .last()
            .is_some_and(|f| f.number >= number)
        {
            self.walk.heading_stack.pop();
        }
        self.walk.after_title = None;
        let (label, text) = blocks::split_heading(node, self.text);
        let heading_text =
            non_empty(text).unwrap_or_else(|| label.as_deref().unwrap_or(self.member).to_string());
        let parent_section = self.walk.effective_section();
        let locator = xhtml::locator(self.member, node, None);
        let section = self.walk.emit_section(
            SectionKind::Section,
            4,
            label,
            heading_text,
            None,
            parent_section,
            locator,
        )?;
        self.count(ContentType::TextSection, 1);
        self.walk.heading_stack.push(HeadingFrame {
            section,
            number,
            scope,
        });
        Ok(())
    }

    /// An aside (§7.7): emit the unit, then walk its children under it with
    /// role `quote` for a `blockquote` aside and `paragraph` otherwise. A
    /// pending §7.9 rule 3 caption is held for the first `code_block`
    /// walked under it; if none consumes it, its heading is emitted as a
    /// heading block at the end so its text is not lost.
    fn handle_aside(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        let parent = self.parent_of(frame);
        let emission = blocks::emit_aside(&mut block_ctx!(self), node, &parent)?;
        self.count(ContentType::Aside, 1);
        self.walk.chain(&parent, &emission.aside_id)?;
        let role = if xhtml::local_name(node) == "blockquote" {
            TextBlockRole::Quote
        } else {
            TextBlockRole::Paragraph
        };
        let inner = Frame::fixed(emission.aside_id, role, HeadingMode::Block, false);
        let outer_caption =
            std::mem::replace(&mut self.pending_code_caption, emission.pending_caption);
        self.walk_nodes(node, &emission.children, &inner)?;
        if let Some(heading) = self.pending_code_caption.take().and_then(|c| c.node) {
            let aside_id = self.parent_of(&inner);
            let source = BlockSource::Element(heading);
            self.emit_block(&source, &aside_id, TextBlockRole::Heading, &inner, false)?;
        }
        self.pending_code_caption = outer_caption;
        Ok(())
    }

    /// A list (§7.7): emit the list and items, then walk each item's nodes
    /// under the item with the returned role; items are chained under the
    /// list.
    fn handle_list(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        let parent = self.parent_of(frame);
        let emission = blocks::emit_list(&mut block_ctx!(self), node, &parent)?;
        self.count(ContentType::List, 1);
        self.count(ContentType::ListItem, emission.items.len() as u64);
        self.walk.chain(&parent, &emission.list_id)?;
        for (item_id, group) in emission.items {
            self.walk.chain(&emission.list_id, &item_id)?;
            for (child, role) in group {
                let inner = Frame::fixed(item_id.to_string(), role, HeadingMode::Role, false);
                self.walk_generic(child, &inner, false)?;
            }
        }
        Ok(())
    }

    /// A table (§7.8): emit it through `blocks`, chain the table and its
    /// caption under the parent, and assign `appears_on` to the caption and
    /// cells in document order, emitting the markers between them as pages.
    fn handle_table(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        let parent = self.parent_of(frame);
        let emission = blocks::emit_table(&mut block_ctx!(self), node, &parent)?;
        self.count(ContentType::Table, 1);
        self.count(ContentType::TableRow, table_row_count(node));
        self.count(ContentType::TableCell, emission.cells.len() as u64);
        self.walk.chain(&parent, &emission.table_id)?;

        let mut leaves: Vec<(String, roxmltree::Node<'t, 'input>)> = Vec::new();
        if let Some(caption_id) = emission.caption_id {
            self.count(ContentType::Caption, 1);
            self.walk.chain(&parent, &caption_id)?;
            if let Some(caption) = child_element(node, "caption") {
                leaves.push((caption_id, caption));
            }
        }
        leaves.extend(emission.cells);
        leaves.sort_by_key(|(_, leaf)| leaf.id().get_usize());

        let markers = self.markers_in(&[node]);
        let mut cursor = 0;
        for (leaf_id, leaf) in &leaves {
            let start = leaf.id().get_usize();
            let end = subtree_end(*leaf);
            while markers
                .get(cursor)
                .is_some_and(|m| m.id().get_usize() < start)
            {
                self.emit_marker(markers[cursor])?;
                cursor += 1;
            }
            let inside_start = cursor;
            while markers
                .get(cursor)
                .is_some_and(|m| m.id().get_usize() < end)
            {
                cursor += 1;
            }
            self.assign_leaf_markers(Some(leaf_id), &markers[inside_start..cursor])?;
        }
        while cursor < markers.len() {
            self.emit_marker(markers[cursor])?;
            cursor += 1;
        }
        Ok(())
    }

    /// A figure (§7.9): one `figure` per image with the detected caption,
    /// the caption unit paired to all of them, `appears_on` for each, then
    /// the non-image, non-caption children of a `figure` element walked
    /// under the figure's parent.
    fn handle_figure(
        &mut self,
        node: roxmltree::Node<'t, 'input>,
        before: Option<&BlockSource<'t, 'input>>,
        after: Option<&BlockSource<'t, 'input>>,
        frame: &Frame,
    ) -> WorkerResult<()> {
        let parent = self.parent_of(frame);
        let caption = blocks::detect_caption(node, before, after, self.text);
        let package = self.package;
        let ids = blocks::emit_figure(
            &mut block_ctx!(self),
            node,
            &parent,
            package,
            &mut *self.archive,
            caption.as_ref(),
        )?;
        self.count(ContentType::Figure, ids.len() as u64);
        for (id, image) in ids.iter().zip(figure_images(node)) {
            self.walk.chain(&parent, id)?;
            self.assign_leaf(Some(id), &[image])?;
        }
        // §7.9: a caption exists only as the pair of a subject. A `figure`
        // element holding no `img`/`svg` yields no figure, so its
        // `figcaption` or heading is not a caption; it is then walked
        // below as an ordinary block under the figure's parent.
        let caption = if ids.is_empty() { None } else { caption };
        if let Some(candidate) = &caption {
            let caption_id = blocks::pair_caption(&mut block_ctx!(self), &ids, candidate, &parent)?;
            self.count(ContentType::Caption, 1);
            self.walk.chain(&parent, &caption_id)?;
            if let Some(caption_node) = candidate.node {
                self.assign_leaf(Some(&caption_id), &[caption_node])?;
            }
        }
        if xhtml::local_name(node) != "figure" {
            return Ok(());
        }
        // With no image, a `figcaption` child is walked like any other
        // non-image child instead of being consumed; with images it is
        // always consumed (an empty one drops, as before).
        let caption_node = caption.as_ref().and_then(|c| c.node).map(|n| n.id());
        let rest: Vec<roxmltree::Node<'t, 'input>> = node
            .children()
            .filter(|child| {
                !child.is_element()
                    || ((ids.is_empty() || xhtml::local_name(*child) != "figcaption")
                        && Some(child.id()) != caption_node
                        && !holds_image(*child))
            })
            .collect();
        let inner = Frame::fixed(parent, frame.role, HeadingMode::Block, true);
        self.walk_nodes(node, &rest, &inner)
    }

    /// A `pre` (§7.7): one `code_block`, titled by the pending §7.9 rule 3
    /// caption when this is the first code block under an `example` aside.
    fn handle_pre(&mut self, node: roxmltree::Node<'t, 'input>, frame: &Frame) -> WorkerResult<()> {
        let parent = self.parent_of(frame);
        let caption = self.pending_code_caption.take();
        let id = blocks::emit_code(&mut block_ctx!(self), node, &parent, caption.as_ref())?;
        self.count(ContentType::CodeBlock, 1);
        self.walk.chain(&parent, &id)?;
        if let Some(candidate) = &caption {
            let caption_id = blocks::pair_caption(
                &mut block_ctx!(self),
                std::slice::from_ref(&id),
                candidate,
                &parent,
            )?;
            self.count(ContentType::Caption, 1);
            self.walk.chain(&parent, &caption_id)?;
            if let Some(caption_node) = candidate.node {
                self.assign_leaf(Some(&caption_id), &[caption_node])?;
            }
        }
        self.assign_leaf(Some(&id), &[node])
    }
}

/// A mixed-content run as a block source: `nodeRange` spans the run's
/// first and last child-node indexes under `parent` (§9.2).
fn run_source<'t, 'input>(
    parent: roxmltree::Node<'t, 'input>,
    nodes: Vec<roxmltree::Node<'t, 'input>>,
) -> BlockSource<'t, 'input> {
    let index = |node: &roxmltree::Node| node.prev_siblings().count().saturating_sub(1) as u64;
    let first = nodes.first().map_or(0, index);
    let last = nodes.last().map_or(first, index);
    BlockSource::Run {
        parent,
        nodes,
        node_range: [first, last],
    }
}

/// Locator of a block source, for the dropped-block warning.
fn source_locator(member: &str, source: &BlockSource) -> Locator {
    match source {
        BlockSource::Element(node) => xhtml::locator(member, *node, None),
        BlockSource::Run {
            parent, node_range, ..
        } => xhtml::locator(member, *parent, Some(*node_range)),
    }
}

/// `Some` only for a non-empty string.
fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
