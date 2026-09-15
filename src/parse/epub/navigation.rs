//! Navigation source (SPEC-epub §5.3): EPUB 3 `nav` document or EPUB 2 NCX,
//! the table-of-contents tree, landmark and guide kind hints, and page-list
//! labels.
//!
//! Targets are resolved here at member granularity (§5.4 via
//! `links::resolve` plus an archive lookup); fragment lookup inside the
//! target document is the structure walk's job. Every unresolved target is
//! warned here as `epub_nav_target_unresolved`, keyed by the package href
//! because navigation is read before any content document is walked
//! (§11.5), so consumers of `NavNode` read `resolved` and never re-warn.

use std::collections::BTreeMap;

use crate::limits::EpubLimits;
use crate::model::body::SectionKind;
use crate::model::locator::{DomPathLocator, Locator};
use crate::parse::epub::archive::Archive;
use crate::parse::epub::kinds;
use crate::parse::epub::links::{self, HrefOutcome};
use crate::parse::epub::package::Package;
use crate::parse::epub::xhtml;
use crate::parse::epub::{Emitter, EpubFailure, EpubStage};

/// Manifest `properties` token that marks the EPUB 3 navigation document.
const NAV_PROPERTY: &str = "nav";

/// Media type of an EPUB 2 NCX manifest item.
const NCX_MEDIA_TYPE: &str = "application/x-dtbncx+xml";

/// `epub:type` token of the table-of-contents `nav` (§5.3).
const NAV_TYPE_TOC: &str = "toc";

/// `epub:type` token of the landmarks `nav` (§5.3, §7.2 rule 3).
const NAV_TYPE_LANDMARKS: &str = "landmarks";

/// `epub:type` token of the page-list `nav` (§5.3, §7.6).
const NAV_TYPE_PAGE_LIST: &str = "page-list";

/// Warning code when §5.3 selects no navigation source.
const WARNING_NAVIGATION_MISSING: &str = "epub_navigation_missing";

/// Warning code for a navigation, landmark, page-list, or guide target
/// that does not resolve to an archive member (§5.4).
const WARNING_NAV_TARGET_UNRESOLVED: &str = "epub_nav_target_unresolved";

/// Element path of the package document itself (§7.1), the locator of
/// warnings raised before any content document is read.
const PACKAGE_DOCUMENT_PATH: &str = "/package[1]";

/// Which navigation source §5.3 selected; recorded in `toolIdentity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NavigationSource {
    Nav,
    Ncx,
    None,
}

impl NavigationSource {
    /// Stable name for `toolIdentity.navigationSource` and the service log.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Nav => "nav",
            Self::Ncx => "ncx",
            Self::None => "none",
        }
    }
}

/// A navigation target: normalized member name plus optional fragment.
pub(crate) struct Target {
    pub member: String,
    pub fragment: Option<String>,
}

/// One node of the table-of-contents tree.
pub(crate) struct NavNode {
    pub label: String,
    /// The href as written in the navigation member, for the §11.3 report.
    /// `None` for an entry that declares no target (an EPUB 3 `span`).
    pub href: Option<String>,
    /// `None` when `href` is `None` and when `href` did not resolve
    /// (already warned; see `resolved`).
    pub target: Option<Target>,
    /// Kind hint from a landmark or guide entry targeting this node.
    pub kind_hint: Option<SectionKind>,
    pub children: Vec<NavNode>,
    /// `false` exactly when `href` is present and reached no archive
    /// member (§5.4); an entry with no href has nothing to resolve and is
    /// `true` (§7.4 rule 4 emits it without a warning).
    pub resolved: bool,
}

/// The chosen navigation source's contents.
pub(crate) struct Navigation {
    pub source: NavigationSource,
    pub toc: Vec<NavNode>,
    /// Page labels keyed by `(member, fragment)` from a `page-list` nav.
    pub page_labels: BTreeMap<(String, Option<String>), String>,
}

/// Lookup key shared by kind hints and page labels: a resolved target.
type TargetKey = (String, Option<String>);

/// Kind hints from landmarks and guide entries, keyed by resolved target.
type KindHints = BTreeMap<TargetKey, SectionKind>;

/// Select and read the navigation source by the §5.3 rules; a chosen
/// source that is missing or malformed is a failure.
pub(crate) fn read(
    archive: &mut Archive,
    package: &Package,
    limits: &EpubLimits,
    emitter: &mut Emitter,
) -> Result<Navigation, EpubFailure> {
    let Some((source, member)) = select_source(package) else {
        emitter.warning(
            WARNING_NAVIGATION_MISSING,
            &package.href,
            Some(package_locator(&package.href)),
        );
        return Ok(Navigation {
            source: NavigationSource::None,
            toc: Vec::new(),
            page_labels: BTreeMap::new(),
        });
    };

    // The package declared this member; its absence is a failure (§5.3),
    // unlike a content href that merely fails to resolve.
    let bytes = archive
        .read(member)
        .map_err(|failure| failure.with_stage(EpubStage::Navigation))?
        .ok_or_else(|| {
            EpubFailure::new(
                EpubStage::Navigation,
                format!("navigation member {member} named by the package is not in the archive"),
            )
        })?;
    let decoded = xhtml::decode(&bytes, member, limits)
        .map_err(|failure| failure.with_stage(EpubStage::Navigation))?;
    let rewritten = xhtml::rewrite_entities(&decoded);
    let document = xhtml::parse(&rewritten, member, limits)
        .map_err(|failure| failure.with_stage(EpubStage::Navigation))?;

    // Reads are done; the rest only asks the archive whether members exist.
    let mut reader = Reader {
        archive,
        member,
        package_href: &package.href,
        emitter,
    };
    // Landmarks are inserted before guide entries so the chosen EPUB 3
    // source wins when both hint the same target; `or_insert` keeps the
    // first entry per target within each feature.
    let mut hints = KindHints::new();
    let mut page_labels = BTreeMap::new();
    let toc = match source {
        NavigationSource::Nav => {
            if let Some(nav) = find_nav(&document, NAV_TYPE_LANDMARKS) {
                reader.landmark_hints(nav, &mut hints);
            }
            reader.guide_hints(package, &mut hints);
            if let Some(nav) = find_nav(&document, NAV_TYPE_PAGE_LIST) {
                reader.page_labels(nav, &mut page_labels);
            }
            // A nav document with no `toc` nav delivers an empty tree, not
            // a failure: §5.3 fails only on a missing or malformed member.
            match find_nav(&document, NAV_TYPE_TOC).and_then(first_element(NAV_LIST)) {
                Some(list) => reader.nav_tree(list, &hints),
                None => Vec::new(),
            }
        }
        NavigationSource::Ncx => {
            reader.guide_hints(package, &mut hints);
            match first_element(NCX_MAP)(document.root_element()) {
                Some(map) => reader.ncx_tree(map, &hints),
                None => Vec::new(),
            }
        }
        NavigationSource::None => Vec::new(),
    };

    Ok(Navigation {
        source,
        toc,
        page_labels,
    })
}

/// EPUB 3 list element holding `li` entries under a `nav`.
const NAV_LIST: &str = "ol";
/// EPUB 3 list entry.
const NAV_ITEM: &str = "li";
/// EPUB 3 entry with a target.
const NAV_LINK: &str = "a";
/// EPUB 3 entry without a target (label only).
const NAV_SPAN: &str = "span";
/// NCX tree root under `ncx`.
const NCX_MAP: &str = "navMap";
/// NCX tree node.
const NCX_POINT: &str = "navPoint";
/// NCX label wrapper; its `text` child carries the label.
const NCX_LABEL: &str = "navLabel";
/// NCX label text element.
const NCX_TEXT: &str = "text";
/// NCX target element; its `src` attribute is the href.
const NCX_CONTENT: &str = "content";

/// §5.3 selection: the `nav` manifest item, else the spine `toc` item when
/// it is an NCX, else none. Returns the source kind and the normalized
/// member name to read. Manifest order is by item id (the map's order), so
/// with several `nav` items the lowest id wins.
fn select_source(package: &Package) -> Option<(NavigationSource, &str)> {
    let nav = package
        .manifest
        .values()
        .find(|item| {
            item.properties
                .iter()
                .any(|property| property == NAV_PROPERTY)
        })
        .map(|item| (NavigationSource::Nav, item.href.as_str()));
    if nav.is_some() {
        return nav;
    }
    package
        .toc_id
        .as_ref()
        .and_then(|id| package.manifest.get(id))
        .filter(|item| item.media_type == NCX_MEDIA_TYPE)
        .map(|item| (NavigationSource::Ncx, item.href.as_str()))
}

/// Locator of the package document for warnings raised before any content
/// document is walked (§11.5).
fn package_locator(package_href: &str) -> Locator {
    Locator::DomPath(DomPathLocator {
        document: package_href.to_string(),
        path: PACKAGE_DOCUMENT_PATH.to_string(),
        element_id: None,
        node_range: None,
    })
}

/// Target-resolving state shared by the tree builders: the archive for
/// existence checks, the navigation member as the href base, the package
/// href as the warning key, and the emitter for unresolved-target warnings.
struct Reader<'r, 'a> {
    archive: &'r Archive,
    member: &'r str,
    package_href: &'r str,
    emitter: &'r mut Emitter<'a>,
}

impl Reader<'_, '_> {
    /// Resolve one href written in the navigation member (§5.4). `None`
    /// means the target does not reach an archive member (external,
    /// malformed, or missing), and the warning has been recorded; callers
    /// never warn again. Navigation is read before any content document is
    /// walked, so the warning is keyed by the package href (§11.5) while its
    /// locator still names `node` inside the navigation member.
    fn resolve(&mut self, href: &str, node: roxmltree::Node) -> Option<Target> {
        match links::resolve(self.member, href) {
            HrefOutcome::Internal(target) if self.archive.contains(&target.member) => Some(target),
            HrefOutcome::Internal(_) | HrefOutcome::External | HrefOutcome::Unresolvable => {
                self.emitter.warning(
                    WARNING_NAV_TARGET_UNRESOLVED,
                    self.package_href,
                    Some(xhtml::locator(self.member, node, None)),
                );
                None
            }
        }
    }

    /// EPUB 3 `ol > li` tree under `list`: each `li` contributes its first
    /// `a` (label and target) or `span` (label only) and every nested `ol`.
    /// Recursion depth is bounded by `max_element_depth`, already enforced
    /// by `xhtml::parse`.
    fn nav_tree(&mut self, list: roxmltree::Node, hints: &KindHints) -> Vec<NavNode> {
        let mut nodes = Vec::new();
        for item in child_elements(list, NAV_ITEM) {
            let entry = item.children().find(|child| {
                child.is_element() && matches!(xhtml::local_name(*child), NAV_LINK | NAV_SPAN)
            });
            // An `li` with neither `a` nor `span` is malformed under the
            // EPUB 3 content model; it keeps its place with an empty label
            // so its nested entries are not lost.
            let label = entry.map(normalized_text).unwrap_or_default();
            // An `a` without `href` is treated like a `span`: it declares
            // no target, so there is nothing to resolve or warn about.
            let link = entry
                .filter(|entry| xhtml::local_name(*entry) == NAV_LINK)
                .and_then(|entry| entry.attribute("href").map(|href| (entry, href)));
            let href = link.map(|(_, href)| href.to_string());
            let target = link.and_then(|(entry, href)| self.resolve(href, entry));
            let children = child_elements(item, NAV_LIST)
                .flat_map(|nested| self.nav_tree(nested, hints))
                .collect();
            nodes.push(node(label, href, target, children, hints));
        }
        nodes
    }

    /// NCX `navPoint` tree under `parent` (`navMap` or an enclosing
    /// `navPoint`): label from `navLabel/text`, target from `content/@src`.
    /// Depth is bounded as for `nav_tree`.
    fn ncx_tree(&mut self, parent: roxmltree::Node, hints: &KindHints) -> Vec<NavNode> {
        let mut nodes = Vec::new();
        for point in child_elements(parent, NCX_POINT) {
            let label = first_element(NCX_LABEL)(point)
                .and_then(first_element(NCX_TEXT))
                .map(normalized_text)
                .unwrap_or_default();
            // A `navPoint` without `content/@src` declares no target, the
            // NCX counterpart of an EPUB 3 `span` entry.
            let link = first_element(NCX_CONTENT)(point)
                .and_then(|content| content.attribute("src").map(|src| (content, src)));
            let href = link.map(|(_, src)| src.to_string());
            let target = link.and_then(|(content, src)| self.resolve(src, content));
            let children = self.ncx_tree(point, hints);
            nodes.push(node(label, href, target, children, hints));
        }
        nodes
    }

    /// Kind hints from a landmarks `nav`: every `a` with an `href` whose
    /// `epub:type` maps through the kind table (§7.2 rule 3). Every entry is
    /// resolved so an unresolvable landmark is warned even when its type
    /// contributes no hint.
    fn landmark_hints(&mut self, nav: roxmltree::Node, hints: &mut KindHints) {
        for link in nav
            .descendants()
            .filter(|node| node.is_element() && xhtml::local_name(*node) == NAV_LINK)
        {
            let Some(href) = link.attribute("href") else {
                continue;
            };
            let Some(target) = self.resolve(href, link) else {
                continue;
            };
            if let Some(kind) = xhtml::epub_type(link).and_then(kinds::kind_from_semantic) {
                hints.entry(target_key(&target)).or_insert(kind);
            }
        }
    }

    /// Kind hints from the package `guide` (§5.2, §7.2 rule 3). Guide
    /// hrefs are written relative to the package document, so they do not
    /// go through `resolve`; an unresolved one is warned under the same
    /// package key, located at the package document since the guide
    /// element's node is not retained.
    fn guide_hints(&mut self, package: &Package, hints: &mut KindHints) {
        for reference in &package.guide {
            let target = match links::resolve(&package.href, &reference.href) {
                HrefOutcome::Internal(target) if self.archive.contains(&target.member) => target,
                HrefOutcome::Internal(_) | HrefOutcome::External | HrefOutcome::Unresolvable => {
                    self.emitter.warning(
                        WARNING_NAV_TARGET_UNRESOLVED,
                        &package.href,
                        Some(package_locator(&package.href)),
                    );
                    continue;
                }
            };
            if let Some(kind) = kinds::kind_from_semantic(&reference.kind) {
                hints.entry(target_key(&target)).or_insert(kind);
            }
        }
    }

    /// Page labels from a page-list `nav` (§7.6): every `a` with an `href`,
    /// keyed by resolved target; the first entry per target wins.
    fn page_labels(&mut self, nav: roxmltree::Node, labels: &mut BTreeMap<TargetKey, String>) {
        for link in nav
            .descendants()
            .filter(|node| node.is_element() && xhtml::local_name(*node) == NAV_LINK)
        {
            let Some(href) = link.attribute("href") else {
                continue;
            };
            let Some(target) = self.resolve(href, link) else {
                continue;
            };
            labels
                .entry(target_key(&target))
                .or_insert_with(|| normalized_text(link));
        }
    }
}

/// Assemble one tree node: `resolved` is false exactly when an href was
/// written but reached no archive member (`target` is `None` while `href`
/// is `Some`); the kind hint is looked up by the resolved target.
fn node(
    label: String,
    href: Option<String>,
    target: Option<Target>,
    children: Vec<NavNode>,
    hints: &KindHints,
) -> NavNode {
    let kind_hint = target
        .as_ref()
        .and_then(|target| hints.get(&target_key(target)).copied());
    NavNode {
        label,
        resolved: href.is_none() || target.is_some(),
        href,
        target,
        kind_hint,
        children,
    }
}

/// Map key of a target; owned because the maps outlive the borrowed target.
fn target_key(target: &Target) -> TargetKey {
    (
        target.member.to_string(),
        target.fragment.as_ref().map(String::to_string),
    )
}

/// The first `nav` element in document order whose `epub:type` carries
/// `token`.
fn find_nav<'a, 'i>(
    document: &'a roxmltree::Document<'i>,
    token: &str,
) -> Option<roxmltree::Node<'a, 'i>> {
    document.descendants().find(|node| {
        node.is_element()
            && xhtml::local_name(*node) == "nav"
            && xhtml::epub_type(*node)
                .is_some_and(|value| value.split_ascii_whitespace().any(|t| t == token))
    })
}

/// Element children of `parent` with local name `name`, in document order.
fn child_elements<'a, 'i>(
    parent: roxmltree::Node<'a, 'i>,
    name: &'static str,
) -> impl Iterator<Item = roxmltree::Node<'a, 'i>> {
    parent
        .children()
        .filter(move |child| child.is_element() && xhtml::local_name(*child) == name)
}

/// A finder for the first element child named `name`, shaped for
/// `Option::and_then` chains.
fn first_element<'a, 'i: 'a>(
    name: &'static str,
) -> impl Fn(roxmltree::Node<'a, 'i>) -> Option<roxmltree::Node<'a, 'i>> {
    move |parent| child_elements(parent, name).next()
}

/// Whitespace-normalized label text (§5.3): every descendant text node in
/// document order, each whitespace run (including NBSP and newlines)
/// collapsed to one space, trimmed. Inline markup inside a label adds
/// nothing.
fn normalized_text(node: roxmltree::Node) -> String {
    let mut out = String::new();
    let mut pending_space = false;
    for text in node
        .descendants()
        .filter(|node| node.is_text())
        .filter_map(|node| node.text())
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
    out
}
