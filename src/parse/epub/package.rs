//! Container and package document (SPEC-epub §5.1 `mimetype`, §5.2): the
//! `META-INF/container.xml` rootfile, OPF metadata into `DocumentBody`, the
//! manifest, the spine, and the `guide` kind hints.
//!
//! Stage policy: failures raised here carry the stage of the §11.2 step
//! they belong to (`Container` for the container, `Package` for the
//! package); helper failures from `archive` and `xhtml` arrive at stage
//! `Archive` and are re-staged by the caller in `mod.rs`, which is the
//! boundary that knows the step.

use std::collections::BTreeMap;

use crate::limits::EpubLimits;
use crate::model::body::DocumentBody;
use crate::parse::epub::archive::Archive;
use crate::parse::epub::links::{self, HrefOutcome};
use crate::parse::epub::text::{self, TextContext};
use crate::parse::epub::xhtml;
use crate::parse::epub::{Emitter, EpubFailure, EpubStage};

/// Archive member naming the package document (§5.2).
const CONTAINER_MEMBER: &str = "META-INF/container.xml";

/// The `mimetype` member every EPUB carries first (§5.1).
const MIMETYPE_MEMBER: &str = "mimetype";

/// Required content of the `mimetype` member after trimming (§5.1). Wave D
/// exports `MIME_TYPE_EPUB` from `acquisition.rs`; this local copy is
/// replaced by that import when the route is wired.
const EPUB_MIME_TYPE: &str = "application/epub+zip";

/// `media-type` of the container `rootfile` that names the package (§5.2).
const PACKAGE_MEDIA_TYPE: &str = "application/oebps-package+xml";

/// Package `version` values accepted without warning: `2.0` exactly, and
/// any `3.*` (§5.2).
const VERSION_EPUB2: &str = "2.0";
const VERSION_EPUB3_PREFIX: &str = "3.";

/// Warning codes raised by this module (§11.5).
const WARNING_MIMETYPE_MISSING: &str = "epub_mimetype_missing";
const WARNING_MIMETYPE_MISMATCH: &str = "epub_mimetype_mismatch";
const WARNING_PACKAGE_VERSION: &str = "epub_package_version";
const WARNING_SPINE_ITEM_MISSING: &str = "epub_spine_item_missing";

/// One manifest item; `href` is the normalized archive member name.
pub(crate) struct ManifestItem {
    pub href: String,
    pub media_type: String,
    pub properties: Vec<String>,
}

/// One spine `itemref` resolved to its manifest member.
pub(crate) struct SpineItem {
    pub idref: String,
    pub href: String,
    pub linear: bool,
}

/// One OPF `guide` reference (§5.2), a kind hint for §7.2 rule 3. `href`
/// is kept as written (it may carry a fragment); consumers resolve it with
/// `links::resolve` against `Package::href`, the same way a landmark entry
/// resolves against its navigation document.
pub(crate) struct GuideReference {
    pub kind: String,
    pub href: String,
}

/// The parsed package document.
pub(crate) struct Package {
    /// Normalized member name of the package document.
    pub href: String,
    pub version: String,
    pub metadata: DocumentBody,
    /// Manifest items keyed by `id`.
    pub manifest: BTreeMap<String, ManifestItem>,
    pub spine: Vec<SpineItem>,
    /// The spine's `toc` attribute (EPUB 2 NCX id).
    pub toc_id: Option<String>,
    pub guide: Vec<GuideReference>,
}

/// Read the `mimetype` member and raise `epub_mimetype_missing` or
/// `epub_mimetype_mismatch` keyed by `package_href`; never a failure on its
/// own (§5.1). Called after `read_package` so the key is known. The only
/// error is an archive cap or read fault, which the caller re-stages.
pub(crate) fn check_mimetype(
    archive: &mut Archive,
    package_href: &str,
    emitter: &mut Emitter,
) -> Result<(), EpubFailure> {
    match archive.read(MIMETYPE_MEMBER)? {
        None => emitter.warning(WARNING_MIMETYPE_MISSING, package_href, None),
        Some(bytes) => {
            // Bytes that are not UTF-8 cannot equal the expected value, so
            // they fall into the mismatch arm rather than failing.
            let matches = std::str::from_utf8(&bytes)
                .ok()
                .is_some_and(|value| value.trim() == EPUB_MIME_TYPE);
            if !matches {
                emitter.warning(WARNING_MIMETYPE_MISMATCH, package_href, None);
            }
        }
    }
    Ok(())
}

/// Read `META-INF/container.xml` and return the first package rootfile href
/// as a normalized member name. A missing container, a container that does
/// not parse, no `rootfile` with the package media type, or a `full-path`
/// naming no member is a failure at stage `Container` (§5.2).
///
/// The container is an XML member like any other, so the §6 rules apply to
/// it through `xhtml`: BOM and declared-encoding handling and
/// `max_document_bytes` in `decode`, the entity pre-pass, and
/// `max_element_depth` in `parse`. Their failures arrive at stage `Archive`
/// and are re-staged to `Container` by the caller in `mod.rs` (module doc).
pub(crate) fn read_container(
    archive: &mut Archive,
    limits: &EpubLimits,
) -> Result<String, EpubFailure> {
    let bytes = archive
        .read(CONTAINER_MEMBER)?
        .ok_or_else(|| container_failure(format!("{CONTAINER_MEMBER} is missing")))?;
    let decoded = xhtml::decode(&bytes, CONTAINER_MEMBER, limits)?;
    let rewritten = xhtml::rewrite_entities(&decoded);
    let document = xhtml::parse(&rewritten, CONTAINER_MEMBER, limits)?;
    let full_path = document
        .descendants()
        .filter(|node| node.is_element() && xhtml::local_name(*node) == "rootfile")
        .find(|node| {
            node.attribute("media-type")
                .is_some_and(|media_type| media_type.trim() == PACKAGE_MEDIA_TYPE)
        })
        .and_then(|node| node.attribute("full-path"))
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            container_failure(format!(
                "{CONTAINER_MEMBER} has no rootfile with media-type {PACKAGE_MEDIA_TYPE}"
            ))
        })?;
    let member = normalize_root_path(full_path).ok_or_else(|| {
        container_failure(format!(
            "rootfile full-path '{full_path}' escapes the archive root"
        ))
    })?;
    if !archive.contains(&member) {
        return Err(container_failure(format!(
            "rootfile member '{member}' does not exist in the archive"
        )));
    }
    Ok(member)
}

/// Read and parse the package document named by `rootfile` (§5.2): version
/// with its warning, `DocumentBody` metadata, the manifest indexed by id
/// with hrefs resolved against the package directory, the guide, and the
/// spine. A spine with no resolvable content document is a failure.
///
/// `rootfile` is already a normalized member name that `read_container`
/// verified exists, so an absent member here is reported as a failure
/// rather than treated as impossible.
pub(crate) fn read_package(
    archive: &mut Archive,
    rootfile: &str,
    limits: &EpubLimits,
    emitter: &mut Emitter,
) -> Result<Package, EpubFailure> {
    let bytes = archive
        .read(rootfile)?
        .ok_or_else(|| package_failure(format!("package member '{rootfile}' does not exist")))?;
    let decoded = xhtml::decode(&bytes, rootfile, limits)?;
    let rewritten = xhtml::rewrite_entities(&decoded);
    let document = xhtml::parse(&rewritten, rootfile, limits)?;
    let root = document.root_element();

    // §5.2: the version is recorded as written (empty when absent) and only
    // warned about, never a failure.
    let version = root.attribute("version").map(str::trim).unwrap_or("");
    if !version_is_accepted(version) {
        emitter.warning(
            WARNING_PACKAGE_VERSION,
            rootfile,
            Some(xhtml::locator(rootfile, root, None)),
        );
    }

    let metadata = child_element(root, "metadata").map_or_else(empty_metadata, read_metadata);
    let manifest = child_element(root, "manifest")
        .map_or_else(BTreeMap::new, |manifest| read_manifest(manifest, rootfile));
    let guide = child_element(root, "guide").map_or_else(Vec::new, read_guide);

    let (spine, toc_id) = match child_element(root, "spine") {
        Some(spine) => read_spine(spine, &manifest, archive, rootfile, emitter),
        None => (Vec::new(), None),
    };
    if spine.is_empty() {
        return Err(package_failure(
            "spine has no resolvable content documents".to_string(),
        ));
    }

    Ok(Package {
        href: rootfile.to_string(),
        version: version.to_string(),
        metadata,
        manifest,
        spine,
        toc_id,
        guide,
    })
}

/// Whether a package `version` is one §5.2 accepts silently.
fn version_is_accepted(version: &str) -> bool {
    version == VERSION_EPUB2 || version.starts_with(VERSION_EPUB3_PREFIX)
}

/// `DocumentBody` with every field absent, for a package with no
/// `metadata` element.
fn empty_metadata() -> DocumentBody {
    DocumentBody {
        title: None,
        creators: None,
        publisher: None,
        language: None,
        identifiers: None,
        date: None,
        description: None,
    }
}

/// §5.2 metadata rules over the `metadata` subtree: first `title`,
/// `publisher`, `language`, `date`, and `description`; every `creator` and
/// `identifier` in document order. Elements match by local name so the
/// EPUB 2 `dc-metadata` wrapper and either DC prefix are covered. Values are
/// trimmed and empty ones omitted; `description` alone goes through the §8
/// text rules because it may carry markup.
fn read_metadata(metadata: roxmltree::Node) -> DocumentBody {
    let mut body = empty_metadata();
    let mut creators = Vec::new();
    let mut identifiers = Vec::new();
    // The description context has no note references (there are none in
    // package metadata) and no package language yet: the language is one
    // of the fields being read.
    let description_context = TextContext {
        is_noteref: &|_| false,
        package_language: None,
    };
    for node in metadata.descendants().filter(|node| node.is_element()) {
        match xhtml::local_name(node) {
            "title" => first_value(&mut body.title, node),
            "publisher" => first_value(&mut body.publisher, node),
            "language" => first_value(&mut body.language, node),
            "date" => first_value(&mut body.date, node),
            "creator" => creators.extend(element_value(node)),
            "identifier" => identifiers.extend(element_value(node)),
            "description" if body.description.is_none() => {
                let stripped = text::extract(node, &description_context);
                if !stripped.is_empty() {
                    body.description = Some(stripped);
                }
            }
            _ => {}
        }
    }
    if !creators.is_empty() {
        body.creators = Some(creators);
    }
    if !identifiers.is_empty() {
        body.identifiers = Some(identifiers);
    }
    body
}

/// Fill `slot` from `node` only when it is still empty and the node carries
/// a non-empty value: the "first non-empty" rule of §5.2.
fn first_value(slot: &mut Option<String>, node: roxmltree::Node) {
    if slot.is_none() {
        *slot = element_value(node);
    }
}

/// Trimmed text content of one element: descendant text nodes concatenated
/// as written (§5.2 "as written"), `None` when empty after trimming.
fn element_value(node: roxmltree::Node) -> Option<String> {
    let mut value = String::new();
    for descendant in node.descendants().filter(|node| node.is_text()) {
        if let Some(text) = descendant.text() {
            value.push_str(text);
        }
    }
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Manifest `item` children indexed by `id` (§5.2). An item is kept only
/// when it has an `id`, an `href`, and that href resolves to an archive
/// member by §5.4 against the package directory; an external or
/// unresolvable href cannot name a member, so the item is omitted and any
/// spine reference to it surfaces as `epub_spine_item_missing`. The first
/// item with a given id wins.
fn read_manifest(manifest: roxmltree::Node, package_href: &str) -> BTreeMap<String, ManifestItem> {
    let mut items = BTreeMap::new();
    for item in manifest
        .children()
        .filter(|node| node.is_element() && xhtml::local_name(*node) == "item")
    {
        let Some(id) = item
            .attribute("id")
            .map(str::trim)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        let Some(href) = item.attribute("href").map(str::trim) else {
            continue;
        };
        let HrefOutcome::Internal(target) = links::resolve(package_href, href) else {
            continue;
        };
        let properties = item
            .attribute("properties")
            .map(|value| value.split_ascii_whitespace().map(str::to_string).collect())
            .unwrap_or_default();
        items.entry(id.to_string()).or_insert(ManifestItem {
            href: target.member,
            media_type: item
                .attribute("media-type")
                .map(str::trim)
                .unwrap_or_default()
                .to_string(),
            properties,
        });
    }
    items
}

/// Guide `reference` children with both `type` and `href` (§5.2), in
/// document order; entries missing either attribute carry no hint.
fn read_guide(guide: roxmltree::Node) -> Vec<GuideReference> {
    guide
        .children()
        .filter(|node| node.is_element() && xhtml::local_name(*node) == "reference")
        .filter_map(|reference| {
            let kind = reference.attribute("type").map(str::trim)?;
            let href = reference.attribute("href").map(str::trim)?;
            (!kind.is_empty() && !href.is_empty()).then(|| GuideReference {
                kind: kind.to_string(),
                href: href.to_string(),
            })
        })
        .collect()
}

/// Spine `itemref` children in reading order plus the `toc` attribute
/// (§5.2). An `idref` with no manifest item, or whose manifest member is not
/// in the archive, is warned as `epub_spine_item_missing` (keyed by the
/// package href, located at the `itemref`) and skipped; `linear="no"` items
/// are kept in place with `linear` false for the walk and the raw report.
fn read_spine(
    spine: roxmltree::Node,
    manifest: &BTreeMap<String, ManifestItem>,
    archive: &Archive,
    package_href: &str,
    emitter: &mut Emitter,
) -> (Vec<SpineItem>, Option<String>) {
    let toc_id = spine
        .attribute("toc")
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    let mut items = Vec::new();
    for itemref in spine
        .children()
        .filter(|node| node.is_element() && xhtml::local_name(*node) == "itemref")
    {
        let idref = itemref
            .attribute("idref")
            .map(str::trim)
            .unwrap_or_default();
        let resolved = manifest
            .get(idref)
            .filter(|item| archive.contains(&item.href));
        let Some(item) = resolved else {
            emitter.warning(
                WARNING_SPINE_ITEM_MISSING,
                package_href,
                Some(xhtml::locator(package_href, itemref, None)),
            );
            continue;
        };
        let linear = itemref
            .attribute("linear")
            .is_none_or(|value| !value.trim().eq_ignore_ascii_case("no"));
        items.push(SpineItem {
            idref: idref.to_string(),
            href: item.href.to_string(),
            linear,
        });
    }
    (items, toc_id)
}

/// First element child of `parent` with the given local name.
fn child_element<'a, 'input>(
    parent: roxmltree::Node<'a, 'input>,
    name: &str,
) -> Option<roxmltree::Node<'a, 'input>> {
    parent
        .children()
        .find(|node| node.is_element() && xhtml::local_name(*node) == name)
}

/// Normalize a container `full-path`, which the OCF spec defines relative to
/// the archive root: empty and `.` segments are dropped and `..` pops, so
/// the result matches the archive's normalized member index. `None` when
/// the path escapes the root or is empty. No percent-decoding: `full-path`
/// is a plain path, and `read_container` has no member to resolve it
/// against, so §5.4 (`links::resolve`) does not apply here.
fn normalize_root_path(path: &str) -> Option<String> {
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            other => segments.push(other),
        }
    }
    (!segments.is_empty()).then(|| segments.join("/"))
}

/// A failure at stage `Container`.
fn container_failure(detail: String) -> EpubFailure {
    EpubFailure::new(EpubStage::Container, detail)
}

/// A failure at stage `Package`.
fn package_failure(detail: String) -> EpubFailure {
    EpubFailure::new(EpubStage::Package, detail)
}
