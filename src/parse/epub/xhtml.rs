//! XML member handling (SPEC-epub §6, §9.1): encoding rule, entity pre-pass,
//! `roxmltree` parsing with the depth cap, DOM helpers, element paths, and
//! `dom_path` locators.
//!
//! Failures raised here carry stage `Archive` (the `EpubFailure` contract for
//! helpers that do not know the §11.2 boundary); the caller re-stages them
//! with `EpubFailure::with_stage` before propagating.

use std::collections::BTreeMap;

use crate::limits::EpubLimits;
use crate::model::locator::{DomPathLocator, Locator};
use crate::parse::epub::entities;
use crate::parse::epub::{EpubFailure, EpubStage};

/// Namespace URI of `epub:type` (§6).
const EPUB_NAMESPACE: &str = "http://www.idpf.org/2007/ops";

/// UTF-8 byte-order mark; stripped before decoding.
const BOM_UTF8: [u8; 3] = [0xEF, 0xBB, 0xBF];
/// UTF-16 big-endian byte-order mark.
const BOM_UTF16_BE: [u8; 2] = [0xFE, 0xFF];
/// UTF-16 little-endian byte-order mark.
const BOM_UTF16_LE: [u8; 2] = [0xFF, 0xFE];

/// Longest named reference the pre-pass will consider (`&name;` with the
/// name up to this many bytes). XHTML 1.1 names are at most eight bytes;
/// bounding the lookahead keeps the pass linear on inputs with many `&`.
const MAX_ENTITY_NAME_BYTES: usize = 16;

/// The five predefined XML references left for the parser (§6).
const XML_ENTITIES: [&str; 5] = ["amp", "lt", "gt", "quot", "apos"];

/// §7.5 inline element local names, excluding the conditional `math`.
const INLINE_ELEMENTS: [&str; 28] = [
    "a", "abbr", "b", "bdi", "bdo", "br", "cite", "code", "data", "dfn", "em", "i", "img", "kbd",
    "mark", "q", "s", "samp", "small", "span", "strong", "sub", "sup", "time", "tt", "u", "var",
    "wbr",
];

/// §7.5 elements skipped entirely, unconditionally.
const SKIPPED_ELEMENTS: [&str; 4] = ["head", "script", "style", "template"];

/// `epub:type` tokens that make a `nav` element skipped (§7.5).
const SKIPPED_NAV_TYPES: [&str; 3] = ["toc", "landmarks", "page-list"];

/// Decode one XML member's bytes by the §6 encoding rule under
/// `max_document_bytes`: UTF-16 when a byte-order mark is present, UTF-8
/// under no declaration or a UTF-8/UTF-16 declaration, ASCII only under any
/// other declaration.
pub(crate) fn decode(
    bytes: &[u8],
    member: &str,
    limits: &EpubLimits,
) -> Result<String, EpubFailure> {
    let text = if bytes.starts_with(&BOM_UTF16_BE) {
        decode_utf16(&bytes[BOM_UTF16_BE.len()..], member, u16::from_be_bytes)?
    } else if bytes.starts_with(&BOM_UTF16_LE) {
        decode_utf16(&bytes[BOM_UTF16_LE.len()..], member, u16::from_le_bytes)?
    } else {
        let body = bytes.strip_prefix(&BOM_UTF8).unwrap_or(bytes);
        // A foreign declaration admits only the ASCII subset, so the first
        // high byte fails naming the member and the declaration; ASCII is a
        // UTF-8 subset, so the same decode below serves both cases.
        if let Some(encoding) = declared_encoding(body).filter(|e| !is_unicode_encoding(e))
            && body.iter().any(|byte| *byte >= 0x80)
        {
            return Err(EpubFailure::new(
                EpubStage::Archive,
                format!(
                    "member {member} declares encoding {encoding}; \
                     only ASCII content is accepted under that declaration"
                ),
            ));
        }
        String::from_utf8(body.to_vec()).map_err(|error| {
            EpubFailure::new(
                EpubStage::Archive,
                format!("member {member} is not valid UTF-8: {error}"),
            )
        })?
    };
    if text.len() > limits.max_document_bytes {
        return Err(EpubFailure::new(
            EpubStage::Archive,
            format!(
                "member {member} decodes to {} bytes, exceeding max_document_bytes {}",
                text.len(),
                limits.max_document_bytes
            ),
        ));
    }
    Ok(text)
}

/// Decode UTF-16 code units after the byte-order mark with the given byte
/// order; an odd trailing byte or an unpaired surrogate fails naming the
/// member.
fn decode_utf16(
    body: &[u8],
    member: &str,
    unit: fn([u8; 2]) -> u16,
) -> Result<String, EpubFailure> {
    if !body.len().is_multiple_of(2) {
        return Err(EpubFailure::new(
            EpubStage::Archive,
            format!("member {member} has a UTF-16 byte-order mark but an odd byte length"),
        ));
    }
    let units = body.chunks_exact(2).map(|pair| unit([pair[0], pair[1]]));
    char::decode_utf16(units)
        .collect::<Result<String, _>>()
        .map_err(|error| {
            EpubFailure::new(
                EpubStage::Archive,
                format!("member {member} is not valid UTF-16: {error}"),
            )
        })
}

/// Whether a declared encoding name means UTF-8 or UTF-16 (§6: such members
/// are decoded as UTF-8 when no byte-order mark is present).
fn is_unicode_encoding(encoding: &str) -> bool {
    encoding.eq_ignore_ascii_case("utf-8")
        || encoding.eq_ignore_ascii_case("utf8")
        || encoding.eq_ignore_ascii_case("utf-16")
        || encoding.eq_ignore_ascii_case("utf16")
}

/// The `encoding` pseudo-attribute of a leading XML declaration, read from
/// the raw bytes before any decoding. The declaration is ASCII by the XML
/// spec, so byte-level scanning is exact; anything not shaped like
/// `<?xml ... encoding="..." ...?>` yields `None`.
fn declared_encoding(body: &[u8]) -> Option<&str> {
    let body = body.strip_prefix(b"<?xml")?;
    let end = body.windows(2).position(|window| window == b"?>")?;
    let declaration = &body[..end];
    let key = declaration
        .windows(8)
        .position(|window| window == b"encoding")?;
    let rest = &declaration[key + 8..];
    let rest = rest.strip_prefix(b"=").or_else(|| {
        // Whitespace is permitted around `=` in the XML declaration.
        let trimmed = rest.iter().position(|byte| !byte.is_ascii_whitespace())?;
        rest[trimmed..].strip_prefix(b"=")
    })?;
    let start = rest.iter().position(|byte| !byte.is_ascii_whitespace())?;
    let quote = rest[start];
    if quote != b'"' && quote != b'\'' {
        return None;
    }
    let value = &rest[start + 1..];
    let close = value.iter().position(|byte| *byte == quote)?;
    std::str::from_utf8(&value[..close]).ok()
}

/// Linear pre-pass rewriting XHTML named character references to numeric
/// references via `entities::codepoint`. References not in the table, and the
/// five XML entities, are copied unchanged for the parser to judge.
pub(crate) fn rewrite_entities(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    let mut cursor = 0;
    while let Some(offset) = bytes[cursor..].iter().position(|byte| *byte == b'&') {
        let amp = cursor + offset;
        // Bounded lookahead for `;` keeps the pass linear.
        let window_end = (amp + 1 + MAX_ENTITY_NAME_BYTES + 1).min(bytes.len());
        let Some(semi) = bytes[amp + 1..window_end]
            .iter()
            .position(|byte| *byte == b';')
            .map(|position| amp + 1 + position)
        else {
            cursor = amp + 1;
            continue;
        };
        let name = &bytes[amp + 1..semi];
        // Names are ASCII alphanumerics; `&#...;` and non-ASCII bytes are
        // never table entries. The slice below is on ASCII boundaries only
        // when this holds, so it doubles as the UTF-8 boundary guard.
        let is_candidate = !name.is_empty() && name.iter().all(u8::is_ascii_alphanumeric);
        // The `ok()` cannot fail after `is_candidate`, but the error arm is
        // kept so no unwrap is needed.
        let replacement = if is_candidate {
            std::str::from_utf8(name)
                .ok()
                .filter(|name| !XML_ENTITIES.contains(name))
                .and_then(entities::codepoint)
        } else {
            None
        };
        if let Some(codepoint) = replacement {
            out.push_str(&text[copied..amp]);
            out.push_str(&format!("&#{codepoint};"));
            copied = semi + 1;
            cursor = semi + 1;
        } else {
            cursor = amp + 1;
        }
    }
    out.push_str(&text[copied..]);
    out
}

/// Parse a decoded member, enforcing `max_element_depth`. A DTD is allowed
/// because XHTML members carry a `DOCTYPE`; `roxmltree` still guards entity
/// expansion. A malformed member fails naming the member and the parser's
/// error (§6).
pub(crate) fn parse<'a>(
    text: &'a str,
    member: &str,
    limits: &EpubLimits,
) -> Result<roxmltree::Document<'a>, EpubFailure> {
    let options = roxmltree::ParsingOptions {
        allow_dtd: true,
        ..roxmltree::ParsingOptions::default()
    };
    let document = roxmltree::Document::parse_with_options(text, options).map_err(|error| {
        EpubFailure::new(
            EpubStage::Archive,
            format!("member {member} is not well-formed XML: {error}"),
        )
    })?;
    check_depth(&document, member, limits.max_element_depth)?;
    Ok(document)
}

/// Fail when any element nests deeper than `max_depth` (the document element
/// is depth 1). Iterative with an explicit stack so the check itself never
/// recurses on the input's shape.
fn check_depth(
    document: &roxmltree::Document,
    member: &str,
    max_depth: usize,
) -> Result<(), EpubFailure> {
    let mut stack = vec![(document.root_element(), 1usize)];
    while let Some((node, depth)) = stack.pop() {
        if depth > max_depth {
            return Err(EpubFailure::new(
                EpubStage::Archive,
                format!("member {member} nests elements deeper than max_element_depth {max_depth}"),
            ));
        }
        stack.extend(
            node.children()
                .filter(roxmltree::Node::is_element)
                .map(|child| (child, depth + 1)),
        );
    }
    Ok(())
}

/// Local name of an element node; namespaces are ignored (§6). Empty for
/// non-element nodes.
pub(crate) fn local_name<'a>(node: roxmltree::Node<'a, '_>) -> &'a str {
    node.tag_name().name()
}

/// The `epub:type` attribute (local name `type` in the EPUB namespace).
pub(crate) fn epub_type<'a>(node: roxmltree::Node<'a, '_>) -> Option<&'a str> {
    node.attribute((EPUB_NAMESPACE, "type"))
}

/// The HTMLBook `data-type` attribute.
pub(crate) fn data_type<'a>(node: roxmltree::Node<'a, '_>) -> Option<&'a str> {
    node.attribute("data-type")
}

/// §9.1 element path from the document element to `node`: one
/// `/<localname>[<n>]` step per element, `n` counting same-named element
/// siblings from 1. Computed on the parsed tree, which the entity pre-pass
/// does not restructure.
pub(crate) fn element_path(node: roxmltree::Node) -> String {
    // `ancestors()` starts at the node itself and ends at the Root node,
    // which is not an element and is filtered out.
    let mut chain: Vec<roxmltree::Node> = node.ancestors().filter(|n| n.is_element()).collect();
    chain.reverse();
    let mut path = String::new();
    for element in chain {
        let name = local_name(element);
        let ordinal = element
            .prev_siblings()
            .filter(|sibling| sibling.is_element() && local_name(*sibling) == name)
            .count()
            + 1;
        path.push_str(&format!("/{name}[{ordinal}]"));
    }
    path
}

/// Build the `dom_path` locator for `node` in `document`, with an optional
/// child-node range for mixed-content runs (§9.2). `elementId` is set when
/// the element carries an `id`.
pub(crate) fn locator(
    document: &str,
    node: roxmltree::Node,
    node_range: Option<[u64; 2]>,
) -> Locator {
    Locator::DomPath(DomPathLocator {
        document: document.to_string(),
        path: element_path(node),
        element_id: node.attribute("id").map(str::to_string),
        node_range,
    })
}

/// Whether the element is in the §7.5 inline list. `math` is inline only
/// when its parent element contains non-whitespace text outside the `math`.
pub(crate) fn is_inline(node: roxmltree::Node) -> bool {
    let name = local_name(node);
    if name == "math" {
        return node.parent_element().is_some_and(|parent| {
            parent
                .children()
                .filter(|child| child.id() != node.id())
                .any(has_non_whitespace_text)
        });
    }
    INLINE_ELEMENTS.contains(&name)
}

/// Whether `node` or any descendant is a text node with a non-whitespace
/// character.
fn has_non_whitespace_text(node: roxmltree::Node) -> bool {
    node.descendants()
        .filter(|n| n.is_text())
        .any(|n| n.text().is_some_and(|t| !t.trim().is_empty()))
}

/// Whether the element is in the §7.5 skipped list: `head`, `script`,
/// `style`, `template`, a `nav` whose `epub:type` carries `toc`,
/// `landmarks`, or `page-list`, or any element with a `hidden` attribute.
pub(crate) fn is_skipped(node: roxmltree::Node) -> bool {
    let name = local_name(node);
    if SKIPPED_ELEMENTS.contains(&name) || node.has_attribute("hidden") {
        return true;
    }
    name == "nav"
        && epub_type(node).is_some_and(|value| {
            value
                .split_ascii_whitespace()
                .any(|token| SKIPPED_NAV_TYPES.contains(&token))
        })
}

/// Index of every element `id` in the document to its node. On duplicate
/// ids the first element in document order wins, matching how a fragment
/// target resolves.
pub(crate) fn id_index(doc: &roxmltree::Document) -> BTreeMap<String, roxmltree::NodeId> {
    let mut index = BTreeMap::new();
    for node in doc.descendants().filter(|n| n.is_element()) {
        if let Some(id) = node.attribute("id") {
            index.entry(id.to_string()).or_insert_with(|| node.id());
        }
    }
    index
}
