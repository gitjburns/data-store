//! Href resolution, note references, and cross-references (SPEC-epub §5.4,
//! §10.3, §10.4): resolve hrefs against the referencing member, classify
//! links against the pass-one footnote index, and emit `references` edges.
//!
//! Ownership: `FootnoteIndex` is filled by pass one (`structure::index_footnotes`)
//! and read during pass two; `UnitIndex` and the `LinkRecord` list are filled
//! during pass two and consumed once by `resolve_all` after the last unit, so
//! a link whose target lies in a later spine document still resolves.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::locator::Locator;
use crate::model::{RELATIONSHIP_ROLES_REFERENCES, UnitRelationshipType};
use crate::parse::epub::navigation::Target;
use crate::parse::epub::report::StructureReport;
use crate::parse::epub::{Emitter, WorkerResult};

/// §11.5 warning code for an internal link that resolved to no unit.
const WARNING_LINK_UNRESOLVED: &str = "epub_link_unresolved";

/// Result of resolving one href by the §5.4 rules.
pub(crate) enum HrefOutcome {
    Internal(Target),
    /// Carries a URI scheme; never resolved.
    External,
    /// Percent-decoding, UTF-8, or root-escape failure.
    Unresolvable,
}

/// Resolve `href` against `base_member` (§5.4): strip the fragment, detect
/// a URI scheme (external, never resolved), percent-decode the path
/// requiring UTF-8, resolve `.` and `..` against the base member's
/// directory, and reject a result that escapes the archive root. The
/// fragment is kept as written; an empty fragment (`href="#"`) is absent.
/// An empty path (`#id`) targets the base member itself. The produced
/// member name uses the same segment normalization as the archive index,
/// so it is directly usable as a lookup key.
pub(crate) fn resolve(base_member: &str, href: &str) -> HrefOutcome {
    let (path, fragment) = match href.split_once('#') {
        Some((path, fragment)) => (path, Some(fragment)),
        None => (href, None),
    };
    if has_scheme(path) {
        return HrefOutcome::External;
    }
    let fragment = fragment
        .filter(|fragment| !fragment.is_empty())
        .map(str::to_string);
    if path.is_empty() {
        return HrefOutcome::Internal(Target {
            member: base_member.to_string(),
            fragment,
        });
    }
    let Some(decoded) = percent_decode(path) else {
        return HrefOutcome::Unresolvable;
    };
    match resolve_path(base_member, &decoded) {
        Some(member) => HrefOutcome::Internal(Target { member, fragment }),
        None => HrefOutcome::Unresolvable,
    }
}

/// Whether `path` (fragment already stripped) begins with an RFC 3986
/// scheme: an ASCII letter, then letters, digits, `+`, `-`, or `.`, then
/// `:`, all before the first `/`. A `:` anywhere else is path data.
fn has_scheme(path: &str) -> bool {
    let first_segment = path.split('/').next().unwrap_or_default();
    let Some((scheme, _)) = first_segment.split_once(':') else {
        return false;
    };
    let mut chars = scheme.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// Decode `%XX` escapes into bytes and require the result to be valid
/// UTF-8. `None` for a `%` not followed by two hex digits or for a
/// non-UTF-8 result; both are §5.4 resolution failures.
fn percent_decode(path: &str) -> Option<String> {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = hex_value(*bytes.get(index + 1)?)?;
            let low = hex_value(*bytes.get(index + 2)?)?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

/// Value of one ASCII hex digit.
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Resolve a decoded path against the directory of `base_member` (or the
/// archive root when the path is absolute) with the archive's segment
/// rules: empty and `.` segments dropped, `..` popping the previous
/// segment. `None` when `..` would leave the root or when nothing remains.
fn resolve_path(base_member: &str, path: &str) -> Option<String> {
    let mut segments: Vec<&str> = if path.starts_with('/') {
        Vec::new()
    } else {
        // The base member is already normalized, so its directory has no
        // empty or dot segments to filter.
        base_member
            .rsplit_once('/')
            .map(|(directory, _)| directory.split('/').collect())
            .unwrap_or_default()
    };
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            other => segments.push(other),
        }
    }
    if segments.is_empty() {
        return None;
    }
    Some(segments.join("/"))
}

/// Pass-one index: which `(member, id)` targets are, or lie inside,
/// footnote blocks (§10.3, plan Section 2). Pass one inserts every id that
/// qualifies, including every id of a document classified `notes` at
/// document granularity, so a lookup is a plain membership test.
#[derive(Default)]
pub(crate) struct FootnoteIndex {
    /// Footnote-target ids keyed by normalized member name.
    ids_by_member: BTreeMap<String, BTreeSet<String>>,
}

impl FootnoteIndex {
    /// An index with no footnote targets.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record that the element with `id` in `member` is, or lies inside,
    /// a footnote block.
    pub(crate) fn insert(&mut self, member: &str, id: &str) {
        self.ids_by_member
            .entry(member.to_string())
            .or_default()
            .insert(id.to_string());
    }

    /// Whether `target` resolves to a footnote block. A fragment-less
    /// target names a document, never a block, so it is never a footnote.
    pub(crate) fn is_footnote(&self, target: &Target) -> bool {
        target.fragment.as_deref().is_some_and(|id| {
            self.ids_by_member
                .get(&target.member)
                .is_some_and(|ids| ids.contains(id))
        })
    }
}

/// One internal link collected in pass two, resolved after the walk.
pub(crate) struct LinkRecord {
    /// Evidence unit containing the link.
    pub from_local_id: String,
    /// Document the link appears in; keys the §11.5 warning.
    pub document: String,
    /// The href as written, for the §11.3 report.
    pub href: String,
    /// Locator of the containing block, the first-instance locator of an
    /// `epub_link_unresolved` warning.
    pub locator: Locator,
    pub target: Target,
    pub role_hint: LinkRole,
}

/// The `references` role a link resolves to (§10.3, §10.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkRole {
    Footnote,
    CrossReference,
    IndexLocator,
}

impl LinkRole {
    /// The `relationshipRole` wire value, taken from the model's single
    /// definition; the order of `RELATIONSHIP_ROLES_REFERENCES` is
    /// footnote, cross_reference, index_locator.
    fn wire_name(self) -> &'static str {
        match self {
            Self::Footnote => RELATIONSHIP_ROLES_REFERENCES[0],
            Self::CrossReference => RELATIONSHIP_ROLES_REFERENCES[1],
            Self::IndexLocator => RELATIONSHIP_ROLES_REFERENCES[2],
        }
    }
}

/// The unit a target key resolves to, and whether a later `insert` may
/// still replace it.
struct UnitEntry {
    local_id: String,
    /// Set by `pin`: the §10.4 navigation-target rule fixes the section
    /// unit as the resolution even though the heading block emitted inside
    /// it also contains the target element.
    pinned: bool,
}

/// Pass-two index from `(member, id)` to the local id of the innermost unit
/// whose source element contains the element carrying that id, plus a
/// per-document entry for fragment-less targets (§10.4).
///
/// Filling contract for the walk: `insert` is called in containment order,
/// outermost unit first, so the last insert for a key is the innermost
/// unit; an id whose element lies in no unit is inserted with the section
/// current at its walk position; `pin` records a navigation target's
/// `text_section` and blocks later replacement; `insert_document` records
/// the unit a fragment-less link to that member resolves to. A key never
/// inserted is an unresolved link.
#[derive(Default)]
pub(crate) struct UnitIndex {
    /// Resolutions for `(member, id)` keys.
    by_id: BTreeMap<String, BTreeMap<String, UnitEntry>>,
    /// Resolutions for fragment-less targets, keyed by member.
    by_document: BTreeMap<String, String>,
}

impl UnitIndex {
    /// An index with no resolutions.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record `local_id` as the innermost unit so far for `(member, id)`;
    /// replaces an earlier entry unless that entry was pinned.
    pub(crate) fn insert(&mut self, member: &str, id: &str, local_id: &str) {
        let ids = self.by_id.entry(member.to_string()).or_default();
        match ids.get_mut(id) {
            Some(entry) if entry.pinned => {}
            Some(entry) => entry.local_id = local_id.to_string(),
            None => {
                ids.insert(
                    id.to_string(),
                    UnitEntry {
                        local_id: local_id.to_string(),
                        pinned: false,
                    },
                );
            }
        }
    }

    /// Record `local_id` as the final resolution for `(member, id)`: the
    /// `text_section` emitted for a navigation target (§10.4). Later
    /// `insert` calls for the key are ignored.
    pub(crate) fn pin(&mut self, member: &str, id: &str, local_id: &str) {
        self.by_id.entry(member.to_string()).or_default().insert(
            id.to_string(),
            UnitEntry {
                local_id: local_id.to_string(),
                pinned: true,
            },
        );
    }

    /// Record the unit a fragment-less link to `member` resolves to; a
    /// repeat call replaces the earlier value.
    pub(crate) fn insert_document(&mut self, member: &str, local_id: &str) {
        self.by_document
            .insert(member.to_string(), local_id.to_string());
    }

    /// The local id `target` resolves to, if any.
    fn lookup(&self, target: &Target) -> Option<&str> {
        match target.fragment.as_deref() {
            Some(id) => self
                .by_id
                .get(&target.member)
                .and_then(|ids| ids.get(id))
                .map(|entry| entry.local_id.as_str()),
            None => self.by_document.get(&target.member).map(String::as_str),
        }
    }
}

/// Resolve every collected link to a `references` edge or an
/// `epub_link_unresolved` warning (§10.3, §10.4). Edges are buffered in
/// link source order, which is the §10.1 order for `references`; each
/// unresolved link counts once under its document's warning aggregate and
/// is listed by href in the report. Only a candidate-cap failure from the
/// emitter stops the loop.
pub(crate) fn resolve_all(
    links: &[LinkRecord],
    units: &UnitIndex,
    emitter: &mut Emitter,
    report: &mut StructureReport,
) -> WorkerResult<()> {
    for link in links {
        match units.lookup(&link.target) {
            Some(to_local_id) => emitter.relationship(
                &link.from_local_id,
                to_local_id,
                UnitRelationshipType::References,
                Some(link.role_hint.wire_name()),
            )?,
            None => {
                // The record is borrowed through the contracted slice and
                // the warning aggregate takes ownership of its first
                // locator, so that locator is cloned here.
                emitter.warning(
                    WARNING_LINK_UNRESOLVED,
                    &link.document,
                    Some(link.locator.clone()),
                );
                report.unresolved_link(&link.document, &link.href);
            }
        }
    }
    Ok(())
}
