//! Text extraction (SPEC-epub §8) for text-bearing blocks, captions, cells,
//! terms, and definitions, plus the `pre` and language rules.
//!
//! Steps 1 to 3 (concatenate, remove, inline math) are one recursive walk
//! shared by every entry point; step 4 (whitespace normalization) applies
//! only to block text, never to `pre` content.

use std::borrow::Cow;

use crate::parse::epub::xhtml;

/// Namespace of `xml:lang`, matched by expanded name because `roxmltree`
/// resolves prefixes (§6 ignores namespaces only for element names).
const XML_NAMESPACE: &str = "http://www.w3.org/XML/1998/namespace";

/// Unicode space separators (general category Zs, U+0020 excluded) that §8
/// step 4 converts to U+0020 before collapsing; NBSP is U+00A0.
const SPACE_SEPARATORS: &[char] = &[
    '\u{00A0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}',
    '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200A}', '\u{202F}', '\u{205F}', '\u{3000}',
];

/// `epub:type` token and `role` value of a §7.6 rule 1 page marker.
const PAGEBREAK_EPUB_TYPE: &str = "pagebreak";
const PAGEBREAK_ROLE: &str = "doc-pagebreak";

/// Per-document extraction context: the note-reference predicate (§8 step
/// 2 removes note-reference anchors) and the package language fallback.
pub(crate) struct TextContext<'a> {
    pub is_noteref: &'a dyn Fn(roxmltree::Node) -> bool,
    pub package_language: Option<&'a str>,
}

/// Note-reference predicate as seen by the walk: `None` for the entry point
/// whose contract carries no context (`extract_pre`), where note references
/// are therefore not removed.
type NoterefPredicate<'p> = Option<&'p dyn Fn(roxmltree::Node) -> bool>;

/// Whitespace policy of one walk. `Block` applies the `br` newline rule and
/// separates non-inline child elements by newlines (§7.8 cell flattening);
/// `Pre` keeps every character verbatim and lets `br` contribute nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Block,
    Pre,
}

/// One fragment of collected text. `Break` (a `br`) is kept distinct from
/// `Boundary` (a non-inline child element's edge) because step 4 preserves
/// a blank line only between two consecutive `br`. Newlines inside `Text`
/// are ordinary whitespace (§8 step 4): `br` is the only line-break source.
enum Piece<'a> {
    Text(Cow<'a, str>),
    Break,
    Boundary,
}

/// §8 steps 1 to 4 over one element. The element itself is never subject to
/// step 2 removal or a boundary newline; only its descendants are.
pub(crate) fn extract(node: roxmltree::Node, ctx: &TextContext) -> String {
    let mut pieces = Vec::new();
    collect(node, true, Mode::Block, Some(ctx.is_noteref), &mut pieces);
    normalize(&pieces)
}

/// §8 steps 1 to 4 over one mixed-content run of sibling nodes. Each node is
/// a child of the run's parent, so step 2 removal applies to every one of
/// them (a run may start with an empty anchor).
pub(crate) fn extract_run(nodes: &[roxmltree::Node], ctx: &TextContext) -> String {
    let mut pieces = Vec::new();
    for node in nodes {
        collect(*node, false, Mode::Block, Some(ctx.is_noteref), &mut pieces);
    }
    normalize(&pieces)
}

/// `pre` text: steps 1 to 3 with whitespace preserved, CRLF normalized to
/// LF, and one leading newline stripped (§7.7, §8). Step 4 never applies.
/// The contract carries no context, so note-reference anchors inside `pre`
/// are kept.
pub(crate) fn extract_pre(node: roxmltree::Node) -> String {
    let mut pieces = Vec::new();
    collect(node, true, Mode::Pre, None, &mut pieces);
    let mut verbatim = String::new();
    for piece in &pieces {
        if let Piece::Text(text) = piece {
            verbatim.push_str(text);
        }
    }
    let normalized = verbatim.replace("\r\n", "\n");
    normalized
        .strip_prefix('\n')
        .map_or(normalized.clone(), str::to_string)
}

/// Nearest ancestor-or-self `xml:lang` or `lang`, else the package language.
/// `xml:lang` wins on the same element because XHTML defines it as the
/// authoritative form.
pub(crate) fn language(node: roxmltree::Node, package_language: Option<&str>) -> Option<String> {
    for ancestor in node.ancestors() {
        if !ancestor.is_element() {
            continue;
        }
        let declared = ancestor
            .attribute((XML_NAMESPACE, "lang"))
            .or_else(|| ancestor.attribute("lang"))
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(value) = declared {
            return Some(value.to_string());
        }
    }
    package_language
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Steps 1 to 3 over one node's subtree, in document order. `root` marks the
/// entry element, which is exempt from removal and boundary newlines so a
/// caller can extract a `math`, `a`, or block element directly. Comments,
/// processing instructions, and (by `roxmltree` merging) nothing else
/// contribute text.
fn collect<'a>(
    node: roxmltree::Node<'a, '_>,
    root: bool,
    mode: Mode,
    noteref: NoterefPredicate<'_>,
    out: &mut Vec<Piece<'a>>,
) {
    if node.is_text() {
        if let Some(text) = node.text() {
            out.push(Piece::Text(Cow::Borrowed(text)));
        }
        return;
    }
    if !node.is_element() {
        return;
    }
    let name = xhtml::local_name(node);
    if !root && is_removed(node, name, noteref) {
        return;
    }
    match name {
        // Step 1: `br` is one newline in block mode; §8 last paragraph
        // withholds that rule from `pre`, where it contributes nothing.
        "br" => {
            if mode == Mode::Block {
                out.push(Piece::Break);
            }
            return;
        }
        // Step 3: any `math` met inside a walk is inline by construction
        // (block `math` is a container the walker never descends into); the
        // root case serves the §7.7 formula row through the same rule.
        "math" => {
            out.push(Piece::Text(Cow::Owned(math_text(node))));
            return;
        }
        _ => {}
    }
    // Inline element boundaries add nothing (step 1). A non-inline child
    // only occurs when a caller extracts a cell or caption holding block
    // children, which §7.8 flattens with newlines between them.
    let boundary = !root && mode == Mode::Block && !xhtml::is_inline(node);
    if boundary {
        out.push(Piece::Boundary);
    }
    for child in node.children() {
        collect(child, false, mode, noteref, out);
    }
    if boundary {
        out.push(Piece::Boundary);
    }
}

/// Step 2 removal list: note-reference anchors, empty anchors (HTMLBook
/// `indexterm` among them), `sup` holding only a note reference, `img`,
/// §7.6 rule 1 markers, and inline `script`/`style`. Element ids are never
/// page markers (§7.6), so an empty `span` with a page-like id is kept.
fn is_removed(node: roxmltree::Node, name: &str, noteref: NoterefPredicate<'_>) -> bool {
    if is_pagebreak_element(node) {
        return true;
    }
    match name {
        "img" | "script" | "style" => true,
        "a" => noteref.is_some_and(|is_noteref| is_noteref(node)) || is_empty_inline(node),
        "sup" => holds_only_noteref(node, noteref),
        _ => false,
    }
}

/// §7.6 rule 1: `epub:type` carrying the `pagebreak` token or `role`
/// carrying `doc-pagebreak`; both attributes are space-separated token lists.
fn is_pagebreak_element(node: roxmltree::Node) -> bool {
    let epub_pagebreak = xhtml::epub_type(node).is_some_and(|value| {
        value
            .split_ascii_whitespace()
            .any(|token| token == PAGEBREAK_EPUB_TYPE)
    });
    epub_pagebreak
        || node.attribute("role").is_some_and(|value| {
            value
                .split_ascii_whitespace()
                .any(|token| token == PAGEBREAK_ROLE)
        })
}

/// An inline element with no element children and no non-whitespace text:
/// the "empty anchor" of step 2.
fn is_empty_inline(node: roxmltree::Node) -> bool {
    node.children().all(|child| {
        child.is_text()
            && child
                .text()
                .is_none_or(|text| text.chars().all(char::is_whitespace))
    })
}

/// A `sup` whose only content is a note reference: at least one child `a`
/// that is a note reference, and nothing else but whitespace text.
fn holds_only_noteref(node: roxmltree::Node, noteref: NoterefPredicate<'_>) -> bool {
    let Some(is_noteref) = noteref else {
        return false;
    };
    let mut saw_noteref = false;
    for child in node.children() {
        if child.is_element() {
            if xhtml::local_name(child) == "a" && is_noteref(child) {
                saw_noteref = true;
                continue;
            }
            return false;
        }
        if child.is_text()
            && child
                .text()
                .is_some_and(|text| text.chars().any(|ch| !ch.is_whitespace()))
        {
            return false;
        }
    }
    saw_noteref
}

/// §7.7 `math` text: `alttext`, else the text of the first `annotation`
/// descendant, else the flattened content. Flattened content collapses all
/// whitespace to single spaces so pretty-printed MathML never contributes
/// line breaks to the enclosing block.
fn math_text(node: roxmltree::Node) -> String {
    if let Some(alt) = node.attribute("alttext").map(str::trim)
        && !alt.is_empty()
    {
        return alt.to_string();
    }
    let annotation = node
        .descendants()
        .find(|d| d.is_element() && xhtml::local_name(*d) == "annotation")
        .map(flatten_text)
        .filter(|text| !text.is_empty());
    annotation.unwrap_or_else(|| flatten_text(node))
}

/// Descendant text joined into one whitespace-collapsed, trimmed line.
fn flatten_text(node: roxmltree::Node) -> String {
    let mut out = String::new();
    let mut pending_space = false;
    for descendant in node.descendants() {
        let Some(text) = descendant.is_text().then(|| descendant.text()).flatten() else {
            continue;
        };
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

/// Step 4 over collected pieces. Lines end only at `Boundary` and at
/// `Break`; a newline or carriage return inside source text is ordinary
/// whitespace, pushed as a space so `collapse_line` folds it into the
/// surrounding run (`br` is the only line-break source). Each line is
/// space-normalized and trimmed; empty lines are dropped except that one
/// blank line separates two non-empty lines when two or more `br` (and
/// nothing else non-empty) lie between them. Leading and trailing blank
/// lines never survive, which is the whole-text trim.
fn normalize(pieces: &[Piece]) -> String {
    let mut assembler = LineAssembler::default();
    for piece in pieces {
        match piece {
            Piece::Text(text) => {
                for ch in text.chars() {
                    if ch == '\n' || ch == '\r' {
                        assembler.line.push(' ');
                    } else {
                        assembler.line.push(ch);
                    }
                }
            }
            Piece::Boundary => assembler.end_line(false),
            Piece::Break => assembler.end_line(true),
        }
    }
    assembler.end_line(false);
    assembler.out
}

/// Line-by-line state of `normalize`. `breaks_since_text` counts `br` seen
/// since the last non-empty line was emitted; it decides the blank-line rule
/// and resets on every emitted line.
#[derive(Default)]
struct LineAssembler {
    out: String,
    line: String,
    breaks_since_text: usize,
}

impl LineAssembler {
    /// Close the current line. The `br` that closed it (if any) is counted
    /// after the line is emitted so it belongs to the gap that follows.
    fn end_line(&mut self, from_break: bool) {
        let text = collapse_line(&self.line);
        self.line.clear();
        if !text.is_empty() {
            if !self.out.is_empty() {
                self.out.push('\n');
                if self.breaks_since_text >= 2 {
                    self.out.push('\n');
                }
            }
            self.out.push_str(&text);
            self.breaks_since_text = 0;
        }
        if from_break {
            self.breaks_since_text += 1;
        }
    }
}

/// One line of step 4: space separators and tabs become U+0020, runs
/// collapse to one space, and the line is trimmed.
fn collapse_line(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut pending_space = false;
    for ch in line.chars() {
        let is_space = ch == ' ' || ch == '\t' || SPACE_SEPARATORS.contains(&ch);
        if is_space {
            pending_space = true;
        } else {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            out.push(ch);
        }
    }
    // Trailing pending space is dropped; other trailing whitespace (Unicode
    // line or paragraph separators, which `normalize` does not fold) still
    // needs the trim.
    out.trim().to_string()
}
