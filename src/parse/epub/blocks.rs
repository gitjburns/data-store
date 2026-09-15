//! Container mapping, tables, figures, captions, and notes (SPEC-epub §7.5
//! to §7.11). Container emitters emit the container unit only and return
//! what `structure::walk_spine` needs to walk the children itself with that
//! container as parent; they never walk children.
//!
//! Emission contract shared by every emitter here: a unit is streamed with
//! its `dom_path` locator (§9.2), its `contains` edge from `parent` (§10.2:
//! the parent is known at emission, so the edge is emitted here rather than
//! by the walk), every `id` in its source subtree registered in `UnitIndex`
//! (outermost unit first, so a later inner unit overrides), and every
//! internal link inside an evidence unit collected as a `LinkRecord`
//! (§10.3, §10.4). `precedes` chaining is the walk's for siblings it
//! orders; this module chains only the sibling groups it alone emits (rows
//! under a table, cells under a row). `appears_on` is the walk's.
//!
//! Structure only (§1.3): no function here reads text to infer structure;
//! captions, labels, footnotes, and page markers come from markup alone.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::error::ApiError;
use crate::model::body::{
    AsideBody, AsideKind, CaptionBody, CodeBlockBody, FigureBody, ListBody, ListItemBody, ListKind,
    SectionKind, TableBody, TableCellBody, TableHeader, TableRowBody, TableRowRole, TextBlockBody,
    TextBlockRole,
};
use crate::model::{ContentType, Locator, UnitRelationshipType};
use crate::parse::bundle::CandidateContentUnit;
use crate::parse::epub::archive::Archive;
use crate::parse::epub::links::{self, HrefOutcome, LinkRecord, LinkRole, UnitIndex};
use crate::parse::epub::package::Package;
use crate::parse::epub::report::StructureReport;
use crate::parse::epub::text::{self, TextContext};
use crate::parse::epub::xhtml;
use crate::parse::epub::{Emitter, EpubStage, WorkerError, WorkerResult};

/// §11.5 warning codes raised by this module.
const WARNING_IMAGE_MISSING: &str = "epub_image_missing";
const WARNING_IMAGE_TOO_LARGE: &str = "epub_image_too_large";
const WARNING_SVG_INLINE: &str = "epub_svg_inline";
const WARNING_LINK_UNRESOLVED: &str = "epub_link_unresolved";

/// §7.6 rule 1 marker tokens: `epub:type` token and `role` value.
const PAGEBREAK_EPUB_TYPE: &str = "pagebreak";
const PAGEBREAK_ROLE: &str = "doc-pagebreak";

/// §7.6 rule 2 processing-instruction target and its label attribute.
const DP_PI_TARGET: &str = "dp";
const DP_FOLIO_ATTRIBUTE: &str = "folio";

/// Class token marking a declared number (§7.3 `span.label`).
const LABEL_CLASS: &str = "label";

/// `epub:type` token of a pseudo-heading (§7.5 heading rule).
const TITLE_EPUB_TYPE: &str = "title";

/// `epub:type` / `data-type` value marking an attribution (§7.7).
const ATTRIBUTION_TYPE: &str = "attribution";

/// `epub:type` token of a footnote backlink (§7.10).
const BACKLINK_EPUB_TYPE: &str = "backlink";

/// HTMLBook `data-type` marking a `pre` or `code` as a listing (§7.7).
const PROGRAMLISTING_TYPE: &str = "programlisting";

/// Class-token prefixes that declare a code language (§7.7).
const LANGUAGE_CLASS_PREFIXES: [&str; 2] = ["language-", "lang-"];

/// `h1`–`h6`, the heading elements of §7.5.
const HEADING_ELEMENTS: [&str; 6] = ["h1", "h2", "h3", "h4", "h5", "h6"];

/// Elements that carry an aside kind through `epub:type`/`data-type`
/// (§7.7); `aside` itself is an aside with or without a type.
const ASIDE_CANDIDATES: [&str; 3] = ["div", "section", "blockquote"];

/// §7.7 aside kind table for `epub:type` and `data-type` tokens.
const ASIDE_KIND_TABLE: [(&str, AsideKind); 8] = [
    ("note", AsideKind::Note),
    ("tip", AsideKind::Tip),
    ("warning", AsideKind::Warning),
    ("caution", AsideKind::Caution),
    ("important", AsideKind::Important),
    ("sidebar", AsideKind::Sidebar),
    ("epigraph", AsideKind::Epigraph),
    ("example", AsideKind::Example),
];

/// §7.10 rule 1 tokens on the block or an ancestor (`epub:type`), plus the
/// HTMLBook `data-type="footnote"` value.
const FOOTNOTE_TYPES: [&str; 3] = ["footnote", "endnote", "rearnote"];
const FOOTNOTE_DATA_TYPE: &str = "footnote";

/// §7.10 rule 2 container tokens (`epub:type`), plus the HTMLBook
/// `data-type="footnotes"` value.
const FOOTNOTE_CONTAINER_TYPES: [&str; 3] = ["footnotes", "endnotes", "rearnotes"];
const FOOTNOTE_CONTAINER_DATA_TYPE: &str = "footnotes";

/// HTML's declared limits on cell spans; larger declared values are clamped
/// so a hostile attribute cannot size the occupancy grid arbitrarily.
const MAX_COLUMN_SPAN: u64 = 1000;
const MAX_ROW_SPAN: u64 = 65534;

// Three lifetimes, all distinct: 'a is the emitter's own borrow (limits held
// for the whole parse); 'd is the current content document's text and parsed
// tree, which is shorter-lived than the emitter; 'e is the borrow of the
// emitter and the other mutable state for one block call. `&'e mut
// Emitter<'a>` with `document: &'d str` compiles because 'a is never tied to
// 'd; sharing them would pin every document's text for the emitter's whole
// lifetime.
/// Everything one block emission needs from the walk.
pub(crate) struct BlockContext<'e, 'a, 'd> {
    /// Normalized member name of the document being walked.
    pub document: &'d str,
    /// Kind of the section current at this block (§7.10 rule 3, §10.4
    /// `index_locator`).
    pub section_kind: SectionKind,
    pub text: &'e TextContext<'d>,
    pub emitter: &'e mut Emitter<'a>,
    /// Internal links collected for `links::resolve_all`.
    pub links: &'e mut Vec<LinkRecord>,
    pub units: &'e mut UnitIndex,
    pub report: &'e mut StructureReport,
}

/// One block's source: a whole element, or a mixed-content run of child
/// nodes under `parent` (§7.5) whose `node_range` becomes the locator's
/// `nodeRange`.
pub(crate) enum BlockSource<'d, 'input> {
    Element(roxmltree::Node<'d, 'input>),
    Run {
        parent: roxmltree::Node<'d, 'input>,
        nodes: Vec<roxmltree::Node<'d, 'input>>,
        node_range: [u64; 2],
    },
}

impl<'d, 'input> BlockSource<'d, 'input> {
    /// The element the block's attributes, ancestry, and language are read
    /// from: the element itself, or the run's parent.
    fn element(&self) -> roxmltree::Node<'d, 'input> {
        match self {
            Self::Element(node) => *node,
            Self::Run { parent, .. } => *parent,
        }
    }

    /// The nodes whose subtrees make up the block, for id registration,
    /// label search, and link collection.
    fn roots(&self) -> Vec<roxmltree::Node<'d, 'input>> {
        match self {
            Self::Element(node) => vec![*node],
            // The walk owns the run vector; the roots are copied out (Node
            // is Copy) so this borrow does not outlive the call.
            Self::Run { nodes, .. } => nodes.to_vec(),
        }
    }
}

/// A detected caption (§7.9). Every caption lies inside its subject
/// (sibling blocks are never captions), so `node` is always `Some` for
/// candidates this module builds; `rule` is the §7.9 rule number, for the
/// report.
pub(crate) struct CaptionCandidate<'d, 'input> {
    pub node: Option<roxmltree::Node<'d, 'input>>,
    pub label: Option<String>,
    pub text: String,
    pub rule: u8,
}

/// A list's emitted ids and, per `list_item`, its id and the nodes to walk
/// with their role. Each returned node is walked as a generic container
/// with that role under the item: the `li` itself for `ul`/`ol` (role
/// `paragraph`), and each `dt` (role `term`) and `dd` (role `definition`)
/// for `dl`, so an item with bare text becomes one `text_block` located on
/// that element (§7.5, §7.7).
pub(crate) struct ListEmission<'d, 'input> {
    pub list_id: String,
    pub items: Vec<(String, Vec<(roxmltree::Node<'d, 'input>, TextBlockRole)>)>,
}

/// An aside's emitted id, its child nodes to walk, and the §7.9 rule 3
/// caption `walk_spine` applies to the first `code_block` under it.
pub(crate) struct AsideEmission<'d, 'input> {
    pub aside_id: String,
    pub children: Vec<roxmltree::Node<'d, 'input>>,
    pub pending_caption: Option<CaptionCandidate<'d, 'input>>,
}

/// A table's emitted ids: the table, its `caption` unit when one was
/// paired (a sibling of the table under the same parent, so `walk_spine`
/// chains it with `precedes` among the table's siblings), and every cell id
/// with its source node in emission order, so `walk_spine` can assign
/// `appears_on` to cells it never walks. Rows and cells are already
/// contained and `precedes`-chained by `emit_table`.
pub(crate) struct TableEmission<'d, 'input> {
    pub table_id: String,
    pub caption_id: Option<String>,
    pub cells: Vec<(String, roxmltree::Node<'d, 'input>)>,
}

/// §7.6 rule 1 on one element (`epub:type="pagebreak"` or
/// `role="doc-pagebreak"`); `Some(label)` when it is a marker. The
/// `page-list` label for the element's id overrides the label derived from
/// `title`, then `aria-label`, then text content. Element ids are never
/// markers; rule 2 (`<?dp?>`) is `dp_page_marker`.
pub(crate) fn page_marker(
    node: roxmltree::Node,
    labels: &BTreeMap<(String, Option<String>), String>,
    document: &str,
) -> Option<Option<String>> {
    if !node.is_element() || !is_pagebreak_element(node) {
        return None;
    }
    // The map is keyed by owned (member, fragment); one key is built per
    // marker element, which is rare enough not to matter.
    let listed = node.attribute("id").and_then(|id| {
        labels
            .get(&(document.to_string(), Some(id.to_string())))
            .map(String::as_str)
    });
    let derived = || {
        node.attribute("title")
            .or_else(|| node.attribute("aria-label"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| non_empty(flattened_text(node)))
    };
    Some(listed.map(str::to_string).or_else(derived))
}

/// §7.6 rule 2 on one processing instruction: `<?dp n="…" folio="…"?>`
/// is a marker labeled by `folio`; any other node is not a marker.
pub(crate) fn dp_page_marker(pi: roxmltree::Node) -> Option<Option<String>> {
    let instruction = pi.is_pi().then(|| pi.pi()).flatten()?;
    if instruction.target != DP_PI_TARGET {
        return None;
    }
    let label = instruction
        .value
        .and_then(|value| pseudo_attribute(value, DP_FOLIO_ATTRIBUTE))
        .and_then(non_empty);
    Some(label)
}

/// Emit one `text_block`; `None` when dropped as empty (§7.5, §8). The
/// caller counts the drop under `epub_empty_blocks_dropped`. `role` is the
/// caller's context role; the §7.7 precedence is applied on top of it:
/// `footnote` (§7.10) first, the caller's `title`/`subtitle` next, then an
/// attribution mark (explicit `epub:type`/`data-type`, or a `cite`-only
/// block inside a `blockquote`), else the caller's role. Heading elements
/// keep `heading` (§7.7). `label` comes only from a `span.label` inside
/// the block (§7.10); the label text stays in `text`.
pub(crate) fn emit_text_block(
    ctx: &mut BlockContext,
    source: &BlockSource,
    parent: &str,
    role: TextBlockRole,
) -> WorkerResult<Option<String>> {
    let extracted = match source {
        BlockSource::Element(node) => text::extract(*node, ctx.text),
        BlockSource::Run { nodes, .. } => text::extract_run(nodes, ctx.text),
    };
    if extracted.is_empty() {
        return Ok(None);
    }
    let element = source.element();
    let roots = source.roots();
    let role = resolve_role(element, &roots, role, ctx.section_kind);
    let body = TextBlockBody {
        text: extracted,
        role,
        label: label_span(&roots).map(|span| text::extract(span, ctx.text)),
        language: text::language(element, ctx.text.package_language),
    };
    let locator = match source {
        BlockSource::Element(node) => xhtml::locator(ctx.document, *node, None),
        BlockSource::Run {
            parent, node_range, ..
        } => xhtml::locator(ctx.document, *parent, Some(*node_range)),
    };
    let local_id = emit_unit(ctx, ContentType::TextBlock, &body, parent, &locator, &roots)?;
    collect_links(
        ctx,
        &local_id,
        &locator,
        &roots,
        role == TextBlockRole::Footnote,
    );
    Ok(Some(local_id))
}

/// §7.8: emit the table, its `caption`, its rows, and its cells. Rows are
/// walked in `thead`/`tbody`/`tfoot` order as they appear (direct `tr`
/// children are body rows); cells occupy a grid honoring `rowspan` and
/// `colspan`; `th` cells and every `thead` cell are listed in `headers`.
/// The table unit streams first so its body can carry the grid extent and
/// caption text; the caption unit (contained by `parent`, §7.9) follows,
/// then rows with their cells, chained by `precedes` within each group.
/// Returns the table id, the caption id, and the cell ids with their nodes
/// (`TableEmission`) for the walk's sibling chaining and `appears_on`.
pub(crate) fn emit_table<'d, 'input>(
    ctx: &mut BlockContext,
    node: roxmltree::Node<'d, 'input>,
    parent: &str,
) -> WorkerResult<TableEmission<'d, 'input>> {
    let grid = build_grid(node, ctx.text);
    let caption = detect_caption(node, None, None, ctx.text);
    let body = TableBody {
        caption: caption.as_ref().map(|candidate| candidate.text.clone()),
        row_count: grid.row_count,
        column_count: grid.column_count,
        headers: (!grid.headers.is_empty()).then_some(grid.headers),
    };
    let table_locator = xhtml::locator(ctx.document, node, None);
    let table_id = emit_unit(
        ctx,
        ContentType::Table,
        &body,
        parent,
        &table_locator,
        &[node],
    )?;
    let caption_id = match &caption {
        Some(candidate) => Some(pair_caption(
            ctx,
            std::slice::from_ref(&table_id),
            candidate,
            parent,
        )?),
        None => None,
    };

    // Cell ids in emission order, paired with their source nodes for the
    // walk's `appears_on` assignment.
    let mut cells: Vec<(String, roxmltree::Node<'d, 'input>)> = Vec::new();
    let mut previous_row: Option<String> = None;
    for row in grid.rows {
        let row_body = TableRowBody {
            row_index: row.row_index,
            role: Some(row.role),
        };
        let row_locator = xhtml::locator(ctx.document, row.node, None);
        let row_id = emit_unit(
            ctx,
            ContentType::TableRow,
            &row_body,
            &table_id,
            &row_locator,
            &[row.node],
        )?;
        if let Some(previous) = &previous_row {
            ctx.emitter
                .relationship(previous, &row_id, UnitRelationshipType::Precedes, None)?;
        }
        previous_row = Some(row_id.clone());

        // `precedes` chains cells within this row only: the previous cell
        // is the last entry pushed since the row started.
        let row_start = cells.len();
        for cell in row.cells {
            let cell_body = TableCellBody {
                row_index: cell.row_index,
                column_index: cell.column_index,
                row_span: (cell.row_span > 1).then_some(cell.row_span),
                column_span: (cell.column_span > 1).then_some(cell.column_span),
                text: cell.text,
            };
            // Every cell carries a locator (§7.8), and cells are evidence
            // units for links (§10.3).
            let cell_locator = xhtml::locator(ctx.document, cell.node, None);
            let cell_id = emit_unit(
                ctx,
                ContentType::TableCell,
                &cell_body,
                &row_id,
                &cell_locator,
                &[cell.node],
            )?;
            collect_links(ctx, &cell_id, &cell_locator, &[cell.node], false);
            if let Some((previous, _)) = cells.get(row_start..).and_then(<[_]>::last) {
                ctx.emitter.relationship(
                    previous,
                    &cell_id,
                    UnitRelationshipType::Precedes,
                    None,
                )?;
            }
            cells.push((cell_id, cell.node));
        }
    }
    Ok(TableEmission {
        table_id,
        caption_id,
        cells,
    })
}

/// `ul`, `ol`, `dl`: emit the `list` and `list_item` units only (§7.7).
/// For `ul`/`ol` each `li` child is one item; for `dl` each `dt` and its
/// following `dd` elements up to the next `dt` form one item (a leading
/// `dd` opens an item with no term). Element children of other names are
/// not items and are not walked.
pub(crate) fn emit_list<'d, 'input>(
    ctx: &mut BlockContext,
    node: roxmltree::Node<'d, 'input>,
    parent: &str,
) -> WorkerResult<ListEmission<'d, 'input>> {
    let name = xhtml::local_name(node);
    let kind = match name {
        "ol" => ListKind::Ordered,
        "dl" => ListKind::Definition,
        _ => ListKind::Unordered,
    };
    let body = ListBody {
        kind,
        start: (kind == ListKind::Ordered)
            .then(|| node.attribute("start").and_then(parse_u64))
            .flatten(),
    };
    let list_locator = xhtml::locator(ctx.document, node, None);
    let list_id = emit_unit(
        ctx,
        ContentType::List,
        &body,
        parent,
        &list_locator,
        &[node],
    )?;

    // Group the element children into items: one per `li`, or one per
    // `dt` run of `dt` + following `dd`s.
    let mut groups: Vec<Vec<(roxmltree::Node<'d, 'input>, TextBlockRole)>> = Vec::new();
    for child in node.children().filter(|child| child.is_element()) {
        match (kind, xhtml::local_name(child)) {
            (ListKind::Definition, "dt") => groups.push(vec![(child, TextBlockRole::Term)]),
            // A `dt` always opens a group, so a `dd` always joins the last
            // one; a leading `dd` opens a term-less group.
            (ListKind::Definition, "dd") => match groups.last_mut() {
                Some(group) => group.push((child, TextBlockRole::Definition)),
                None => groups.push(vec![(child, TextBlockRole::Definition)]),
            },
            (ListKind::Definition, _) => {}
            (_, "li") => groups.push(vec![(child, TextBlockRole::Paragraph)]),
            (_, _) => {}
        }
    }

    let mut items = Vec::with_capacity(groups.len());
    for (index, group) in groups.into_iter().enumerate() {
        let item_body = ListItemBody {
            ordinal: index as u64 + 1,
            label: None,
        };
        // The item's locator and id registration cover every element in
        // the group: the `li`, or the `dt` and its `dd`s. The locator is on
        // the first element (the `dt` or the `li`).
        let nodes: Vec<roxmltree::Node<'d, 'input>> = group.iter().map(|(n, _)| *n).collect();
        let Some(first) = nodes.first() else {
            continue;
        };
        let item_locator = xhtml::locator(ctx.document, *first, None);
        let item_id = emit_unit(
            ctx,
            ContentType::ListItem,
            &item_body,
            &list_id,
            &item_locator,
            &nodes,
        )?;
        items.push((item_id, group));
    }
    Ok(ListEmission { list_id, items })
}

/// §7.9: one `figure` per image, archived through `Emitter::image`; the
/// caption fills `FigureBody.caption` before streaming. `node` is a
/// `figure` element (every descendant `img`/`svg` outside its `figcaption`
/// is one image), an `img` not inside a `figure`, or an `svg`. Each figure
/// is located on its image; the figure element's own subtree ids are
/// registered to the first figure so a link to the figure resolves.
pub(crate) fn emit_figure(
    ctx: &mut BlockContext,
    node: roxmltree::Node,
    parent: &str,
    package: &Package,
    archive: &mut Archive,
    caption: Option<&CaptionCandidate>,
) -> WorkerResult<Vec<String>> {
    let images: Vec<roxmltree::Node> = match xhtml::local_name(node) {
        "figure" => node
            .descendants()
            .filter(|d| d.is_element() && matches!(xhtml::local_name(*d), "img" | "svg"))
            .filter(|d| {
                !d.ancestors()
                    .any(|a| a.is_element() && xhtml::local_name(a) == "figcaption")
            })
            .collect(),
        _ => vec![node],
    };
    let caption_text = caption.map(|candidate| candidate.text.clone());
    let mut ids = Vec::with_capacity(images.len());
    for (index, image) in images.iter().enumerate() {
        let locator = xhtml::locator(ctx.document, *image, None);
        let body = if xhtml::local_name(*image) == "svg" {
            // §7.7: no bytes archived; alt text from `title` or `desc`. The
            // warning aggregate owns its locator; the unit keeps this one.
            ctx.emitter
                .warning(WARNING_SVG_INLINE, ctx.document, Some(locator.clone()));
            FigureBody {
                image_hash: None,
                image_media_type: None,
                image_size_bytes: None,
                alt_text: svg_alt_text(*image),
                caption: caption_text.clone(),
            }
        } else {
            let archived = archive_image(ctx, *image, &locator, package, archive)?;
            FigureBody {
                image_hash: archived.hash,
                image_media_type: archived.media_type,
                image_size_bytes: archived.size_bytes,
                alt_text: image
                    .attribute("alt")
                    .map(str::trim)
                    .filter(|alt| !alt.is_empty())
                    .map(str::to_string),
                caption: caption_text.clone(),
            }
        };
        // The first figure registers the whole source subtree (the figure
        // element's id among them); later figures register only their own
        // image so the first mapping stands.
        let id_roots: Vec<roxmltree::Node> = if index == 0 { vec![node] } else { vec![*image] };
        let id = emit_unit(ctx, ContentType::Figure, &body, parent, &locator, &id_roots)?;
        ids.push(id);
    }
    Ok(ids)
}

/// Emit the `aside` unit only (§7.7): `kind` from `aside_kind` (`unknown`
/// for an untyped `aside`), `title` from the first heading inside. For an
/// `example` aside that contains a `pre`, that heading is returned as the
/// pending §7.9 rule 3 caption for the first `code_block` instead of being
/// walked as a heading block. `children` are every child node of the
/// element, in order, for the walk to classify.
pub(crate) fn emit_aside<'d, 'input>(
    ctx: &mut BlockContext,
    node: roxmltree::Node<'d, 'input>,
    parent: &str,
) -> WorkerResult<AsideEmission<'d, 'input>> {
    let kind = aside_kind(node).unwrap_or(AsideKind::Unknown);
    let heading = first_heading_inside(node);
    let body = AsideBody {
        kind,
        title: heading
            .map(|heading| text::extract(heading, ctx.text))
            .and_then(non_empty),
    };
    let locator = xhtml::locator(ctx.document, node, None);
    let aside_id = emit_unit(ctx, ContentType::Aside, &body, parent, &locator, &[node])?;
    let pending_caption = if kind == AsideKind::Example && contains_element(node, "pre") {
        heading.and_then(|heading| caption_from(heading, 3, ctx.text))
    } else {
        None
    };
    Ok(AsideEmission {
        aside_id,
        children: node.children().collect(),
        pending_caption,
    })
}

/// Emit one `code_block`; only `pre` produces one (§7.7). `code` is the
/// verbatim `pre` text; `language` by the §7.7 rule; the caption fills
/// `title` (its text) and `label` before streaming.
pub(crate) fn emit_code(
    ctx: &mut BlockContext,
    node: roxmltree::Node,
    parent: &str,
    caption: Option<&CaptionCandidate>,
) -> WorkerResult<String> {
    let body = CodeBlockBody {
        code: text::extract_pre(node),
        language: code_language(node),
        label: caption.and_then(|candidate| candidate.label.clone()),
        title: caption.map(|candidate| candidate.text.clone()),
    };
    let locator = xhtml::locator(ctx.document, node, None);
    let local_id = emit_unit(
        ctx,
        ContentType::CodeBlock,
        &body,
        parent,
        &locator,
        &[node],
    )?;
    collect_links(ctx, &local_id, &locator, &[node], false);
    Ok(local_id)
}

/// §7.3: a `span.label` inside the heading becomes `label`, the remainder
/// `headingText`; otherwise the whole text is `headingText` and `label` is
/// absent. No text pattern is applied. The remainder is the run of the
/// heading's children with a direct-child label span excluded; a label
/// span nested deeper still yields `label` but its text remains in
/// `headingText`, because nothing here rewrites extracted text. Line
/// breaks (`br`) inside the heading become one space in both values
/// (§7.3); the heading's own `text_block` keeps them.
/// `walk_spine` calls this for `TextSectionBody`.
pub(crate) fn split_heading(node: roxmltree::Node, text: &TextContext) -> (Option<String>, String) {
    let Some(span) = label_span(&[node]) else {
        return (None, single_line(&text::extract(node, text)));
    };
    let label = non_empty(single_line(&text::extract(span, text)));
    let remainder = if span.parent().is_some_and(|parent| parent.id() == node.id()) {
        let others: Vec<roxmltree::Node> = node
            .children()
            .filter(|child| child.id() != span.id())
            .collect();
        text::extract_run(&others, text)
    } else {
        text::extract(node, text)
    };
    (label, single_line(&remainder))
}

/// §7.3 line-break rule: join the non-empty lines of §8-normalized text
/// with one space. §8 already trims each line and keeps at most one blank
/// line between two non-empty ones, so no further collapsing is needed.
fn single_line(text: &str) -> String {
    text.lines()
        .filter(|line| !line.is_empty())
        .collect::<Vec<&str>>()
        .join(" ")
}

/// Emit a block `math` element as a `text_block` role `formula` (§7.7):
/// text from `alttext`, else an `annotation` child, else the flattened
/// content, through the shared math rule of `text::extract`.
pub(crate) fn emit_formula(
    ctx: &mut BlockContext,
    node: roxmltree::Node,
    parent: &str,
) -> WorkerResult<String> {
    let body = TextBlockBody {
        text: text::extract(node, ctx.text),
        role: TextBlockRole::Formula,
        label: None,
        language: text::language(node, ctx.text.package_language),
    };
    let locator = xhtml::locator(ctx.document, node, None);
    emit_unit(
        ctx,
        ContentType::TextBlock,
        &body,
        parent,
        &locator,
        &[node],
    )
}

/// §7.9 rules 1 to 3, first match wins: `figcaption` inside a `figure` or
/// `caption` inside a `table` (rule 1); the first heading inside a
/// `figure` (rule 2); the first heading inside an `example` aside (rule
/// 3). `before`/`after` are the adjacent sibling blocks, which are never
/// captions; they are accepted only for the lookahead contract. A
/// candidate whose text is empty is no caption.
pub(crate) fn detect_caption<'d, 'input>(
    subject: roxmltree::Node<'d, 'input>,
    before: Option<&BlockSource<'d, 'input>>,
    after: Option<&BlockSource<'d, 'input>>,
    text: &TextContext,
) -> Option<CaptionCandidate<'d, 'input>> {
    // Sibling blocks are never captions (§7.9); the parameters are
    // deliberately unread.
    let _ = (before, after);
    match xhtml::local_name(subject) {
        "table" => child_element(subject, "caption").and_then(|node| caption_from(node, 1, text)),
        "figure" => descendant_element(subject, "figcaption")
            .and_then(|node| caption_from(node, 1, text))
            .or_else(|| first_heading_inside(subject).and_then(|node| caption_from(node, 2, text))),
        _ if aside_kind(subject) == Some(AsideKind::Example) => {
            first_heading_inside(subject).and_then(|node| caption_from(node, 3, text))
        }
        _ => None,
    }
}

/// Emit the `caption` unit (contained by `parent`, the subject's parent)
/// and, per subject in order, `caption_of` then `has_caption` (§7.9,
/// §10.1); returns the caption id. Each pairing is counted in the report
/// under the candidate's rule. Captions are evidence text, so their links
/// are collected.
pub(crate) fn pair_caption(
    ctx: &mut BlockContext,
    subjects: &[String],
    caption: &CaptionCandidate,
    parent: &str,
) -> WorkerResult<String> {
    // Every candidate this module produces carries its source node; a
    // node-less candidate has no locator and cannot be a unit (§9.2).
    let Some(node) = caption.node else {
        return Err(WorkerError::Fault(ApiError::InternalIo {
            message: format!(
                "caption candidate (rule {}) in {} has no source node",
                caption.rule, ctx.document
            ),
        }));
    };
    let body = CaptionBody {
        text: caption.text.clone(),
        label: caption.label.clone(),
    };
    let locator = xhtml::locator(ctx.document, node, None);
    let caption_id = emit_unit(ctx, ContentType::Caption, &body, parent, &locator, &[node])?;
    collect_links(ctx, &caption_id, &locator, &[node], false);
    for subject in subjects {
        ctx.emitter
            .relationship(&caption_id, subject, UnitRelationshipType::CaptionOf, None)?;
        ctx.emitter
            .relationship(subject, &caption_id, UnitRelationshipType::HasCaption, None)?;
        ctx.report.caption_pairing(ctx.document, caption.rule);
    }
    Ok(caption_id)
}

/// §7.10 footnote-block rules, semantic only: the block or an ancestor
/// carries footnote semantics (rule 1); the block lies in a footnotes
/// container (rule 2); the current section is kind `notes` (rule 3, the
/// caller decides that the block is a paragraph).
pub(crate) fn is_footnote_block(node: roxmltree::Node, section_kind: SectionKind) -> bool {
    if section_kind == SectionKind::Notes {
        return true;
    }
    node.ancestors().filter(|a| a.is_element()).any(|ancestor| {
        let epub = xhtml::epub_type(ancestor).unwrap_or_default();
        let data = xhtml::data_type(ancestor)
            .map(str::trim)
            .unwrap_or_default();
        has_token(epub, &FOOTNOTE_TYPES)
            || data == FOOTNOTE_DATA_TYPE
            || has_token(epub, &FOOTNOTE_CONTAINER_TYPES)
            || data == FOOTNOTE_CONTAINER_DATA_TYPE
    })
}

/// Aside kind from `epub:type`/`data-type` (§7.7): an `aside` is always an
/// aside (`unknown` without a recognized type); a `div`, `section`, or
/// `blockquote` is one only with a recognized type. A footnote-typed
/// element emits no aside (§7.7), so it is `None` whatever its name.
pub(crate) fn aside_kind(node: roxmltree::Node) -> Option<AsideKind> {
    if !node.is_element() {
        return None;
    }
    let name = xhtml::local_name(node);
    let epub = xhtml::epub_type(node).unwrap_or_default();
    let data = xhtml::data_type(node).unwrap_or_default();
    if has_token(epub, &FOOTNOTE_TYPES) || has_token(data, &FOOTNOTE_TYPES) {
        return None;
    }
    let kind = epub
        .split_ascii_whitespace()
        .chain(data.split_ascii_whitespace())
        .find_map(|token| {
            ASIDE_KIND_TABLE
                .iter()
                .find(|(name, _)| *name == token)
                .map(|(_, kind)| *kind)
        });
    match (name, kind) {
        ("aside", kind) => Some(kind.unwrap_or(AsideKind::Unknown)),
        (name, Some(kind)) if ASIDE_CANDIDATES.contains(&name) => Some(kind),
        _ => None,
    }
}

/// Stream one unit with its locator and `contains` edge from `parent`, and
/// register every `id` in the `id_roots` subtrees to it (§10.4 innermost
/// rule: this call is made before any inner unit's, so later inserts win).
/// Serialization of a typed body this module built is a worker invariant;
/// its failure is a fault, not a parse outcome.
fn emit_unit<B: Serialize>(
    ctx: &mut BlockContext,
    content_type: ContentType,
    body: &B,
    parent: &str,
    locator: &Locator,
    id_roots: &[roxmltree::Node],
) -> WorkerResult<String> {
    let local_id = ctx.emitter.next_local_id(content_type);
    let body = serde_json::to_value(body).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to serialize {} body: {source}",
            content_type.wire_name()
        ),
    })?;
    for root in id_roots {
        for element in root.descendants().filter(|n| n.is_element()) {
            if let Some(id) = element.attribute("id") {
                ctx.units.insert(ctx.document, id, &local_id);
            }
        }
    }
    ctx.emitter.unit(CandidateContentUnit {
        // The unit record owns one copy of the id; the caller receives the
        // other for its edges and return value.
        local_id: local_id.clone(),
        content_type,
        body,
        parent_local_id: Some(parent.to_string()),
        // Overwritten by the emitter with the stream position.
        sequence_index: 0,
        // Every unit carries exactly one locator (§9.2); the record owns
        // its copy while the caller keeps the original for link records.
        locators: vec![locator.clone()],
    })?;
    ctx.emitter
        .relationship(parent, &local_id, UnitRelationshipType::Contains, None)?;
    Ok(local_id)
}

/// Collect every internal link inside an evidence unit (§10.3, §10.4): an
/// `a` with an internal href becomes a `LinkRecord` from `from_local_id`
/// with role `footnote` when the walk's predicate calls it a note
/// reference, `index_locator` inside an `index` section, else
/// `cross_reference`. An unresolvable href is warned and reported here,
/// since it has no target for `resolve_all`. External links contribute
/// text only. In a footnote block, a backlink (an `a` with
/// `epub:type="backlink"`, or the first `a` before any text when its href
/// is internal) produces no edge (§7.10).
fn collect_links(
    ctx: &mut BlockContext,
    from_local_id: &str,
    locator: &Locator,
    roots: &[roxmltree::Node],
    footnote: bool,
) {
    let mut saw_text = false;
    let mut saw_anchor = false;
    for root in roots {
        for node in root.descendants() {
            if node.is_text() {
                saw_text |= node
                    .text()
                    .is_some_and(|t| t.chars().any(|c| !c.is_whitespace()));
                continue;
            }
            if !node.is_element() || xhtml::local_name(node) != "a" {
                continue;
            }
            let Some(href) = node.attribute("href").map(str::trim) else {
                continue;
            };
            let at_start = !saw_text && !saw_anchor;
            saw_anchor = true;
            let outcome = links::resolve(ctx.document, href);
            let is_backlink = footnote
                && (has_token(
                    xhtml::epub_type(node).unwrap_or_default(),
                    &[BACKLINK_EPUB_TYPE],
                ) || (at_start && matches!(outcome, HrefOutcome::Internal(_))));
            if is_backlink {
                continue;
            }
            match outcome {
                HrefOutcome::External => {}
                HrefOutcome::Unresolvable => {
                    // The aggregate takes ownership of the first locator;
                    // the block keeps its own.
                    ctx.emitter.warning(
                        WARNING_LINK_UNRESOLVED,
                        ctx.document,
                        Some(locator.clone()),
                    );
                    ctx.report.unresolved_link(ctx.document, href);
                }
                HrefOutcome::Internal(target) => {
                    let role_hint = if (ctx.text.is_noteref)(node) {
                        LinkRole::Footnote
                    } else if ctx.section_kind == SectionKind::Index {
                        LinkRole::IndexLocator
                    } else {
                        LinkRole::CrossReference
                    };
                    ctx.links.push(LinkRecord {
                        from_local_id: from_local_id.to_string(),
                        document: ctx.document.to_string(),
                        href: href.to_string(),
                        // Each record owns its locator by contract.
                        locator: locator.clone(),
                        target,
                        role_hint,
                    });
                }
            }
        }
    }
}

/// §7.7 role precedence over the caller's context role: `footnote`
/// (rules 1 and 2 on any non-heading block; rule 3 on a paragraph), the
/// caller's `title`/`subtitle`, an attribution mark, else the caller's
/// role. Heading elements are always `heading`.
fn resolve_role(
    element: roxmltree::Node,
    roots: &[roxmltree::Node],
    role: TextBlockRole,
    section_kind: SectionKind,
) -> TextBlockRole {
    if role == TextBlockRole::Heading {
        return role;
    }
    // Rule 3 applies to paragraphs only; rules 1 and 2 to every block.
    let rule3_kind = if role == TextBlockRole::Paragraph {
        section_kind
    } else {
        SectionKind::Unknown
    };
    if is_footnote_block(element, rule3_kind) {
        return TextBlockRole::Footnote;
    }
    if matches!(role, TextBlockRole::Title | TextBlockRole::Subtitle) {
        return role;
    }
    let marked = xhtml::epub_type(element).is_some_and(|v| has_token(v, &[ATTRIBUTION_TYPE]))
        || xhtml::data_type(element).is_some_and(|v| v.trim() == ATTRIBUTION_TYPE);
    if marked || (role == TextBlockRole::Quote && only_element_child_is_cite(roots)) {
        return TextBlockRole::Attribution;
    }
    role
}

/// §7.7 `blockquote` rule: the block's only element child is a `cite`.
/// For a run, the run's nodes are the children.
fn only_element_child_is_cite(roots: &[roxmltree::Node]) -> bool {
    let children: Vec<roxmltree::Node> = match roots {
        [single] => single.children().filter(|c| c.is_element()).collect(),
        run => run.iter().copied().filter(|n| n.is_element()).collect(),
    };
    matches!(children.as_slice(), [only] if xhtml::local_name(*only) == "cite")
}

/// The first `span` with class token `label` inside any of `roots`
/// (§7.3, §7.10), in document order; a root that is itself such a span
/// counts.
fn label_span<'d, 'input>(
    roots: &[roxmltree::Node<'d, 'input>],
) -> Option<roxmltree::Node<'d, 'input>> {
    roots.iter().find_map(|root| {
        root.descendants().find(|n| {
            n.is_element()
                && xhtml::local_name(*n) == "span"
                && n.attribute("class")
                    .is_some_and(|class| has_token(class, &[LABEL_CLASS]))
        })
    })
}

/// Build a caption candidate from its source element: `label` from a
/// `span.label` inside it, `text` the full caption text; `None` when the
/// text is empty.
fn caption_from<'d, 'input>(
    node: roxmltree::Node<'d, 'input>,
    rule: u8,
    text: &TextContext,
) -> Option<CaptionCandidate<'d, 'input>> {
    let caption_text = non_empty(text::extract(node, text))?;
    Some(CaptionCandidate {
        node: Some(node),
        label: label_span(&[node])
            .map(|span| text::extract(span, text))
            .and_then(non_empty),
        text: caption_text,
        rule,
    })
}

/// The first heading inside `node` (§7.5 heading rule): the first `h1`–`h6`
/// descendant, else the first descendant with `epub:type="title"` when no
/// `h` element is present.
fn first_heading_inside<'d, 'input>(
    node: roxmltree::Node<'d, 'input>,
) -> Option<roxmltree::Node<'d, 'input>> {
    let inside = || {
        node.descendants()
            .filter(move |d| d.is_element() && d.id() != node.id())
    };
    inside()
        .find(|d| HEADING_ELEMENTS.contains(&xhtml::local_name(*d)))
        .or_else(|| {
            inside()
                .find(|d| xhtml::epub_type(*d).is_some_and(|v| has_token(v, &[TITLE_EPUB_TYPE])))
        })
}

/// One row of the §7.8 grid with its cells, ready to emit.
struct GridRow<'d, 'input> {
    node: roxmltree::Node<'d, 'input>,
    row_index: u64,
    role: TableRowRole,
    cells: Vec<GridCell<'d, 'input>>,
}

/// One cell placed on the §7.8 grid.
struct GridCell<'d, 'input> {
    node: roxmltree::Node<'d, 'input>,
    row_index: u64,
    column_index: u64,
    row_span: u64,
    column_span: u64,
    text: Option<String>,
}

/// The §7.8 grid: rows with placed cells, the extent, and the headers.
struct Grid<'d, 'input> {
    rows: Vec<GridRow<'d, 'input>>,
    row_count: u64,
    column_count: u64,
    headers: Vec<TableHeader>,
}

/// Place every cell of `table` on a grid honoring `rowspan` and `colspan`
/// (§7.8). Rows come from `thead`/`tbody`/`tfoot` children in the order
/// they appear, plus direct `tr` children as body rows. A column's
/// remaining rowspan from rows above blocks placement there; the extent is
/// the largest row and column any cell reaches.
fn build_grid<'d, 'input>(
    table: roxmltree::Node<'d, 'input>,
    text: &TextContext,
) -> Grid<'d, 'input> {
    let mut rows = Vec::new();
    let mut headers = Vec::new();
    // Remaining rows each column stays occupied by a cell from above.
    let mut occupied: Vec<u64> = Vec::new();
    let mut row_count = 0u64;
    let mut column_count = 0u64;

    let mut row_sources: Vec<(roxmltree::Node<'d, 'input>, TableRowRole)> = Vec::new();
    for child in table.children().filter(|c| c.is_element()) {
        let role = match xhtml::local_name(child) {
            "thead" => TableRowRole::Header,
            "tfoot" => TableRowRole::Footer,
            "tbody" => TableRowRole::Body,
            "tr" => {
                row_sources.push((child, TableRowRole::Body));
                continue;
            }
            _ => continue,
        };
        row_sources.extend(
            child
                .children()
                .filter(|c| c.is_element() && xhtml::local_name(*c) == "tr")
                .map(|tr| (tr, role)),
        );
    }

    for (row_index, (tr, role)) in row_sources.into_iter().enumerate() {
        let row_index = row_index as u64;
        for remaining in &mut occupied {
            *remaining = remaining.saturating_sub(1);
        }
        let mut column = 0usize;
        let mut cells = Vec::new();
        for cell in tr
            .children()
            .filter(|c| c.is_element() && matches!(xhtml::local_name(*c), "td" | "th"))
        {
            while occupied.get(column).is_some_and(|remaining| *remaining > 0) {
                column += 1;
            }
            let column_span = span_attribute(cell, "colspan", MAX_COLUMN_SPAN);
            let row_span = span_attribute(cell, "rowspan", MAX_ROW_SPAN);
            let end = column + column_span as usize;
            if occupied.len() < end {
                occupied.resize(end, 0);
            }
            for slot in &mut occupied[column..end] {
                *slot = row_span;
            }
            let column_index = column as u64;
            let cell_text = non_empty(text::extract(cell, text));
            if xhtml::local_name(cell) == "th" || role == TableRowRole::Header {
                headers.push(TableHeader {
                    row_index,
                    column_index,
                    text: cell_text.clone().unwrap_or_default(),
                    row_span: (row_span > 1).then_some(row_span),
                    column_span: (column_span > 1).then_some(column_span),
                });
            }
            row_count = row_count.max(row_index + row_span);
            column_count = column_count.max(column_index + column_span);
            cells.push(GridCell {
                node: cell,
                row_index,
                column_index,
                row_span,
                column_span,
                text: cell_text,
            });
            column = end;
        }
        row_count = row_count.max(row_index + 1);
        rows.push(GridRow {
            node: tr,
            row_index,
            role,
            cells,
        });
    }
    Grid {
        rows,
        row_count,
        column_count,
        headers,
    }
}

/// A `rowspan`/`colspan` value: a positive integer clamped to `max`;
/// absent, zero, or unparsable counts as 1.
fn span_attribute(cell: roxmltree::Node, name: &str, max: u64) -> u64 {
    cell.attribute(name)
        .and_then(parse_u64)
        .filter(|value| *value > 0)
        .map_or(1, |value| value.min(max))
}

/// Outcome of archiving one `img` (§7.9): the hash and size when written,
/// and the manifest media type for the member when known.
struct ArchivedImage {
    hash: Option<String>,
    size_bytes: Option<u64>,
    media_type: Option<String>,
}

/// Resolve an `img`'s `src` to an archive member and archive its bytes
/// (§7.9): a missing, external, or unresolvable source warns
/// `epub_image_missing`; an oversized image warns `epub_image_too_large`
/// and sets no hash. An archive cap hit while reading is a recorded failure
/// re-staged to the document being walked. Warning locators are cloned
/// because the aggregate owns its first-instance locator while the figure
/// unit keeps the original.
fn archive_image(
    ctx: &mut BlockContext,
    image: roxmltree::Node,
    locator: &Locator,
    package: &Package,
    archive: &mut Archive,
) -> WorkerResult<ArchivedImage> {
    let none = ArchivedImage {
        hash: None,
        size_bytes: None,
        media_type: None,
    };
    let member = image.attribute("src").map(str::trim).and_then(|src| {
        match links::resolve(ctx.document, src) {
            HrefOutcome::Internal(target) => Some(target.member),
            HrefOutcome::External | HrefOutcome::Unresolvable => None,
        }
    });
    let Some(member) = member else {
        ctx.emitter
            .warning(WARNING_IMAGE_MISSING, ctx.document, Some(locator.clone()));
        return Ok(none);
    };
    let bytes = archive
        .read(&member)
        .map_err(|failure| failure.with_stage(EpubStage::Document(ctx.document.to_string())))?;
    let Some(bytes) = bytes else {
        ctx.emitter
            .warning(WARNING_IMAGE_MISSING, ctx.document, Some(locator.clone()));
        return Ok(none);
    };
    let media_type = package
        .manifest
        .values()
        .find(|item| item.href == member)
        .map(|item| item.media_type.trim())
        .filter(|media_type| !media_type.is_empty())
        .map(str::to_string);
    match ctx.emitter.image(&bytes)? {
        Some(hash) => Ok(ArchivedImage {
            hash: Some(hash),
            size_bytes: Some(bytes.len() as u64),
            media_type,
        }),
        None => {
            ctx.emitter
                .warning(WARNING_IMAGE_TOO_LARGE, ctx.document, Some(locator.clone()));
            Ok(ArchivedImage {
                hash: None,
                size_bytes: None,
                media_type,
            })
        }
    }
}

/// §7.7 `svg` alt text: the first non-empty `title`, else `desc`,
/// descendant text.
fn svg_alt_text(svg: roxmltree::Node) -> Option<String> {
    descendant_element(svg, "title")
        .map(flattened_text)
        .and_then(non_empty)
        .or_else(|| {
            descendant_element(svg, "desc")
                .map(flattened_text)
                .and_then(non_empty)
        })
}

/// §7.7 code language: `data-code-language` on the `pre`; else a
/// `language-X`/`lang-X` class token on the `pre` or its first `code`
/// child; else the first bare class token on either, only when either
/// carries `data-type="programlisting"`; else absent.
fn code_language(pre: roxmltree::Node) -> Option<String> {
    if let Some(declared) = pre
        .attribute("data-code-language")
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Some(declared.to_string());
    }
    let code = child_element(pre, "code");
    let carriers: Vec<roxmltree::Node> = std::iter::once(pre).chain(code).collect();
    let prefixed = carriers.iter().find_map(|node| {
        node.attribute("class")?
            .split_ascii_whitespace()
            .find_map(|token| {
                LANGUAGE_CLASS_PREFIXES
                    .iter()
                    .find_map(|prefix| token.strip_prefix(prefix))
            })
            .filter(|language| !language.is_empty())
            .map(str::to_string)
    });
    if prefixed.is_some() {
        return prefixed;
    }
    let is_listing = carriers.iter().any(|node| {
        xhtml::data_type(*node).is_some_and(|value| value.trim() == PROGRAMLISTING_TYPE)
    });
    if !is_listing {
        return None;
    }
    carriers.iter().find_map(|node| {
        node.attribute("class")?
            .split_ascii_whitespace()
            .next()
            .map(str::to_string)
    })
}

/// §7.6 rule 1: `epub:type` carrying the `pagebreak` token or `role`
/// carrying `doc-pagebreak`.
fn is_pagebreak_element(node: roxmltree::Node) -> bool {
    xhtml::epub_type(node).is_some_and(|v| has_token(v, &[PAGEBREAK_EPUB_TYPE]))
        || node
            .attribute("role")
            .is_some_and(|v| has_token(v, &[PAGEBREAK_ROLE]))
}

/// Value of one pseudo-attribute (`name="value"` or `name='value'`) in a
/// processing instruction's data, matched on whole names so `n` does not
/// match inside `folio`.
fn pseudo_attribute(data: &str, name: &str) -> Option<String> {
    let mut rest = data;
    while let Some(position) = rest.find(name) {
        let before_ok = position == 0
            || rest[..position]
                .chars()
                .next_back()
                .is_some_and(char::is_whitespace);
        let after = rest[position + name.len()..].trim_start();
        if before_ok && let Some(value) = after.strip_prefix('=') {
            let value = value.trim_start();
            let quote = value.chars().next().filter(|c| *c == '"' || *c == '\'')?;
            let inner = &value[quote.len_utf8()..];
            let close = inner.find(quote)?;
            return Some(inner[..close].to_string());
        }
        rest = &rest[position + name.len()..];
    }
    None
}

/// Whether a space-separated token list contains any of `wanted`.
fn has_token(value: &str, wanted: &[&str]) -> bool {
    value
        .split_ascii_whitespace()
        .any(|token| wanted.contains(&token))
}

/// Whether `node` has a descendant element named `name`.
fn contains_element(node: roxmltree::Node, name: &str) -> bool {
    descendant_element(node, name).is_some()
}

/// First descendant element (excluding `node`) with local name `name`.
fn descendant_element<'d, 'input>(
    node: roxmltree::Node<'d, 'input>,
    name: &str,
) -> Option<roxmltree::Node<'d, 'input>> {
    node.descendants()
        .find(|d| d.is_element() && d.id() != node.id() && xhtml::local_name(*d) == name)
}

/// First element child of `parent` with the given local name.
fn child_element<'d, 'input>(
    parent: roxmltree::Node<'d, 'input>,
    name: &str,
) -> Option<roxmltree::Node<'d, 'input>> {
    parent
        .children()
        .find(|c| c.is_element() && xhtml::local_name(*c) == name)
}

/// Descendant text joined with whitespace collapsed to single spaces and
/// trimmed; for attribute-like uses (page labels, `svg` titles) where the
/// §8 block rules do not apply.
fn flattened_text(node: roxmltree::Node) -> String {
    let mut out = String::new();
    let mut pending_space = false;
    for descendant in node.descendants().filter(|d| d.is_text()) {
        for ch in descendant.text().unwrap_or_default().chars() {
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

/// A trimmed attribute value parsed as `u64`.
fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

/// `Some` only for a non-empty string.
fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
