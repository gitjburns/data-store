# SPEC-EPUB — EPUB Parser Engine and Content Model v0.4

Status: approved design, not yet implemented. This document is the contract for
the EPUB parser worker and for the content-model revision it requires. Until
implementation lands, the canonical spec, SPEC-SERVER.md, PROTOCOL.md,
ARCHITECTURE.md, README.md, INSTALL.md, DIAGNOSTICS.md, and
config.example.toml describe the pre-revision system; Section 13 lists the
updates each of them needs afterwards. Where this document and those documents
disagree, this document wins for EPUB and for the content model.

Terminology follows the canonical spec (ContentUnit, UnitRelationship, Locator,
ParseRun, capability profile, conformance report). Section references of the
form §N without a document name refer to the canonical spec.

## 1. Scope

### 1.1 In scope

- A new parser worker for EPUB 2 and EPUB 3 sources, routed by MIME type
  `application/epub+zip`, running in-process on the scheduler thread.
- Content model revision v0.4: new unit types, revised typed bodies, revised
  block roles, a narrowed locator union, a narrowed relationship set, new
  conformance dimensions, and importer archival of image artifacts.
- Decommissioning of the Docling and MuPDF PDF workers and every setting,
  dependency, route, and code path that exists only for them.
- The plain-text worker remains and is the ingestion path for every format
  that is not EPUB; conversion into plain text is external to this service.

### 1.2 Out of scope

- DRM-protected archives. Encrypted members are a recorded parse failure.
- Fixed-layout rendering semantics, media overlays, scripting, and multiple
  renditions. Content is parsed as reflowable XHTML; only the first `rootfile`
  is read.
- Right-to-left and vertical writing modes. Text is extracted; direction is
  not recorded.
- Inline SVG content and MathML rendering beyond alternate text.
- Remote resources. Only archive members are read.
- Inline markup inside unit text. Text is plain (Section 8).
- Print-page geometry. Pages carry an ordinal and a label only.
- Automated tests. Verification is Section 14.

### 1.3 Sample corpus

The rules in this document are grounded in six files in the repository root.
Each rule that exists because of one sample names it.

| Sample | Producer | Notable structure |
| --- | --- | --- |
| I_Am_a_Strange_Loop.epub | Publisher, EPUB 2, NCX | `div`-based paragraphs, `<?dp?>` page markers, `<h1>` sections anchored by empty `<div><a/></div>`, unlinked endnotes, index |
| TCP_IP_Illustrated_Vol_1.epub | Calibre from Pearson MOBI, EPUB 2 | `h2`–`h5` headings, tables as images with captions before them, code as `blockquote > p > tt` with `<br/>`, superscript endnote links, `blockquote` as layout, generated class names |
| The_Complete_Story_of_Civilization_Vol_1-11.epub | Publisher, EPUB 2 | 629 documents, six-level NCX, real `<table>`, captioned figures after images, in-file and cross-file footnotes with backlinks, `<a id="page_N"/>` markers, `<h2>` containing `<br/>` |
| The_Elements_of_Style.epub | Calibre PDF reflow, EPUB 2 | Degraded: headings split across bold paragraphs, spacer paragraphs, doubled spaces, truncated NCX labels, page ids on first paragraph |
| AI_Engineering.epub | O'Reilly HTMLBook, EPUB 3 | `nav` document plus NCX, `<section data-type>` nesting, `<h6>` captions, `<table>` with `<caption>`/`<thead>`/`<th>`, `<pre data-type="programlisting">`, admonitions, sidebars, `noteref`/`footnote`, MathML, `<dl>`, index terms |
| Designing_Data-Intensive_Applications.epub | O'Reilly HTMLBook, EPUB 3, Kobo-processed | Every sentence wrapped in `span.koboSpan`, scripts and styles in every document, epigraphs, titled examples, 1.6 MB index document, references as footnotes |

All six are well-formed XML in every XHTML, OPF, and NCX member.

## 2. Content model v0.4

This section is normative for the whole service, not only for EPUB. It
supersedes canonical §15, §17, §18, and §19 on every point it addresses.

### 2.1 ContentType

Closed set:

```
document, page, text_section, text_block, list, list_item, aside,
table, table_row, table_cell, figure, caption, code_block
```

Removed: `image_region` (no producer).

Interpretation: `document` is the single root unit of a parse and carries
source metadata. `page` is a print-page marker. `text_section` is a logical
container with a kind and an optional heading. `text_block` is the atomic
textual evidence unit. `list`, `list_item`, and `aside` are containers.
`table`, `table_row`, and `table_cell` are the tabular decomposition. `figure`
is a visual object. `caption` is an independent caption unit. `code_block` is a
code fragment.

Evidence-bearing types, whose text feeds chunking, ColBERT, annotation, and
passages: `text_block`, `caption`, `table_cell`, `code_block`. All other types
carry no evidence text.

### 2.2 Typed bodies

Every body rejects unknown fields. Optional fields are omitted when absent.
No body field may reference another unit; pairing and containment are
relationships only (§16.1 body-hash rule).

```ts
type DocumentBody = {
  title?: string
  creators?: string[]
  publisher?: string
  language?: string
  identifiers?: string[]
  date?: string
  description?: string
}

type PageBody = {
  ordinal: number          // 1-based position among the parse's page markers
  label?: string           // printed folio as declared, e.g. "xiv", "218"
}

type TextSectionBody = {
  kind: SectionKind
  headingText?: string
  headingLevel: number     // depth in the section tree; children of document = 1
  label?: string           // declared number or ordinal, e.g. "Chapter 1.", "3.2.1"
  sectionPath: string[]    // heading trail from level 1 to this section, inclusive
}

type SectionKind =
  | "part" | "chapter" | "section"
  | "preface" | "foreword" | "introduction" | "prologue"
  | "epilogue" | "afterword" | "conclusion"
  | "appendix" | "glossary" | "bibliography" | "index" | "notes"
  | "acknowledgments" | "dedication" | "epigraph"
  | "titlepage" | "copyright_page" | "cover" | "toc" | "colophon"
  | "unknown"

type TextBlockBody = {
  text: string
  role: TextBlockRole
  label?: string           // declared marker: footnote number, item number
  language?: string        // BCP 47 tag from the nearest declared language
}

type TextBlockRole =
  | "paragraph" | "heading" | "title" | "subtitle"
  | "term" | "definition"
  | "footnote" | "quote" | "attribution" | "formula" | "unknown"

type ListBody = {
  kind: "ordered" | "unordered" | "definition"
  start?: number           // declared start for ordered lists
}

type ListItemBody = {
  ordinal: number          // 1-based position within the list
  label?: string           // declared marker text when the source renders one
}

type AsideBody = {
  kind: "note" | "tip" | "warning" | "caution" | "important"
      | "sidebar" | "epigraph" | "example" | "unknown"
  title?: string
}

type TableBody = {
  caption?: string
  rowCount: number
  columnCount: number
  headers?: TableHeader[]
}

type TableHeader = {
  rowIndex: number
  columnIndex: number
  text: string
  rowSpan?: number
  columnSpan?: number
}

type TableRowBody = {
  rowIndex: number
  role?: "header" | "body" | "footer"
}

type TableCellBody = {
  rowIndex: number
  columnIndex: number
  rowSpan?: number
  columnSpan?: number
  text?: string
}

type FigureBody = {
  imageHash?: string       // SHA-256 hex of the archived image bytes
  imageMediaType?: string
  imageSizeBytes?: number
  altText?: string
  caption?: string
}

type CaptionBody = {
  text: string
  label?: string           // e.g. "Figure 1-1.", "Table 3-1."
}

type CodeBlockBody = {
  code: string
  language?: string
  label?: string           // e.g. "Example 2-1."
  title?: string
}
```

Removed fields relative to v0.3: `PageBody.width/height/rotation/renderedImageUri`;
`TextSectionBody.normalizedText`; `TextBlockBody.normalizedText`;
`TableBody.normalizedMarkdown/normalizedCsvUri/normalizedHtmlUri`;
`TableCellBody.normalizedText/value/valueType/headerRefs`;
`FigureBody.imageUri/figureType/ocrText`; `CaptionBody.normalizedText/captionForUnitIds`;
`CodeBlockBody.normalizedCode/startLine/endLine`. Removed roles: `header`,
`footer`, `list_item` (now a container type).

Text projection for `textHash`: `text` for `text_block`, `caption`, and
`table_cell`; `code` for `code_block`; absent for every other type.

### 2.3 Locators

Closed union:

```ts
type Locator = DomPathLocator | CharRangeLocator

type DomPathLocator = {
  kind: "dom_path"
  document: string         // package-relative href of the content document
  path: string             // element path, Section 9.1
  elementId?: string       // the element's id attribute when present
  nodeRange?: [number, number]  // child-node index range for a text run within the element
}

type CharRangeLocator = { kind: "char_range"; start: number; end: number }
```

Removed: `page_bbox`, `byte_range`, `time_range`, `xml_path`, `table_cell`,
`repo_path`, and `CoordinateSystem`.

### 2.4 UnitRelationship

Closed set:

```
contains, precedes, appears_on, caption_of, has_caption, references
```

Removed: `physically_contains`, `logically_contains`, `follows`,
`continues_on`, `derived_from`. Section resolution walks `contains` upward to
the nearest `text_section`.

`relationshipRole` values:

| Type | Roles |
| --- | --- |
| `references` | `footnote`, `cross_reference`, `index_locator` |
| all others | none |

### 2.5 ParseMetrics

```ts
type ParseMetrics = {
  unitCount?: number
  relationshipCount?: number
  pageCount?: number
  sectionCount?: number
  listCount?: number
  asideCount?: number
  tableCount?: number
  figureCount?: number
  codeBlockCount?: number
  annotationCount?: number
  projectionCount?: number
}
```

Removed: `ocrRegionCount`.

### 2.6 Conformance dimensions

Existing: `locator_coverage`, `relationship_coverage`,
`caption_pairing_rate`, `table_decomposition_rate` (a table counts as
decomposed when a `table_cell` is reachable through `contains`).

Added, each present only when its subject population is non-empty:

- `list_decomposition_rate`: fraction of `list` units with at least one
  `list_item` reachable through `contains`.
- `section_kind_coverage`: fraction of `text_section` units whose kind is not
  `unknown`.

### 2.7 Bundle artifacts

The staged bundle's `artifacts/` directory holds image bytes, one file per
distinct image, named by the lowercase SHA-256 hex of its bytes. The importer:

1. Recomputes each file's hash; a name that does not equal the hash is a
   contract violation (recorded parse failure).
2. Writes each file to the artifact store and lists it in the canonical parse
   bundle manifest with `artifactType = "image"`.
3. Does not rewrite bodies. `FigureBody.imageHash` is the store key; the store
   resolves hash to blob.

## 3. Routing and identity

### 3.1 MIME and route

- `mime_type_for_native_uri` maps the `.epub` extension (case-insensitive) to
  `application/epub+zip`, exported as `MIME_TYPE_EPUB`.
- `ParseRoute` has exactly `Epub` and `PlainText`.
- The unparseable-MIME health count excludes both routed types.
- Both routes resolve sources through `resolve_contained_source`. The
  PDF-only `resolve_source_reference` is removed.
- The annotation dry run reaches the EPUB worker through the shared parse
  prefix with no additional wiring.

### 3.2 Parser identity

- `parserName = "epub"`, `parserVersion = "1"`.
- `parserConfigHash` is the canonical hash of
  `{ mappingVersion, sectionRulesVersion, kindPatternsVersion, entityTableVersion }`,
  each a string constant in the worker. Any change to a mapping rule, a
  section rule, a kind pattern, or the entity table bumps its constant.
- Admission caps (Section 4) are not identity: they decide whether a parse
  completes, not what a completed parse contains.
- No dependency-lock hash. A well-formed document parses identically across
  compatible parser-crate versions; a crate change that alters output is a
  mapping-version bump by the maintainer.

### 3.3 Capability profile

```
emitsContentTypes:      document, page, text_section, text_block, list,
                        list_item, aside, table, table_row, table_cell,
                        figure, caption, code_block
emitsRelationshipTypes: contains, precedes, appears_on, caption_of,
                        has_caption, references
emitsLocatorKinds:      dom_path
```

## 4. Configuration

New section, all keys required, all values admission budgets:

```toml
[epub]
# Archive members accepted before the parse fails as a recorded outcome.
max_members = 10000
# Decompressed bytes accepted for any single member.
max_member_bytes = 67108864
# Decompressed bytes accepted across all members read.
max_total_member_bytes = 536870912
# Decoded bytes accepted for any XML member (package, navigation, content).
max_document_bytes = 16777216
# Bytes accepted for one image; larger images are not archived (warning).
max_image_bytes = 16777216
# Element nesting depth accepted in any XML member.
max_element_depth = 256
```

The worker has no wall-clock budget. A config-backed timeout is deferred to the
ingestion pipeline refactor.

Removed sections and keys: `[pdf]`, `[docling]`, and under `[parsing]`:
`process_log_bytes`, `process_read_chunk_bytes`, `mupdf_poll_ms`,
`docling_poll_ms`, `docling_feedback_initial_ms`, `docling_feedback_interval_ms`,
`docling_sample_seconds`, `activity_sample_grace_ms`, `activity_poll_ms`,
`cleanup_regex_backtrack_limit`. Remaining `[parsing]` keys:
`max_candidate_units`, `max_candidate_relationships`,
`max_candidate_warnings`, `max_unit_body_bytes`.

The bundle writer's bounded stdout/stderr logs remain in the bundle contract
as empty files for in-process workers.

## 5. Archive and package

### 5.1 Archive

- Read with the `zip` crate, deflate only. Members are read into memory
  through a reader capped at `max_member_bytes`; declared sizes are not
  trusted. Running total is capped at `max_total_member_bytes`; member count at
  `max_members`. Exceeding any cap is a recorded parse failure naming the cap.
- Nothing is extracted to disk. Member names are used only as lookup keys
  after normalization (Section 5.4).
- Encrypted members, unsupported compression methods, and an archive that is
  not a zip file are recorded parse failures.
- The `mimetype` member is read when present. Absent: warning
  `epub_mimetype_missing`. Present with a value other than
  `application/epub+zip` after trimming: warning `epub_mimetype_mismatch`.
  Neither is a failure; the container and package decide.

### 5.2 Container and package

- `META-INF/container.xml` must exist and parse. The first `rootfile` whose
  `media-type` is `application/oebps-package+xml` names the package document.
  Missing container, missing rootfile, or a rootfile member that does not
  exist is a recorded parse failure.
- The package document must parse as XML. `version` is recorded; `2.0` and
  `3.*` are accepted; any other value is a warning `epub_package_version`,
  not a failure.
- Metadata read into `DocumentBody`: first `dc:title`; every `dc:creator` in
  document order; first `dc:publisher`; first `dc:language`; every
  `dc:identifier` value as written; first `dc:date`; first `dc:description`
  with markup stripped by the text rules of Section 8. All values are trimmed;
  empty values are omitted.
- Manifest items are indexed by `id` with `href`, `media-type`, and
  `properties`. Hrefs resolve against the package document's directory.
- Spine `itemref` elements define reading order. An `idref` with no manifest
  item is a warning `epub_spine_item_missing` and is skipped. `linear="no"`
  items are emitted in place and recorded in the raw report. A spine with no
  resolvable content documents is a recorded parse failure.

### 5.3 Navigation

Exactly one navigation source is chosen:

1. The manifest item whose `properties` contains `nav` (EPUB 3).
2. Otherwise the manifest item named by the spine's `toc` attribute whose
   media type is `application/x-dtbncx+xml` (EPUB 2 NCX).
3. Otherwise none: warning `epub_navigation_missing`; sections derive from
   headings only (Section 7.4).

A chosen navigation member that is missing or fails to parse is a recorded
parse failure: the source declares a table of contents it cannot deliver.

Navigation tree: for EPUB 3, the `nav` element whose `epub:type` is `toc`,
its nested `ol > li` structure; each `li` contributes its `a` (label and
href) or `span` (label, no target) and its nested `ol`. For NCX, the
`navMap > navPoint` tree with `navLabel/text` and `content/@src`. Labels are
whitespace-normalized. Targets are split into document href and fragment.

Landmarks: an EPUB 3 `nav` with `epub:type="landmarks"` and an NCX `guide`
supply kind hints (Section 7.2). A `nav` with `epub:type="page-list"`
supplies page labels (Section 7.6).

### 5.4 Href resolution

An href is resolved by: stripping a fragment; percent-decoding, with the
result required to be valid UTF-8; resolving `.` and `..` segments against
the referencing member's directory; rejecting any result that escapes the
archive root. Resolution failure is not a parse failure; the referencing
feature records an unresolved target (navigation: `epub_nav_target_unresolved`;
links: Section 10.4; images: `epub_image_missing`). Hrefs with a URI scheme
are external and never resolved.

## 6. XML and text encoding

- Every XML member is decoded as UTF-8, or UTF-16 when a byte-order mark is
  present. A member declaring any other encoding is accepted only when every
  byte is below 0x80 and is then decoded as ASCII; a byte at or above 0x80
  under such a declaration is a recorded parse failure naming the member and
  the declared encoding.
- Before parsing, a bounded pre-pass rewrites named character references from
  the XHTML 1.1 entity set to numeric references. The table lives in its own
  source file and is versioned by `entityTableVersion`. References not in the
  table and not one of the five XML entities are left for the parser, which
  rejects them.
- Members are parsed with `roxmltree`. A member that is not well-formed is a
  recorded parse failure naming the member and the parser's error. Nesting
  beyond `max_element_depth` is a recorded parse failure.
- Element names are matched by local name; namespaces are ignored except
  `epub:type`, which is matched by local name `type` in the EPUB namespace.
- Comments, processing instructions other than `<?dp ... ?>` (Section 7.6),
  and CDATA are treated as absent.

## 7. Structure

### 7.1 The document root

The first emitted unit is the `document` unit, locator
`{document: <package href>, path: "/package[1]"}`. Every level-1 section and
every unit that precedes the first section is contained by it.

### 7.2 Section kinds

Kind is determined by the first rule that matches, in this order:

1. `epub:type` on the content document's `body`, on the nearest enclosing
   `section` or `div`, or on the navigation target element, mapped by the
   kind table.
2. HTMLBook `data-type` on the same elements, mapped by the kind table.
3. A landmarks or guide entry whose target resolves to the section's target.
4. The navigation label or heading text matched against the kind pattern
   table: leading words such as `chapter`, `part`, `book`, `volume`,
   `appendix`, `preface`, `foreword`, `introduction`, `prologue`, `epilogue`,
   `afterword`, `conclusion`, `glossary`, `bibliography`, `index`, `notes`,
   `endnotes`, `acknowledgments`, `dedication`, `contents`, `copyright`,
   `colophon`, `cover`, `title page`, case-insensitive, with punctuation and
   roman or arabic numerals ignored.
5. `unknown`.

Rule 4 exists for the EPUB 2 samples, none of which carries semantic types.
The kind and pattern tables live in their own source file, versioned by
`kindPatternsVersion`. A section created from a heading with no other signal
is `section`.

### 7.3 Section labels and headings

- HTMLBook marks the number in `span.label` inside the heading; the span text
  becomes `label`, the remainder `headingText` (AI Engineering, DDIA).
- Otherwise a leading token matching `<kindword> <numeral>[.:-]` or a bare
  `<numeral>.<numeral>...` prefix becomes `label` and the rest `headingText`
  (TCP/IP `3.2.1. The IEEE 802 ...`, Civilization `CHAPTER IX`).
- A heading whose text contains a line break (`<br/>`) is split at the first
  break: first line is `label` when it matches a kind word, else the whole
  text is `headingText` (Civilization `CHAPTER IX<br/>Babylonia`).
- `sectionPath` entries are `label` and `headingText` joined by one space when
  both exist, else whichever exists.
- `headingLevel` is tree depth, never the `h` number.

### 7.4 Section tree construction

The navigation tree is authoritative for hierarchy; spine order is
authoritative for content order. Construction:

1. Every navigation node becomes a `text_section`. Its target is a document
   href plus optional fragment. Nodes nest as in the navigation tree.
2. Content documents are walked in spine order, block by block (Section 7.5).
   The current section is the deepest navigation section whose target has
   been passed in reading order, across document boundaries. A target is
   passed when the walk reaches the element carrying the fragment id, or any
   element for a fragment-less target at the start of its document.
3. A navigation section is emitted at the position where its target is
   passed. Its `headingText` is the text of the target element when that
   element is a heading (Section 7.5 heading rule), else of the first heading
   that follows the target before any other block with text, else the
   navigation label. The Strange Loop sample anchors sections on an empty
   `div` immediately before the `h1`; the Elements sample anchors on a bold
   paragraph, which stays a paragraph while the label becomes the heading.
4. A navigation node whose target does not resolve is emitted immediately
   after its previous sibling, or at its parent's position when first, with
   `headingText` from the label and warning `epub_nav_target_unresolved`.
5. A heading element that is not a navigation target and is not inside an
   aside, figure, table, list, or blockquote opens a subsection under the
   current section. Subsections derived from headings nest among themselves
   by `h` number: a heading of number N closes open heading-derived
   subsections of number ≥ N. Reaching a navigation target closes every
   heading-derived subsection. When the heading is the first heading inside a
   `section` element, the subsection also closes at the end of that element.
   This rule carries TCP/IP `h4`/`h5` headings below its three-level NCX.
6. A content document that no navigation node targets, and whose content is
   reached while no section from a previous document is current, gets one
   synthesized section: kind by Section 7.2, `headingText` from its first
   heading, else its `<title>` when it differs from the document title, else
   its href. Cover, copyright, and dedication documents typically arrive this
   way.
7. Content that precedes every section in the parse is contained by the
   `document` unit.
8. With no navigation source, rule 5 alone builds the tree from headings,
   and rule 6 applies to every document.

### 7.5 Block walk

Within a content document the walk starts at `body` and visits element
children in document order, treating each as one of:

- **Skipped entirely**: `head`, `script`, `style`, `template`, `nav` whose
  `epub:type` is `toc`, `landmarks`, or `page-list`, and any element with a
  `hidden` attribute. A `nav` document listed in the spine is skipped
  wholesale; its content is the section tree.
- **Page marker** (Section 7.6).
- **Heading**: `h1`–`h6`, and any element with `epub:type="title"` when no
  `h` element is present in its container. A heading inside an aside, figure,
  table, list item, or blockquote is that container's title or caption, not a
  section heading. A heading outside those containers is emitted as a
  `text_block` with role `heading` contained by the section it opens or
  belongs to, so heading text is retrievable evidence, and also populates that
  section's `headingText`.
- **Container**: `section`, `article`, `div`, `aside`, `blockquote`,
  `figure`, `table`, `ul`, `ol`, `dl`, `li`, `dt`, `dd`, `pre`, `math`,
  `header`, `footer`, `main`, `details`, `summary`, `center`. Mapping is in
  Section 7.7. Unlisted block-level elements are walked as generic
  containers and counted under `epub_unknown_element` per local name.
- **Text-bearing block**: an element whose children are text nodes and
  inline elements only, containing at least one non-whitespace character
  after extraction. It becomes one `text_block`.
- **Mixed content**: an element with both block-level children and text or
  inline runs. Each maximal run of text and inline elements between block
  children becomes one `text_block`; block children are handled in place.
  The Strange Loop sample nests a list-like `div` inside a paragraph `div`.
- **Whitespace-only block**: dropped and counted under
  `epub_empty_blocks_dropped`. The Elements sample and the Calibre samples
  emit these as spacers.

Inline elements: `a`, `abbr`, `b`, `bdi`, `bdo`, `br`, `cite`, `code`,
`data`, `dfn`, `em`, `i`, `img`, `kbd`, `mark`, `q`, `s`, `samp`, `small`,
`span`, `strong`, `sub`, `sup`, `time`, `tt`, `u`, `var`, `wbr`, and `math`
when it has a block ancestor with text. Retailer wrappers such as
`span.koboSpan` are inline and transparent.

### 7.6 Page markers

A page marker is any of, in precedence order within one document:

1. An element with `epub:type="pagebreak"` or `role="doc-pagebreak"`; label
   from `title`, then `aria-label`, then text content.
2. A `<?dp n="…" folio="…"?>` processing instruction; label from `folio`
   (Strange Loop).
3. An empty `a` or `span` whose `id` matches `^(page|pg|p)[_-]?([0-9]+|[ivxlcdm]+)$`
   case-insensitive; label from the captured group (Civilization).
4. A block element whose `id` matches the same pattern; the marker is at the
   block's start and the block is still emitted (Elements).

A `page-list` navigation supplies labels by target id and overrides labels
derived above. Each marker becomes a `page` unit with `ordinal` in reading
order across the parse and `label` when known; unlabeled markers get warning
`epub_page_marker_unlabeled` aggregated per document. Every unit whose start
lies at or after marker k and before marker k+1 gets `appears_on` to page k;
a unit spanning a marker gets edges to both pages. Documents with no markers
produce no pages and no edges.

### 7.7 Container mapping

| Source | Unit | Rules |
| --- | --- | --- |
| `section`, `article`, `div`, `header`, `footer`, `main`, `center` without an aside kind | none | Walked; contributes `data-type`/`epub:type` to section kind and closes heading subsections (7.4 rule 5). |
| `aside`, or `div`/`section`/`blockquote` whose `epub:type` or `data-type` is `note`, `tip`, `warning`, `caution`, `important`, `sidebar`, `epigraph`, or `example` | `aside` | `kind` from the type; `title` from the first heading inside, which is also emitted as a `text_block` role `heading` contained by the aside. An `aside` with no recognized type is kind `unknown`. HTMLBook `example` contains a `pre`: the heading becomes a `caption` paired to the `code_block` (7.9), and the `code_block` takes `label`/`title` from it. |
| `blockquote` without an aside kind | none | Walked; its text-bearing blocks get role `quote`; a descendant with `epub:type="attribution"`, `data-type="attribution"`, or a `cite` block gets role `attribution`. Calibre uses `blockquote` for indentation (TCP/IP); this is emitted faithfully as `quote` and recorded as a deviation (Section 12). |
| `ul`, `ol` | `list` | `kind` `unordered`/`ordered`; `start` from the attribute. Each `li` becomes a `list_item` with `ordinal`; the item's text-bearing blocks are `text_block` role `paragraph` contained by the item; nested lists are contained by the item. |
| `dl` | `list` | `kind` `definition`. Each `dt` and its following `dd` elements up to the next `dt` form one `list_item`; the `dt` text is a `text_block` role `term`, each `dd` text-bearing block role `definition`. |
| `table` | `table`, `table_row`, `table_cell` | Section 7.8. |
| `figure`, `img` | `figure` | Section 7.9. |
| `pre` | `code_block` | `code` is the verbatim text with CRLF normalized to LF and one leading newline stripped; `language` from `data-code-language`, then a `class` token of the form `language-X`, `lang-X`, or `X` when the `pre` or its first `code` child carries `data-type="programlisting"`; else absent. |
| monospace block | `code_block` | A text-bearing block, outside `pre`, whose every text character is inside `tt`, `code`, `kbd`, or `samp` descendants and whose extracted text contains at least one line break. Language absent. Counted under `epub_code_heuristic`. Exists for TCP/IP listings. |
| block `math` | `text_block` role `formula` | Text from `alttext`, else the text content of an `annotation` child, else the flattened text. |
| inline `math` | none | Its `alttext` or flattened text is inlined into the enclosing block's text. |
| `svg` | `figure` | No bytes archived; `altText` from `title` or `desc`; warning `epub_svg_inline`. |
| `hr`, `wbr` | none | Ignored. |

### 7.8 Tables

- `caption` becomes a `caption` unit paired to the table (7.9 pairing) and
  fills `TableBody.caption`.
- Rows are walked in `thead`, `tbody`, `tfoot` order as they appear;
  `thead` rows get role `header`, `tfoot` rows `footer`, others `body`.
- Cells occupy a grid honoring `rowspan` and `colspan`; `rowIndex` and
  `columnIndex` are grid positions; `rowCount` and `columnCount` are the
  grid's extent. Spans are declared only when greater than one.
- `th` cells, and every cell in a `thead` row, are listed in `headers`.
- Cell `text` is the extracted text of the cell's content; block children
  inside a cell are flattened into one text separated by newlines. Footnote
  rows (AI Engineering `tr.footnotes`) are ordinary rows.
- Containment: table `contains` each row; row `contains` each cell. Every
  cell carries a locator.

### 7.9 Figures and captions

Figure detection: a `figure` element, or an `img` not inside a `figure`. A
`figure` with several `img` children yields one figure per image, all sharing
the caption. An `img` inside a text-bearing block yields a `figure` emitted
immediately after that block, contained by the same parent; its `alt` is not
inlined into the block text.

Image archival: the `src` resolves to a manifest member; its bytes are hashed
and, when at most `max_image_bytes`, written to `artifacts/<sha256>`;
`imageHash`, `imageMediaType` (from the manifest), and `imageSizeBytes` are
set. Oversized images set no hash and record `epub_image_too_large`; missing
members record `epub_image_missing`. `altText` is the `alt` attribute when
non-empty.

Caption detection, first match wins:

1. `figcaption`, or `caption` for tables.
2. Inside a `figure`: a heading element, or a text-bearing block whose text
   matches the caption label pattern (O'Reilly `h6`).
3. Adjacent sibling: the text-bearing block immediately before or immediately
   after the figure, in that order of preference, whose text matches the
   caption label pattern
   `^(figure|fig\.?|table|illustration|plate|map|exhibit|listing|example)\s*[\divxlc]`
   case-insensitive, and that is not already paired (TCP/IP captions precede
   images; Civilization captions follow them in `p.figcap`).

A detected caption becomes a `caption` unit with `label` from the leading
label pattern and `text` as the full caption text, contained by the same
parent as its subject, and paired both ways: `caption_of` from caption to
subject and `has_caption` from subject to caption. The subject's own
`caption` field receives the caption text. Captions are also evidence text.
A figure with no caption pairs nothing. A caption pattern block that has no
adjacent figure, table, or code block stays a `text_block` role `paragraph`.

### 7.10 Notes and footnotes

A block is a footnote when any of:

- it carries, or is inside an element carrying, `data-type="footnote"`,
  `epub:type` of `footnote`, `endnote`, `rearnote`, or `noteref` target
  semantics (`aside` with `epub:type="footnote"`);
- it is inside a container with `data-type="footnotes"` or `epub:type` of
  `footnotes`, `endnotes`, or `rearnotes`;
- it is a paragraph within a section of kind `notes`.

Footnote blocks get role `footnote`. `label` is the leading marker when the
text starts with a marker pattern `^(\d+|[a-z]|[ivxlcdm]+)[.)]?\s` or
`^page\s+[\divxlc]+\s` (Strange Loop keys notes by print page); the text is
left intact. A backlink `a` at the start of a footnote is removed from the
text (Civilization, DDIA).

### 7.11 Title and subtitle blocks

Within a section, a text-bearing block that is not a heading element and whose
normalized text equals the section's `label`, its `headingText`, or the
navigation label is emitted with role `title`. A block immediately following a
`title` block, before any `paragraph`, whose normalized text is a remaining
component of the navigation label is role `subtitle`. Chapter-opener `div`
titles in the Strange Loop sample resolve this way; everything else with no
signal is `paragraph`.

## 8. Text extraction

Applied to every text-bearing block, caption, cell, term, and definition:

1. Concatenate the text of all descendant text nodes in document order. Inline
   element boundaries add nothing; `br` contributes one newline.
2. Remove the text of: `a` elements that are note references (Section 10.3);
   empty anchors, including HTMLBook `a[data-type="indexterm"]`; `sup`
   elements whose only content is a note reference; `img` (handled as figure);
   `script` and `style` if encountered inline.
3. Replace inline `math` with its text (Section 7.7).
4. Normalize: convert NBSP and other Unicode space separators to U+0020;
   collapse runs of spaces and tabs to one space; trim each line; drop empty
   lines except that a single blank line is preserved between non-empty
   lines when it came from two consecutive `br`; trim the whole.
5. No repairs. Doubled spaces from PDF reflow are collapsed by step 4; split
   headings, hyphenation, and quote styles are emitted as found.

`pre` content skips steps 1 and 4 beyond CRLF normalization. Unicode
normalization to NFC happens at import, not in the worker.

`language`: the nearest ancestor `lang` or `xml:lang`, else the package
language.

Text-bearing blocks whose extracted text is empty after step 4 are dropped
(Section 7.5).

## 9. Locators

### 9.1 Element path

`path` is the sequence of steps from the document element to the target
element, each `/<localname>[<n>]` where `n` is the 1-based index of the
element among siblings with the same local name. Example:
`/html[1]/body[1]/section[1]/div[1]/p[5]`. Paths are computed on the parsed
tree after the entity pre-pass, which does not change structure.

### 9.2 Assignment

- Every unit carries exactly one `dom_path` locator.
- `document`: the package document, path `/package[1]`.
- `page`: the marker element, or for `<?dp?>` the following element with
  `nodeRange` naming the instruction's position.
- Sections: the navigation target element when resolved, else the heading
  element, else the document `body`.
- Text blocks from mixed content: the parent element with `nodeRange` set to
  the child-node index range of the run.
- Every other unit: its source element.
- `elementId` is set when the element has an `id`.

Locator coverage is therefore 1.0 for every EPUB parse.

## 10. Relationships

### 10.1 Emission order

After all units: `contains` edges in unit order; then `precedes` edges per
sibling group in unit order; then `appears_on` in unit order; then caption
pairs in caption order, `caption_of` before `has_caption`; then `references`
in source order of the links. Sequence indexes follow emission.

### 10.2 Containment and order

Every unit except `document` has exactly one parent and one `contains` edge
from it; `parentLocalId` names the same parent. Siblings under one parent are
chained by `precedes` in reading order. The `document` unit's children are the
level-1 sections and any pre-section content.

### 10.3 Note references

A link is a note reference when it carries `data-type="noteref"` or
`epub:type="noteref"`, or when it is inside a `sup` and its target resolves to
a footnote block, or when its target resolves to a footnote block and the link
text is a marker pattern (Section 7.10). A note reference produces
`references` with role `footnote` from the block containing the link to the
footnote block, and is removed from the block text.

### 10.4 Cross-references and index locators

Every other `a` with a fragment or member-relative href is an internal link.
If its target element lies inside an emitted unit, a `references` edge is
produced from the containing block to that unit, role `index_locator` when the
containing block is inside a section of kind `index`, else `cross_reference`.
Links to the `document` unit's own section targets resolve to those sections.
Unresolved internal links are counted per document under
`epub_link_unresolved` and listed in the raw report. External links contribute
their text only.

Backlinks from a footnote to its referrer are not emitted.

## 11. Worker execution

### 11.1 Module layout

```
src/parse/epub/
  mod.rs          worker entry, identity, capability profile, staging
  archive.rs      capped zip member reading
  package.rs      container, OPF metadata, manifest, spine
  navigation.rs   nav document, NCX, landmarks, page-list
  xhtml.rs        entity pre-pass, decoding, DOM helpers, element paths
  text.rs         text extraction
  structure.rs    section tree and block walk
  blocks.rs       container mapping, tables, figures, captions, notes
  links.rs        href resolution, note references, cross-references
  report.rs       raw structure report
  entities.rs     XHTML entity table (external-artifact rule)
  kinds.rs        section kind and pattern tables
```

Dependencies added: `zip` with default features off and `deflate` only,
`roxmltree`, and `regex`.

The worker follows the plain-text worker's outcome model: source-caused
failures seal a failure bundle and return `Ok`; only staging faults return
`Err`.

### 11.2 Sequence

1. Open the bundle writer with the worker identity.
2. Read the archive member directory; enforce member count.
3. Read `mimetype`, container, package; read the navigation source.
4. Walk spine documents in order, building units, relationships, page
   markers, images, and the report incrementally. Unit and relationship
   records are streamed to the bundle as they are finalized; images are
   written to `artifacts/` as encountered, deduplicated by hash.
5. Write `parser_raw/epub_structure.json`.
6. Seal the bundle with metrics and `ParserResult`; `toolIdentity` records
   the package version and the navigation source kind.

Candidate counts are checked against `[parsing]` caps as they grow; exceeding
one is a recorded parse failure.

### 11.3 Raw report

`parser_raw/epub_structure.json` contains: package version and metadata as
read, manifest counts by media type, spine items with linear flags, the
navigation tree with each node's resolution status and emitted unit id, per
document: units emitted by type, blocks dropped, unknown elements by local
name, page markers with labels and ordinals, unresolved links with hrefs,
caption pairings by rule number, code heuristic applications, and the kind
rule that fired for each section. It contains no content text beyond
navigation labels and headings.

### 11.4 Diagnostics

Service-log events, following DIAGNOSTICS.md boundary rules:

| Event | Level | Fields |
| --- | --- | --- |
| `parse.epub_worker.started` | INFO | source_id, source_path |
| `epub.archive.opened` | INFO | member_count, total_member_bytes |
| `epub.package.read` | INFO | package_version, spine_count, manifest_count, navigation_source |
| `epub.document.mapped` | DEBUG | href, unit_count, dropped_blocks, unresolved_links, page_markers |
| `epub.mapping.completed` | INFO | unit_count, relationship_count, section_count, page_count, table_count, figure_count, code_block_count, image_count, image_bytes, warning_count, elapsed_ms |
| `parse.epub_worker.completed` | INFO | bundle_dir, elapsed_ms |
| `parse.epub_worker.parse_failed_recorded` | WARN | detail, stage, elapsed_ms |
| `parse.epub_worker.worker_faulted` | ERROR | error, elapsed_ms |

Stage names for failures: `archive`, `container`, `package`, `navigation`,
`document:<href>`, `caps`.

### 11.5 Warnings

Warnings are aggregated per code per document so `max_candidate_warnings`
bounds them: one warning per (code, document) with a count in the message,
and the first instance's locator. Codes: `epub_mimetype_missing`,
`epub_mimetype_mismatch`, `epub_package_version`, `epub_spine_item_missing`,
`epub_navigation_missing`, `epub_nav_target_unresolved`, `epub_unknown_element`,
`epub_empty_blocks_dropped`, `epub_page_marker_unlabeled`, `epub_image_missing`,
`epub_image_too_large`, `epub_svg_inline`, `epub_code_heuristic`,
`epub_link_unresolved`.

## 12. Recorded deviations

- `blockquote` used for layout is emitted as role `quote` (TCP/IP). No
  heuristic distinguishes layout from quotation.
- Lists rendered as `br`-separated lines inside a block, or as styled
  paragraphs, remain single `text_block` units with newlines (Strange Loop
  lists, Civilization numbered notes, Elements lettered items).
- Headings split across paragraphs by PDF reflow remain paragraphs; the
  section heading comes from the navigation label (Elements).
- Index entries are emitted as ordinary blocks under an `index` section with
  `index_locator` edges; retrieval-side handling of index noise is outside
  this document.
- Alt text that is auto-generated by the producer is emitted as found (AI
  Engineering).
- Print pages have no geometry; `page` units carry an ordinal and a label only.

## 13. Implementation consequences

### 13.1 PDF decommissioning

Remove: `src/parse/pdf.rs`, `pdf_worker.rs`, `mupdf_worker.rs`,
`mupdf_cleanup.rs` and its `mupdf_cleanup/` directory, `native_pdf.rs`,
`src/docling.rs`, `src/docling_activity.rs`, the MuPDF internal command
dispatch in `main.rs`, `PdfParser` and its threading through
`ParsePrefixContext` and `DryRunInputs`, `ParseRoute::Pdf`, `MIME_TYPE_PDF`,
`resolve_source_reference`, the `mupdf` and `fancy-regex` dependencies, the
PDF arm of `src/parse/cleanup.rs` and its regex repair passes, the
`PdfConfig`/`DoclingConfig` types and validation,
`src/bin/pdf-extract-diagnostic.rs`, and the `[parsing]` process and sampling
keys. Startup, health, identity capture, and monitoring lose their PDF-engine
fields.

### 13.2 Model revision

Update `src/model/unit.rs`, `body.rs`, `locator.rs`, `relationship.rs`,
`parse.rs` (metrics) to Section 2. Every exhaustive match over `ContentType`,
`TextBlockRole`, `Locator`, and `UnitRelationshipType` updates with it:
`assembly::evidence::evidence_text`, `projections::chunk::extract_targeting_text`
(which now chunks `code` for `code_block`, aligning it with the other three
readers), `projections::multivector::evidence_text`,
`annotations::producer::evidence_text`,
`query::passages` role checks (prose is `paragraph`, `quote`, `definition`,
`unknown`; no furniture), `sections.rs` (walk `contains` only), the importer's
text projection, `conformance.rs` (Section 2.6), and the web and CLI unit
renderers. No schema `CHECK` constrains `content_type` or
`relationship_type`, so no setup script is needed.

`query::passages::SearchResult` loses `pageNumbers`; PROTOCOL.md, the CLI
result renderer, and the web console drop the field with no replacement.

### 13.3 Importer

Archive `artifacts/` per Section 2.7. Add the artifact entries to the
canonical bundle manifest. No other gate changes: the capability profile,
body validation, ID assignment, and local-reference checks already cover the
new types.

### 13.4 Configuration

Add `[epub]` and `EpubLimits`; remove the sections and keys in Section 4;
fold the limits into `RuntimeLimits` and identity capture.

### 13.5 Plain-text worker

Unchanged in behavior. Its `text_block` bodies gain the required `role`
(`paragraph`) and lose `normalizedText`; its cleanup keeps the plain-text
arm only.

### 13.6 Documents to update after implementation

canonical spec (§4, §5, §12, §15, §17, §18, §19, §36, revision summary to
0.4); SPEC-SERVER.md (§2.8 removed, §2 `[epub]`, §10 rewritten, §19);
PROTOCOL.md (type, body, locator, relationship, role, and metrics lists, and
`pageNumbers` removal);
ARCHITECTURE.md (§1, §3, §8 PDF references, worker list, module list);
README.md (pipeline diagram, configuration table, MuPDF paragraphs);
INSTALL.md (Docling and MuPDF setup removed, EPUB added);
DIAGNOSTICS.md (Docling and MuPDF boundary entries replaced by EPUB events);
SPEC-CLIENT.md, SPEC-web-ui.md, and QUICKSTART.md (PDF engine and page-line
references);
config.example.toml (Section 4).

## 14. Acceptance

Verification is the repository's Cargo checks plus a parse of each sample
under the configured corpus root and a fresh or rebuilt index, inspected
through the CLI and the raw report.
Expected properties, all derived from the surveys in Section 1.3:

| Sample | Expected |
| --- | --- |
| Strange Loop | 24 chapter sections with nested subsections from the NCX; 427 page units with folio labels; `title` and `subtitle` blocks on chapter openers; notes section kind `notes` with `footnote` roles labeled `Page …`; index section kind `index` |
| TCP/IP | Sections nested to `h5` under a three-level NCX; captions labeled `Figure`/`Table` paired to figures for every `Figure N-N.`/`Table N-N.` block adjacent to an image; code blocks from the monospace heuristic; `footnote` references into the endnotes document; no page units |
| Civilization | Six-level section tree from the NCX; 37 tables decomposed with cells; captions paired after images in the photo-insert documents; both in-file and cross-file footnote references; page units from `page_N` anchors; parse completes within caps |
| Elements | Sections from NCX labels; anchored bold paragraphs remain paragraphs; spacer blocks dropped and counted; no failures |
| AI Engineering | `nav` chosen over NCX; sections to depth 4 with `label` from `span.label`; 49 tables decomposed with header rows; 162 figures with paired `h6` captions; 54 code blocks with language; 23 sidebars and 61 admonitions as asides; `dl` as definition lists; footnote references resolved; MathML formulas as `formula` blocks; index locators emitted |
| DDIA | Kobo spans transparent in text; scripts and styles skipped; epigraphs as asides; titled examples as caption-paired code blocks; 1.6 MB index document parsed within caps; over a thousand footnote references resolved |

Every sample: `locator_coverage = 1.0`, `relationship_coverage = 1.0`, no
`parse_failed` outcome, and every warning code present is explained by a
property of the sample listed in Section 1.3.
