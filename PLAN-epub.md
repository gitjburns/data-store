# PLAN-EPUB — Implementation plan for SPEC-epub.md

Read SPEC-epub.md first. This plan adds what the spec does not carry: session
decisions, verified repository facts, the orchestration model, the phase
scopes, and the EPUB worker module contract. Where this plan and the spec
disagree, the spec wins on content; this plan wins on process.

## 1. Context

SPEC-epub.md replaces the Docling and MuPDF PDF workers with an in-process
EPUB worker and revises the content model to v0.4. The existing fabric corpus
is disposable. The work lands in one session driven by an orchestrator that
delegates every edit, check, and review to subagents.

## 2. Decisions not recorded in the spec

- **No data compatibility.** The corpus under the configured `index_root` is
  discarded. No reader tolerates pre-v0.4 bodies, locators, or relationships.
  Acceptance begins with `--setup-storage` on a fresh index root or
  `--rebuild-all`.
- **Configuration files.** Only `config.example.toml` is edited. The
  operational files `config.toml`, `config.toml.local`, and
  `config.toml.remote` are not touched. The completion report leads with
  "Action required" naming those three files, the `[epub]` keys to add, and
  the `[pdf]`, `[docling]`, and `[parsing]` keys to remove.
- **Cargo.toml.** Editing it directly is approved within the owning phase:
  Phase 1 removes `mupdf` and `fancy-regex`; Phase 4 adds `zip` (default
  features off, `deflate` only), `roxmltree`, and `regex`. No other
  dependency change.
- **Out of scope, listed in the completion report as candidates for the
  ingestion pipeline refactor:** a config-backed EPUB worker timeout; the
  activation dominance gate in `src/activation.rs` (untouched); stale PDF
  references in `SPEC-projection-decouple.md` (not edited).
- **Acceptance ownership.** The user copies the six sample EPUBs from the
  repository root into the configured `storage.corpus_root`, runs setup or
  rebuild, and starts the server. The orchestrator runs the CLI verbs and the
  bundle lookup under per-command approval. Section 14 mismatches are
  reported as findings, never adjusted to pass.
- **Footnote resolution needs two passes.** Section 10.3 decides whether a
  link is a note reference by whether its target is a footnote block, and
  Section 8 removes note-reference text at extraction time. Targets may lie in
  later spine documents. The worker therefore walks the spine twice: pass one
  parses each document and records, per `(member, element id)`, whether the
  element is a footnote by Section 7.10 rules 1 and 2, or lies in a document
  whose navigation section has kind `notes` (rule 3 at document granularity);
  pass two emits. Heading-derived `notes` subsections inside a document are
  not footnote containers for link classification. This is the one precision
  loss accepted by this plan.

## 3. Verified repository facts

Verified 2026-09-13 against the tree. Agents may rely on these without
re-deriving them; anything else is read at phase time.

- `sql/fabric/schema.sql` has no `CHECK` on `content_type`,
  `relationship_type`, or `locators_json`. No setup script is needed.
- MIME constants and the extension mapper live in `src/acquisition.rs`
  (`MIME_TYPE_PDF` 1428, `MIME_TYPE_PLAIN_TEXT` 1430, private
  `mime_type_for_native_uri` 1435), not in `src/parse/`.
- `ParseRoute { Pdf, PlainText }` is `src/scheduler.rs:292`; its four matches
  are inside `parse_chain_prefix` (2520): routing 2556, profile 2576,
  containment 2647, worker call 2699. `ParsePrefixContext { storage, pdf }`
  is 341, constructed at 1318 and 1632. `pdf: PdfParser` is threaded through
  `start` 1085, `run_scheduler` 1180, `run_annotation_dry_run_pass` 1595.
- Unparseable-MIME counting binds exactly two MIME parameters at
  `src/scheduler.rs:984` (SQL at 233) and `src/monitoring_storage.rs:180`.
- PDF-only files and line counts: `src/parse/pdf.rs` 94, `pdf_worker.rs`
  1515, `mupdf_worker.rs` 868, `mupdf_cleanup.rs` 243,
  `mupdf_cleanup/text.rs` 419, `native_pdf.rs` 282, `src/docling.rs` 1364,
  `src/docling_activity.rs` 733, `src/bin/pdf-extract-diagnostic.rs` 350
  (auto-discovered binary; not in `[[bin]]`). `mod` declarations:
  `src/parse/mod.rs:12-16`, `src/main.rs:12-13`. External call sites:
  `main.rs:264` (`run_internal_command`), `main.rs:579` and `1289`
  (`PdfParser::from_config`), `src/dry_run.rs:80`.
- `mupdf` is used only by `native_pdf.rs`; `fancy-regex` only by
  `mupdf_cleanup*`. Neither is behind a Cargo feature. Shell scripts have no
  PDF references.
- PDF config: `PdfConfig` `src/config.rs:193`, `PdfEngine` 203,
  `DoclingConfig` 213, `ServiceConfig.pdf` 52, `.docling` 54, validation
  687-731. `ParsingLimits` is `src/parsing_limits.rs:8`; positivity checks
  `src/limits.rs:344-353`; `RuntimeLimits` `src/limits.rs:136`; folded by
  `ServiceConfig::runtime_limits` `src/config.rs:648`.
- Identity: `PdfIdentity` `src/identity.rs:194`, `DoclingIdentity` 204,
  fields 177-179, population 317-329. Errors: `ApiError::DoclingUnavailable`
  and `DoclingConversion` `src/error.rs:32-35`, wire codes 152-153.
  `src/restore.rs:38` doc comment names "docling" in a forbidden-grep
  invariant. `config.example.toml:46` comment mentions Docling artifacts.
- Bundle writer `src/parse/bundle.rs`: `BundleWriter::create(staging_root,
  BundleIdentity, RuntimeLimits)` 461, `append_candidate_unit` 522,
  `append_candidate_relationship` 531, `append_warning` 540,
  `parser_raw_dir()` 547, `artifacts_dir()` 557 (currently dead code; Phase 4
  makes it live), `finish(self, &ParserResult, &ParseMetrics, stdout, stderr)`
  580. `ParserResult.tool_identity: BTreeMap<String, String>` 213.
  `CandidateContentUnit` 239 `{ local_id, content_type, body, parent_local_id,
  sequence_index, locators }`; `CandidateUnitRelationship` 267;
  `CandidateWarning` 286 `{ code, message, severity, locator, unit_local_id }`.
  Reader-side caps 941-965.
- Worker template: `src/parse/text_worker.rs` — `run_text_parse(index_root:
  &StorageContext, source_absolute_path: &Path, source_id, source_hash) ->
  Result<PathBuf, ApiError>` 122; capability profile 71; config hash 57;
  `finish_failed` 361. `StorageContext` derefs to `Path` and exposes
  `limits()` (`src/runtime.rs:64`).
- Importer: `import_parser_bundle` `src/parse/importer.rs:234`;
  `validate_hard_gates` 768 with `content_type_body_matches`
  (`src/model/body.rs:19-44`) at 827; `text_projection_hash` 1029;
  `write_canonical_parse_bundle` 1240 with `files.insert` sites 1260-1325;
  `archive_parser_raw` 1113 is the `put_bytes` precedent;
  `ArtifactStore::put_bytes` `src/artifact_store.rs:181`.
- Conformance: `measure` `src/parse/conformance.rs:48`;
  `measure_table_decomposition_rate` 162 and `has_table_cell_descendant` 200
  are the mirror for `list_decomposition_rate`; dimension constants 34-37,
  insertion 76-87. `ConformanceReport` `src/model/parse.rs:153`.
- Model consumers (Phase 2 site list): five synchronized text extractors
  `src/assembly/evidence.rs:580`, `src/projections/chunk.rs:406`,
  `src/projections/multivector.rs:635`, `src/annotations/producer.rs:522`,
  `src/projections/view.rs:383`; furniture rule copies
  `src/query/passages.rs:433-448`, `src/query/channels.rs:1091-1099`,
  `src/query/annotation.rs:792-795` (SQL), `src/projections/section_dense.rs:1300-1313`;
  containment SQL `src/sections.rs:11-19` and archived mirror
  `src/projections/section_dense.rs:1053-1135`; page citations
  `src/query/passages.rs:76, 90, 292-307, 456-470, 951-954`,
  `src/bin/data-store.rs:555, 2401-2406`, `assets/web/app.js:680-686`;
  web body renderer `assets/web/app.js:961-1004`, relationship list
  1251-1263; cleanup role gates `src/parse/cleanup.rs:170-233, 262-316, 344`.
  `FigureType`, `TableRowRole`, `TableCellValue`, `TableCellValueType` have no
  consumer.
- `src/snapshot/verify.rs:1125` recomputes text hashes through
  `assembly::evidence::evidence_text`; with no data compatibility this needs
  no special handling.
- PROTOCOL.md ranges: content types 703-709, locators 710-718, relationship
  types 765-770, metrics 1219-1224, conformance 1226-1240, `pageNumbers` 461
  and 510, TOC 19-51.
- Toolchain: edition 2024, rustc 1.96. `std::sync::LazyLock` is available.
- The six sample EPUBs are in the repository root with the names in Section
  1.3.

## 4. Orchestration model

- **The orchestrator edits nothing and reads no source.** It writes briefs,
  launches agents, relays reports, obtains approvals, and tracks phase state.
- **Implementation agents are forks** (`subagent_type: "fork"`), so they
  inherit the onboarding, the spec, and this plan. One agent per phase unless
  a phase says otherwise. An agent runs `cargo fmt`, `cargo check`, and
  `cargo clippy` before reporting.
- **Verification agents are fresh `general-purpose` agents.** They receive
  the phase brief, the spec, this plan, and read the diff. They check scope
  completeness against the phase brief, PRINCIPLES.md and AGENTS.md comment
  and error-handling rules, consistency with neighbouring code, and that no
  file outside the phase scope changed. They edit nothing.
- **Gates.** A phase starts when the user approves its brief. Approval
  authorizes writes to the files the brief names and the Cargo checks. An
  agent that needs anything else stops and reports; the orchestrator brings
  the item to the user and resumes the same agent by message. Config edits
  beyond `config.example.toml`, new dependencies beyond Section 2, and any
  behavior the spec does not state are always escalations.
- **Report shape**, mandatory for every agent: files changed with one line
  each; Cargo results (verbatim output on any failure or warning); residual
  risk; open escalations. Nothing else.
- **Agents run no project code.** `cargo` commands only. The acceptance CLI
  verbs are run by the orchestrator under per-command approval.
- **Sequencing.** Phases 1 through 3 are sequential. Phase 4 runs in the
  waves of Section 6.4 and may overlap with Phases 5 and 6. Integration and
  acceptance are last.

## 5. Brief template

Each brief contains: phase name; spec sections that govern it; the file list
from Section 6; the steps; the verification commands; the report shape; and
the escalation rule. Briefs restate nothing from the spec or this plan; they
cite section numbers.

## 6. Phases

### 6.1 Phase 1 — PDF decommissioning (spec 13.1)

Files deleted: the nine PDF-only files in Section 3. Files edited:
`Cargo.toml`, `src/parse/mod.rs`, `src/main.rs`, `src/scheduler.rs`,
`src/dry_run.rs`, `src/source.rs`, `src/acquisition.rs`, `src/config.rs`,
`src/parsing_limits.rs`, `src/limits.rs`, `src/identity.rs`, `src/error.rs`,
`src/restore.rs` (doc comment only), `src/monitoring_storage.rs`,
`src/parse/cleanup.rs`, `config.example.toml`.

Steps:
1. Remove the nine files and their `mod` lines. Remove `mupdf` and
   `fancy-regex` from `Cargo.toml`.
2. `ParseRoute` becomes `{ PlainText }`. Remove `pdf` from
   `ParsePrefixContext` and the three threading signatures. Remove
   `MIME_TYPE_PDF` and the `.pdf` arm; the unparseable-MIME SQL and the
   monitoring predicate bind one MIME until Phase 4 adds EPUB. Remove
   `resolve_source_reference`; PlainText already uses
   `resolve_contained_source`.
3. Remove `PdfConfig`, `PdfEngine`, `DoclingConfig`, their `ServiceConfig`
   fields and validation, the ten `[parsing]` keys named in spec Section 4
   from `ParsingLimits` and `limits.rs`, `PdfIdentity`, `DoclingIdentity`,
   and the two `ApiError` Docling variants with their wire codes.
4. `cleanup.rs`: remove `CleanupKind::Pdf`, the page-joining arm, and
   `prose::clean_prose`'s PDF flag. Keep the plain-text arm. The
   header/footer furniture removal stays until Phase 2 removes the roles.
5. `config.example.toml`: delete `[pdf]` and `[docling]`, the ten `[parsing]`
   keys, and the Docling mention in the `[storage]` comment.
6. `BundleWriter::finish` still takes stdout/stderr slices bounded by
   `process_log_bytes`. With that key removed, bound them by a constant in
   `bundle.rs` or drop the bound; the agent chooses and comments the choice.

Compiles with PlainText as the only route.

### 6.2 Phase 2 — Content model v0.4 (spec 2, 13.2, 13.5)

Files: `src/model/unit.rs`, `body.rs`, `locator.rs`, `relationship.rs`,
`parse.rs`, `src/model/mod.rs`; every site in the Section 3 consumer list;
`src/parse/text_worker.rs`, `src/parse/cleanup.rs`, `src/parse/importer.rs`
(`text_projection_hash`, `content_type_body_matches` dispatch),
`src/parse/conformance.rs`, `src/sections.rs`, `src/query/passages.rs`,
`src/query/channels.rs`, `src/query/annotation.rs`,
`src/projections/section_dense.rs`, `src/bin/data-store.rs`,
`assets/web/app.js`.

Steps:
1. Rewrite the model files to spec Section 2 exactly: types, bodies with
   `deny_unknown_fields`, `TextBlockBody.role` required, the two-kind
   locator union, the six relationship types, `ParseMetrics` counts.
   `wire_name` matches and `content_type_body_matches` gain the new types.
2. Follow compile errors through every match. The five text extractors
   select `text` for `text_block`, `caption`, `table_cell`, `code` for
   `code_block`, none otherwise, with no `normalizedText` fallback; the
   chunker now includes `code_block`. Update the cross-reference comments in
   all five.
3. Furniture rule: `Header` and `Footer` roles no longer exist. Remove the
   exclusions in `passages.rs`, `channels.rs`, `annotation.rs` SQL, and
   `section_dense.rs`. Prose in `passages.rs` is `paragraph`, `quote`,
   `definition`, `unknown`.
4. `sections.rs` and the `section_dense.rs` mirror walk `contains` only.
5. `cleanup.rs` plain-text arm: remove furniture removal and role gates that
   named `Header`/`Footer`; `text_worker.rs` writes `role: paragraph`, no
   `normalized_text`.
6. `conformance.rs`: add `list_decomposition_rate` (mirror of the table
   rate, subject `list`, target `list_item`) and `section_kind_coverage`
   (fraction of `text_section` bodies with `kind != "unknown"`), each
   inserted only when its population is non-empty. `ConformanceReport`
   gains the two optional fields alongside the existing pair.
7. Remove `pageNumbers`: `SearchResult.page_numbers`, `PassagePart.pages`,
   the aggregation and merge sites, the CLI DTO field and render lines, and
   the web console block. No replacement.
8. Web console `derivedBodyParts` and `RELATIONSHIP_TYPES` follow the new
   sets; CLI unit rendering follows the new body fields.

### 6.3 Phase 3 — `[epub]` configuration (spec 4, 13.4)

Files: `src/config.rs`, a new `src/epub_limits.rs` mirroring
`parsing_limits.rs`, `src/limits.rs`, `src/identity.rs`,
`config.example.toml`.

Steps: `EpubLimits` with the six keys, `deny_unknown_fields`, positivity
checks in `RuntimeLimits::validate`, field `epub` on `ServiceConfig` and
`RuntimeLimits`, folded in `runtime_limits()`, captured in identity. The
`[epub]` section goes where `[pdf]` was, with one comment line per key
taken from spec Section 4.

### 6.4 Phase 4 — EPUB worker (spec 3, 5–12)

Files: `Cargo.toml`; new `src/parse/epub/` modules per Section 7 of this
plan; `src/parse/mod.rs`; `src/acquisition.rs`; `src/scheduler.rs`;
`src/monitoring_storage.rs`.

Waves, each a set of agents that may run concurrently, each wave gated on the
previous wave's `cargo check`:

- **Wave A (leaf modules):** `entities.rs`, `kinds.rs`, `archive.rs`,
  `xhtml.rs`, `text.rs`, `report.rs`. Also `mod.rs` skeleton: constants,
  error types, `Emitter`, capability profile, config hash, and
  `run_epub_parse` calling a stub `structure::walk_spine`.
- **Wave B:** `package.rs`, `navigation.rs`, `links.rs`.
- **Wave C:** `blocks.rs`, then `structure.rs` (sequential: `structure`
  depends on `blocks`).
- **Wave D (integration, one agent):** wire `ParseRoute::Epub`,
  `MIME_TYPE_EPUB` and the `.epub` arm in `acquisition.rs`, the profile and
  worker-call arms in `parse_chain_prefix`, the third MIME parameter in
  both unparseable-MIME sites, the dry-run path (no change expected beyond
  compile), and the Section 11.4 log events. Full Cargo checks.

Every module carries the Section 7 contract; agents may add private items
freely and must not change a contracted signature without escalation.

### 6.5 Phase 5 — Importer image archival (spec 2.7, 13.3)

Files: `src/parse/importer.rs`, `src/parse/bundle.rs` (reader side).

Steps: `read_bundle` lists `artifacts/` files with their bytes or paths under
the existing reader caps; the importer recomputes each file's SHA-256 and
fails the parse as a contract violation on mismatch; stores each via
`ArtifactStore::put_bytes`; inserts each into the canonical bundle manifest
with `artifact_type = "image"` keyed `artifacts/<hash>`. Bodies are not
rewritten. May run concurrently with Phase 4.

### 6.6 Phase 6 — Documentation (spec 13.6)

Files, each read at this phase: `canonical_content_graph_retrieval_fabric_v_0_3.md`,
`SPEC-SERVER.md`, `PROTOCOL.md`, `ARCHITECTURE.md`, `README.md`,
`INSTALL.md`, `DIAGNOSTICS.md`, `SPEC-CLIENT.md`, `SPEC-web-ui.md`,
`QUICKSTART.md`. Three agents, split by file, may run concurrently with
Phase 4. Each edit describes the v0.4 system as the spec states it; no
history or migration prose. `config.example.toml` is owned by Phases 1
and 3.

### 6.7 Phase 7 — Acceptance (spec 14)

User steps, in the project root:

```sh
cp *.epub "$(rg -o 'corpus_root = "\K[^"]+' config.toml)"/
data-store-service --config config.toml --setup-storage   # fresh index_root
# or: data-store --config config.toml --rebuild-all         # during startup delay
# then start the server as usual
```

Orchestrator steps, each under approval, run from the project root:

```sh
data-store --config config.toml --health
data-store --config config.toml --health-details
data-store --config config.toml --held-parses
data-store --config config.toml --source <sourceId>
data-store --config config.toml --unit <unitId>
data-store --config config.toml --query <text>
```

Raw report: the parse run's `artifact_bundle_uri` is read from
`parse_runs` in `<index_root>/fabric/fabric.sqlite3` by a read-only query;
the manifest lists `parser_raw_output` entries; `epub_structure.json` is the
blob at `<index_root>/fabric/artifacts/sha256/<2hex>/<hash>`. Compare
against the Section 14 table. Every warning code present must map to a
Section 1.3 property of that sample.

## 7. EPUB worker module contract

All items `pub(crate)` unless noted. Types not shown are private. Every
function carries a purpose comment per AGENTS.md.

**`mod.rs`**

```rust
pub(crate) const EPUB_PARSER_NAME: &str = "epub";
pub(crate) const EPUB_PARSER_VERSION: &str = "1";
pub(crate) const MAPPING_VERSION: &str = "1";
pub(crate) const SECTION_RULES_VERSION: &str = "1";
pub(crate) fn epub_capability_profile() -> Result<ParserCapabilityProfile, ApiError>;
pub(crate) fn run_epub_parse(index_root: &StorageContext, source_absolute_path: &Path,
    source_id: &str, source_hash: &str) -> Result<PathBuf, ApiError>;

pub(crate) enum EpubStage { Archive, Container, Package, Navigation, Document(String), Caps }
pub(crate) struct EpubFailure { pub stage: EpubStage, pub detail: String }   // recorded outcome
pub(crate) enum WorkerError { Recorded(EpubFailure), Fault(ApiError) }       // Fault = staging I/O only
pub(crate) type WorkerResult<T> = Result<T, WorkerError>;

pub(crate) struct Emitter<'a> { /* BundleWriter, EpubLimits, ParsingLimits, counters, image hash set */ }
impl Emitter<'_> {
    pub(crate) fn unit(&mut self, unit: CandidateContentUnit) -> WorkerResult<()>;      // streams; checks caps
    pub(crate) fn relationship(&mut self, from: &str, to: &str, kind: UnitRelationshipType,
        role: Option<&str>) -> WorkerResult<()>;                                        // buffers until finish
    pub(crate) fn warning(&mut self, code: &str, document: &str, locator: Option<Locator>); // aggregates per (code, document)
    pub(crate) fn image(&mut self, bytes: &[u8]) -> WorkerResult<Option<String>>;       // writes artifacts/<sha256>; None when oversized
    pub(crate) fn next_local_id(&mut self, content_type: ContentType) -> String;        // "<wire_name>-<n>"
}
```

`run_epub_parse` owns: `BundleWriter::create`, the Section 11.4 events, the
two-pass spine walk, `report::write`, the Section 10.1 relationship flush,
metrics, `ParserResult` with `tool_identity = { "packageVersion", "navigationSource" }`,
and mapping `EpubFailure` to a sealed failure (`Ok`) and `Fault` to `Err`.
The config hash is the canonical hash of the four version constants keyed
`mappingVersion`, `sectionRulesVersion`, `kindPatternsVersion`,
`entityTableVersion`.

**`entities.rs`**

```rust
pub(crate) const ENTITY_TABLE_VERSION: &str = "1";
pub(crate) fn codepoint(name: &str) -> Option<u32>;   // XHTML 1.1 named entities; not the five XML ones
```

**`kinds.rs`**

```rust
pub(crate) const KIND_PATTERNS_VERSION: &str = "1";
pub(crate) enum SectionKind { /* spec 2.2 */ }  impl SectionKind { pub(crate) fn wire_name(self) -> &'static str }
pub(crate) fn kind_from_semantic(value: &str) -> Option<SectionKind>;   // epub:type, data-type, landmark, guide
pub(crate) fn kind_from_text(text: &str) -> Option<SectionKind>;        // spec 7.2 rule 4
pub(crate) fn split_label(heading: &str) -> (Option<String>, String);   // spec 7.3 label/headingText
pub(crate) static PAGE_ID: LazyLock<Regex>;            // spec 7.6 rule 3
pub(crate) static CAPTION_LABEL: LazyLock<Regex>;      // spec 7.9 rule 3
pub(crate) static FOOTNOTE_MARKER: LazyLock<Regex>;    // spec 7.10, first form
pub(crate) static FOOTNOTE_PAGE_MARKER: LazyLock<Regex>; // spec 7.10, second form
```

**`archive.rs`**

```rust
pub(crate) struct Archive { /* zip::ZipArchive<File>, normalized name index, running total */ }
pub(crate) fn open(path: &Path, limits: &EpubLimits) -> Result<Archive, EpubFailure>; // member count, encryption, method
impl Archive {
    pub(crate) fn contains(&self, member: &str) -> bool;
    pub(crate) fn read(&mut self, member: &str) -> Result<Option<Vec<u8>>, EpubFailure>; // per-member and total caps
    pub(crate) fn member_count(&self) -> usize;
    pub(crate) fn total_bytes_read(&self) -> u64;
}
```

**`xhtml.rs`**

```rust
pub(crate) fn decode(bytes: &[u8], member: &str, limits: &EpubLimits) -> Result<String, EpubFailure>; // spec 6 encoding rule, max_document_bytes
pub(crate) fn rewrite_entities(text: &str) -> String;                       // bounded pre-pass via entities::codepoint
pub(crate) fn parse<'a>(text: &'a str, member: &str, limits: &EpubLimits) -> Result<roxmltree::Document<'a>, EpubFailure>; // depth check
pub(crate) fn local_name<'a>(node: roxmltree::Node<'a, '_>) -> &'a str;
pub(crate) fn epub_type<'a>(node: roxmltree::Node<'a, '_>) -> Option<&'a str>;
pub(crate) fn data_type<'a>(node: roxmltree::Node<'a, '_>) -> Option<&'a str>;
pub(crate) fn element_path(node: roxmltree::Node) -> String;               // spec 9.1
pub(crate) fn locator(document: &str, node: roxmltree::Node, node_range: Option<[u64; 2]>) -> Locator;
pub(crate) fn is_inline(node: roxmltree::Node) -> bool;                    // spec 7.5 inline list
pub(crate) fn is_skipped(node: roxmltree::Node) -> bool;                   // spec 7.5 skipped list
pub(crate) fn id_index(doc: &roxmltree::Document) -> BTreeMap<String, roxmltree::NodeId>;
```

**`text.rs`**

```rust
pub(crate) struct TextContext<'a> { pub is_noteref: &'a dyn Fn(roxmltree::Node) -> bool, pub package_language: Option<&'a str> }
pub(crate) fn extract(node: roxmltree::Node, ctx: &TextContext) -> String;        // spec 8 steps 1–4
pub(crate) fn extract_run(nodes: &[roxmltree::Node], ctx: &TextContext) -> String; // mixed-content run
pub(crate) fn extract_pre(node: roxmltree::Node) -> String;                       // CRLF→LF, one leading newline stripped
pub(crate) fn language(node: roxmltree::Node, package_language: Option<&str>) -> Option<String>;
pub(crate) fn is_monospace_block(node: roxmltree::Node) -> bool;                  // spec 7.7 code heuristic
```

**`report.rs`**

```rust
pub(crate) struct StructureReport { /* serde Serialize; spec 11.3 fields */ }
impl StructureReport {
    pub(crate) fn new(package_version: &str, metadata: &DocumentBody) -> Self;
    // one recording method per 11.3 fact: manifest_counts, spine_item, nav_node, document_summary,
    // unresolved_link, caption_pairing, code_heuristic, section_kind_rule, page_marker
    pub(crate) fn write(&self, raw_dir: &Path) -> Result<(), ApiError>;  // parser_raw/epub_structure.json
}
```

**`package.rs`**

```rust
pub(crate) struct ManifestItem { pub href: String /* normalized member */, pub media_type: String, pub properties: Vec<String> }
pub(crate) struct SpineItem { pub idref: String, pub href: String, pub linear: bool }
pub(crate) struct Package { pub href: String, pub version: String, pub metadata: DocumentBody,
    pub manifest: BTreeMap<String, ManifestItem>, pub spine: Vec<SpineItem>, pub toc_id: Option<String> }
pub(crate) fn check_mimetype(archive: &mut Archive, emitter: &mut Emitter) -> Result<(), EpubFailure>;
pub(crate) fn read_container(archive: &mut Archive) -> Result<String, EpubFailure>;   // rootfile href
pub(crate) fn read_package(archive: &mut Archive, rootfile: &str, limits: &EpubLimits,
    emitter: &mut Emitter) -> Result<Package, EpubFailure>;
```

**`navigation.rs`**

```rust
pub(crate) enum NavigationSource { Nav, Ncx, None }   impl NavigationSource { pub(crate) fn wire_name(self) -> &'static str }
pub(crate) struct Target { pub member: String, pub fragment: Option<String> }
pub(crate) struct NavNode { pub label: String, pub target: Option<Target>, pub kind_hint: Option<SectionKind>,
    pub children: Vec<NavNode>, pub resolved: bool }
pub(crate) struct Navigation { pub source: NavigationSource, pub toc: Vec<NavNode>,
    pub page_labels: BTreeMap<(String, Option<String>), String> }
pub(crate) fn read(archive: &mut Archive, package: &Package, limits: &EpubLimits,
    emitter: &mut Emitter) -> Result<Navigation, EpubFailure>;   // spec 5.3 selection and failure rule
```

**`links.rs`**

```rust
pub(crate) enum HrefOutcome { Internal(Target), External, Unresolvable }
pub(crate) fn resolve(base_member: &str, href: &str) -> HrefOutcome;                 // spec 5.4
pub(crate) struct FootnoteIndex { /* (member, id) -> bool from pass one */ }
impl FootnoteIndex { pub(crate) fn is_footnote(&self, target: &Target) -> bool; }
pub(crate) struct LinkRecord { pub from_local_id: String, pub target: Target, pub role_hint: LinkRole }
pub(crate) enum LinkRole { Footnote, CrossReference, IndexLocator }
pub(crate) struct UnitIndex { /* (member, id) -> unit local id, filled in pass two */ }
pub(crate) fn resolve_all(links: &[LinkRecord], units: &UnitIndex, emitter: &mut Emitter,
    report: &mut StructureReport) -> WorkerResult<()>;                                   // spec 10.3–10.4 edges and warnings
```

**`blocks.rs`**

```rust
pub(crate) struct BlockContext<'a> { pub document: &'a str, pub section_kind: SectionKind,
    pub text: &'a TextContext<'a>, pub emitter: &'a mut Emitter<'a>, pub links: &'a mut Vec<LinkRecord>,
    pub units: &'a mut UnitIndex, pub report: &'a mut StructureReport }
pub(crate) fn page_marker(node: roxmltree::Node, labels: &BTreeMap<(String, Option<String>), String>,
    document: &str) -> Option<Option<String>>;                                    // spec 7.6 rules 1, 3, 4; Some(label)
pub(crate) fn dp_page_marker(pi: roxmltree::Node) -> Option<Option<String>>;    // spec 7.6 rule 2
pub(crate) fn emit_text_block(ctx: &mut BlockContext, node_or_run: ..., parent: &str, role: TextBlockRole) -> WorkerResult<Option<String>>;
pub(crate) fn emit_table(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str) -> WorkerResult<String>;   // spec 7.8
pub(crate) fn emit_list(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str) -> WorkerResult<String>;    // ul, ol, dl
pub(crate) fn emit_figure(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str, package: &Package,
    archive: &mut Archive) -> WorkerResult<Vec<String>>;                            // spec 7.9, one per image
pub(crate) fn emit_aside(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str) -> WorkerResult<String>;
pub(crate) fn emit_code(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str, heuristic: bool) -> WorkerResult<String>;
pub(crate) fn emit_formula(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str) -> WorkerResult<String>;
pub(crate) fn detect_caption(...) -> Option<CaptionCandidate>;                     // spec 7.9 rules 1–3
pub(crate) fn pair_caption(ctx: &mut BlockContext, subject: &str, caption: CaptionCandidate, parent: &str) -> WorkerResult<()>;
pub(crate) fn is_footnote_block(node: roxmltree::Node, section_kind: SectionKind) -> bool;   // spec 7.10
pub(crate) fn footnote_label(text: &str) -> Option<String>;
pub(crate) fn aside_kind(node: roxmltree::Node) -> Option<AsideKindWire>;
```

Exact argument shapes for `emit_text_block` and `detect_caption` are chosen
by the Wave C agent and recorded in the module's doc comment.

**`structure.rs`**

```rust
pub(crate) fn index_footnotes(archive: &mut Archive, package: &Package, navigation: &Navigation,
    limits: &EpubLimits) -> Result<FootnoteIndex, EpubFailure>;                   // pass one
pub(crate) fn walk_spine(archive: &mut Archive, package: &Package, navigation: &Navigation,
    footnotes: &FootnoteIndex, limits: &EpubLimits, emitter: &mut Emitter,
    report: &mut StructureReport) -> WorkerResult<()>;                            // pass two: spec 7.1, 7.4, 7.5, 7.6, 7.11, then links::resolve_all
```

`walk_spine` owns the section stack, the current-section rule, page
ordinals, `appears_on` assignment, title/subtitle detection, and the
`document` unit. It calls `blocks.rs` for every container and text block and
collects `LinkRecord`s for `links::resolve_all` at the end.

## 8. Completion report

The final report to the user leads with:

```
Action required: edit config.toml, config.toml.local, config.toml.remote
  add [epub] with the six keys from config.example.toml
  remove [pdf], [docling], and the ten [parsing] keys listed in SPEC-epub.md Section 4
Then: data-store-service --config config.toml --setup-storage (fresh index_root) or --rebuild-all
```

followed by: phases completed with verification results; Section 14 findings
per sample; stale references left in `SPEC-projection-decouple.md`; the
deferred items from Section 2.
