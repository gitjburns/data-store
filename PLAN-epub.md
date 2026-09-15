# PLAN-EPUB — Implementation plan for SPEC-epub.md

Read SPEC-epub.md first. This plan adds what the spec does not carry: session
decisions, verified repository facts, the orchestration model, the phase
scopes, and the EPUB worker module contract. Where this plan and the spec
disagree, the spec wins on content; this plan wins on process.

## 1. Context

SPEC-epub.md replaces the Docling and MuPDF PDF workers with an in-process
EPUB worker and revises the content model to v0.4. The existing fabric corpus
is disposable. The work spans several sessions. Each session runs one Workflow
covering one phase, or one or two waves of Phase 4, driven by an orchestrator
that delegates every source edit, Cargo check, and review to single-task
subagents. A session targets 250K tokens and never exceeds 500K, counting the
orchestrator and its agents together; onboarding costs about 100K of that.

## 2. Decisions not recorded in the spec

- **No data compatibility.** The corpus under the configured `index_root` is
  discarded. No reader tolerates pre-v0.4 bodies, locators, or relationships.
  Acceptance begins with `--setup-storage` on a fresh index root or
  `--rebuild-all`.
- **Configuration files.** Agents edit only `config.example.toml`. The
  operational `config.toml` is the user's. The three `[diagnostics]` keys
  `progress_log_chars`, `activity_process_name_chars`, and
  `activity_error_chars` were removed with the Docling activity sampler.
- **Cargo.toml.** Editing it directly is approved within the owning phase:
  Phase 1 removes `mupdf` and `fancy-regex`; Phase 4 adds `zip` (default
  features off, `deflate` only) and `roxmltree`. No other dependency change. The first `cargo check` after each of these edits
  rewrites `Cargo.lock`; that is the accepted consequence of the approved
  edit, not a dependency-management command.
- **Out of scope, listed in the completion report as candidates for the
  ingestion pipeline refactor:** a config-backed EPUB worker timeout; the
  activation dominance gate in `src/activation.rs` (untouched); stale PDF
  references in `SPEC-projection-decouple.md` (not edited).
- **Footnote resolution needs two passes.** Section 10.3 decides whether a
  link is a note reference by whether its target is a footnote block, and
  Section 8 removes note-reference text at extraction time. Targets may lie in
  later spine documents. The worker therefore walks the spine twice: pass one
  parses each document and records, per `(member, element id)`, whether the
  element is, or lies inside, a footnote block by Section 7.10 rules 1 and 2,
  or lies in a document for which any navigation node targeting it, or the
  navigation section current at its start, has kind `notes` (rule 3 at
  document granularity); pass two emits. Heading-derived `notes` subsections inside a document are
  not footnote containers for link classification. This is the one precision
  loss accepted by this plan.

## 3. Verified repository facts

Verified 2026-09-13 against the tree before any phase ran. Agents may rely
on these without re-deriving them; anything else is read at phase time. Line
numbers are valid only until Phase 1 edits the file; after that, locate by
the item name with `rg -n`.

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
  `append_candidate_relationship` 531, `append_warning` (removed in Phase 1
  as dead code; Wave A step 1 restores it, streaming to `warnings.jsonl`),
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
- Binaries and CLI verbs for operating the service, verified against README.md:
  server `data-store-service` with `--config`, `--setup-storage`; client
  `data-store` with `--config`, `--rebuild-all`, `--health`,
  `--health-details`, `--held-parses`, `--source <sourceId>`,
  `--unit <unitId>`, `--query <text...>`. Artifact blob path
  `<index_root>/fabric/artifacts/sha256/<2hex>/<hash>` verified against
  ARCHITECTURE.md §2.2.
- Not verified: which locator kind `src/parse/text_worker.rs` emits.
  `char_range` stays in the spec 2.3 union for it. Phase 2 agent (b) reads
  the worker and, if it emits a removed kind, escalates before editing.

## 4. Orchestration model

- **One phase per Workflow; one Workflow per session, or two when the
  Sequencing bullet allows (Phase 3 after Phase 2, Phase 5 after wave D).**
  The orchestrator
  authors the script, the user approves it, and the orchestrator launches it.
  Phase 4 is the exception: its waves (Section 6.4) may be split across
  sessions, at most two waves per Workflow. The orchestrator edits no source and
  reads no source. One exception: subagents may not delete files, so the
  orchestrator deletes the nine PDF-only files of Section 3 itself, under the
  user's explicit instruction, before the Phase 1 Workflow launches.
- **Agents read no onboarding documents.** An agent reads only the files in
  its brief's file list and files the compiler names. Everything else it
  needs is in the brief (Section 5), which the orchestrator writes from
  SPEC-epub.md and this plan. Implementation agents run `cargo check` and
  `cargo clippy` from the project root before reporting; they do not format.
  After the last implementation agent of each phase or wave, one format
  agent runs `cargo fmt`, `cargo check`, and `cargo clippy`, edits nothing
  else, and reports; the verifier runs after it. Agents run no other project
  code and no git.
- **One task per agent.** A task is one module, or one bounded edit set that
  names its files and the items to change. Section 6 gives the agent split
  and order for every phase.
- **One verifier per phase, or per wave in Phase 4.** After the
  implementation agents, one verifier
  agent checks, against the briefs: every file in each files-changed list is
  in the brief's file list or is a declared compile fix; every new or edited
  function has a purpose comment; no `unwrap`/`expect` outside the
  PRINCIPLES.md exceptions; no new `async`; no stub body remains in files the
  phase owns; behavior matches the quoted spec text. A fix agent runs once,
  only when the verifier reports findings. Findings still open after the fix
  go to the user in the completion report; there is no second round.
- **Stops.** The Workflow returns when an agent reports an escalation or a
  scope violation. The orchestrator brings the item to the user and resumes
  the same run from the point of stop. Config edits beyond
  `config.example.toml`, new dependencies beyond Section 2, and any behavior
  the spec does not state are always escalations.
- **Approvals known in advance are collected up front.** The session's
  opening message lists every approval the phase will need (script, file
  deletions, `Cargo.toml` and `config.example.toml` edits, network access
  for a dependency fetch, verification commands) and one "proceed" covers
  them all. Only unexpected items, the Stops above, interrupt a running
  phase. The status update at session end is included in that up-front
  approval.
- **Mechanical compile fixes outside the file list are allowed.** An agent
  may edit an unlisted file when the compiler requires it and the fix changes
  no behavior beyond the approved intent. Each such file is listed under
  files changed with the reason, and the phase's verifier checks that no
  behavior changed. Any other edit to an unlisted file is a scope violation.
- **Report shape**, mandatory for every agent: files changed with one line
  each; Cargo results (verbatim output on any failure or warning); residual
  risk; open escalations. Nothing else.
- **Sequencing.** Phases 1, 2, and 3 run in that order, one session each;
  Phase 3 may share a session with Phase 2 when budget allows. Phase 4 waves
  A through D run in order. Phase 5 runs after wave D. Phase 6 runs in its
  own session any time after Phase 3 and is last.
- **Within a phase, agents run sequentially in the Section 6 order** unless
  Section 6 says otherwise. Cargo checks must pass clean for the format
  agent of a phase or wave. An implementation agent whose check fails only
  because of items owned by a later or concurrently running agent lists
  those errors as expected in its report; that is not a failure.

## 5. Brief template

Each brief is a self-contained agent prompt in the Workflow script. It
contains, in this order:

1. Phase, wave, and task name.
2. The governing spec text, quoted verbatim from SPEC-epub.md, and the
   Section 7 contract for any module the task owns, quoted verbatim.
3. The Section 3 facts the task needs: file paths, line numbers, names.
4. The owned file list (the edit allowlist the verifier checks), the
   read-only file list (files the agent may read but not edit, such as
   `epub/mod.rs` for Wave B and C agents or `src/model/body.rs` for Phase 2
   consumers), and the steps.
5. The coding rules block, verbatim:
   - Every function carries a comment immediately before it stating purpose
     or key invariant, never restating the signature. Public items use doc
     comments. Comment non-obvious invariants, ownership, ordering, and
     error policy inline.
   - No `unwrap` or `expect` in runtime code; propagate `Result` with source
     context preserved. The only permitted uses are startup fail-fast,
     verification-only code, and a locally proven invariant with a comment
     stating the proof at the call site. No boxed dynamic errors, `clone`,
     `Arc`, or `allow` without a stated reason.
   - No new `async`. SQLite work stays synchronous. No runtime schema or
     data migration. No tests, test modules, or fixtures.
   - Typed structs with `deny_unknown_fields` for data this service owns.
     Shared enums and constants are defined once and imported.
   - Large SQL, prompts, tables, or regex sets live in named constants or
     dedicated files, not inside function bodies.
   - Log lifecycle boundaries and errors with local context; never log
     document text, secrets, or large payloads.
   - Run only `cargo check`, `cargo clippy`, `cargo fmt` when this brief
     says so, and read-only commands (`rg`, `sed -n`, `ls`, `cat`). Do not
     delete, move, or copy files, run git, start servers, or run project
     binaries. Read nothing outside the owned and read-only file lists
     except files the compiler names.
6. The report shape, the escalation rule, and the mechanical-compile-fix
   exception, all quoted from Section 4.

Briefs do not cite documents the agent cannot read; they carry the text.
Phase 6 briefs are the one exception: a documentation agent's file list
includes SPEC-epub.md in full.

## 6. Phases

### 6.1 Phase 1 — PDF decommissioning (spec 13.1)

Files deleted: the nine PDF-only files in Section 3. Files edited:
`Cargo.toml`, `src/parse/mod.rs`, `src/main.rs`, `src/scheduler.rs`,
`src/dry_run.rs`, `src/source.rs`, `src/acquisition.rs`, `src/config.rs`,
`src/parsing_limits.rs`, `src/limits.rs`, `src/identity.rs`, `src/error.rs`,
`src/restore.rs` (doc comment only), `src/monitoring_storage.rs`,
`src/parse/cleanup.rs`, `src/parse/bundle.rs` (step 6 only),
`config.example.toml`.

Steps:
0. Orchestrator, immediately before agent (a) launches: delete the nine
   files (Section 4 exception). Agent (a) removes their `mod` lines and the
   `mupdf` dependency in the same step, so the tree never has a `mod` line
   for a missing file or the auto-discovered
   `src/bin/pdf-extract-diagnostic.rs` without `mupdf`.
1. Remove the nine files' `mod` lines. Remove `mupdf` and `fancy-regex` from
   `Cargo.toml`. In `main.rs` remove the MuPDF `run_internal_command`
   dispatch, both `PdfParser::from_config` uses, and the `pdf` argument at
   the `start` and `run_scheduler` call sites; in `dry_run.rs` remove the
   `pdf` field of `DryRunInputs` and its use.
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
   `process_log_bytes`. With that key removed, bound them by a constant
   `PROCESS_LOG_CAPTURE_BYTES` in `bundle.rs` whose value is the
   `process_log_bytes` value `config.example.toml` carried before step 5,
   with a comment that in-process workers pass empty slices.
7. Agent (e) also runs `rg -n -i 'docling|mupdf|pdf_engine|pdfengine' src assets`
   and removes every remaining reference (spec 13.1 names startup, health,
   identity capture, and monitoring). Files this sweep names are in agent
   (e)'s owned list by this rule; each is reported with the reason.

Compiles with PlainText as the only route.

Agents, sequential: (a) `Cargo.toml`, `src/parse/mod.rs`, `src/main.rs`,
`src/dry_run.rs` (step 1, after step 0); (b) `config.rs`,
`parsing_limits.rs`, `limits.rs`, `identity.rs`, `error.rs`, `restore.rs`
comment (step 3); (c) `scheduler.rs`, `source.rs`, `acquisition.rs`,
`monitoring_storage.rs` (step 2); (d) `cleanup.rs` (step 4); (e) `bundle.rs`
and `config.example.toml` (steps 5, 6, and 7). Then the format agent, then
the verifier.

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
   `SectionKind`, `TextBlockRole`, `ListKind`, `AsideKind`, and
   `TableRowRole` are enums with wire names in `body.rs`, the single
   definition Phase 4 imports. `wire_name` matches and
   `content_type_body_matches` gain the new types.
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
   The removed relationship and role names also appear as string literals
   in SQL and in `conformance.rs` (`has_table_cell_descendant`), which the
   compiler will not flag: run
   `rg -n "physically_contains|logically_contains|follows|continues_on|derived_from|'header'|'footer'|image_region|normalizedText|normalized_text|page_bbox|pageNumbers|captionForUnitIds" src assets`
   and update every hit. `caption_pairing_rate` in `conformance.rs` is
   redefined over `caption_of` edges (spec 2.6).
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

Agents, sequential: (a) the six model files, `wire_name`, and
`content_type_body_matches` (step 1); (b) the five text extractors,
`text_projection_hash`, `text_worker.rs`, `cleanup.rs` (steps 2 and 5);
(c) `passages.rs`, `channels.rs`, `annotation.rs`, `section_dense.rs`,
`sections.rs` (steps 3, 4, and the server side of 7); (d) `conformance.rs`
and `parse.rs` report fields (step 6); (e) `src/bin/data-store.rs` and
`assets/web/app.js` (client side of 7, and 8). Then the format agent, then
the verifier. The tree
does not compile between (a) and (e), so if the budget runs short the
session stops at an agent boundary, Section 9 records the last completed
agent and the current compile errors, and the next session resumes with the
next agent.

### 6.3 Phase 3 — `[epub]` configuration (spec 4, 13.4)

Files: `src/config.rs`, a new `src/epub_limits.rs` mirroring
`parsing_limits.rs`, `src/limits.rs`, `src/identity.rs`,
`config.example.toml`.

Steps: `EpubLimits` with the six keys, `deny_unknown_fields`, positivity
checks in `RuntimeLimits::validate`, field `epub` on `ServiceConfig` and
`RuntimeLimits`, folded in `runtime_limits()`, captured in identity. The
`[epub]` section goes where `[pdf]` was, with one comment line per key
taken from spec Section 4. One implementation agent, then the format agent,
then the verifier.

### 6.4 Phase 4 — EPUB worker (spec 3, 5–12)

Files: `Cargo.toml`; new `src/parse/epub/` modules per Section 7 of this
plan; `src/parse/mod.rs`; `src/acquisition.rs`; `src/scheduler.rs`;
`src/monitoring_storage.rs`.

Waves, one agent per named module, agents within a wave concurrent because
their files are disjoint, each wave gated on the previous wave's
`cargo check`, and a format agent then a verifier after each wave. A Workflow covers one or two
waves; Section 9 records which wave is next.

- **Wave A (leaf modules), two steps.** Step 1, one agent: add the three
  dependencies to `Cargo.toml`, add `mod epub;` to `src/parse/mod.rs`,
  restore `BundleWriter::append_warning` in `src/parse/bundle.rs` (Phase 1
  removed it as dead code), write
  the `mod.rs` skeleton (constants, error types, `Emitter`, capability
  profile, config hash, and `run_epub_parse` written in full against the
  stub signatures: the spec 11.2 sequence, the 11.4 events, the two-pass
  walk, `report::write`, the 10.1 relationship flush, metrics, and
  `ParserResult`), and write every other Section 7 module as a compiling
  stub carrying its contracted signatures: each fallible body returns an
  `EpubFailure` at stage `Package`; each non-fallible body returns the
  type's empty, `false`, or `None` value; stub parameters are prefixed `_`;
  (the regex statics of the pre-amendment contract are gone). Because
  nothing calls into `src/parse/epub/` until Wave D, step 1 puts
  `#![allow(dead_code)] // removed in Wave D when the route is wired` at the
  top of `epub/mod.rs`; without it the mandatory clean `cargo clippy` cannot
  hold. The first `cargo check` after step 1 fetches the three crates and
  needs network access. Step 2, six concurrent agents, one each, replacing
  the stubs: `entities.rs`, `kinds.rs`, `archive.rs`, `xhtml.rs`, `text.rs`,
  `report.rs`. Each later wave's verifier confirms no stub body and no `_`
  parameter prefix remains in that wave's files.
- **Wave B:** `package.rs`, `navigation.rs`, `links.rs`.
- **Wave C:** `blocks.rs`, then `structure.rs` (sequential: `structure`
  depends on `blocks`).
- **Wave D (integration, one agent):** wire `ParseRoute::Epub`,
  `MIME_TYPE_EPUB` and the `.epub` arm in `acquisition.rs`, the profile,
  containment, and worker-call arms in `parse_chain_prefix`, the second MIME
  parameter in both unparseable-MIME sites, the dry-run path (no change
  expected beyond compile), and removal of the Wave A `allow(dead_code)`.
  Full Cargo checks.

Every module carries the Section 7 contract; agents may add private items
freely and must not change a contracted signature without escalation.

### 6.5 Phase 5 — Importer image archival (spec 2.7, 13.3)

Files: `src/parse/importer.rs`, `src/parse/bundle.rs` (reader side).

Steps: `read_bundle` lists `artifacts/` files with their bytes or paths under
the existing reader caps; the importer recomputes each file's SHA-256 and
fails the parse as a contract violation on mismatch; stores each via
`ArtifactStore::put_bytes`; inserts each into the canonical bundle manifest
with `artifact_type = "image"` keyed `artifacts/<hash>`. Bodies are not
rewritten. One implementation agent, then the format agent, then the
verifier. Runs after Phase 4
wave D, in the same or a later session.

### 6.6 Phase 6 — Documentation (spec 13.6)

Files, each read at this phase: `canonical_content_graph_retrieval_fabric_v_0_3.md`,
`SPEC-SERVER.md`, `PROTOCOL.md`, `ARCHITECTURE.md`, `README.md`,
`INSTALL.md`, `DIAGNOSTICS.md`, `SPEC-CLIENT.md`, `SPEC-web-ui.md`,
`QUICKSTART.md`. Three agents, split by file, concurrent, in one Workflow in
a session of their own any time after Phase 3; each brief's file list
includes SPEC-epub.md in full (Section 5 exception). Then one verifier; no
format agent, since no Rust changes.
Each edit describes the v0.4 system as the spec states it; no
history or migration prose. `config.example.toml` is owned by Phases 1
and 3. The canonical spec file keeps its `v_0_3` filename (agents cannot
move files); its title and revision summary say 0.4. The orchestrator sets
the SPEC-epub.md status line to "implemented" when Phase 6 completes.

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
impl EpubFailure { pub(crate) fn with_stage(self, stage: EpubStage) -> Self; } // helpers (archive, xhtml) return stage Archive; the caller that knows the boundary re-stages before propagating
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

`run_epub_parse` owns: `BundleWriter::create`, the Section 11.4 events except
`epub.document.mapped` (owned by `walk_spine`, which holds its fields), the
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
use crate::model::body::SectionKind;   // defined once in the model (Phase 2); no local copy
pub(crate) fn kind_from_semantic(value: &str) -> Option<SectionKind>;   // epub:type, data-type, landmark, guide
// No text-pattern items: structure only (spec 1.3).
```

**`archive.rs`**

```rust
pub(crate) struct Archive { /* zip::ZipArchive<File>, normalized name index, running total */ }
pub(crate) fn open(path: &Path, limits: &EpubLimits) -> Result<Archive, EpubFailure>; // member count, encryption, method
impl Archive {
    pub(crate) fn contains(&self, member: &str) -> bool;
    pub(crate) fn read(&mut self, member: &str) -> Result<Option<Vec<u8>>, EpubFailure>; // per-member cap; total cap counts each member once (spec 5.1)
    pub(crate) fn member_count(&self) -> usize;
    pub(crate) fn declared_total_bytes(&self) -> u64;   // sum of central-directory declared sizes; logged by epub.archive.opened, never used for caps
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
```

**`report.rs`**

```rust
pub(crate) struct StructureReport { /* serde Serialize; spec 11.3 fields */ }
impl StructureReport {
    pub(crate) fn new(package_version: &str, metadata: &DocumentBody) -> Self;
    // one recording method per 11.3 fact: manifest_counts, spine_item, nav_node, document_summary,
    // unresolved_link, caption_pairing, section_kind_rule, page_marker
    pub(crate) fn write(&self, raw_dir: &Path) -> Result<(), ApiError>;  // parser_raw/epub_structure.json
}
```

**`package.rs`**

```rust
pub(crate) struct ManifestItem { pub href: String /* normalized member */, pub media_type: String, pub properties: Vec<String> }
pub(crate) struct SpineItem { pub idref: String, pub href: String, pub linear: bool }
pub(crate) struct GuideReference { pub kind: String, pub href: String }   // OPF guide entry, spec 5.2
pub(crate) struct Package { pub href: String, pub version: String, pub metadata: DocumentBody,
    pub manifest: BTreeMap<String, ManifestItem>, pub spine: Vec<SpineItem>, pub toc_id: Option<String>,
    pub guide: Vec<GuideReference> }
pub(crate) fn check_mimetype(archive: &mut Archive, package_href: &str, emitter: &mut Emitter) -> Result<(), EpubFailure>; // called after read_package; warnings keyed by package_href
pub(crate) fn read_container(archive: &mut Archive, limits: &EpubLimits) -> Result<String, EpubFailure>;   // rootfile href; spec 6 rules apply to the container too
pub(crate) fn read_package(archive: &mut Archive, rootfile: &str, limits: &EpubLimits,
    emitter: &mut Emitter) -> Result<Package, EpubFailure>;
```

**`navigation.rs`**

```rust
pub(crate) enum NavigationSource { Nav, Ncx, None }   impl NavigationSource { pub(crate) fn wire_name(self) -> &'static str }
pub(crate) struct Target { pub member: String, pub fragment: Option<String> }
pub(crate) struct NavNode { pub label: String, pub href: Option<String> /* as written; None for a span entry */, pub target: Option<Target>, pub kind_hint: Option<SectionKind>,
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
pub(crate) struct LinkRecord { pub from_local_id: String, pub document: String, pub href: String,
    pub locator: Locator, pub target: Target, pub role_hint: LinkRole }        // document/href/locator feed spec 11.5 warnings and the 11.3 report
pub(crate) enum LinkRole { Footnote, CrossReference, IndexLocator }
pub(crate) struct UnitIndex { /* (member, id) -> unit local id, filled in pass two */ }
pub(crate) fn resolve_all(links: &[LinkRecord], units: &UnitIndex, emitter: &mut Emitter,
    report: &mut StructureReport) -> WorkerResult<()>;                                   // spec 10.3–10.4 edges and warnings
```

**`blocks.rs`**

```rust
// Three lifetimes, all distinct: 'a is the emitter's own borrow (limits held for
// the whole parse); 'd is the current content document's text and parsed tree,
// which is shorter-lived than the emitter; 'e is the borrow of the emitter and
// the other mutable state for one block call. `&'e mut Emitter<'a>` with
// `document: &'d str` compiles because 'a is never tied to 'd; sharing them
// would pin every document's text for the emitter's whole lifetime.
pub(crate) struct BlockContext<'e, 'a, 'd> { pub document: &'d str, pub section_kind: SectionKind,
    pub text: &'e TextContext<'d>, pub emitter: &'e mut Emitter<'a>, pub links: &'e mut Vec<LinkRecord>,
    pub units: &'e mut UnitIndex, pub report: &'e mut StructureReport }
// One block's source: a whole element, or a mixed-content run of child nodes
// under `parent` (spec 7.5), whose `node_range` becomes the locator's nodeRange.
pub(crate) enum BlockSource<'d, 'input> { Element(roxmltree::Node<'d, 'input>),
    Run { parent: roxmltree::Node<'d, 'input>, nodes: Vec<roxmltree::Node<'d, 'input>>, node_range: [u64; 2] } }
// Every caption lies inside its subject (spec 1.3), so no position field.
pub(crate) struct CaptionCandidate<'d, 'input> { pub node: Option<roxmltree::Node<'d, 'input>>,
    pub label: Option<String>, pub text: String, pub rule: u8 }                  // node is Some for Inside (Node is Copy); rule = spec 7.9 rule number, for the report
// Container emitters emit the container unit only and return what walk_spine
// needs to walk the children itself with that parent; they never walk children.
pub(crate) struct ListEmission<'d, 'input> { pub list_id: String,
    pub items: Vec<(String, Vec<(roxmltree::Node<'d, 'input>, TextBlockRole)>)> } // per list_item: its id and the child nodes to walk with their role (term/definition for dl)
pub(crate) struct AsideEmission<'d, 'input> { pub aside_id: String, pub children: Vec<roxmltree::Node<'d, 'input>>,
    pub pending_caption: Option<CaptionCandidate<'d, 'input>> }                  // pending_caption: spec 7.9 rule 3, applied by walk_spine to the first code_block emitted under this aside
pub(crate) fn page_marker(node: roxmltree::Node, labels: &BTreeMap<(String, Option<String>), String>,
    document: &str) -> Option<Option<String>>;                                    // spec 7.6 rule 1; Some(label)
pub(crate) fn dp_page_marker(pi: roxmltree::Node) -> Option<Option<String>>;    // spec 7.6 rule 2
pub(crate) fn emit_text_block(ctx: &mut BlockContext, source: &BlockSource, parent: &str, role: TextBlockRole)
    -> WorkerResult<Option<String>>;                                              // None when dropped as empty
pub(crate) struct TableEmission<'d, 'input> { pub table_id: String, pub caption_id: Option<String>,
    pub cells: Vec<(String, roxmltree::Node<'d, 'input>)> }                    // cell ids with their source nodes so walk_spine can assign appears_on; caption_id so it can chain precedes among siblings
pub(crate) fn emit_table<'d, 'input>(ctx: &mut BlockContext, node: roxmltree::Node<'d, 'input>, parent: &str)
    -> WorkerResult<TableEmission<'d, 'input>>;                                   // spec 7.8; owns rows, cells, their contains/precedes edges, and its <caption> internally
pub(crate) fn emit_list<'d, 'input>(ctx: &mut BlockContext, node: roxmltree::Node<'d, 'input>, parent: &str)
    -> WorkerResult<ListEmission<'d, 'input>>;                                    // ul, ol, dl: emits list and list_item units only
pub(crate) fn emit_figure(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str, package: &Package,
    archive: &mut Archive, caption: Option<&CaptionCandidate>) -> WorkerResult<Vec<String>>; // spec 7.9, one per image; caption fills FigureBody.caption before streaming
pub(crate) fn emit_aside<'d, 'input>(ctx: &mut BlockContext, node: roxmltree::Node<'d, 'input>, parent: &str)
    -> WorkerResult<AsideEmission<'d, 'input>>;                                   // emits the aside unit only
pub(crate) fn emit_code(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str,
    caption: Option<&CaptionCandidate>) -> WorkerResult<String>;                  // pre only; caption fills CodeBlockBody.title/label before streaming
pub(crate) fn split_heading(node: roxmltree::Node, text: &TextContext) -> (Option<String>, String); // spec 7.3: span.label -> label, remainder -> headingText; no text split; walk_spine calls this for TextSectionBody
pub(crate) fn emit_formula(ctx: &mut BlockContext, node: roxmltree::Node, parent: &str) -> WorkerResult<String>;
pub(crate) fn detect_caption<'d, 'input>(subject: roxmltree::Node<'d, 'input>, before: Option<&BlockSource<'d, 'input>>,
    after: Option<&BlockSource<'d, 'input>>, text: &TextContext) -> Option<CaptionCandidate<'d, 'input>>; // spec 7.9 rules 1–3 (figcaption/caption, heading inside figure, example-aside heading); before/after are never captions and are retained only for the lookahead contract
pub(crate) fn pair_caption(ctx: &mut BlockContext, subjects: &[String], caption: &CaptionCandidate, parent: &str) -> WorkerResult<String>; // emits the caption unit and caption_of/has_caption edges only; returns the caption id
pub(crate) fn is_footnote_block(node: roxmltree::Node, section_kind: SectionKind) -> bool;   // spec 7.10, semantic rules only
pub(crate) fn aside_kind(node: roxmltree::Node) -> Option<crate::model::body::AsideKind>;   // model enum, not a local copy
```

All signatures above are fixed; the Wave A stub carries them verbatim.

**`structure.rs`**

```rust
pub(crate) fn index_footnotes(archive: &mut Archive, package: &Package, navigation: &Navigation,
    limits: &EpubLimits) -> Result<FootnoteIndex, EpubFailure>;                   // pass one
pub(crate) fn walk_spine(archive: &mut Archive, package: &Package, navigation: &Navigation,
    footnotes: &FootnoteIndex, limits: &EpubLimits, emitter: &mut Emitter,
    report: &mut StructureReport) -> WorkerResult<()>;                            // pass two: spec 7.1, 7.4, 7.5, 7.6, 7.11, then links::resolve_all
```

`walk_spine` emits `epub.document.mapped` at each document boundary and
owns the section stack, the current-section rule, page
ordinals, `appears_on` assignment, title/subtitle detection, the one-block
lookahead of spec 11.2, the `document` unit, and every child walk: it calls
`blocks.rs` to emit each container or text block, then walks the returned
child nodes itself with that container as parent. It holds an
`AsideEmission::pending_caption` until the first `code_block` under that
aside, and collects `LinkRecord`s for `links::resolve_all` at the end.

## 8. Completion report

Every session ends with the agents' reports rolled up per Section 4's report
shape, open verifier findings, and the Section 9 update.

## 9. Status

Updated in place at the end of each session. The "Next" line names the
phase, wave, or agent the next session starts with and any state it must
know.

Next: nothing. Development is complete; the service runs on `config.toml`.
Section 3 line numbers are stale; locate by name. The Section 7 contract is
current. Known deviations: the EPUB worker has no wall-clock timeout
(canonical spec §12.1 rule 5; SPEC-epub.md Section 4 defers it);
`SPEC_VERSION` in `src/identity.rs` is `"0.4"`.

- Phase 1 — PDF decommissioning: complete. `PROCESS_LOG_CAPTURE_BYTES =
  65536`.
- Phase 2 — Content model v0.4: complete. `PLAIN_TEXT_PARSER_VERSION` and
  `CLEANUP_VERSION` were not bumped.
- Phase 3 — `[epub]` configuration: complete. `src/epub_limits.rs` is
  declared with `#[path]` beside `parsing_limits` in `src/limits.rs`.
- Phase 4 — EPUB worker: complete. Dependencies are `zip` and `roxmltree`.
- Phase 5 — Importer image archival: complete. The reader records artifact
  paths; the importer re-hashes, fails the parse on a name mismatch, stores
  via `put_bytes`, and manifests as `artifact_type = "image"`.
- Phase 6 — Documentation: complete. The canonical spec file keeps its
  `v_0_3` name with a 0.4 title.
