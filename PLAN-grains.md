# PLAN-GRAINS — Retrieval and annotation grains

Status: in progress (2026-09-14). This document is the contract for the
grain revision. Where it disagrees with README.md, ARCHITECTURE.md,
SPEC-SERVER.md, or PROTOCOL.md, this document wins until Phase 7 updates them.

## 1. Problem

Canonical units are the citation grain. With the EPUB parser a unit is one
paragraph, caption, heading, or table cell. Three subsystems embed or annotate
at unit grain: ColBERT matrices (`unit_multivector_projections`), annotation
excerpts (`src/annotations/producer.rs`), and annotation source windows and
cohorts (`src/projections/annotation.rs`). Fine chunks and section windows
pack units but never cross section boundaries, so a heading-only section is
embedded alone. Table cells are chunked, embedded, and annotated as prose.

## 2. Grain contract

Four grains. Each grain is a run of consecutive members of the grain below
it in reading order. Tokens are measured with the ColBERT tokenizer copy
already used by the chunker (truncation and padding disabled, special tokens
retained).

| Grain | Key | Cap | Members | Persisted | Consumers |
| --- | --- | --- | --- | --- | --- |
| Fine | `indexing.fine_max_tokens` | 256 | evidence units | `chunk_projections` | fine dense vectors, lexical index |
| ColBERT | `indexing.colbert_max_tokens` | 512 | fine chunks | `colbert_windows` | MaxSim |
| Context | `indexing.context_max_tokens` | 768 | fine chunks | section-dense artifact | context dense vectors, annotation source windows, annotation cohorts |
| Excerpt | `indexing.excerpt_windows` | 4 context windows | context windows | provenance only | annotation calls |

Minimum fill: `indexing.min_fill_ratio` (0.5); minimum = `cap * ratio`.
Runs pack greedily in reading order. When the next member would overflow the
cap and the open run is below the minimum, that member is split at a measured
boundary (sentence, then word, as the chunker does today) so the open run
reaches at least the minimum, and the remainder continues as the next
member. A member that alone exceeds the cap is split the same way. At
document end a final run below the minimum merges into the preceding run
when the combined run fits the cap; otherwise it stays, the one permitted
sub-minimum run. Fragments record every split range.

Splitting happens at the fine grain only. The ColBERT and context grains
never split a fine chunk; startup validation requires
`fine_max_tokens <= cap - floor(cap * min_fill_ratio)` for both higher caps,
which makes the split case unreachable there.

Reading order of fine chunks is `chunk_projections.chunk_index`, assigned by
the chunker, unique per parse. It is the only order authority for every
higher grain.

Runs never depend on section boundaries. Section identity is carried as
metadata (the section path of the run's first member) and used only for the
model-input prefix.

Model input versus canonical text: every grain has canonical text (member
texts joined by one blank line) and model input (the section path joined by
" / ", a blank line, then the canonical text). Canonical text feeds hashes,
provenance ranges, the annotation source matcher, and citations. Model input
feeds embedding and annotation calls. The prefix is never part of any range.

Membership records: every persisted run records `fragments`, an ordered list
of `{ unitId, startChar, endChar }` in Unicode scalar offsets over the unit's
evidence text, end exclusive. Byte offsets are not stored anywhere.

## 3. Evidence derivation

Applied once, at the fine chunker's input in `src/projections/chunk.rs`.
Every higher grain inherits it.

- Evidence-bearing units are `text_block`, `caption`, `table_cell`,
  `code_block`, as today.
- `text_block` with role `heading` is not evidence. It contributes to the
  section path only.
- Table cells are not chunked individually. All cells of one `table_row`
  form one member whose text is the cell texts joined by a tab and whose
  fragments list every cell with its range within its own cell text. The
  member's unit ids are the cell ids.
- Everything else is one member per unit.

The remaining per-unit evidence readers are `assembly::evidence::evidence_text`
(citations and raw evidence) and `projections::view` (derived view). The
readers in `projections::multivector` and `annotations::producer` are removed
because those subsystems consume grains, not units.

## 4. Configuration

`[indexing]` becomes exactly:

```toml
[indexing]
# ColBERT-token cap for fine chunks (paragraph grain): fine dense vectors and lexical index.
fine_max_tokens = 256
# ColBERT-token cap for ColBERT windows; must equal the ColBERT model's document limit.
colbert_max_tokens = 512
# ColBERT-token cap for context windows (page grain): context dense vectors and annotation source windows.
context_max_tokens = 768
# Consecutive context windows forming one annotation excerpt.
excerpt_windows = 4
# Minimum fill of any run as a fraction of its cap; shorter runs merge with a neighbor.
min_fill_ratio = 0.5
```

Removed keys: `indexing.min_search_unit_chars`, `indexing.chunk_max_tokens`,
`indexing.annotation_window_max_tokens`, `indexing.section_max_tokens`,
`models.annotator.max_input_chars`. Startup validates: `colbert_max_tokens`
equals the ColBERT tokenizer document limit; `context_max_tokens` plus the
longest section path prefix budget (256 tokens) fits `models.dense.max_tokens`
and `models.reranker.max_tokens`; `min_fill_ratio` in (0, 1); `excerpt_windows`
positive. The annotation intermediate batch bound (`chains::bounded_batches`)
is the excerpt's character count.

Agents edit `config.example.toml` only. The operator applies the same key
changes to `config.toml`.

## 5. Phases

Each phase: implementation agents in order, then one format agent
(`cargo fmt`, `cargo check`, `cargo clippy`), then one verifier, then at most
one fix agent. Every agent reports files changed, Cargo results, residual
risk, open escalations. An agent reads only the files in its brief and files
the compiler names, plus this document.

### Phase 1 — Configuration and grain contract

Files: `config.example.toml`, `src/limits.rs`, `src/config.rs`,
`src/identity.rs`, `src/main.rs` (startup validation site), and every
compile site that read a removed key (`src/projections/chunk.rs`,
`src/projections/section_dense.rs`, `src/projections/annotation.rs`,
`src/annotations/*.rs`). Compile sites keep behavior by reading the
replacement key: `chunk_max_tokens` → `fine_max_tokens`, `section_max_tokens`
→ `context_max_tokens`, `annotation_window_max_tokens` → `colbert_max_tokens`,
`min_search_unit_chars` → removed (the chunker's character drop rule is
deleted; the minimum-fill rule arrives in Phase 2), `max_input_chars` → a
derived value computed as `context_max_tokens * excerpt_windows * 8`
characters until Phase 5 replaces it.

### Phase 2 — Fine chunker and evidence derivation

Files: `src/projections/chunk.rs`, `sql/fabric/schema.sql`
(`chunk_projections` gains `fragments_json TEXT NOT NULL` and
`section_path_json TEXT NOT NULL`), `src/projections/multivector.rs` and
`src/annotations/producer.rs` (reader removal only where already unused;
otherwise left for later phases), `src/projections/lexical.rs` and
`src/projections/dense.rs` (model input uses the prefix; lexical indexes
canonical text). Section 3 derivation and Section 2 minimum-fill rule.
Chunk ids and `input_unit_ids` remain; `fragments` are added.

### Phase 4 — Context windows

Files: `src/projections/section_dense.rs`, `src/snapshot/verify.rs` (reads
the artifact), plus the `chunk_index` column added to `chunk_projections`
(schema, hot-plane contract, chunker insert, chunk readers, snapshot
comparison). Windows are runs of fine chunks per Section 2, built from
`chunk_projections` rows in the same transaction. Fragments in scalar
offsets. `section_id`/`section_path` from the first member. Grouping by
section is removed.

### Phase 5 — Annotation excerpts and attribution

Files: `src/annotations/producer.rs`, `src/annotations/chains.rs`,
`src/annotations/stages.rs`, `src/annotations/worker.rs`,
`src/annotations/memo.rs`, `src/annotations/excerpt.rs` (deleted by the
orchestrator), `src/annotations/progress.rs` if it names excerpts.
The planner reads the active parse's context windows in order and groups
runs of `excerpt_windows`; one `InvocationKind::Excerpt { index }` for all
three producers; targets are the excerpt's fragments. `invoke` passes the
canonical text and the section path as a separate prompt field.
Attribution: entity rows keep the fragments whose text matches the name via
`source_text_matches`; relation rows keep fragments matching any evidence
quote; summaries keep all; no match keeps all. `producer_version` = "3".

### Phase 6 — Annotation cohorts by context window

Files: `src/projections/annotation.rs`, `src/projections/worker.rs`,
`src/query/annotation.rs`. Cohorts keyed by context window id; the source
representation is the window's canonical text; the validator checks that
the annotation's attributed fragments intersect the window's fragments;
exact-window query matches key by window and map to ColBERT windows through
shared fine-chunk membership.

### Phase 3a — ColBERT windows: storage and build

Files: `sql/fabric/schema.sql` (`colbert_windows` replaces
`unit_multivector_projections`: `id, projection_id, source_id, parse_id,
chunk_ids_json, fragments_json, token_count, dimension, matrix_blob,
created_at`), `src/projections/multivector.rs`, `src/projections/mod.rs`.
The loader returns window ids with member chunk ids. A temporary adapter
keeps `run_maxsim_stage` compiling by exposing each window under its first
fragment's unit id; it is marked `// removed in Phase 3b`.

### Phase 3b — ColBERT windows: query keying

Files: `src/query/rerank.rs`, `src/query/channels.rs`,
`src/query/passages.rs`, `src/query/execute.rs`, `src/query/annotation.rs`,
`PROTOCOL.md` (`maxsim` diagnostics). Adapter removed. MaxSim keys are
window ids; graph and annotation hits map to windows through chunk
membership; passages build from window fragments.

### Phase 3c — ColBERT windows: forensics

Files: `src/snapshot.rs`, `src/snapshot/verify.rs`, `src/restore.rs`,
`src/deletion.rs`, `src/state.rs`, `src/monitoring_storage.rs`. Plane
descriptor, archive, verification, restore, deletion order, and counters
for `colbert_windows`.

### Phase 7 — Documentation

Files: `README.md`, `ARCHITECTURE.md`, `PROTOCOL.md`, `SPEC-SERVER.md`,
`DIAGNOSTICS.md`, `INSTALL.md`, `QUICKSTART.md`. No configured value
appears in prose; settings are described and `config.example.toml` is the
reference. The four grains replace unit-grain descriptions.

### Phase 8 — Verification

Cargo checks; then, run by the operator: fresh index root,
`--setup-storage`, ingest, inspection of run sizes, transcript excerpt
sizes, vocabulary attribution, query diagnostics, snapshot/restore round trip.

## 6. Status

Next: Phase 8 (operator-run verification). Phases 1, 2, 4, 5, 6, 3a, 3b, 3c, 7 complete and verified; the tree formats and passes `cargo check` and `cargo clippy`.

Open items are recorded in PLAN-fix-stupid-things.md.