# PLAN-FIX-STUPID-THINGS — Structural flaws left after PLAN-grains.md

Read PLAN-grains.md first. Each item below is a known flaw in the tree as
built, with the fix, its files, and the order to do them. Run PLAN-grains.md
Phase 8 on the sample corpus before starting; a runtime failure there
reorders this list.

## Order

1. Items 2 and 3 together (packer consolidation shrinks item 1's surface).
2. Item 1.
3. Item 4.

## 1. ColBERT is embedded twice for the same source text

Annotation cohorts (`src/projections/annotation.rs`) archive their own ColBERT
partitions of the context window's canonical text, duplicating the matrices in
`colbert_windows`. Semantic hits and hits consolidated into an annotation
window (`src/query/annotation.rs`) are scored against the archived partitions,
not through `colbert_windows`.

Fix: the cohort's source representation references the `colbert_windows` ids
that overlap its context window (shared fine-chunk membership) instead of
carrying matrices; exact-window scoring loads those windows through
`multivector::load_colbert_windows`; the archived validation in
`src/snapshot/verify.rs` checks the referenced window ids exist in the archived
`colbert_windows` rows; PROTOCOL.md `annotationMaxsim` describes window-keyed
scores.

Files: `src/projections/annotation.rs`, `src/query/annotation.rs`,
`src/snapshot/verify.rs`, `PROTOCOL.md`.

## 2. The annotation dry run samples nothing

`--annotation-dry-run` stops at the importer's ready boundary and never builds
context windows, so the Phase 5 planner returns an empty plan with
`annotator_plan.windows_unpublished` for every sampled source.

Fix: split window construction from embedding in
`src/projections/section_dense.rs` so `build_windows` needs only the ColBERT
tokenizer; the dry-run pass (`src/dry_run.rs`) builds fine chunks and context
windows without loading model weights and hands the windows to the planner.
The planner reads windows through the same reference/visitor as the worker.

Files: `src/projections/section_dense.rs`, `src/projections/chunk.rs`,
`src/dry_run.rs`, `src/main.rs` (dry-run tokenizer loading).

## 3. Two copies of the run packer

`section_dense::build_windows` and `multivector::pack_windows` implement the
PLAN-grains.md Section 2 rule for the context and ColBERT grains separately.

Fix: one `pub(crate)` packer in `src/projections/chunk.rs` over a member trait
(`token_count`, `text`, `fragments`, `section_path`) with the cap, minimum,
and no-split rule as parameters; both builders call it. Delete both copies.

Files: `src/projections/chunk.rs`, `src/projections/section_dense.rs`,
`src/projections/multivector.rs`.

## 4. One section-tree walk per member during chunking

`chunk::read_parse_members` calls `sections::read_section` once per member,
one ancestry walk of SQL round trips each, inside the projection transaction.

Fix: a bulk resolver in `src/sections.rs` that reads the parse's `contains`
edges and section bodies once and answers every member from memory; the
chunker calls it once per parse.

Files: `src/sections.rs`, `src/projections/chunk.rs`.

## Pre-existing, recorded elsewhere

The dense builder holds the projection writer lock across its HTTP fan-out
(`src/projections/dense.rs`); ARCHITECTURE.md §8 records the banked fix.

## Status

Next: nothing started. Run PLAN-grains.md Phase 8 first.
