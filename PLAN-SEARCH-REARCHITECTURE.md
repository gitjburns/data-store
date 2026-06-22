# Search Rearchitecture Plan

## Purpose

Search needs to be as close to instantaneous as possible while scaling well
beyond the current development corpus. The current search hot path should not be
treated as the target architecture. This plan defines a local-first retrieval
architecture that keeps stored data and metadata on the machine while allowing
remote model APIs when explicitly configured.

## Requirements

- Keep all persisted application data, source text, metadata, indexes, vectors,
  and search artifacts local.
- Remote model APIs may be used for inference when explicitly configured, with
  the understanding that request payloads sent to those APIs leave the machine
  transiently.
- Preserve immutable document-version semantics.
- Preserve explicit active-version publication and rollback behavior.
- Preserve one captured search-visible state for the full lifetime of a search
  request.
- Avoid Java-based search infrastructure.
- Keep runtime schema and data migrations out of normal service execution.
- Keep diagnostics sufficient to identify retrieval latency, index generation,
  publish, rollback, and search-stage failures.

## Architecture Direction

SQLite remains the control plane and durable administrative source of truth:

- document versions;
- active source-version pointers;
- ingest status and metadata;
- rollback metadata;
- administrative state needed to audit what was published.

SQLite should not remain the hot search data plane. Active search should read
from an immutable local search generation built from the active document set.

Each search generation should contain:

- a Tantivy lexical index for BM25-style retrieval;
- a LanceDB dense vector index/table for approximate nearest-neighbor retrieval;
- a local ColBERT document-token vector store keyed by unit ID;
- a local unit metadata/content store for materializing search results;
- generation metadata describing coverage, component paths, creation time, and
  validation state.

## Search Flow

1. Capture the active search generation at request admission.
2. Embed the query through the configured model path or remote model API.
3. Run Tantivy lexical retrieval and LanceDB dense retrieval against the same
   captured generation.
4. Fuse lexical and dense candidates with reciprocal rank fusion or another
   explicit fusion method.
5. Fetch ColBERT document-token vectors only for the bounded fused candidate
   pool.
6. Compute local ColBERT MaxSim scores over that bounded pool.
7. Run final reranking only over a tightly bounded result set, or make final
   reranking optional if latency targets require it.
8. Materialize results from local generation metadata/content and return
   diagnostics for each stage.

## ColBERT Scale Strategy

The first implementation should treat ColBERT as a bounded local reranker rather
than a corpus-wide retrieval engine.

This keeps ColBERT cost proportional to the fused candidate pool instead of the
whole corpus. Tantivy and LanceDB are responsible for high-recall first-stage
retrieval. ColBERT refines the combined candidate set.

The ColBERT storage boundary should be designed so a later true ColBERT
retrieval index remains possible. The first storage design should therefore
avoid tying token-vector access to SQLite blobs or another layout that makes
future segment-level or inverted-list indexing difficult.

Candidate storage options to evaluate:

- Lance/Arrow-style local columnar vector storage;
- memory-mapped segment files with compact offset metadata;
- another local segment format optimized for fetching token matrices by unit ID.

Quality risk:

- bounded ColBERT reranking cannot recover units missed by both Tantivy and
  LanceDB first-stage retrieval.

Mitigation:

- benchmark first-stage recall with representative queries;
- tune lexical, dense, and fused candidate caps explicitly;
- keep per-stage diagnostics so misses can be attributed to retrieval, fusion,
  ColBERT, or final reranking.

## Publish And Rollback Model

Index generation publishing must preserve the current service's visibility
semantics.

Ingest should build new search artifacts off to the side. A generation becomes
search-visible only after its Tantivy index, LanceDB table, ColBERT store,
metadata/content store, and coverage metadata have been written and validated.

Publishing must make these facts coherent:

- SQLite active-version state;
- active search generation pointer;
- dense/vector/lexical/ColBERT generation contents;
- durable diagnostics describing the publish boundary.

Rollback should publish a generation whose coverage matches the rolled-back
active version set. It must not mutate immutable document versions. It may select
an existing generation when available or build a new generation deliberately as
part of the rollback workflow.

Normal runtime must not create hidden migrations or repair incompatible search
artifacts silently. Missing or inconsistent active search generations should fail
clearly.

## Phase 1: Architecture Spike

Build a disposable local indexing spike before replacing runtime search.

Scope:

- Read existing SQLite unit metadata and vectors through an explicit tool or
  internal experimental binary.
- Build a disposable Tantivy lexical index.
- Build a disposable LanceDB dense vector table/index.
- Build or prototype a local ColBERT token-vector store.
- Run representative search queries through Tantivy, LanceDB, fusion, ColBERT
  fetch, and ColBERT scoring.
- Record per-stage timings and practical index sizes.

Non-goals:

- no runtime search replacement;
- no server startup or lifecycle changes;
- no runtime schema migration;
- no production generation publish path;
- no rollback changes.

Exit criteria:

- lexical retrieval timing is measured independently;
- dense ANN timing is measured independently;
- fused retrieval timing is measured independently;
- ColBERT fetch and MaxSim timing are measured independently;
- index build time and local disk footprint are measured;
- the results are sufficient to choose whether to proceed with this stack.

## Phase 2: Search Generation Abstraction

Introduce the generation model without changing public search behavior.

Scope:

- Define the search generation directory layout under the configured index root.
- Define generation metadata and validation rules.
- Define active generation pointer semantics.
- Define diagnostics for generation build, validation, publish, and load.
- Keep SQLite as the control-plane source of truth.

Approval gate:

- generation metadata format;
- active pointer storage;
- validation behavior;
- any config changes.

## Phase 3: Generation Build And Publish

Build production search generations during ingest and rollback workflows.

Scope:

- Build Tantivy, LanceDB, ColBERT, and metadata/content artifacts off to the
  side.
- Validate generation coverage against the intended active document versions.
- Publish the generation only after all required artifacts are complete.
- Keep failure behavior explicit and diagnosable.

Approval gate:

- publish ordering;
- rollback generation behavior;
- cleanup behavior for failed or superseded generations.

## Phase 4: Search Runtime Replacement

Route search to the active generation.

Scope:

- Capture the active generation at request admission.
- Replace SQLite FTS BM25 with Tantivy lexical retrieval.
- Replace exact dense scan with LanceDB ANN retrieval.
- Keep or revise RRF fusion deliberately.
- Fetch ColBERT token vectors from the local generation store.
- Preserve public result fields or explicitly version any changed contract.
- Update raw diagnostics to expose Tantivy, LanceDB, fusion, ColBERT fetch, and
  scoring timings.

Approval gate:

- public response changes;
- raw diagnostics changes;
- candidate cap defaults;
- final reranker behavior.

## Phase 5: Latency Controls

Make latency targets operationally enforceable.

Scope:

- Add explicit caps for lexical candidates, dense candidates, fused candidates,
  ColBERT candidates, and final reranker candidates.
- Decide whether final reranking is mandatory, optional, or mode-dependent.
- Add diagnostics that show when caps affect recall or result completeness.

Approval gate:

- config shape;
- default cap values;
- user-visible behavior when caps are reached.

## Phase 6: Rebuild Tooling

Provide deliberate tooling for existing data.

Scope:

- Add an explicit rebuild command or script for search generations.
- Validate rebuilt generation coverage against SQLite active versions.
- Refuse to publish incomplete or inconsistent generations.
- Keep rebuild outside normal runtime migration paths.

Approval gate:

- command surface;
- rebuild source of truth;
- cleanup and replacement behavior.

## Open Design Questions

- Which local ColBERT token-vector storage format best balances fetch latency,
  build complexity, disk footprint, and future true ColBERT retrieval support?
- Should final reranking remain required for every search, or should latency
  targets allow a fast mode that stops after ColBERT?
- Should Tantivy and LanceDB indexes include retained inactive versions, or
  should each generation contain only the currently active document versions?
- How should old generations be retained, garbage-collected, or reused for
  rollback?
- What recall and latency benchmarks are required before replacing runtime
  search?
