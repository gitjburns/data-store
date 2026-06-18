# Data Store Benchmarks — Phased Implementation Plan

Working document. Each phase is implemented in its own session and tracks its own
progress here. Update the `Status` and `Notes` of each phase as work proceeds;
this file is the source of truth for progress.

Status legend: `Not started` | `In progress` | `Done`.

## Goal

Make per-stage benchmarks **server-authoritative** for both `search` and
`ingest`, with the CLI client acting as a **pure renderer**. This fixes the
current bug where the client discards the server's measured stage latencies and
displays its own wall-clock estimates between streamed events instead.

## Background (current state — verified in code)

- The server already measures every stage latency for search (`execute_search`,
  `src/http.rs`) and ingest (`execute_ingest`, `src/http.rs:316-880`), but only
  writes them to the tracing log; the result payloads do not carry them
  (ingest summary at `http.rs:863-870`; search top-level only inside
  `raw.search.*`).
- The CLI builds its entire `Benchmarks:` output from client wall-clock between
  streamed stage events (`SearchBenchmarkTimer` / `IngestBenchmarkTimer`),
  ignoring the server's authoritative numbers. The only server-authoritative
  rows today are the nested retrieval substages
  (`retrieval_benchmark_breakdown`, `src/bin/data-store.rs:1912-1945`).

## Settled design decisions

1. The server computes one authoritative `OperationBenchmarks` object per
   operation and returns it in the result payload. The client renders it; the
   client measures nothing.
2. Shared typed contract in `src/types.rs`:
   - `OperationBenchmarks { stages: Vec<BenchmarkStage>, totalMs: u64 }`
   - `BenchmarkStage { stage: String, elapsedMs: u64, children: Vec<BenchmarkStage> }`
     (`children` empty for leaf stages; only `retrieving_candidates` has
     children).
3. `benchmarks` is **additive**. The existing lossless `raw` payload and all
   logs stay unchanged (Observability Is Lossless).
4. Drop `http_to_first_status` entirely (negligible; the only row that required
   client-side timing). With it gone, every row is server-authoritative.
5. `search_preparation` is **server-authoritative** (validation + admission +
   snapshot capture), not a client estimate.
6. `Total = server latencyMs` (whole-operation duration). Rows may not sum
   exactly to `Total`; the small unattributed remainder is shown honestly rather
   than hidden by forcing `Total = sum-of-rows`.

## Key verified facts that de-risk implementation

- Client response types are plain `#[derive(Deserialize)]` with **no
  `deny_unknown_fields`** (`src/bin/data-store.rs:372-394`). Adding `benchmarks`
  to a server response does NOT break the client; server-side phases can land
  before the client renders them. (The client already ignores the server's
  `latencyMs` field today.)
- Result payload serialization: `OperationEmitter::result<T: Serialize>` →
  `serde_json::to_value` (`src/http.rs:1687-1713`). Any field added to
  `SearchResponse` / `IngestResponse` flows into the result event automatically.
- The client deserializes the terminal payload into its own `SearchResponse` /
  `IngestResponse` at `src/bin/data-store.rs:1352`. Add a matching `benchmarks`
  field there to consume it.
- Retrieval substage latencies are produced into
  `storage_output.raw["retrieval"].{queryVectorValidationLatencyMs,
  denseLatencyMs, bm25LatencyMs, rrfFusionLatencyMs,
  candidateMaterializationLatencyMs, rawDiagnosticsLatencyMs}`
  (`src/storage.rs:2536, 2720-2724`). They are NOT typed fields on the returned
  struct. Build the `retrieving_candidates` children by reading these from
  `storage_output.raw` in `execute_search`. No `storage.rs` change required.
- `search_running` / `ingest_running` statuses are emitted in the dispatch
  wrapper (`src/http.rs:2526, 2536`) before `execute_*`. The search prep window
  equals `embedding_started − started` (`src/http.rs:936, 1011`).
- `result_assembly_started` already exists (its elapsed is logged at
  `src/http.rs:1414`). For the `retrieving_candidates` parent total, confirm the
  retrieval-stage Instant (agent-reported `retrieval_started` near
  `src/http.rs:1074`); if absent, wrap the storage retrieval call with an
  `Instant`.
- Client timing code to remove in cleanup (Phase 4):
  `SearchBenchmarkTimer` (590, 1727), `IngestBenchmarkTimer` (629, 1830),
  `SearchBenchmarkRole` / `SearchStageSpec` / `SEARCH_STAGE_SPECS` /
  `INGEST_STAGE_SPECS` (132-186), `ActiveBenchmarkStage` / `BenchmarkEntry` /
  `BenchmarkBreakdownEntry` / `BenchmarkReport` (601-634), label consts (28-29),
  `search_stage_spec` / `tracked_ingest_benchmark_stage` (1896-1909),
  `retrieval_benchmark_breakdown` (1912-1945), `OperationOutput.benchmarks` /
  `OperationStreamOutput.benchmarks` (580, 587), and the timer wiring in
  `read_operation_stream` (1368-1369, 1443-1470, 1481-1491). Keep
  `format_benchmark_duration` (2023; also used by BM25 diagnostics).

## Verification commands (run from `service/data-store/`)

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- Manual: the user starts the service (agent does not start/stop servers), then
  run `data-store --search "..." 3` and `data-store --ingest <file>` and inspect
  the `Benchmarks:` output.

## Process rule

Each phase is implemented in its own session and requires **explicit user
approval before any code change** (repo rule). Keep this file's `Status` and
`Notes` current as work proceeds.

---

## Phase 1 — Server: search benchmarks

- Status: Done
- Effort: ~8–12K tokens (estimate)
- Confidence: ~92% (estimate)
- Files: `src/types.rs`, `src/http.rs` (`execute_search`)

Steps:
1. Pre-read the `execute_search` retrieval / colbert / rerank / result-assembly
   regions to confirm the exact Instant variable names (`retrieval_started`,
   `result_assembly_started`, etc.). If a retrieval-stage Instant is missing,
   add one around the storage retrieval call.
2. `types.rs`: add `OperationBenchmarks` + `BenchmarkStage` (Serialize, camelCase
   field renames), and `benchmarks: OperationBenchmarks` on `SearchResponse`.
3. `http.rs` `execute_search`: compute `search_preparation`
   (`embedding_started − started`) and the `retrieving_candidates` parent total
   (stage Instant); store the already-measured `result_assembling`. Read the six
   substage values from `storage_output.raw["retrieval"]` for
   `retrieving_candidates.children`. Assemble `OperationBenchmarks` in order
   `[search_preparation, embedding_query, retrieving_candidates(+children),
   colbert_scoring, reranking, result_assembling]`, with `totalMs = latency_ms`.
   Set it on `SearchResponse`. Leave `raw` and all logs unchanged.

Verify: `cargo fmt`, `cargo check`, `cargo check --features metal`. The client is
unaffected (ignores the unknown field); the old CLI benchmarks still display.

Risk notes: low. Additive only; no client break (verified no
`deny_unknown_fields`); children sourced from the existing raw shape.

Notes:
- `types.rs`: added `OperationBenchmarks { stages, totalMs }` and
  `BenchmarkStage { stage, elapsedMs, children }` (camelCase; `children` always
  serialized, `[]` for leaves); added `benchmarks: OperationBenchmarks` to
  `SearchResponse` (before `raw`).
- `http.rs` `execute_search`: bound `result_assembling_latency_ms` (reused in the
  existing completion log, value-identical). Assembled `OperationBenchmarks` in
  order `[search_preparation (embedding_started − started), embedding_query,
  retrieving_candidates(+6 children), colbert_scoring, reranking,
  result_assembling]`, `totalMs = latency_ms`. Built before `raw` consumes
  `storage_output.raw` (json! moves the value). `raw` and all logs unchanged.
- Decision A — child stage names: `query_vector_validation, dense, bm25,
  rrf_fusion, candidate_materialization, raw_diagnostics`.
- Decision B — added `retrieval_substage_ms` helper returning
  `ApiError::StorageOperation` on a missing/non-numeric substage key (no silent
  default). Also added `leaf_benchmark_stage` helper to avoid repeated leaf
  literals.
- Verified: `cargo fmt`, `cargo check`, `cargo check --features metal` all clean,
  no warnings. Manual client-side check deferred (client renders `benchmarks`
  starting in Phase 2; no `deny_unknown_fields` so the new field is inert until
  then).

---

## Phase 2 — Client: render server benchmarks + search README

- Status: Done
- Effort: ~8–11K tokens (estimate)
- Confidence: ~92% (estimate)
- Files: `src/bin/data-store.rs`, `INSTALL.md` (search Benchmarks section; the
  benchmark CLI wording lives in `INSTALL.md`, not `README.md`)

Steps:
1. Add client mirror `OperationBenchmarks` / `BenchmarkStage`
   (`#[derive(Deserialize)]`) and a `benchmarks: Option<OperationBenchmarks>`
   field on the client `SearchResponse` (and `IngestResponse`, so Phase 3 needs
   no further client change).
2. Rewrite `render_operation_benchmarks` to render an `OperationBenchmarks`
   recursively (stages → children → `Total`). Update `render_search` /
   `render_ingest` and their call sites (1195 / 1204 / 1213) to pass
   `response.benchmarks`; stop passing the client-timed `output.benchmarks`.
3. INSTALL.md: update the search Benchmarks description — remove "client-side", drop
   `http_to_first_status`, and correct the total wording.

Verify: `cargo check` (+ `--features metal`); run a search and confirm
authoritative rows (including nested retrieval substages) and `Total`.
Dead-code warnings for the now-unused client timers are expected here and are
removed in Phase 4.

Risk notes: low. Renderer swap; old timer code remains but unused
(intentional, cleared in Phase 4).

Notes:
- `data-store.rs`: added `OperationBenchmarks`/`BenchmarkStage` deserialize
  mirrors; `benchmarks: Option<OperationBenchmarks>` on client `SearchResponse`
  and `IngestResponse` (`Option` so Phase 3 ingest needs no client change).
  Rewrote `render_operation_benchmarks` to recurse the server stage tree via a
  new `render_benchmark_stage(depth)` helper; removed the client
  `retrieval_breakdown` path from rendering. Call sites (ingest, search,
  search-full) stopped passing the client-timed benchmarks.
- `INSTALL.md`: search Benchmarks section is now server-authoritative; dropped
  `http_to_first_status` and "client-side"; clarified that rows may not sum to
  `Total`. (Doc target corrected from `README.md` to `INSTALL.md`, where the
  benchmark CLI wording actually lives.)
- Verified `cargo fmt`, `cargo check`, `cargo check --features metal` clean
  except the 4 intentional dead-code warnings (`OperationOutput.benchmarks`,
  `BenchmarkBreakdownEntry`, `BenchmarkReport`, `retrieval_benchmark_breakdown`)
  — all scheduled for Phase 4 deletion.
- Manual render confirmed by operator: server-authoritative rows with nested
  `retrieving_candidates` substages and a server `Total`; the small row-sum
  remainder (e.g. rows 13.880s vs `Total` 13.885s) is shown, not hidden.

---

## Phase 3 — Server: ingest benchmarks (incl. nested storage substages) + ingest INSTALL doc

- Status: Done
- Effort: ~15–20K tokens (estimate)
- Confidence: ~91% (estimate; the only non-mechanical risk is the `storage.rs`
  return-type change, which is compiler-guided with a single caller)
- Files: `src/types.rs` (`IngestResponse`), `src/storage.rs` (`ingest_document`),
  `src/http.rs` (`execute_ingest`), `INSTALL.md` (ingest Benchmarks section).
  No `src/bin/data-store.rs` change: the recursive `render_benchmark_stage`
  (`data-store.rs:2238-2246`) already renders nested children, and Phase 2 added
  `benchmarks: Option<OperationBenchmarks>` to the client `IngestResponse`.

Scope note: this is wider than the original Phase 3 framing ("values already
measured; `types.rs` + `http.rs` only"). The agreed design adds
**server-authoritative nested storage substages**, which requires new `Instant`s
inside `storage.rs::ingest_document` and a return-type change there. The change
is **purely additive measurement**: it does not reorder the durable
persist/commit/publish/cache-swap sequence and removes no logging (respects the
ARCHITECTURE same-transaction publish invariant and Observability-Is-Lossless).

Resolved design decision — final ingest stage tree. Only descriptive stages are
named; every constant-and-tiny boundary is folded into the honest remainder
(never hidden), exactly as search does:

```text
docling_converting        <- conversion_latency_ms        (http.rs:556)
unit_splitting            <- splitting_latency_ms          (http.rs:604)
dense_embedding           <- dense_embedding_latency_ms    (http.rs:686)
colbert_embedding         <- colbert_embedding_latency_ms  (http.rs:777)
storage_publishing        <- storage_latency_ms            (http.rs:834)
    vector_validation     <- NEW Instant   (storage.rs:1695-1741)
    document_persistence  <- NEW Instant   (storage.rs:1835-1972)
    cache_preparation     <- NEW Instant   (storage.rs:2000-2076)
    commit                <- NEW Instant   (storage.rs:2148-2172)
totalMs = latency_ms                        (http.rs:844)
```

Dropped as both invariable and immaterial (absorbed into the remainder):
`ingest_preparation` (in-memory validation/admission/readiness; no snapshot
capture, unlike search's `search_preparation`), `source_resolution` (filesystem
stat), `duplicate_check` (one small SQLite lookup), `active_version_write`
(single one-row UPSERT, no fsync before commit), `cache_swap` (in-memory move
assignment). `Total` may exceed the sum of rows; the remainder is shown honestly.

Steps:
1. `types.rs`: add `benchmarks: OperationBenchmarks` to `IngestResponse` (reuse
   the existing `OperationBenchmarks` / `BenchmarkStage` types from Phase 1).
2. `storage.rs` `ingest_document`: add 4 `Instant`s bounding `vector_validation`,
   `document_persistence`, `cache_preparation`, and `commit`; return a new
   `IngestStoragePhaseLatencies { vector_validation_ms, document_persistence_ms,
   cache_preparation_ms, commit_ms }` in place of `()`. Additive only — no
   reordering, no log changes.
3. `http.rs` `execute_ingest`: consume the returned struct at the `ingest_document`
   call (`http.rs:805`); assemble `OperationBenchmarks` with the 5 stages above
   (`storage_publishing` carrying the 4 children via `leaf_benchmark_stage`),
   `totalMs = latency_ms`. Set it on `IngestResponse`. Leave `raw` and all logs
   unchanged. No `duplicate_check` binding (dropped).
4. `INSTALL.md`: rewrite the ingest `Benchmarks:` section — server-authoritative,
   the new 5-stage tree, drop "client-side" and "sum of those rows," and add the
   server-total-with-honest-remainder wording (mirrors the search section).

Verify: `cargo fmt`, `cargo check`, `cargo check --features metal`; then the
operator runs `data-store --ingest <file>` and confirms the rendered tree
(nested `storage_publishing` substages) and the server `Total`.

Risk notes: low–moderate. The `storage.rs` signature change is the only
non-mechanical part and is compiler-guided (one caller, `http.rs:805`). Keep the
change additive: do not reorder or remove any existing persist/commit/publish
logging.

Notes:
- `storage.rs`: changed `ingest_document` return type `()` →
  `IngestStoragePhaseLatencies` (new `#[derive(Debug, Clone, Copy)]` struct with
  `vector_validation_ms`, `document_persistence_ms`, `cache_preparation_ms`,
  `commit_ms`). Decision: the struct lives in `storage.rs` (storage-layer return
  contract, not a serialized API type; it is folded into `OperationBenchmarks` at
  the `http.rs` boundary). Added 4 `Instant`s bounding the validation work, the
  document+unit persistence loop, the cache-lock+prepare region, and `tx.commit()`.
  Additive only: no reordering of persist/commit/publish/cache-swap, no log
  added/removed/moved. Success path returns the struct at the function's final
  expression; all error arms already returned `Err` and were untouched.
- `types.rs`: added `pub benchmarks: OperationBenchmarks` as the LAST field of
  `IngestResponse` (no `raw` field exists, so SearchResponse's "before raw"
  placement maps to "last").
- `http.rs` `execute_ingest`: bound the returned latencies (`Ok(latencies) =>
  latencies`; the match is now `;`-terminated). Assembled `OperationBenchmarks` in
  order `[docling_converting, unit_splitting, dense_embedding, colbert_embedding,
  storage_publishing(+4 children: vector_validation, document_persistence,
  cache_preparation, commit)]`, `totalMs = latency_ms`, via the existing
  `leaf_benchmark_stage` helper; set `benchmarks` on `IngestResponse`. `raw` and
  all logs unchanged. `source_resolution`/prep/duplicate-check/active-row-write/
  cache-swap intentionally fall into the honest `Total − sum(rows)` remainder.
- `data-store.rs`: no change (verified — client already deserializes and
  recursively renders `benchmarks`; the legacy client-side ingest timer remains
  dead-but-present and is Phase 4's cleanup).
- `INSTALL.md`: rewrote the ingest `Benchmarks:` paragraph to be
  server-authoritative with the nested `storage_publishing` substages and
  honest-remainder wording, mirroring the search section.
- Pre-edit verification: confirmed real line numbers against source — stage
  latency vars (`conversion_latency_ms` 556, `splitting_latency_ms` 604,
  `dense_embedding_latency_ms` 686, `colbert_embedding_latency_ms` 777,
  `storage_latency_ms` 834, `latency_ms` 844) and the single `ingest_document`
  caller (`http.rs:805`) all exact.
- Verified: `cargo fmt` (reformatted `storage.rs` whitespace only), `cargo check`,
  and `cargo check --features metal` all clean except the 4 pre-existing dead
  client-timing warnings scheduled for Phase 4 deletion. Manual operator render
  check (`data-store --ingest <file>`) deferred to the user; the agent does not
  start/stop the service.

---

## Phase 4 — Client cleanup: delete dead client-timing code

- Status: Not started
- Effort: ~6–9K tokens (estimate)
- Confidence: ~95% (estimate; compiler-guided)
- Files: `src/bin/data-store.rs`

Steps: delete the timing structs / specs / consts / functions and wiring listed
under "Key verified facts" (timers, specs, roles,
`BenchmarkReport` / `BenchmarkEntry` / `BenchmarkBreakdownEntry`, label consts,
`retrieval_benchmark_breakdown`, `OperationOutput.benchmarks` /
`OperationStreamOutput.benchmarks`, `read_operation_stream` timer wiring, and the
`send_operation_output` benchmark plumbing). Keep `format_benchmark_duration`.

Verify: `cargo fmt`, `cargo check` (+ `--features metal`) clean with no
dead-code warnings; re-run search + ingest to confirm output is unchanged from
Phase 2 / Phase 3.

Risk notes: lowest. Pure deletion guided by the compiler.

Notes:
- (record findings / commit refs here)

---

## Summary

- Total estimated effort: ~37–52K tokens across 4 phases. Phase 3 expanded to
  ~15–20K to add server-authoritative nested storage substages; the others
  remain < 15K.
- Confidence per phase: > 90%.
- The end-to-end risk drivers (cross-phase runtime breakage; retrieval substage
  sourcing) were verified away before phasing, which is what keeps each phase
  small and high-confidence.

Sequencing rationale: server-first is safe because the client ignores unknown
fields, so Phase 1 ships authoritative search data with zero client impact;
Phase 2 switches the client to render it; Phase 3 does the same for ingest
(no client change needed); Phase 4 removes the now-dead client timing last, so
behavior is verified with real data before deletion.
