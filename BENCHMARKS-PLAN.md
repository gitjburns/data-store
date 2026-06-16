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

- Status: Not started
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
- (record findings / commit refs here)

---

## Phase 2 — Client: render server benchmarks + search README

- Status: Not started
- Effort: ~8–11K tokens (estimate)
- Confidence: ~92% (estimate)
- Files: `src/bin/data-store.rs`, `README.md` (search Benchmarks section)

Steps:
1. Add client mirror `OperationBenchmarks` / `BenchmarkStage`
   (`#[derive(Deserialize)]`) and a `benchmarks: Option<OperationBenchmarks>`
   field on the client `SearchResponse` (and `IngestResponse`, so Phase 3 needs
   no further client change).
2. Rewrite `render_operation_benchmarks` to render an `OperationBenchmarks`
   recursively (stages → children → `Total`). Update `render_search` /
   `render_ingest` and their call sites (1195 / 1204 / 1213) to pass
   `response.benchmarks`; stop passing the client-timed `output.benchmarks`.
3. README: update the search Benchmarks description — remove "client-side", drop
   `http_to_first_status`, and correct the total wording.

Verify: `cargo check` (+ `--features metal`); run a search and confirm
authoritative rows (including nested retrieval substages) and `Total`.
Dead-code warnings for the now-unused client timers are expected here and are
removed in Phase 4.

Risk notes: low. Renderer swap; old timer code remains but unused
(intentional, cleared in Phase 4).

Notes:
- (record findings / commit refs here)

---

## Phase 3 — Server: ingest benchmarks + ingest README

- Status: Not started
- Effort: ~8–11K tokens (estimate)
- Confidence: ~92% (estimate)
- Files: `src/types.rs` (`IngestResponse`), `src/http.rs` (`execute_ingest`),
  `README.md` (ingest Benchmarks section)

Open decision (resolve at session start): which ingest stages to show.
Recommendation for consistency with search now showing `search_preparation`:
`[source_resolution, docling_converting, unit_splitting, dense_embedding,
colbert_embedding, storage_publishing]`, `totalMs = latency_ms`.
(`duplicate_check` is negligible; include or omit.)

Steps:
1. `types.rs`: add `benchmarks: OperationBenchmarks` to `IngestResponse`.
2. `http.rs` `execute_ingest`: assemble `OperationBenchmarks` from the existing
   `*_latency_ms` vars (`http.rs:399-844`), `totalMs = latency_ms` (844). Set it
   on `IngestResponse`. Leave logs and raw unchanged.
3. README: update the ingest Benchmarks description (server-authoritative; new
   stage list and total semantics).

Verify: `cargo check` (+ `--features metal`); run an ingest and confirm
authoritative rows and `Total` via the client (already rendering from Phase 2).

Risk notes: low. Mirrors Phase 1; the values are already measured.

Notes:
- (record findings / commit refs here)

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

- Total estimated effort: ~30–43K tokens across 4 phases (each < 15K).
- Confidence per phase: > 90%.
- The end-to-end risk drivers (cross-phase runtime breakage; retrieval substage
  sourcing) were verified away before phasing, which is what keeps each phase
  small and high-confidence.

Sequencing rationale: server-first is safe because the client ignores unknown
fields, so Phase 1 ships authoritative search data with zero client impact;
Phase 2 switches the client to render it; Phase 3 does the same for ingest
(no client change needed); Phase 4 removes the now-dead client timing last, so
behavior is verified with real data before deletion.
