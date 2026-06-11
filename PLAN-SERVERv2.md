# Server v2 Plan: Reranker Backend Abstraction And Remote Inference

## Purpose

This plan begins the Server v2 architecture direction: moving model inference
out of the in-process Candle/Metal runtime and onto a dedicated private
Linux/CUDA inference server (currently vLLM), with a config-selected backend
abstraction that also supports commercial provider endpoints later.

Phase 1 (this plan's implementation scope) covers the reranker stage only:

1. Introduce a reranker backend abstraction with two explicit, config-selected
   implementations: the existing local Candle ModernBERT runtime and a new
   HTTP client speaking the Cohere-compatible rerank contract (vLLM serves
   this contract; Cohere and Jina provider endpoints use the same shape).
2. Decouple the reranker candidate pool size from `topK` via a new config
   value, removing the hard `take(top_k)` cap that exists only because local
   sequential Metal scoring made larger pools too slow.

The reranker is the correct first migration because it is the only stateless
model stage: nothing persisted in SQLite depends on the reranker, so the
backend can be swapped freely without invalidating stored vectors.

## Background And Rationale

Findings from the search-quality assessment that motivate this plan:

- The local reranker scores candidates sequentially, one full ModernBERT
  forward pass per candidate, with a Metal-to-CPU logit readback per candidate
  (`src/inference/reranker.rs:421-430`, `1316-1323`). This made any pool above
  `topK = 10` unacceptably slow, so `build_reranker_candidates` caps the
  reranker input at `top_k` (`src/http.rs:2661`).
- With that cap, the strongest model in the pipeline can only reorder
  ColBERT's top-10; it cannot change which units are returned. Remote batched
  scoring on CUDA removes the latency reason for the cap.
- A backend abstraction with explicit selection (never fallback) matches the
  service's existing accelerator and storage doctrine and creates the seam
  later used for per-corpus embedding backends and provider switching.

Decisions recorded from planning discussion:

- One corpus per service instance. Multi-corpus support is out of scope.
- The platform expects incremental re-embedding when source documents change.
  Full-corpus re-embeds happen only as deliberate, rare migration events
  (model change), never implicitly at runtime.
- Embedding model selection per corpus, benchmark-then-mass-embed onboarding,
  ColBERT keep/drop evaluation, and text-preparation fixes (FTS tokenizer,
  heading inclusion, short-unit merging) are later phases tracked outside
  this plan.

## Note

The code does not need to work until all development phases are complete.

## Current Status

Planning complete as of 2026-06-10. The implementation scope was restructured
the same day into six phases, each estimated at no more than 20k tokens with
code-generation confidence of at least 90%.

Phase 1 (reranker candidate pool size knob) was implemented and verified on
2026-06-10: `reranker_candidate_pool_size` added to `RetrievalConfig` with
serde default 10 and positive-value validation, the search path computes the
effective pool as `max(configured, top_k)` with `build_reranker_candidates`
taking the pool size, and `config.example.toml` documents the new value. All
static checks (`cargo fmt`, `cargo check`, `cargo check --features metal`)
pass. Default behavior is unchanged for existing configs.

Phase 2 (optional reranker diagnostics types) was implemented and verified on
2026-06-10: `RerankerCandidateScore.logit` and `.token_count` are now
`Option<f32>` / `Option<usize>`, populated as `Some(...)` by the local runtime
with unchanged values. `RerankerSmoke.first_logit` is also optional; the
pre-smoke placeholder is `None` instead of a fabricated `0.0`, and health
details render a missing smoke logit as `absent`. Both raw-diagnostics JSON
sites in `src/http.rs` (`raw.reranker.scores` and `finalResults`) insert
`logit`/`tokenCount` fields only when present and never synthesize values;
`mode` remains the local constant. Local-backend JSON output and log values
are unchanged. All static checks (`cargo fmt`, `cargo check`,
`cargo check --features metal`) pass. Note: the plan's original `http.rs`
line references (~2698-2725) had drifted; the actual sites were ~1412-1420
and ~2735-2751.

Phase 3 (config backend selection) was implemented and verified on
2026-06-10 with one approved design deviation: the config-level discriminated
union was deferred to Phase 4. `RerankerModelConfig` remains a flat struct
with a required `backend` tag (new `RerankerBackendKind` enum, `local`/`http`)
and all backend-specific fields as `Option`: `path`/`max_tokens` (local) and
`endpoint`/`model`/`timeout_seconds`/`api_key_file_path` (http). A new
`validate_reranker_backend_fields` helper enforces per-backend required
fields at config load (absolute local path, positive values, `http://` or
`https://` endpoint prefix) and rejects the other backend's fields when
present. Accessors `local_path()`/`local_max_tokens()` return `Result` so
local-only consumers (`artifacts.rs`, `reranker.rs`) never unwrap raw
options; a `backend = "http"` config now parses and validates but fails
inference startup explicitly through those accessors until Phase 5 rewires
initialization. The `http.rs` reranking-start log field became `Option<u32>`
with unchanged local rendering. `config.example.toml` documents both modes;
existing configs must add `backend = "local"` under `[models.reranker]`. All
static checks (`cargo fmt`, `cargo check`, `cargo check --features metal`)
pass. Phase 4 introduces the discriminated union when wrapping the runtime
in `RerankerBackend`.

Phases 4-6 are not started. Each phase awaits its own explicit user approval
before implementation.

The service remains completely offline during all development phases. Phases
do not need to preserve runnable between-phase functionality; each phase must
end compile-clean per the Verification Plan.

## Design Decisions

1. **Enum dispatch.** `RerankerBackend` is an enum with `Local(RerankerRuntime)`
   and `Http(HttpRerankerClient)` variants exposing the same public scoring
   API. Two known variants, keeps the generic progress-closure signature, and
   matches the discriminated-union doctrine. No trait objects.
2. **Config tagged by `backend`.** `[models.reranker]` gains a required
   `backend` field:
   - `backend = "local"` requires the existing `path` and `max_tokens`.
   - `backend = "http"` requires `endpoint`, `model`, and `timeout_seconds`;
     optional `api_key_file_path` (owner-only file, same handling pattern as
     the admin token; read at startup; never logged).
   Cross-field validation happens at config load. Misconfiguration is a
   startup error. There is no fallback between backends.
3. **Cohere-compatible rerank contract.** The HTTP backend POSTs
   `{model, query, documents[], top_n = documents.len()}` to the configured
   endpoint and maps `results[{index, relevance_score}]` back to `unit_id` by
   index. The returned `relevance_score` is the public score,
   provider-authoritative. Live testing uses public provider rerank
   endpoints speaking this contract (e.g. Cohere, Jina). vLLM remains a
   required deployment target through the same contract; its endpoint-path
   and auth specifics are verified when that server is set up, which is out
   of scope for this plan.
4. **Diagnostics honesty.** The HTTP API returns no raw logit or token count.
   `RerankerCandidateScore.logit` and `.token_count` become `Option` values.
   Raw search diagnostics report `mode` per backend
   (`modernbert_sequence_classifier` for local, `http_rerank` for HTTP) and
   omit fields the backend cannot provide. No synthesized values (no inverse
   sigmoid). This is a documented raw-diagnostics shape change in
   `PROTOCOL.md`; the operation-stream event contract and public result
   fields are unchanged.
5. **Pool knob.** New `[retrieval].reranker_candidate_pool_size` with serde
   default `10`, preserving exact current behavior for existing configs. The
   effective pool is `max(configured, requested top_k)`, clamped to the
   available ColBERT-ranked candidates. Final results remain `take(top_k)` by
   reranker score. Raising the pool is a deliberate config edit made after
   the fast backend is live, not part of this change.
6. **Transport.** `reqwest::blocking::Client` (already a dependency, used by
   the CLI) called inside the existing `tokio::task::block_in_place` scope.
   No new async boundary. `Cargo.toml` adds the `rustls-tls` feature to
   reqwest because `https://` public provider endpoints are the live testing
   target; plain `http://` to the private vLLM server remains supported.
7. **Readiness.** The HTTP backend runs the existing smoke-check semantics at
   startup (same smoke query/documents, sent through the real endpoint).
   Smoke failure makes the inference component unready and fails startup
   explicitly. Health details report backend kind and endpoint, never the
   API key.
8. **Model-call gate.** The shared model-execution gate exists to serialize
   local Candle/Metal work. The HTTP backend performs no local accelerator
   work, so the reranking stage acquires the gate only when the backend is
   `Local`. Network scoring must not block ingest embedding or other local
   model stages.

## Implementation Plan

The work is divided into six phases. Each phase requires its own explicit
user approval before implementation, must end compile-clean per the
Verification Plan, and must stay within its estimated effort. Phases 1 and 2
are order-independent; phases 3, 4, and 5 are a sequential type-dependency
chain; phase 6 is last.

### Phase 1: Reranker candidate pool size knob

- `src/config.rs`: add `reranker_candidate_pool_size` to `RetrievalConfig`
  with serde default 10 and positive-value validation.
- `src/http.rs`: `build_reranker_candidates` (~2652-2679) takes the effective
  pool size instead of `top_k`; caller (~1206) computes
  `max(reranker_candidate_pool_size, top_k)`, clamped to the available
  ColBERT-ranked candidates. Final results remain `take(top_k)` by reranker
  score.
- `config.example.toml`: document the new retrieval value with comments.
- The serde default preserves exact current behavior for existing configs.

### Phase 2: Optional reranker diagnostics types

- `src/inference/reranker.rs`: `RerankerCandidateScore.logit` and
  `.token_count` become `Option<f32>` / `Option<usize>`; the local runtime
  populates them as today. No behavior change to local scoring.
- `src/http.rs`: result materialization and raw diagnostics (~2698-2725)
  handle the optional `logit`/`token_count`, omitting absent fields. The
  `mode` value remains the local constant in this phase.

### Phase 3: Config backend selection

- `src/config.rs` (~164-169, 287-306): restructure `RerankerModelConfig`
  around the required `backend` tag with per-backend required fields and
  cross-field validation at config load:
  - `backend = "local"` requires the existing `path` and `max_tokens`.
  - `backend = "http"` requires `endpoint`, `model`, and `timeout_seconds`;
    optional `api_key_file_path`.
- `config.example.toml`: document both backend modes with comments.
- The `http` variant has no runtime consumer until Phase 5. The service is
  offline during development, so no temporary runtime rejection guard is
  added.

### Phase 4: Backend enum with Local variant

- New `src/inference/reranker_backend.rs`: `RerankerBackend` enum with the
  `Local(RerankerRuntime)` variant wrapping the existing runtime and exposing
  the same public scoring API.
- `src/inference/mod.rs` (~19-20, 87-92, 105-112): `InferenceRuntime.reranker`
  becomes `RerankerBackend`; `initialize_with_progress` constructs the
  `Local` variant as today; `health_details` passes through backend details
  including backend kind.
- Call sites dispatch through the enum. Mechanical wrap; zero behavior
  change.

### Phase 5: HTTP reranker client

- `src/inference/reranker_backend.rs`: add the `Http(HttpRerankerClient)`
  variant; `HttpRerankerClient` holds the blocking reqwest client, endpoint,
  model name, timeout, and optional API key read at startup from
  `api_key_file_path`; request/response serde types for the
  Cohere-compatible contract; startup smoke check through the real endpoint;
  scoring entry points mirroring the local API.
- `src/inference/mod.rs`: `initialize_with_progress` branches on the
  configured backend (HTTP constructs the client and runs the HTTP smoke
  check).
- `src/http.rs`: reranking stage (~1235-1320) acquires the model-call gate
  only for the `Local` backend; raw diagnostics emit the per-backend `mode`
  string (`http_rerank` for HTTP); stage lifecycle logging keeps its existing
  shape; progress for the HTTP backend reports a single completion step.
- `Cargo.toml`: add the `rustls-tls` feature to reqwest.
- Diagnostic boundaries per `DIAGNOSTICS-ONBOARDING.md`: request started,
  input ready (candidate count, document chars), completed (HTTP status,
  scores returned, elapsed ms), failed (status code, bounded response-body
  excerpt, elapsed ms). Endpoint and model are logged; the API key never is.

### Phase 6: Documentation and live validation

- `README.md`: reranker backend prerequisites and config description.
- `ARCHITECTURE.md`: model runtime section (reranker backend abstraction,
  no-fallback wording, gate scope), hard invariants addition: the configured
  reranker backend is exclusive; an unreachable HTTP backend fails readiness
  and search explicitly.
- `PROTOCOL.md`: raw reranker diagnostics fields documented as
  backend-dependent (`logit`/`token_count` optional, `mode` values).
- `SPEC-SERVER.md`: reranker stage and config sections updated to match.
- Manual runtime validation per the Verification Plan.

## Out Of Scope

- Embedding or ColBERT backend migration (later phases).
- Evaluation harness, text-preparation fixes, FTS tokenizer changes.
- Any vLLM server-side setup, model deployment, or validation against the
  local vLLM engine. vLLM support through the same Cohere-compatible
  contract remains a requirement of the HTTP backend design.
- Removal of the local Candle reranker implementation.
- Raising the default pool size or changing default `topK`.
- Multi-corpus support.

## Verification Plan

Static verification at the end of every phase (no approval needed beyond the
phase approval):

```bash
cargo fmt --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml --features metal
```

Manual runtime validation happens in Phase 6 only; the service stays offline
during phases 1-5 (requires user approval to start/stop the service and a
reachable public rerank API endpoint; the configured `api_key_file_path` is
required for public providers):

1. `backend = "local"`: service starts, smoke passes, search behavior and
   logs unchanged, results identical to pre-change behavior.
2. `backend = "http"`: service startup smoke exercises the endpoint; CLI
   `search` succeeds; `logs/data-store.log` shows the new HTTP boundary
   events; reranking stage elapsed ms drops; raw diagnostics show
   `mode = "http_rerank"` without logit/token_count.
3. Failure paths: unreachable endpoint fails startup readiness explicitly;
   mid-operation endpoint failure emits a terminal search error with HTTP
   status context, never a silent fallback to local scoring.

## Estimate And Confidence

Per-phase estimates and first-try confidence for the generated code:

| Phase | Scope | Estimate | Confidence |
| --- | --- | --- | --- |
| 1 | Pool-size knob | ~10k tokens | 95% |
| 2 | Optional diagnostics types | ~12k tokens | 95% |
| 3 | Config backend selection | ~12k tokens | 92% |
| 4 | Backend enum, Local variant | ~15k tokens | 90% |
| 5 | HTTP reranker client | ~20k tokens | 90% |
| 6 | Documentation and live validation | ~15k tokens | 90% |

Total estimated development effort: ~84k tokens.

Confidence covers the success of the code generated in each phase, verified
by the static checks. It assumes a public Cohere-compatible rerank endpoint
is available for Phase 6 live validation; external-dependency failures
surfaced there are operational findings, not code-confidence factors.

## Approval Reminder

Implementation must not begin until the user explicitly approves this plan.
Every later phase (embedding backend, ColBERT evaluation, text preparation,
mass re-embed) requires its own plan and separate approval.
