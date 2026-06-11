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

## Current Status

Planning complete as of 2026-06-10. No code changes have been made.
Implementation awaits explicit user approval.

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
   provider-authoritative. vLLM endpoint-path and auth specifics are
   unverified until first live test; expect one fix iteration.
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
   reqwest so `https://` provider endpoints work later with zero further
   changes; plain `http://` to the private vLLM server is unaffected.
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

### 1. Config: backend selection and pool size

- `src/config.rs` (~164-169, 287-306): restructure `RerankerModelConfig`
  around the `backend` tag with per-backend required fields and cross-field
  validation; add `reranker_candidate_pool_size` to `RetrievalConfig` with
  serde default 10 and positive-value validation.
- `config.example.toml`: document both backend modes and the new retrieval
  value with comments.

### 2. Backend enum and HTTP client

- New `src/inference/reranker_backend.rs`: `RerankerBackend` enum;
  `HttpRerankerClient` holding the blocking client, endpoint, model name,
  timeout, and optional API key; request/response serde types; startup smoke
  check; scoring entry points mirroring the local API.
- Diagnostic boundaries per `DIAGNOSTICS-ONBOARDING.md`: request started,
  input ready (candidate count, document chars), completed (HTTP status,
  scores returned, elapsed ms), failed (status code, bounded response-body
  excerpt, elapsed ms). Endpoint and model are logged; the API key never is.

### 3. Local runtime type adjustments

- `src/inference/reranker.rs`: `RerankerCandidateScore.logit` and
  `.token_count` become `Option<f32>` / `Option<usize>`; the local runtime
  populates them as today. No behavior change to local scoring.

### 4. Runtime wiring

- `src/inference/mod.rs` (~19-20, 87-92, 105-112): `InferenceRuntime.reranker`
  becomes `RerankerBackend`; `initialize_with_progress` branches on the
  configured backend (local loads model artifacts as today; HTTP constructs
  the client and runs the HTTP smoke check); `health_details` passes through
  backend details including backend kind.

### 5. Search call-site changes

- `src/http.rs`:
  - `build_reranker_candidates` (~2652-2679) takes the effective pool size
    instead of `top_k`; caller (~1206) computes
    `max(reranker_candidate_pool_size, top_k)`.
  - Reranking stage (~1235-1320) acquires the model-call gate only for the
    `Local` backend.
  - Result materialization and raw diagnostics (~2698-2725) handle optional
    `logit`/`token_count` and emit the per-backend `mode` string.
  - Stage lifecycle logging keeps its existing shape; progress for the HTTP
    backend reports a single completion step.

### 6. Documentation updates

- `README.md`: reranker backend prerequisites and config description.
- `ARCHITECTURE.md`: model runtime section (reranker backend abstraction,
  no-fallback wording, gate scope), hard invariants addition: the configured
  reranker backend is exclusive; an unreachable HTTP backend fails readiness
  and search explicitly.
- `PROTOCOL.md`: raw reranker diagnostics fields documented as
  backend-dependent (`logit`/`token_count` optional, `mode` values).
- `SPEC-SERVER.md`: reranker stage and config sections updated to match.

## Out Of Scope

- Embedding or ColBERT backend migration (later phases).
- Evaluation harness, text-preparation fixes, FTS tokenizer changes.
- Any vLLM server-side setup or model deployment.
- Removal of the local Candle reranker implementation.
- Raising the default pool size or changing default `topK`.
- Multi-corpus support.

## Verification Plan

Static verification (no approval needed beyond this plan):

```bash
cargo fmt --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml --features metal
```

Manual runtime validation (requires user approval to start/stop the service
and a reachable vLLM rerank endpoint):

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

- Estimated development effort: ~90k tokens (range 70-120k), including doc
  updates and check iterations.
- First-try confidence: 65%. Compile-clean and internally correct ~80%; the
  discount reflects unverified vLLM rerank-endpoint specifics (exact path,
  auth header, response field naming). Expect one short fix iteration after
  the first live call against the inference server.

## Approval Reminder

Implementation must not begin until the user explicitly approves this plan.
Every later phase (embedding backend, ColBERT evaluation, text preparation,
mass re-embed) requires its own plan and separate approval.
