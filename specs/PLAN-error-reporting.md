# PLAN — Split runtime model-call errors out of `inference_init`

Status: design approved (Option 2, full split). Not scheduled; execute in a
dedicated session using the parallel-agent workflow in §4.

## 1. Problem and decision record

`ApiError::InferenceInit` (`src/error.rs:23-24`) is the single error variant
for the entire inference subsystem. Its `Display` hardcodes the prefix
`inference initialization failed:` and `error_kind()` returns
`"inference_init"` (`src/error.rs:149`). Runtime model-call failures — query
embedding, ColBERT MaxSim scoring, reranker scoring, HTTP model-backend
round-trips — funnel into the same variant as genuine startup failures, so a
query-time 500 claims the service failed to initialize. Observed live
2026-07-21: an HTTP dense body-read timeout during `POST /query` surfaced as
`error_kind="inference_init"` / "inference initialization failed".

Decision (user-approved): full split. Every inference-subsystem error site is
classified init-path or call-path; call-path sites move to a new variant.

Decided design points:

- New variant `ApiError::InferenceCall { message: String }` in `src/error.rs`:
  - `Display` is bare `{message}` (matching `StorageOperation` et al.; the
    messages already carry endpoint/model/phase context — no prefix).
  - `status_u16()` → 500.
  - `error_kind()` → `"inference_call"`.
- `PROTOCOL.md` error-kind table (§ "Error kinds and status codes", the row
  block at lines ~137-149) gains an additive row:
  `inference_call | 500 | Runtime model-call failure (embedding, scoring, or a
  remote model-backend round-trip).`
  `inference_init` keeps its row; its meaning narrows to true initialization.
- Existing per-module error funnels stay; each affected module gains a second
  funnel `inference_call_error(message: String) -> ApiError` building
  `InferenceCall`, and call-path sites switch to it. Init-path sites are
  untouched.

## 2. Classification rule (the shared input for all agents)

A construction site is **call-path** if it can fire per model invocation after
startup: live embed/score/forward passes, HTTP model-backend request
dispatch, response decoding/validation (dimension, finiteness, norm, index
order), runtime tokenization of query/passage/candidate text, and
projection-build embedding of corpus content.

A site is **init-path** if it fires only while constructing runtime state:
config/artifact validation, weight/tokenizer/device loading, key-file read
and permission checks, and the inference-slot accessors that report "not
initialized"/"failed to initialize".

Known classifications already settled during design:

- `src/state.rs:541,544` (inference slot accessor) — init-path, keep
  `InferenceInit`. The `src/state.rs:27` comment references the display
  prefix; update wording only if the prefix text changes (it does not).
- `src/inference/artifacts.rs` (all 5 sites), `src/inference/mod.rs`
  (`initialize_with_progress`, 2 sites), `src/inference/qwen3.rs`
  `load_qwen3_config` (2 direct sites), `dense_backend.rs`
  `load_with_progress` / `read_api_key` / `validate_api_key_file_permissions`
  — init-path.
- `dense_backend.rs` funnels `dense_http_error` (8 call sites) and
  `http_status_error` (2 call sites), and the direct sites in
  `send_request` / `send_request_once` / `single_vector` — call-path.
- `src/projections/multivector.rs:453` (build-time ColBERT embed wrap) —
  call-path.
- `src/bin/colbert-diagnostic.rs:56` — call-path. Note this bin
  `#[path]`-includes `error.rs` (see the `CutoverBarrierActive` comment in
  `src/error.rs`); it must compile against the new variant.

## 3. Open rulings — resolve with the user before Phase 2

1. **Smoke checks.** Startup smoke checks reuse call-path functions
   (`run_smoke_check` → `send_request`; colbert/reranker smoke paths
   similarly), so a smoke failure would report `inference_call` at startup.
   Accept, or pin smoke wrappers to `InferenceInit`?
2. **Mixed functions.** Any function the mapping phase classifies as serving
   both init and call paths (shared validation helpers are the likely case).
   The mapping output must list them; the user rules on each before
   implementation.

## 4. Parallel-agent workflow

Module files are disjoint, so implementation agents never contend for a file.
Subagents follow AGENTS.md in full: approved-scope edits only, patch hygiene,
function-comment rules, and the allowed verification commands
(`cargo check`, `cargo clippy`; `cargo fmt` within approved scope).

### Phase 0 — foundation (main agent, serial)

Add `InferenceCall` to `src/error.rs` (variant + `status_u16` arm +
`error_kind` arm) and the `PROTOCOL.md` row. Run `cargo fmt`, `cargo check`,
`cargo clippy`. This lands before fan-out so every agent compiles against the
variant.

### Phase 1 — mapping (parallel, read-only)

One agent per scope, each producing a structured map: every function
containing `inference_error(` or a direct `ApiError::InferenceInit`
construction, classified `init | call | mixed` with a one-line justification
per function.

| Agent scope | Inventory (2026-07-21 counts) |
| --- | --- |
| `src/inference/colbert.rs` | 179 `inference_error(` calls; funnel at :3332 |
| `src/inference/reranker.rs` | 111 `inference_error(` calls; funnel at :1824 |
| `src/inference/qwen3.rs` | 43 `inference_error(` calls; funnel at :580; 2 direct sites |
| `src/inference/dense.rs` | 7 `inference_error(` calls; funnel at :520; 5 direct sites |
| `src/inference/dense_backend.rs` | 14 direct sites incl. funnels `dense_http_error` (:777), `http_status_error` (:874) |
| small files: `src/state.rs`, `src/projections/multivector.rs`, `src/inference/mod.rs`, `src/inference/artifacts.rs`, `src/bin/colbert-diagnostic.rs` | §2 pre-classifications to confirm |

Line numbers are 2026-07-21 anchors; agents must re-locate by symbol, not
line.

### Gate — map review (main agent + user)

Main agent merges the maps, checks them against the §2 rule, and presents
`mixed` functions and any §3 rulings to the user. No Phase 2 until ruled.

### Phase 2 — implementation (parallel, one agent per module file)

Each agent, within its single file: add `inference_call_error` beside the
existing funnel (with the required function comment), switch call-path
functions' sites to it per the approved map, leave init-path sites untouched.
`dense_backend.rs` agent reroutes its two funnels and direct call-path sites
instead. Small-files agent applies the §2 classifications.

### Phase 3 — verification (serial)

1. Main agent: `cargo fmt`, `cargo check`, `cargo clippy` (fix
   change-introduced warnings).
2. Re-inventory and diff against the approved map:
   - `rg -n 'inference_error\(|inference_call_error\(' src/`
   - `rg -n 'ApiError::(InferenceInit|InferenceCall)' src/`
3. Verification subagent second pass: classification fidelity against the
   approved map, comment sufficiency, PRINCIPLES.md adherence (error context
   preservation), PROTOCOL.md row present.
4. Live check (requires user approval to run queries): induce or await a
   runtime model-call failure and confirm `error_kind="inference_call"` in
   the log and HTTP envelope; confirm startup logs still report
   `inference_init` for a forced init failure (e.g. temporarily invalid
   artifact path) — only if the user approves that manipulation.

## 5. Acceptance criteria

- Every call-path site (per approved map) constructs `InferenceCall`; every
  init-path site still constructs `InferenceInit`.
- `PROTOCOL.md` documents both kinds; no other contract rows changed.
- `cargo fmt` / `cargo check` / `cargo clippy` clean (no new warnings).
- `src/bin/colbert-diagnostic.rs` builds (it includes `error.rs` by path).
- No behavior change beyond the error kind/message labels: statuses stay 500,
  no retry/fallback/logging-shape changes ride along.
