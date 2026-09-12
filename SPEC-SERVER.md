# SPEC-SERVER — Data Store Service Specification

This is the authoritative specification of the Data Store **server**: an
autonomous canonical-content-graph and retrieval-fabric service. It describes
server responsibilities, configuration, storage contracts, startup and
lifecycle, the administrative async-operation model, health (§1–§7), the
domain contracts of the autonomous subsystems — acquisition, sync, parsing,
activation, projections, annotations, query, forensics, events, errors
(§8–§17) — and commissioning and recorded deviations (§18–§19). It specifies
rules and invariants; wire detail defers to PROTOCOL.md.

Scope and companion documents:

- **PROTOCOL.md** — the exhaustive HTTP wire contract (request/response shapes,
  status codes, error bodies). This spec points there for wire detail and does
  not restate it.
- **SPEC-CLIENT.md** — the bundled `data-store` CLI client.
- **INSTALL.md** — installation, model/artifact placement, and `--setup-storage`.

Precision note: **the service code is authoritative.** Where any narrative below
would conflict with the code, the code wins. The service is designed to be
commissioned and first run deliberately (see §18); statements here describe the
service as built, not observed production behavior.

---

## 1. Server responsibilities

The service is a single standalone process that owns four responsibilities.

### 1.1 The autonomous acquisition→activation pipeline

An in-process **acquisition scheduler** thread runs continuously, on its own OS
thread (not the async runtime), and drives the full content lifecycle without
operator prompting:

1. **Detect** — a filesystem connector enumerates the corpus root and stages new
   or changed source bundles. Complete enumeration also evidences deletions
   (locations that no longer exist).
2. **Acquire** — staged bundles are imported, recording genuine acquisition
   provenance (a `SourceLocation` stamped with the connector's governance
   domain).
3. **Parse** — the drain dispatches a parse chain per queued item (the configured
   Docling or MuPDF engine for PDFs, then canonical unit/relationship construction).
   **A source has at most one active parse**; a fresh parse is built and gated
   against the current active parse.
4. **Build projections** — content-derived retrieval projections (chunk,
   lexical/FTS5, dense, ColBERT/multivector matrices, derived view) are built
   for the candidate parse. The summary and graph projections are **not** built
   here — they derive from semantic annotations and build post-activation
   (§12.1).
5. **Gate / activate** — `activation::gate_and_activate` evaluates the candidate
   against the active predecessor. The outcome is one of: **activate** (pointer
   swap to the new parse), **hold** (retain the candidate as a held parse with a
   `held_reason` for operator disposition), or a recorded parse failure. Cutover
   is serialized per source through a process-global `CutoverRegistry` barrier
   so activation and query-side rejection cannot interleave.

The scheduler runs on an **adaptive cadence**: the effective detection interval
(`cadence_ms`) is established after the first scan and adjusted per cycle. Each
cycle publishes health counters (§7).

### 1.2 Post-activation annotation worker

A separate **annotation worker** thread runs beside the scheduler. It is
discovery-based: each cycle it examines active sources and builds the MVP
semantic annotation types (entity, relation, summary) for source excerpts that lack them,
reusing memoized producer output (`annotation_memo`) on a memo-key hit and
retrying failed chains under the independent execution and malformed-output
budgets in §13. Only malformed outputs advance sampling temperature.
Single-goal stages call the external OpenAI-compatible chat-completions endpoint
(`[models.annotator]`) with thinking, structured output, and streaming diagnostics.

The annotation worker is **deliberately not readiness-critical.** Client-load
failures, such as an unreadable API-key file, **park** it for the run while the
process keeps serving; an unreachable endpoint degrades annotations visibly
through freshness rows, worker logs, and the diagnostic-only annotation health
slot. Invalid service configuration remains fatal at startup (§2).

### 1.3 HTTP transport shell

The Axum HTTP server uses a Tokio multi-thread runtime as a thin transport shell:
the query pipeline and all admin work run on blocking threads. The annotator
also uses async HTTP internally, on an owned single-worker runtime, so rebuild
and shutdown can cancel network waits. Its producer interface, worker lifecycle,
and SQLite work remain synchronous. The route surface is summarized in §6;
PROTOCOL.md is the exhaustive contract.

### 1.4 Snapshot / restore / deletion lifecycle

The service mints forensic snapshots at lifecycle boundaries (pre-activation,
post-activation, pre-deactivation) and on operator request, restores archived
parses, and propagates deletions/reappearances. The operator-facing surfaces are
the admin routes (§6); the autonomous callers are the scheduler and deletion
paths. Both the HTTP rollback-as-restore route and the autonomous reappearance
path drive the **same** shared completion function
(`deletion::restore_and_reactivate_source`), leaving one durable end state.

### 1.5 Health and admission

The service publishes readiness and per-component diagnostics (§7) and admits
searches through a fail-fast in-flight gate.

---

## 2. Configuration

Configuration is one TOML file, loaded and validated once at startup
(`ServiceConfig::load`). Relative paths resolve against the **config file's
parent directory** (canonicalized), never a compile-time or working-directory
base.

**Every config struct sets `#[serde(deny_unknown_fields)]`.** An unknown or
misspelled key anywhere in the file is a fatal startup error — stray
configuration is never silently ignored. Cross-field invariants that TOML cannot
express (positivity, absolute-path requirements, per-backend required/forbidden
field sets) are checked in `ServiceConfig::validate` and fail startup with an
`InvalidConfig` message naming the offending key.

**Config holds operational settings**, including annotation retry policy.
Retrieval and chunker tunables
that were once configuration (top-k, RRF constants, candidate-pool sizes,
chunker limits) have moved to **versioned, hashed policy documents** (the
RetrievalProfile and chunker configuration folded into their config hashes), not
this file. Config may hold the path to an active policy document, not its
values.

The sections below document each `[section]` and its supported keys. See
`config.example.toml` for a complete annotated example.

### 2.1 `[server]`

| Key | Meaning |
| --- | --- |
| `bind_address` | Socket address where the HTTP service binds (e.g. `127.0.0.1:8091`). |
| `startup_delay_seconds` | Required nonnegative integer seconds before ordinary corpus initialization on normal service startup. `0` disables the wait; supplied configurations use `10`. HTTP serves during the window (§4.2). |
| `max_request_body_bytes` | HTTP body limit applied before request JSON is accepted (over-limit → 413). Must be > 0. |
| `max_ingest_source_chars` | Maximum length of an ingest source reference after JSON parsing. Must be > 0. |
| `max_search_query_chars` | Maximum length of a search query after JSON parsing. Must be > 0. |

The three limits are deliberate, operator-visible request-shape protections, not
internal capacity guesses.

### 2.2 `[logging]`

| Key | Meaning |
| --- | --- |
| `file_path` | Service log file; relative resolves against the config directory. Non-empty. |
| `level` | Minimum event level: `trace`, `debug`, `info`, `warn`, or `error`. |

Bootstrap diagnostics print to stdout before file logging initializes;
operational logs switch to the configured file thereafter. Config and CLI
failures before that switch still surface on stdout/stderr.

Work contexts carry process, request/call, and canonical record IDs through
async and blocking boundaries, with known source paths and triggers. HTTP response
receipt is distinct from annotation validation and committed results. Provider
token usage and finish metadata are optional reported facts, never estimates.
Counters label their scope; elapsed, gate-wait, dispatch-wait, and persistence
times describe separate boundaries. `DIAGNOSTICS.md` defines the log contract.

Annotator calls log stage identity and terminal measurements; calls use complete
responses and emit no generation-progress entries. Answer and reasoning characters
are counted separately; token counts come from provider usage when supplied. Retry
logs identify both failure counters, their limits, eligibility delay, and next
action. Model payloads are not retained in these logs.

Existing annotation-related entries include
`annotation_progress="completed / total (percentage)"` for committed document
coverage. The separate `logs/annotator.log` transcript appends the same progress
immediately before `END CALL`, before persistence, so a wave may repeat counts and
its last entry may remain below 100%. Health and service-log commit entries
reflect subsequent commits. Unmeasured progress, including dry runs, is
`unavailable`; progress adds no log entries. DIAGNOSTICS.md defines both formats.

**Forbidden log data.** API keys, bearer/admin tokens, prompt text, model
outputs, document contents, and vector values never enter the service log.
Logs carry bounded diagnostics only — compact boundary facts such as character
counts, elapsed times, statuses, and truncated error excerpts. Enforcement
sites include the annotator client (prompt content and model output are
external-language payloads, logged only through shape and usage metadata), the HTTP reranker
(API key read and `Debug` output never expose the key), and the bearer-auth
guard (a failed token is never in the error or the log). The startup handoff
line carrying the admin token (§4.6) goes to the operator channel only; the
durable log records `token_present` without the value.

### 2.3 `[admin]`

| Key | Meaning |
| --- | --- |
| `token_file_path` | Runtime file where the service writes the current startup-scoped admin bearer token. Relative resolves against the config directory. Non-empty. |

### 2.4 `[client]`

The bundled CLI shares this single config file. The server **parses and
validates** this section (so `deny_unknown_fields` accepts the shared file) but
**never reads it at runtime** — it is client-owned.

| Key | Meaning |
| --- | --- |
| `operation_timeout_seconds` | Timeout for each CLI HTTP request, including each individual Operation poll. Must be > 0. |

The polling loop has no overall deadline; it continues until a terminal status
or request failure. The client-facing API uses JSON responses and polling;
internal annotator SSE streams use the separate model-call timeout (§2.9).

### 2.5 `[inference]`

| Key | Meaning |
| --- | --- |
| `device` | Accelerator backend for local models: `cuda` or `metal`. **Local inference has no CPU fallback.** |
| `device_index` | Device index passed to the selected backend. |

Both keys remain required. They are used only when at least one of dense,
ColBERT, or reranker selects `local`, which also requires the matching Cargo
feature. When all three select `http`, startup skips accelerator initialization;
tokenization and remote-ColBERT MaxSim run on the CPU.

### 2.6 `[storage]`

| Key | Meaning |
| --- | --- |
| `corpus_root` | Root directory for corpus-relative source references. Must be absolute. |
| `index_root` | Service-owned root for SQLite storage and generated conversion artifacts. Must be absolute. The fabric plane and artifact store derive from this (§3). |

### 2.7 `[connectors.filesystem]`

| Key | Meaning |
| --- | --- |
| `governance_domain` | Governance domain stamped on every `SourceLocation` the connector acquires (spec §6 reservation 3): an external governance fact assigned at acquisition, retaggable without re-parse or re-index. Non-empty. |

### 2.8 `[pdf]` and `[docling]`

`[pdf]` is required and selects one engine without fallback:

| Key | Meaning |
| --- | --- |
| `engine` | Exactly `docling` or `mupdf`. Applies to ingestion, explicit reparsing, and annotation dry runs. |
| `document_timeout_seconds` | Positive per-document child-process timeout for either engine. This key is no longer accepted under `[docling]`. |

`[docling]` is required when `engine = "docling"`; it may be omitted for MuPDF.
Whenever supplied, the section is fully validated, including required keys:

| Key | Meaning |
| --- | --- |
| `python_path` | Python executable recorded for the configured Docling environment (diagnostic; the service launches `docling_path` directly). Absolute. |
| `docling_path` | Docling executable launched for PDF→DoclingDocument JSON conversion. Absolute. |
| `pdf_backend` | One of `pypdfium2`, `docling_parse`, `dlparse_v1`, `dlparse_v2`, `dlparse_v4`. |
| `ocr_mode` | One of `auto`, `on`, `off`. |
| `device` | Docling's Python-side device: one of `auto`, `cpu`, `cuda`, `mps`, `xpu` (separate from `[inference].device`). |
| `num_threads` | Docling worker thread count. Must be > 0. |
| `page_batch_size` | Docling page batch size. Must be > 0. |

The shared timeout and Docling's `pdf_backend`, `ocr_mode`, `device`, `num_threads`,
and `page_batch_size` are parser-identity-bearing.
Moving the timeout to `[pdf]` preserves Docling's effective identity for equivalent
settings. MuPDF has a distinct identity covering extraction flags, candidate
mapping and cleanup versions, and the compiled dependency-lock hash. Engine,
timeout, or cleanup changes do not enqueue unchanged indexed sources; explicit
reparsing remains subject to the no-repeat guard (§10.6) and activation gate (§11).

### 2.9 `[models.dense]`, `[models.colbert]`, `[models.reranker]`, `[models.annotator]`

`[models.dense]` — backend-exclusive; **no fallback between backends.**
`backend` is `local` or `http`. Validation requires each backend's fields and
**forbids** the other backend's fields (misconfiguration fails at startup):

| Key | Required when | Meaning |
| --- | --- | --- |
| `backend` | always | `local` (in-process Candle Qwen3 embedding runtime) or `http` (OpenAI-compatible `/v1/embeddings` remote). |
| `dimension` | always | Expected dense vector width. > 0. HTTP responses are validated against it per call. |
| `pooling` | always | Pooling contract (e.g. `last_token`); validated by the local adapter, recorded as the served model's stated fact for `http`. Non-empty. |
| `path` | `local` | Local Qwen3 dense embedding model directory. Absolute. Forbidden for `http`. |
| `max_tokens` | `local` | Dense input token cap. > 0. Forbidden for `http`. |
| `endpoint` | `http` | OpenAI-compatible embeddings URL; must start `http://` or `https://`. Forbidden for `local`. |
| `model` | `http` | Model name sent in embeddings requests. Forbidden for `local`. |
| `timeout_seconds` | `http` | External embeddings request timeout. > 0. Forbidden for `local`. |
| `api_key_file_path` | optional (`http` only) | Owner-only file holding the bearer API key (e.g. `.data-store-dense-api-key`). Relative resolves against the config directory. |

`[models.colbert]` — backend-exclusive; **no fallback between backends.**

| Key | Required when | Meaning |
| --- | --- | --- |
| `backend` | always | `local` (Candle ColBERT-Zero) or `http` (vLLM token embeddings). |
| `dimension` | always | ColBERT-Zero token-vector width: `128`. |
| `query_max_tokens` | always | Query input token cap, from 1 through 518. |
| `document_max_tokens` | always | Document input token cap, from 1 through 518. |
| `path` | `local` | Absolute local model directory. Forbidden for `http`. |
| `endpoint` | `http` | Full HTTP(S) route ending in `/pooling`; no URL credentials, query, or fragment. Forbidden for `local`. |
| `model` | `http` | Non-empty served model name. Forbidden for `local`. |
| `tokenizer_file_path` | `http` | Absolute path to `tokenizer.json` matching the served checkpoint. Forbidden for `local`. |
| `timeout_seconds` | `http` | Positive request timeout. Forbidden for `local`. |
| `api_key_file_path` | optional (`http` only) | Owner-only bearer-key file; relative paths resolve against the config directory. |

The HTTP adapter sends locally formatted token IDs with `task = token_embed`.
It validates indexed token matrices and normalizes each row. Document matrices
are persisted; query-time HTTP inference embeds the query, then CPU MaxSim
scores stored document matrices. Startup verifies a document batch, query
embedding, and CPU scoring; failure prevents readiness. There is no implicit
HTTP retry. This backend loads a tokenizer but no local model weights; an
existing local ColBERT-Zero tokenizer can be reused when it matches the served
checkpoint. See INSTALL.md for the `lightonai/ColBERT-Zero` serving command.

`[models.reranker]` — backend-exclusive; **no fallback between backends.**
`backend` is `local` or `http`. Validation requires each backend's fields and
**forbids** the other backend's fields (misconfiguration fails at startup):

| Key | Required when | Meaning |
| --- | --- | --- |
| `backend` | always | `local` (in-process Candle ModernBERT) or `http` (Cohere-compatible remote). |
| `path` | `local` | Local reranker model directory. Absolute. Forbidden for `http`. |
| `max_tokens` | `local` | Local reranker token cap. > 0. Forbidden for `http`. |
| `endpoint` | `http` | Cohere-compatible rerank URL; must start `http://` or `https://`. Forbidden for `local`. |
| `model` | `http` | Model name sent in rerank requests. Forbidden for `local`. |
| `timeout_seconds` | `http` | External rerank request timeout. > 0. Forbidden for `local`. |
| `api_key_file_path` | optional (`http` only) | Owner-only file holding the rerank API key. |

`[models.annotator]` — the external OpenAI-compatible chat-completions endpoint
for the annotation producers. **Exclusive**: producer failures park annotations
as failed for later retry under §13; there is no fallback model or endpoint.

| Key | Meaning |
| --- | --- |
| `endpoint` | Full chat-completions route URL (not a base URL). Non-empty. |
| `model` | Model name sent in request bodies. Non-empty. |
| `timeout_seconds` | Whole-request timeout for each single-goal model call, including thinking. > 0. |
| `api_key_file_path` | Optional owner-only file holding the bearer API key. |
| `max_input_chars` | Source-excerpt cap in Unicode characters; oversized units split without dropping text. Prompts and prior-stage output are additional. > 0. |
| `annotation_max_retries` | Required nonnegative integer; malformed-output retries after the initial attempt. `0` disables this category's retries. |
| `annotation_retry_interval_seconds` | Required positive fixed interval for malformed-output retries; no backoff ceiling applies. |
| `execution_max_retries` | Required nonnegative integer; execution-failure retries after the initial attempt. `0` disables this category's retries. |
| `execution_retry_initial_delay_seconds` | Required positive initial execution-failure backoff. |
| `execution_retry_max_delay_seconds` | Required positive execution-failure ceiling, at least the initial delay. Independent of `MAX_BACKOFF_MS`. |

The retry settings are required without runtime defaults and enter application
configuration identity. Shipped values are listed in README's annotation settings
and `config.example.toml`. Counter, temperature, and scheduling semantics are in §13.

### 2.10 `[policies]`

Operator-editable **policy documents** are external facts the same way secret
files are: config holds their **paths only**, never their values (the D3 ruling
extended — retrieval knobs already moved to sealed code documents; these two are
operator-editable documents whose *identity* is content-hashed, not their values
inlined into config).

| Key | Meaning |
| --- | --- |
| `entity_match_file_path` | Path to the graph-entry entity-match ruleset (§14.3 fuzzy-match classes and caps). Required. Relative resolves against the config directory. |
| `annotator_naming_file_path` | Required path to the retained annotator naming-rules document. Loaded, validated, hashed, and versioned, but not applied to current annotation prompts (§13). Relative resolves against the config directory. |

Both documents are **strict TOML** (`deny_unknown_fields`), **loaded once at
startup**, and **fatal on invalid** — an unknown key or out-of-range value halts
startup with a config error naming the document. Each is **content-hashed over
its parsed canonical serialization**, so comment- and whitespace-only edits do
not change a document's identity. Both content hashes fold into
`ApplicationIdentity` (§15.3). Because config is startup-only, **editing a policy
document requires a service restart to take effect**. The entity-match document
ships with both fuzzy classes disabled. An empty naming-rule list is valid;
changing naming rules changes application identity, not producer memo identity.

System-assigned versioning of these documents is recorded in the append-only
`policy_versions` table with a `policy.changed` event per advance (§3, §16).

---

## 3. Storage and schema contract

Physical layout derives from `[storage].index_root`:

- **Fabric hot plane** — one SQLite database at
  `{index_root}/fabric/fabric.sqlite3`, with its DDL under `sql/fabric/`. It is
  the only durable store; there is no legacy plane in the end state.
- **Artifact store** — a content-addressed filesystem tree at
  `{index_root}/fabric/artifacts/sha256/<first-2-hex>/<full-hash>` (write-once,
  temp-file + atomic rename).
- **Event log** — the `system_events` table in the hot plane, written inside the
  owning operation's transaction.

**Connection and deadline policy** (all code constants, never config, per spec §35):

- `journal_mode = WAL` — set at setup and **validated fatally at startup**; never
  repaired at runtime.
- `synchronous = FULL` on write-capable connections (durability guarantee: a
  lost-but-served record is a breach).
- `busy_timeout` and per-statement deadlines are code constants (both 5000 ms).
- Read paths open `SQLITE_OPEN_READ_ONLY`; `foreign_keys = ON` per connection; a
  fresh connection per operation.
- The database carries its own `PRAGMA user_version` sequence **starting at 1**
  (current expected version: **1**). Startup validates the version and the table
  contract (a Rust-side mirror compared against `PRAGMA table_info`).

**Runtime never creates or migrates schema** (spec §1.3). Schema arrives only via
the explicit operator-run `--setup-storage` path (§4). A missing or invalid
fabric plane at startup is **not fatal** — the service serves with `ready=false`
and health explains why (§4, §7).

Fabric tables (from `sql/fabric/schema.sql`):

`source_objects`, `source_locations`, `acquisition_records`, `sync_queue`,
`parse_runs`, `content_units`, `unit_relationships`, `retrieval_projections`,
`query_execution_records`, `forensic_snapshots`, `operations`,
`semantic_annotations`, `annotation_memo`, `system_events`, `chunk_projections`,
`chunk_dense_vectors`, `unit_multivector_projections`, `graph_entity_mentions`,
`graph_entity_edges`, `policy_versions` (the append-only system-assigned
registry of operator policy-document content hashes, §2.10), plus the
`chunk_text_index` FTS5 virtual table (the
lexical index over chunk text). (`query_execution_records` exists as a reserved
seam; the QER audit tier that writes it is deferred — see §19.)

---

## 4. Startup and lifecycle

### 4.1 CLI options

| Flag | Effect |
| --- | --- |
| `--config <path>` | Config file path (defaults to `config.toml`). |
| `--setup-storage` | Create/validate the **fabric hot plane** schema, then exit. A single deliberate operator action; the only path that creates or validates schema. |
| `--foreground` | Keep the service attached to the terminal instead of daemonizing. |
| `--smoke-dense` | Run inference readiness smoke checks without binding HTTP, then exit. |
| `--annotation-dry-run <groups-per-source>` | Run the annotation dry-run mode (§4.7), sampling the first N excerpts per source per type. Positive integer; a missing or non-positive value is a fatal CLI error. Service binary only — the `data-store` client rejects it as an unknown argument, same as `--setup-storage`. |

An unknown argument is a fatal CLI error.

`--setup-storage` sets up the fabric plane only (the legacy plane was retired);
on success it prints the database path and exits. It builds at a temp path and
atomic-renames, so a crashed setup is recoverable.

### 4.2 Startup ordering and delay

On a normal start the listener binds before inference initialization, but HTTP
request handling begins after inference is ready. Ordering:

1. Bootstrap: load config, initialize file logging (bootstrap diagnostics to
   stdout).
2. Generate the admin token; enter the service process role (daemonize unless
   `--foreground`).
3. Bind the TCP listener. A bind failure is fatal.
4. **Publish the admin token file** (§4.3). Failure is fatal.
5. Initialize inference. Failure is fatal (and cleans up the token file).
6. Load and validate policy documents and capture application identity (§15.3).
   Construct shared state with ordinary storage admission held.
7. Serve HTTP and wait `server.startup_delay_seconds`. Health, Operation polling,
   rebuild-all, and shutdown remain available; other storage-dependent requests
   return `503`. Readiness stays false and health reports the startup delay.
8. When the delay expires, register policy versions and load caches through a
   blocking task. A successful rebuild already provides registered policies and
   empty caches, so startup skips those steps. Start the scheduler and annotation
   worker and release the startup hold; the scheduler performs staging cleanup
   after gaining storage admission. A missing/invalid fabric plane leaves
   readiness false with health diagnostics.
9. Continue serving until shutdown.

Rebuild-all ends the countdown immediately and clears storage. Successful
clearing allows startup to continue without any remaining delay; an active or
failed rebuild keeps storage paused. Shutdown cancels the wait. Startup output
announces the configured delay; the service log records
its start, completion or cancellation, and corpus initialization outcome. The
delay does not apply to `--setup-storage` or annotation dry-run mode.

### 4.3 Admin token handoff

The service generates a fresh **startup-scoped** admin bearer token each run and
publishes it to the configured `[admin].token_file_path` with **owner-only
(0600)** permissions. The token authorizes the protected routes (§6). The token
file is cleaned up when the current process's serve loop ends.

### 4.4 Readiness

Top-level readiness is the conjunction of exactly two components:
**`inference` AND `sync`**. Inference is true by construction once startup passes
its init step. `sync` is the scheduler's fabric-plane readiness, published into a
shared slot and starting `pending`. The diagnostic-only components (fabric,
annotation, search_admission — §7) are **excluded** from this conjunction; a
degraded diagnostic never makes a running service report unavailable.

### 4.5 Graceful shutdown

Shutdown is driven **only** by `POST /shutdown` → `AppState::request_shutdown`,
which signals a cross-thread latch and annotation cancellation. **There is no
OS-signal handling.** On the signal (or on a transport error in the serve loop)
the service requests shutdown. Annotation dispatch stops, outstanding HTTP waits
are cancelled, and unfinished results are discarded with uncommitted writes
rolled back. Local cancellation does not prove remote inference has stopped.
The service joins the scheduler and annotation worker, waits for any accepted
rebuild owner to reach a terminal boundary, cleans up the token file, and stops.

### 4.6 Daemonization handoff

Unless `--foreground` is given, the service daemonizes: the launcher-facing
parent process re-executes its own binary as a detached child (its own session
via `setsid`) and waits on a **startup handoff channel** — one half of a Unix
socket pair the child inherits (its fd published through
`DATA_STORE_BACKGROUND_STARTUP_FD`, with `FD_CLOEXEC` cleared so it survives
the exec). With `--foreground` the same protocol runs over stdout instead. The
child reports startup over that channel as:

- **Status lines** — one line per startup stage, echoed by the parent so the
  operator sees bind/init progress.
- **Transient progress lines** — prefixed `__data_store_progress__` so the
  parent can render them as overwritable terminal progress rather than
  permanent output.
- **The token line** — exactly one `admin_shutdown_token=<token>` line hands
  the startup-scoped admin token to the operator channel. The parent redacts
  this line when echoing anywhere durable; the service log records only
  `token_present`, never the value.

When startup completes, the child **closes the handoff channel**; the parent
observes EOF and exits, leaving the detached child serving. Startup failures
surface on the same channel before it closes, so the parent can exit non-zero
with the failure visible.

### 4.7 Annotation dry-run mode

`--annotation-dry-run <groups-per-source>` parses the corpus, samples bounded
excerpts, and serves the vocabulary route for inspection before full annotation
and embedding. The mode always runs foreground (it never daemonizes).

Startup sequence, in order:

1. **Policies load and register.** Both policy documents load (fatal on
   invalid, as always) and their versions register. Unlike a normal start —
   where a missing fabric plane degrades to `ready=false` — **a valid fabric
   plane is REQUIRED here and its absence is fatal**: the mode exists to write
   and read annotation rows, so there is nothing to do without a plane.
2. **Identity capture** (§15.3), policy hashes included.
3. **Admin token generated and published** (§4.3) — the vocabulary and
   shutdown routes are protected.
4. **Bind**, then serve a **reduced router**: exactly
   `GET /v1/health`, `GET /annotations/vocabulary`,
   `GET /operations/{operationId}`, and `POST /shutdown`. No other route
   exists in this mode.
5. **The dry-run pass runs on a blocking task while HTTP serves.**
   `POST /shutdown` ends the mode.

**No inference runtime, no scheduler thread, no annotation worker** is started.
Health reports the inference component not-ready with the mode message
`annotation dry-run mode: inference not initialized` — by design; `ready=false`
is the expected state of this mode.

The pass **truncates at import-READY**: parses run through acquisition, the selected
PDF engine or plain-text worker, and import (`parse_runs` reach `ready`). **No
projections are built, no gating or activation runs**, queue rows are left
`in_flight`, and acquisition bundles are retained on disk. The next **normal**
start with the same parser identity adopts this state through the §13.5
no-blind-retry guard's `GateExisting` arm — it rebuilds projections and gates the
existing ready runs without repeating extraction.

Sampling annotates with the **entity and relation** producers only, over the
**first N excerpts per source per type** in plan order (N is the CLI
argument); the **summary producer is excluded** — sampling exists to surface
naming and predicate vocabulary. The sampled annotations are ordinary
`semantic_annotations` rows on parses that are never activated, so vocabulary
inspection in this mode uses `scope=all`. A **failed pass keeps the process
serving** whatever annotations landed, so a partial sample is still
inspectable.

---

## 5. Async-operation administrative model

Every **mutating** admin route executes as an asynchronous **Operation**. The
route inserts a durable `operations` row and returns an Operation id,
and runs the work either **queue-coupled** (via the scheduler drain) or on a
**detached `spawn_blocking`** task. The task transitions the Operation to
`succeeded` on `Ok` or `failed` on `Err`/panic. Clients poll with
`GET /operations/{operationId}`. `POST /rebuild-all` drains current storage work
before inserting its Operation and returning acceptance (§5.1).

The Operation `status` set is closed: **`pending`** (on insert) →
**`running`** (once the worker starts it) → terminal **`succeeded`** or
**`failed`**. The `operationType` set is the spec §34.6 closed enumeration plus
**`parse_discard`** (held-parse discard, §16) and **`rebuild_all`** (§5.1).

**No NDJSON anywhere** (D2 ruling): there is no streamed progress on
administrative operations, and no streamed query transport. Administration is
Operation records + polling only.

**Operation-succeeded ≠ parse-outcome.** An Operation's `succeeded` status means
the **pipeline lifecycle completed** — the async work ran to a clean terminal
state. It does **not** encode the domain verdict of a parse. The domain outcome
(activated, held with a reason, or a recorded parse failure) lives in the
**parse run** (`parse_runs` / the held-parse surface). A polling client that
needs the domain verdict must read the parse run, not just the Operation status.

---

### 5.1 Rebuild all

`POST /rebuild-all` pauses storage admission and signals annotation cancellation
before draining. The annotation worker stops dispatching, cancels outstanding
HTTP waits, discards unfinished results, and rolls back uncommitted writes. It
retains its storage lease until producer threads and local writes have stopped;
clearing cannot race a late write. Admitted HTTP and detached admin work still
drain, and the ingestion scheduler parks at a cycle boundary.
New storage-dependent requests and overlapping rebuilds return `503`; health,
Operation polling, and shutdown remain available.
After draining, the service persists the pending rebuild Operation and returns
`202`; clearing continues on a detached blocking task. Client timeout or
disconnect does not cancel server work. Health and the service log preserve the
last known state when the client does not receive acceptance.

Cancellation consumes neither retry budget and is not a producer failure.
The cancellation watch resets after clearing and advancing the storage
generation. DIAGNOSTICS.md defines the cancellation and drain events; local
cancellation alone cannot establish whether the remote endpoint stopped inference.

The operation clears application tables and the lexical index, retaining the
schema and current rebuild Operation; deletes `fabric/artifacts/` and
`fabric/staging/` under `storage.index_root`; re-registers loaded policies; and
resets caches, worker bookkeeping, cadence, and health counts. Corpus files,
model files, configuration, and service logs remain intact. Normal ingestion
then rebuilds parses, projections, embeddings, and annotations without retained
producer output or memoization.

Success means storage was cleared and rebuild maintenance released. A rebuild
during startup ends the countdown; corpus initialization and worker handoff
continue immediately after clearing. Ingestion is background work and requests
use the progressively rebuilt corpus once admitted. Failed or
interrupted clearing leaves storage paused, including after restart, until an
explicit rebuild retry. After durable acceptance, shutdown before maintenance release
also leaves a failed or interrupted rebuild; annotation dry-run mode refuses
incomplete rebuild state.
The preserved Operation row is the durable recovery marker; SQLite clearing and
filesystem deletion are separate boundaries.

## 6. HTTP route surface

The full wire contract is in **PROTOCOL.md**. Routes split into public
(no bearer) and protected (admin bearer required, spec §34 protection split).

**Public:**

- `GET /v1/health` — readiness and diagnostics (§7).
- `POST /query` — JSON `QueryRequest` in, one JSON response carrying ranked
  passages and their canonical EvidencePack. The synchronous retrieval + assembly
  pipeline runs on a blocking thread.
- `GET /units/{unitId}`, `GET /units/{unitId}/relationships` — unit reads, gated
  so a non-active parse's units are never served.
- `GET /sources/{sourceId}` — source inspection.
- `GET /sync/status` — sync status.

**Protected (admin bearer):**

- `POST /sources` — register a source (async Operation).
- `POST /sources/{sourceId}/parses` — request a parse (async Operation).
- `POST /sources/{sourceId}/parses/{parseId}/activate` — activate a parse (async).
- `POST /parses/{parseId}/accept`, `POST /parses/{parseId}/discard` — held-parse
  disposition (async).
- `POST /snapshots` — mint a forensic snapshot (async).
- `POST /restore` — restore + reactivate a source (async; shares the autonomous
  completion path).
- `POST /rebuild-all` — clear stored corpus state and resume automatic rebuilding
  (async Operation; §5.1).
- `POST /shutdown` — immediate confirmation, then signals shutdown. **Not** an
  Operation row (not async work).
- `GET /parses?status=held` — held-parse listing (spec §13.4 disposition surface).
- `GET /operations/{operationId}` — Operation polling.
- `GET /annotations/vocabulary?annotationType=entity|relation&scope=active|all`
  — annotation vocabulary inspection (grouped entity or relation vocabulary
  drawn from `semantic_annotations`; `scope` defaults to `active`). The
  inspect-and-adjust surface for authoring the operator policy documents (§2.10).

---

## 7. Health

`GET /v1/health` returns a `HealthResponse { service, ready, components[] }`.
Each `HealthComponent` carries `name`, `ready`, `details[]`, and a typed
`counts[]` array of `HealthCount { label, source_system?, value, as_of }`. Counts
are typed numbers (not strings parsed out of `details`), and **every count
carries its own `as_of` marker** — a count without its measurement time is a
guess presented as fact.

Health is assembled from **in-memory slots** published into by the owning
threads; the health read opens no database connection. Slot reads are
poison-recovered (a panicked publisher's last snapshot is still reported so
health keeps answering).

Components:

- **`inference`** (readiness-critical) — runtime readiness details.
- **`sync`** (readiness-critical) — the scheduler's per-cycle slot:
  `fabric_ready`, backlog depths (`pending`/`in_flight`/`failed`),
  `coalesced_total`, last-cycle counters, effective `cadence_ms`, and
  `last_success_at` (achieved freshness as measured truth, never a target).
  Starts not-ready ("validation pending") until the scheduler's first cycle.
- **`logging`** (diagnostic-only) — file path and level.
- **`fabric`** (diagnostic-only) — per-`source_system` fabric backlog/fault
  counts published each scheduler cycle: **`held`**, **`serving_stale`**,
  **`access_lost`**, **`stuck_building`**, **`unparseable_mime`**,
  **`verification_halted`**. Keyed per `source_system` (exactly one at MVP, but
  the shape is a per-system map, not a single global bucket), each with the
  cycle's `as_of`.
- **`annotation`** (diagnostic-only) — the annotation worker's own slot:
  `parked` (+ detail) and counts from the last completed cycle, including
  exhausted work, with an `as_of`. Counts are corpus-aggregate, not
  source-system keyed. Typed summary observations additionally expose each
  discovered document's committed/required progress, entity/relation/summary
  breakdown, mutually exclusive pending/running/failed/retry-waiting/exhausted
  counts, and worker activity including commit and storage waits. Source paths,
  source/parse/plan identity, inventory time, and document measurement time scope
  the observation; PROTOCOL.md defines the wire contract. Historical cycle
  counters retain their existing meaning and do not determine completion.
  `GET /sync/status` reports only the scheduler snapshot.
- **`search_admission`** (diagnostic-only) — the search admission gate window
  (`max_in_flight` / `in_flight`) via `AdmissionGate::snapshot`.

**Only `inference` and `sync` gate the top-level `ready` flag.** The other
components are diagnostic-only by construction.

The annotation worker measures discovered documents before model dispatch and
updates their snapshots after commits and work-state transitions. Subsequent
discovery captures newly active or changed sources. Completion is fresh coverage
of required excerpt/type pairs, including successful empty results and committed
memo reuse; retry attempts and output-item counts do not increase it. Completion
is reconstructed after restart and snapshots reset on rebuild. Unknown totals
and zero required work remain explicit. Percentages are floored to one decimal
place; 100% annotation completion does not assert retrieval projection publication.

Admission itself: the `/query` handler acquires a permit from a fail-fast
in-flight search gate before running the pipeline. Saturation surfaces the
existing `ServiceUnavailable` (503) path; the gate's capacity is a code constant,
not operator-tunable.

---

## 8. Acquisition contract

### 8.1 Connectors are untrusted producers

Connectors stage acquisition bundles; the **importer** owns every canonical
acquisition write. Every manifest field is a claim. Content identity is only
ever the SHA-256 the importer **recomputes over the staged bytes**: a manifest
whose claimed source hash (or claimed byte size) disagrees with the staged
bytes is rejected at the trust boundary.

A malformed or mismatching bundle is a **recorded acquisition outcome**, never
an importer error: the importer writes a failed `AcquisitionRecord` plus an
`acquisition.failed` event in one transaction, and the staged bundle directory
is **deliberately kept** so the operator can inspect the malformed input.
Importer `Err` is reserved for faults of the canonical side itself (SQL,
artifact store, staging filesystem). When the manifest was readable, its claims
are preserved verbatim on the failure record; when it was not, identity fields
carry an explicit unknown-claim literal — the record never fabricates claims.

### 8.2 AcquisitionRecords for failures and successes alike

Every acquisition attempt — success or failure — leaves a durable
`AcquisitionRecord`. Failure records carry a `failure_class` from the closed
set `unreachable`, `access_denied`, `not_found`, `timeout`, `malformed`,
`resource_limit`, `other`, plus a bounded `failure_detail`.

### 8.3 Content identity and dedup

**One SourceObject exists per `source_hash`.** Re-import of identical content
is idempotent: dedup by content hash refreshes the existing rows rather than
duplicating them, so a crash mid-chain can safely replay against a
still-staged bundle. The artifact-store blob write happens *before* (outside)
the import transaction — the store is content-addressed and write-once
idempotent, so a crash between blob write and commit leaves only an orphan
blob a later import reuses, never a committed row whose `storage_uri` dangles.

### 8.4 Locations, renames, and rebinds

`(source_system, native_uri)` is the unique presence key of a
`SourceLocation`. Three transitions exist:

- **Refresh** — the location re-observed with the *same* content: presence is
  re-confirmed (`last_seen_at` advances, status returns to `current`) and any
  stale deletion evidence on the row is cleared; the durable evidence survives
  in the event log, the row reflects current state only.
- **Rebind** — the location re-observed with *different* content: the row is
  repointed at the new SourceObject and **`first_seen_at` is reset** to the
  rebind time. A content change is one location binding ending and another
  beginning; the reused row models the *new* binding, and the old binding's
  history survives in `acquisition_records` and `system_events`.
- **Rename** — content appearing at a new `native_uri` is a new location of
  the same SourceObject (dedup by hash); the vacated location is evidenced as
  deleted by the same enumeration that saw the move.

### 8.5 Deletion evidence — qualifying signals only

A location is marked `deleted` only with qualifying `DeletionEvidence`. The
signal vocabulary is closed: `explicit_delete_event`,
`absent_from_complete_enumeration`, `source_reported_gone`. The MVP filesystem
connector mints only `absent_from_complete_enumeration`, and only from a
**complete** enumeration of the scope. Deletion is **never inferred from
absence counting**, and a failed or partial enumeration asserts *nothing*
about absent items. The complete current-location set is read uncapped by
design: a capped read would treat rows beyond the cap as absent and fabricate
deletion evidence.

### 8.6 Access-lost lifecycle

When a scope enumeration fails **source-side**, the scope's `current`
locations transition to `access_lost` (with a `source.access_lost` event
each). Access-lost is a reachability statement, not a deletion: the document
presumably still exists; observation was lost. **Serving continues** — no
deactivation, no barrier — and the freshness clock stops simply because
`last_seen_at` stops advancing. Access-lost never feeds §15.8 deactivation. A
location coming back from `access_lost` completes the audit pair: the refresh
mints `source.access_restored` on the same transaction.

---

## 9. Sync queue and backpressure contract

### 9.1 Durable coalescing queue

Detected changes flow through the durable `sync_queue` with **latest-state
coalescing**: at most one row exists per `source_key`
(`source_system:native_uri`). A new detection for an existing row — in *any*
state — is absorbed into it (advancing `detected_at`, re-pending the row,
incrementing `coalesced_count`) instead of inserting a second row.

**Coalescing never drops an attached Operation.** A queue-coupled
(HTTP-enqueued) row carries the `operation_id` of its §5 Operation; the
coalesce write keeps whichever of the new-or-existing link is non-null,
because the drain still owes that Operation a terminal transition.

Entry states are `pending`, `in_flight`, `failed`. Completed entries are
**deleted** — the audit trail is the acquisition records the drain produced,
never queue residue. A `failed` entry is terminal until a new detection
re-pends it. `in_flight` rows observed at claim time are reclaimed: the single
scheduler thread drains synchronously, so any observed `in_flight` row is
stale wreckage from a crash, never live work.

### 9.2 Knob-free adaptive cadence

The detection cadence is **knob-free** (spec §35): its interval, growth factors,
and smoothing weights are code constants. Quiet cycles grow the delay
multiplicatively, observed changes pull it down, and undrained backlog or failed
cycles increase it. All paths, including the scan-duration floor, obey the shared
60-second backoff ceiling (`src/util.rs::MAX_BACKOFF_MS`). Cadence logs report
changes after applying the ceiling; `cadence_ms` publishes the effective delay.

The same ceiling applies to dense HTTP retry sleeps. Annotation retries use
their own configured intervals and execution-backoff ceiling (§13). These delay
limits do not bound request timeouts or work duration; long-running cycle work
can still delay the next scan.

### 9.3 Backpressure events

`sync.backpressure_entered` / `sync.backpressure_exited` are **durable,
edge-triggered SystemEvents** recording a real load-shedding regime: entered
when a cycle ends with undrained backlog throttling the cadence, exited when
the backlog clears. They record regime transitions, not per-cycle noise.

### 9.4 Staging lifecycle

Startup sweeps orphaned parser temp workspaces (`bundle-*` `.tmp` directories)
before the first cycle. A consumed acquisition bundle directory is deleted
**only after the entry's whole unit of work completes** (import → parse → gate
→ activate → queue completion), so a crash mid-chain replays against the
still-present bundle idempotently. Failure bundles — rejected acquisition
bundles and failed parser output bundles — are **retained** for diagnostics.

---

## 10. Parsing contract

### 10.1 Parser workers are untrusted producers

Parser workers write **staged output bundles only** — never canonical storage
or hot indexes. Nothing in a bundle is canonical until the importer validates
and imports it. The bundle reader independently **recomputes every file
digest** listed in the bundle manifest before the importer trusts any content;
the manifest itself (which cannot contain its own hash) is validated
structurally — parse, schema version, and full digest coverage of every other
file. Staged bundles are plain JSON claims, not canonical state:
canonicalization happens exactly once, at import, when the core builds the
canonical parse bundle.

PDF workers share `src/parse/pdf.rs` for engine selection and parser identity.
Docling runs its configured CLI; MuPDF runs a private child mode of the same
service executable before normal initialization. The synchronous parent enforces
the document timeout, terminates and reaps timed-out children, and records durable
start/completion/failure diagnostics. Extraction failures become failed parser
bundles; there is no fallback to the other engine.

MuPDF maps every physical page, cleaned paragraph, and image bounds to `page`,
`text_block`, and `figure` candidates. Merged paragraphs retain every contributing
source page/line locator; relationships preserve paragraph order. Cleanup (§10.8)
uses no font size/weight rules, hierarchy inference, or OCR. Pages without embedded
text and unsupported native block categories produce diagnostics.

### 10.2 The importer is the sole canonical writer

Candidate records in a bundle carry parser-local string IDs only. **All
canonical parse-scoped IDs are assigned by the importer, never by workers**;
even the bundle directory name is a worker-chosen unique name with no
canonical meaning. The importer owns every canonical parse-state write
(`parse_runs`, `content_units`, `unit_relationships`, and their events).

### 10.3 Hard gates (spec §13.1)

The importer enforces the §13.1 hard gates over the digest-verified bundle:
definitional structural truths, no thresholds. **Each gate fails with a
distinct error** naming the gate and the offending record: `resource_limits`
(unit/relationship/warning counts, per-unit body size), `id_assignment`
(sequence-index claims), `local_ref_integrity` (duplicate or dangling local
ids), `body_type_mapping` (body shape vs declared content type), and
`capability_profile` (undeclared content or relationship types).

### 10.4 Rejected vs fault

The split mirrors acquisition:

- A bundle that breaks the staged contract, reports a failed parser execution,
  or fails a hard gate is a **recorded parse outcome**: the `parse_runs` row
  moves to `failed` with a bounded error and a `parse.failed` event. Every
  failed attempt writes a durable failure record (spec §13.5 rule 2).
- An infrastructure fault of the canonical side (SQL, artifact store, clock)
  surfaces as an error and **deliberately leaves the run row `building`** —
  marking it `failed` would blame the parse for the service's own fault.
  Stuck-`building` rows surface in health (the fabric component's
  `stuck_building` count, §7), and dispatch proceeds past stale `building`
  wreckage so one crash cannot permanently block a source.
- One recorded boundary: a bundle whose manifest is unreadable or whose
  claimed source does not exist cannot leave *any* durable run row
  (`parse_runs` identity columns are NOT NULL); those unattributable bundles
  surface as explicit errors logged with the bundle path.

### 10.5 Conformance is measured, never gating

Conformance measurement is **pure** — no IO, no clock, no storage — and is
**always measured, never a gate**: no absolute threshold exists. It produces
the `ConformanceReport` whose `dimensions` map the §11.2 dominance rule later
compares. Dimension keys are a stable persisted contract
(`locator_coverage`, `caption_pairing_rate`, `table_decomposition_rate`,
`relationship_coverage`), every dimension oriented so higher is better.

### 10.6 No blind retry (spec §13.5)

Before dispatching a worker, the scheduler evaluates every prior run keyed on
the tuple **(source, parser name, parser version, parser config hash)**.
Because a source id is 1:1 with its `source_hash`, a match means identical
bytes through an identical parser — which fails (or succeeds) identically, so
re-parsing is pointless or forbidden. Outcomes: dispatch (no prior run),
dispatch over stale `building` wreckage (surfaced, so a crash cannot block the
source), gate an existing un-held `ready` run (crash-recovery idempotence), or
skip. Only new content or a new parser identity licenses a re-parse; PDF engine
selection and identity-bearing settings (§2.8) determine this tuple. Returning to
an engine identity already used for that source does not bypass the guard. An
archived parse can instead be restored through the existing snapshot lifecycle.

### 10.7 Pre-worker content-identity check

Immediately before worker execution the scheduler hashes the **live** corpus
file and compares it against the run's bound `source_hash` (computed over the
*staged* bytes at acquisition). On mismatch the parse is **skipped**: binding
old-hash identity to new-byte content would corrupt content identity, and the
skip is self-healing — the changed bytes are re-detected, re-staged, and
re-parsed under their own new SourceObject by the next scan. The residual
instant between this check and the worker's own read is a recorded residual.

### 10.8 Cleanup and original extraction

Docling and plain-text workers apply `src/parse/cleanup.rs` before staging bundles.
Cleanup v2 repairs conservative prose spacing and contractions, preserves
recognized code/math and structured content, and removes explicitly marked leaf
headers/footers. PDF paragraph reflow preserves hard hyphens; only discretionary
soft hyphens are removed. Cross-page joins require matching parents, consecutive
page endpoints, aligned columns, and lowercase continuation. Plain-text line
breaks remain intact. Merges retain original locators and rebuild sibling order.
Whitespace-only lines do not trigger indentation protection; nonblank indented
lines and nonblank lines containing tabs remain protected.

Their `parser_raw/` contains original extractor output, `pre_cleanup.json`
(units and relationships), and `cleanup.json` (version, counts, removals, merge aliases).
The importer archives verified raw bytes before either a ready or verified-failure
commit; `parse_runs.parser_raw_output_uri` points to their artifact manifest.
Unverified bundles retain the existing staged-failure handling.

MuPDF enables native dehyphenation, then applies `src/parse/mupdf_cleanup.rs` in
both production and the diagnostic preview. It removes lines wholly above the
top 50 PDF points or starting within the bottom 25 points, and standalone numeric
or lowercase Roman folios. Block lines join into paragraphs; trailing-hyphen
joins and unterminated/lowercase continuations can span blocks and pages.

Before generic repairs, the junk filter drops paragraphs whose wordlike tokens
are half or fewer of all tokens. After surrounding punctuation is stripped,
wordlike tokens require at least three ASCII letters and contain only letters,
apostrophes, or hyphens. Paragraphs starting with `#` or containing a whole word
`chapter`, `part`, or `book` (case-insensitive) bypass this filter and remain plain
text. Ordered generic punctuation, contraction, quote, echo, and hyphen repairs
then run; echo removal protects doubled initials. There are no book-specific
substitutions.

MuPDF archives the complete native extraction and `mupdf_cleanup.json` through
`parser_raw_output_uri`. The cleanup report preserves source line references,
removed margin/folio/junk text, paragraph preparation, and per-pass repairs.
Its cleanup version participates in parser identity.

Docling and plain-text workers use version 2 and hash `cleanupVersion` into their
parser configuration. Existing documents require explicit reparsing to receive cleanup; startup
does not rewrite stored content. Snapshot references retain raw artifacts, but
the existing snapshot verifier does not recursively verify their nested blobs.

---

## 11. Activation contract

### 11.1 Changed content activates

A source has at most one active parse (`source_objects.active_parse_id`).
**Changed content — a new `sourceHash`, hence a source with no active
predecessor — activates automatically** once its canonical state is complete,
the binary invariants (§10.3) passed, and its required content-derived
projections are fresh (chunk, lexical, dense, multivector, derived view; the
graph projection is deliberately not required — it is annotation-derived and
post-activation, §12.1). The §21.4 required-annotation-set policy is consulted
at the same seam; its MVP content is "nothing blocks activation."

### 11.2 Unchanged content is dominance-gated

A re-parse of **unchanged content** is gated by relative dominance over the
`ConformanceReport.dimensions` maps of candidate and active parse: a **union
comparison, absence-conservative** —

- a dimension present in the active report but absent from the candidate
  compares as **worse** (hold);
- a dimension present only in the candidate never blocks;
- absent from both is equal;
- present in both compares with plain `>=` (all dimensions higher-is-better).

**No absolute quality threshold exists anywhere in the activation path**
(spec §13, §35): every gate is a definitional status/identity check or this
relative comparison between two measured reports.

### 11.3 Held parses

A dominance regression holds the candidate: `heldReason =
conformance_regression` (the only held reason), and the run stays `ready` and
non-queryable pending **explicit asynchronous operator disposition** — the
accept and discard Operations (§6). **At most one held candidate exists per
source**: a newer candidate reaching a disposition (activate *or* hold)
supersedes any older held candidate (`parse.hold_superseded`, moved to
`archiving` and cleaned through §15.7). Accept force-activates the held run
(clearing the hold atomically with activation); discard removes it from the
disposition queue — its canonical parse bundle stays in the artifact store,
because discard removes a candidate, not the audit record.

### 11.4 Per-source cutover barrier

Cutover is serialized per source through a process-global barrier registry.
The pointer swap (`active_parse_id`), the candidate's `ready → active`
transition, the predecessor's `active → archiving` transition, and their
events commit in one transaction **under the held barrier**; the dense-plane
publish/evict happens under the same hold. Query-side, any query whose
captured active set touches a source mid-cutover is rejected with the
**retryable 503 `cutover_barrier_active`** — raised immediately after scope
capture and **before any retrieval stage runs**, so a query is never partially
executed. Barrier holds last milliseconds (spec §31.1); client retry is
sufficient. Snapshots are never minted under a held barrier (§15.2).

---

## 12. Projections contract

### 12.1 The built set and its timing

Projections are rebuildable retrieval-targeting and ranking artifacts over a
parse — **never canonical evidence**. The MVP set:

- **Content-derived, built pre-activation** for the candidate parse: chunk,
  lexical (the `chunk_text_index` FTS5 index over chunk text), dense
  (per-chunk vectors plus section windows), multivector (per-unit ColBERT matrices), derived view.
  These are the activation-required set (§11.1).
- **Annotation-derived, built post-activation** by the annotation worker:
  summary (materializing summary annotations) and graph (entity mentions and
  entity edges derived from entity/relation annotations).

Section windows include heading hierarchy and canonical text up to 2,048 local
ColBERT-tokenizer tokens. Their vectors and exact fragment mappings live in an
immutable artifact referenced by a `dense_vector` envelope with
`index_name = section_dense_v1`. Both this envelope and the fine-passage envelope
must be fresh before activation. The cache reloads both on startup and publishes
both atomically on activation/restore. Missing legacy representations require an
explicit rebuild-all; no schema or runtime migration is performed.

### 12.2 Parse-scoped, active-only

Every projection is parse-scoped and **queryable only for the source's active
parse**; a held or superseded parse's projections are never served.

### 12.3 Freshness lifecycle

Projection freshness is the closed five-value lifecycle **`building` →
`fresh`**, with `failed` (build failed), `stale` (inputs moved on), and
`superseded` (the owning parse superseded at cutover). All envelope rows live
on the shared `retrieval_projections` table through a single persistence path;
lifecycle transitions are status-guarded and append their `projection.*`
events atomically.

### 12.4 Rebuild determinism split

- **Chunk, lexical, and graph** rebuild **deterministically** from durable
  rows: restore re-imports the archived chunk rows and rebuilds the FTS5 index
  and graph tables from re-imported canonical/annotation state, which is why
  the FTS5 and graph planes are never archived.
- **Dense and multivector** are model-dependent — not deterministically
  reproducible from scratch — but **byte-reproducible from their archived
  blobs** via the little-endian f32 codec. Restore and verification
  **re-import the stored bytes and never re-embed**.

The chunker's identity (name, version, and its boundary-affecting limits) is a
hashed code document folded into `chunkerConfigHash`, not configuration.

### 12.5 Chunks are targeting artifacts

Chunks exist to be found — lexical and dense targets that resolve back to
canonical units. **A chunk is never served as evidence**; evidence is always
canonical ContentUnits (§14.3 step 7).

The chunker measures the exact whitespace-normalized
`targeting_text` for boundary decisions and stored `token_count`. Counts include
ColBERT special tokens, with truncation and padding disabled on a per-build
tokenizer copy. Every retained chunk is checked against the 512-token cap before
persistence. Individually oversized words split at valid UTF-8 boundaries and
retain their canonical unit ID; the fitting final suffix can join subsequent
words. Chunks below 400 normalized characters are discarded, including split
remainders.

---

## 13. Annotations contract

The annotation worker (§1.2) builds the MVP semantic annotation types
(entity, relation, summary) **after activation**; annotations never block
activation or the sync pipeline (the §21.4 policy's MVP blocking set is
empty).

- **Excerpt coverage.** Each invocation consumes one source fragment bounded by
  `max_input_chars`. Oversized units are split losslessly at paragraph,
  sentence, whitespace, or Unicode-character boundaries. Provenance
  `inputRefs[].textRange` records start/end Unicode scalar offsets
  (end-exclusive) and the exact UTF-8 text hash.
- **Single-goal chains.** Entity discovery precedes entity typing; statement
  selection precedes per-statement relationship formation and supporting
  quotation selection; summaries cover individual excerpts. Downstream requests
  receive that excerpt and bounded prior-stage outputs from the same chain.
  Prompts and schemas are defined in `src/annotations/stages.rs`; operator
  naming rules are not appended.
- **Concurrency.** Up to 32 independent chains run per worker wave. Dependent
  stages within a chain execute sequentially; SQLite writes remain serial on
  the owning worker thread.
- **Model-call contract.** Every call enables thinking, requests strict
  JSON-schema output, and streams over SSE with a 150,000-token output allowance
  including reasoning. The configured `timeout_seconds` applies to each call.
  A terminal `[DONE]`, `finish_reason = stop`, and nonempty final content are
  required before stage parsing. The endpoint is exclusive, with no fallback.
- **Structural validation.** Required fields, source substrings, name mappings,
  and receipt indexes are checked. Relation bodies retain `evidenceQuotes`.
  Semantic verification is not implemented; structural acceptance does not
  establish factual correctness.
- **Freshness and atomic completion.** `building` → `fresh`, with `failed`
  and `stale`, remains status-guarded with atomic `annotation.*` events.
  A completed chain commits its output set and any memo entry together. Empty
  outputs record fresh coverage without a memo entry. Intermediate stages are
  not checkpointed; failed or interrupted chains restart as a whole.
- **Location-specific completion.** `content_key_hash` includes annotation type,
  target unit IDs, source-unit content hashes, and fragment ranges/text hashes.
  Only fresh coverage satisfies the current plan; failed/building rows without
  a fresh sibling are reopenable. Producer identity is excluded, so changing
  the model does not invalidate already-fresh coverage.
- **Content-based reuse.** `memoization_key_hash` excludes target unit IDs and
  includes producer identity: stage prompts/schemas, model/endpoint, excerpt
  cap, and generation controls. Equal content can reuse output at another
  location while preserving that location's annotation references. Pending
  duplicate memo keys are flushed before another cache lookup. Memo rows
  survive parse archival and cleanup (§15.10).
- **Provenance and publication.** Re-minted annotations carry `memoized` and
  per-item `memoizedFrom`; completion stamps the running producer's memo key.
  Actual sampling temperature is recorded separately from producer identity.
  Existing annotations remain intact; legacy keys cannot satisfy new excerpt
  coverage. Summary/graph publication requires the current plan's keys to be
  fresh and the parse to remain active; unrelated legacy failed rows do not
  block it. Reads serve only the source's current active parse.
- **Independent retry budgets.** Per-annotation, per-process counters separate
  malformed outputs from execution failures. The latter includes network,
  HTTP/protocol errors, token-limit termination, and internal producer faults.
  `annotation_max_retries` and `execution_max_retries` each permit that many
  retries after the initial attempt. Work is exhausted when either counter
  exceeds its allowance; `0` disables retries for that category.
- **Retry timing.** Malformed outputs use the fixed
  `annotation_retry_interval_seconds`, with no backoff ceiling. Execution
  delays double from `execution_retry_initial_delay_seconds` to
  `execution_retry_max_delay_seconds`. Neither path is capped by
  `MAX_BACKOFF_MS`. Eligibility is tracked per annotation with a monotonic
  timer; ineligible work is skipped. Short waits wake the worker before its
  ordinary discovery interval; long waits permit intervening scans.
- **Retry sampling.** Temperature starts at `0.0` and becomes
  `min(invalid_outputs / annotation_max_retries, 1.0)`. The zero-limit case
  performs no division or malformed-output retry. Execution failures do not
  advance temperature. Retry settings enter application configuration identity,
  not producer memo identity.
- **Failure lifecycle.** Call failures stop scheduling further waves; current-wave
  results follow the normal storage and cancellation rules. Exhausted work stays
  failed, is ERROR-logged once, and contributes to the existing health count.
  Cancellation and scheduling deferrals spend neither budget. Counters and
  timers reset on restart or rebuild; they are not persisted. Logs retain the
  specific error, both counters and limits, retry delay, remaining wait, and
  next action.

---

## 14. Query pipeline contract

### 14.1 DP1 — one read snapshot per query

**Every query executes all of its hot-plane reads inside ONE read-only
transaction on one connection — one WAL snapshot — opened as the pipeline's
first act.** The scope-filtered active `(source → parse)` capture is read
inside that transaction, so capture and every later read (lexical, chunk→unit
resolution, multivector matrices, unit content, assembly) share one snapshot.
Nothing in the pipeline opens a second connection or transaction. The dense
planes are in-memory immutable clones captured at the same point. Recorded
tradeoff: the pinned snapshot blocks WAL checkpointing for the query's
duration (bounded by admission), and the snapshot-held duration is logged.

### 14.2 Admission

The `/query` handler acquires a permit from the fail-fast in-flight gate
before the pipeline runs and holds it across the whole blocking pipeline.
Capacity is a code constant — **one in-flight search** at MVP; saturation is
an immediate 503. (§7.)

### 14.3 Stage order

1. **Scope capture** inside the read transaction; **scope is enforced at
   candidate generation, never post-filtered** — out-of-scope sources are
   never captured, so no later stage sees them.
2. **Cutover-barrier probe** over the captured sources, before any retrieval
   stage (§11.4).
3. **Dense + lexical candidate generation**, with independent candidate limits
   of 100 per channel. Resolve chunks to canonical units and exclude explicit
   header/footer units before they consume ranking slots.
   Dense additionally shortlists 20 section windows with the same query vector;
   each nominates up to five units by best fine-chunk cosine within that window.
   Units without fine vectors cannot be nominated. Equal-weight RRF combines
   the direct and section-guided lists, deduplicating to 100 dense candidates
   before outer fusion. Provenance retains the representation and section path.
4. **Graph candidate generation**: entity-name entry, then a
   semantic-only one-hop traversal over annotation-derived mentions/edges, no
   LLM in the query path. Entry matching (the D9 amendment) has an always-on
   **exact** class plus two policy-gated fuzzy classes drawn from the
   entity-match policy document (§2.10), *not* the RetrievalProfile:
   **acronym** (a single query token equals the first-letter acronym of a
   stored name whose token count is at least `min_name_tokens`) and
   **token_prefix** (each query token of at least `min_token_len` characters is
   a leading prefix of the correspondingly positioned stored-name token). Fuzzy
   candidates are capped at `max_fuzzy_candidates`. Graph hits order by tier,
   then match class (`exact` > `acronym` > `token_prefix`), then matched-name
   character length descending, then name ascending, then unitId then parseId
   ascending — a rank-only deterministic order. **With both fuzzy classes
   disabled (the shipped default) the path is byte-identical to the prior
   exact-only behavior:** no per-parse name enumeration runs at all, so every
   match is `exact` and the class component of the order is constant.
5. **RRF across dense, lexical, and graph candidates**, deduplicated to 100
   canonical units, then **ColBERT MaxSim over that pool**. RRF is rank-only;
   its score is not a semantic similarity. The deferred
   `multi_vector` *retrieval channel* (§19) is candidate generation; this
   stage is late-interaction re-scoring of the already-fused pool and is
   built and live. Document matrices are the persisted ones (recomputing
   document vectors at search time is forbidden); only the query is embedded
   live.
6. **Passage construction** from MaxSim-ranked units: canonical reading order
   within one source, parse, and logical section, at most 64 constituent units
   and 512 ColBERT tokens. Overlapping passages merge when they fit; structured
   content retains its boundaries. An oversized single unit becomes a marked
   excerpt, with its full canonical body retained in the EvidencePack.
7. **Final reranker** scores up to 30 passages, or the requested count if larger
   (maximum 100), including their section headings, via the config-selected backend —
   exclusive local ModernBERT or HTTP Cohere-compatible, **no cross-backend
   fallback** (§2.9).
8. **Final selection and evidence**: `maxFinalEvidenceUnits` caps returned
   passages (default 10, maximum 100). Resolve citations and retain exactly the
   selected canonical constituents under AssemblyPolicy v2, inside the same
   read transaction. No automatic neighbor/container expansion follows selection;
   raw safety-limit failures are errors, never partial packs. The response carries
   `results`, `evidencePack`, and optional request-enabled `diagnostics`.

### 14.4 Model-call gate discipline

A process-global exclusive gate serializes live **local** accelerator calls.
The discipline is caller-side: acquire immediately before a live local model
call (local dense query embed, local ColBERT scoring, local reranker), release immediately
after — never held across SQL reads, and **never held across HTTP backends**
(network I/O must not starve the accelerator gate). The HTTP ColBERT backend's
CPU MaxSim also takes no accelerator permit.

### 14.5 Sealed policy documents

Three first-class policy contracts govern the pipeline, all **versioned,
self-hashed code documents** — deliberately *not* configuration (spec §35),
because their identity (`id` + `version` + hash over their canonical
serialization) must be stable and auditable across processes:

- **RetrievalProfile** (spec §24.2) — channels, candidate pool sizes, RRF
  constant, overfetch, hop budget, final-passage caps. Version 3 adds section
  shortlist and per-window nomination limits.
- **AssemblyPolicy** (spec §25) — retention of selected canonical passage
  constituents and explicit raw evidence safety ceilings.
- **Required-annotation-set policy** (spec §21.4) — which annotation types
  gate activation (MVP: none).

Config may later hold a *path* to an external policy document, never its
values; consumers read the active document, never a config field.

---

## 15. Forensics contract

### 15.1 Snapshot triggers

`ForensicSnapshot` minting is a closed trigger set. Built at MVP:
**`pre_activation`** (before gating a candidate), **`post_activation`** (after
a cutover), **`pre_deactivation`** (before deactivating a source), and the
operator-requested **`manual`** / **`incident`** entry (subject columns NULL).
**`scheduled`** and **`pre_deployment`** are inert recorded deferrals — the
types exist, no trigger constructs them.

### 15.2 Minting: manifest first, row last

Minting is two-phase: every heavy artifact is written to the write-once
content-addressed artifact store first (idempotent, outside any SQL
transaction), the **self-hashed §30.4 manifest** is archived last, and only
then does the `forensic_snapshots` metadata row plus its `snapshot.completed`
event commit in one transaction. A crash mid-mint leaves orphaned-but-valid
blobs and no row — never a row pointing at absent bytes. The
archived-vs-referenced boundary: planes existing only in the hot plane are
archived
(dense/multivector blobs as raw bytes, so restore re-imports rather than
re-embeds); artifacts already durable in the store (the canonical parse
bundle, the raw source bytes) are referenced by existing uri+hash, never
re-copied; the FTS5 and graph planes are not archived (deterministic rebuild
covers them, §12.4). **Snapshot creation never runs under a held cutover
barrier**: the scheduler snapshots around the activation gate, and the
deactivation path snapshots *before* acquiring the barrier.

### 15.3 Application identity

Every snapshot stamps the §30.2 **ApplicationIdentity**, captured once at
startup and threaded explicitly (never a global): the system version
(`CARGO_PKG_VERSION`), the spec version constant (**"0.3"**), the compiled
build features (via `cfg!`, sorted), and an aggregate configuration hash over
a canonical projection of the audit-relevant config. The configuration hash
covers **secret file paths only, never secret values** — it changes when a
credential file is repointed but never encodes a secret.

### 15.4 Replay profile

Every snapshot stamps a ReplayProfile: **evidence replay `bit_exact`**,
**retrieval and generation replay `not_supported`** (§19). The optional
`channelReplayModes` / `declaredTolerances` fields are omitted at MVP — they
are the Guarantee-3 verified-recompute seam (§19).

### 15.5 Verification tiers

- **Mechanical** — run on **every** snapshot: confirm the archived manifest is
  complete and self-hash-valid, and re-hash every blob-backed artifact
  reference against its recorded hash.
- **Deletion gate** — mechanical **plus** deterministic-rebuild comparison:
  each archived plane is compared against the live hot plane by re-importing
  archived bytes and re-deriving from archived rows. **No model call exists
  anywhere in verification or restore** (hard invariant): vectors are decoded
  from stored blobs and compared, never regenerated.

Section artifacts are explicit manifest references. Verification checks exact
canonical text/fragment coverage, ownership, and complete envelope membership.
Restore rejects snapshots without the required dense representations before its
write transaction; accepted section vectors are reused from archived bytes.

### 15.6 The deletion gate blocks deletion

Superseded hot state is removed only through **archive → verify → delete**,
and the deletion gate runs *before* any delete write. A gate failure **halts
the affected source's lifecycle before any write**: superseded state is
retained, nothing is deleted, there is no auto-retry past a failed gate, and
the halt surfaces in health (the fabric component's `verification_halted`
count, §7).

### 15.7 The three exits into `archiving`

Three lifecycle exits move a parse to `archiving`, and all three complete
`archiving → archived` **only** through archive-verify-delete:

1. **Activation predecessor** — the outgoing active parse at cutover; its
   deletion gate verifies over the `post_activation` snapshot of the newly
   activated candidate, and the cleanup completes the predecessor's
   `archiving → archived` transition atomically with the delete sweep.
2. **Superseded held candidate** — an older held candidate displaced by a
   newer disposition (§11.3); gate over the candidate's **own**
   `pre_activation` snapshot (which archived its vector planes). Terminal.
3. **Discarded held candidate** — operator discard (§11.3); same gating as 2.
   Terminal.

The **deactivation cleanup arm is different by design**: after a source
deactivation it deletes the hot data planes only and **leaves the parse status
untouched** (still `active`) — deactivation is reversible (§15.9), the hot
delete is not, so reversibility lives in the flag, not the parse status.

### 15.8 Deletion propagation

When a source's **last** `current` location is gone (qualifying evidence only,
§8.5), the source leaves the queryable plane:

1. Mint the `pre_deactivation` snapshot — **outside the barrier** (snapshots
   are I/O-heavy; barrier holds must last milliseconds).
2. Acquire the per-source cutover barrier.
3. One transaction: set `source_objects.deactivated_at` and append the
   `source.deactivated` event atomically.
4. Still barriered: evict the source's dense plane.
5. Release; the scheduler then drives the trailing archive-verify-delete hot
   cleanup outside the barrier.

It is `deactivated_at` — not location status — that removes a source from
All-scope search; `active_parse_id` is deliberately left intact so
deactivation is a reversible flag-set. A source stays searchable while *any*
`current` location survives.

### 15.9 Reappearance

When a deactivated source regains a `current` location with the **same
content** (guaranteed structurally: acquisition refreshes a location to
`current` only for the same source; different content rebinds to a new
SourceObject), the source is **restored from its ForensicSnapshot's archived
artifacts preserving all IDs — no re-parse, no re-embedding** — the
non-archived planes are rebuilt deterministically, and `deactivated_at` is
cleared. The HTTP restore route and the autonomous reappearance path drive the
same completion function (§1.4).

### 15.10 What survives cleanup

`annotation_memo` rows deliberately survive parse archival, hot cleanup, and
every deletion path — cross-parse reuse is the cache's entire purpose (§13).

---

## 16. System events

The `system_events` log is the audit surface of the autonomous pipeline. The
`eventType` vocabulary is a **closed set of 37 types**, by family:

- `acquisition.*` — `succeeded`, `failed`.
- `source.*` — `ingested`, `location_added`, `location_deleted`,
  `access_lost`, `access_restored`, `deactivated`, `reactivated`.
- `parse.*` — `started`, `ready`, `held`, `hold_superseded`, `accepted`,
  `discarded`, `activated`, `failed`, `archived`.
- `sync.*` — `backpressure_entered`, `backpressure_exited`.
- `annotation.*` — `requested`, `completed`, `failed`, `stale`.
- `projection.*` — `requested`, `completed`, `failed`, `stale`, `superseded`.
- `assembly_policy.changed` — exists but is reserved: the sealed MVP policy
  documents never change at runtime, so it is never minted at MVP.
- `policy.changed` — minted per system-assigned version advance of an operator
  policy document (§2.10), atomically with the `policy_versions` row.
- `snapshot.*` — `started`, `completed`, `failed`.
- `drill.*` — `completed`, `failed`.
- `query.executed`.

**Append invariant:** events append on the **caller's connection**, so an
event commits (or rolls back) atomically with the state change it records —
the audit trail can never claim an event for a state change that did not
durably happen, or vice versa. Append failures surface as explicit errors with
operation context and are **never swallowed** by callers.

Recorded **additive extensions** to the spec's §33 closed enumeration: the
`annotation.*` family (the spec defines semantic annotations but omits their
lifecycle events), `projection.superseded` (the spec defines the transition
but omits its event), the `policy.changed` event (§2.10 operator policy-document
versioning), and — in spec §34.6 — the `parse_discard` operationType (§5).

---

## 17. Error model overview

The wire contract (bodies, exact shapes) is PROTOCOL.md; this is the
server-side domain map. Every error carries a stable kind label and a fixed
HTTP status:

- **Client boundary** — 400 (`bad_request`), 401 (`unauthorized`), 404
  (`not_found`: an absent resource and a non-active parse's units are
  indistinguishable by design), 413 (`payload_too_large`), 422
  (`docling_conversion`).
- **Availability, retryable** — 503: `service_unavailable` (admission
  saturation, not-ready) and `cutover_barrier_active` (query rejected
  mid-cutover **before any retrieval stage**; barrier holds last milliseconds,
  client retry is sufficient).
- **Internal faults** — 500: storage, IO, inference-init, and
  annotation-producer faults, plus the lifecycle-integrity failures
  **`snapshot_verification_failed`** and **`restore_failed`**. These two are
  500-class faults, not retryable conditions: a failed verification gate halts
  the affected source's lifecycle with no auto-retry (§15.6).

Operation status semantics are §5; the parse-outcome split (Operation
`succeeded` ≠ domain verdict) also lives there.

---

## 18. Commissioning

The service is developed end-state-only: nothing depended on it being startable
or serving during the build programme, and it had **not been run before
commissioning**. First runtime execution is a deliberate, individually approved
step. Statements in this spec describe the service **as built** — its wire and
storage contracts and its validated startup behavior — not observed production
behavior.

---

## 19. Recorded deviations and residuals

These are deliberate, recorded deviations, not defects:

- **QER audit tier deferred.** There are no §24/§28 QER model types in the code.
  `queryExecutionRecordId` is omitted from the query response; per-query
  `QueryPlan` / `planHash` and durable retrieval/ranking trace persistence are
  deferred. `query_execution_records` exists only as a reserved schema seam. The
  delivered system answers evidence questions at serve time only; retrospective
  per-query reconstruction is not available until this tier lands.
- **`multi_vector` retrieval channel deferred.** ColBERT multivector matrices are
  still built and **persisted** (`unit_multivector_projections`, archived as raw
  bytes so a restore re-imports them), but the `multi_vector` retrieval channel
  is deferred post-MVP (an exhaustive MaxSim candidate scan measured infeasible
  at the actual corpus/hardware). **This deferral is candidate generation only —
  it does not mean "no late interaction": the ColBERT MaxSim rerank stage over
  the already-fused candidate pool is retained and live at MVP (§14.3).**
- **Guarantee-4 gap (external calls before the audit tier).** The annotation
  producers (and any HTTP reranker) make external model calls **before** the
  audit tier exists to record them under Guarantee 4. This gap is accepted as
  recorded at the 2026-07-11 rescope; external calls are covered operationally
  by onboarding/diagnostics documentation, not by a durable per-call audit
  record.
- **Replay claims at MVP.** Every snapshot stamps a replay profile in which
  **evidence replay is `bit_exact`** (recorded evidence replays byte-for-byte)
  while **retrieval and generation replay are `not_supported`** — the service
  does not claim a replay mode it cannot demonstrate. `record_replay` is never
  emitted at MVP (it belongs to the deferred QER tier).
- **Guarantee-3 verified recompute deferred.** Probe query sets, measured
  per-channel tolerances, and numeric-environment capture are not built. The
  code seam exists and is deliberately empty: the optional
  `ReplayProfile.channelReplayModes` / `declaredTolerances` manifest fields are
  omitted from every MVP snapshot (§15.4) and are reserved for this tier.
- **Entitlement layer deferred.** Nothing resolves callers to allowed source
  sets. The code seams exist: the opaque `callerContext` query field is
  accepted and passed through unread, sources carry their acquisition-stamped
  `governance_domain` (§2.7), and scope is already enforced at candidate
  generation (§14.3) — the layer, when built, intersects entitlements into the
  resolved scope at that same point.
- **Compliance-driven erasure deferred.** A designed, audited purge operation
  over the immutable stores (spec §11.5) is a named deferral. It **must not be
  improvised**: no existing deletion path (§15.8) erases artifact-store
  content, and ad-hoc removal from the write-once stores is outside every
  contract in this spec.

No guarantees beyond those the code implements are claimed here.
