# Data Store — Canonical Content-Graph and Retrieval Fabric

The data store is a standalone, autonomous content-graph and retrieval service.
Point it at a corpus directory and it runs itself: a filesystem connector detects
source files, acquires them with genuine provenance, parses them, gates and
activates a single active parse per source, builds semantic annotations, and
serves retrieval evidence from projections. Operators mostly observe. The admin
routes are rare overrides for the cases where the autonomous pipeline needs a
deliberate hand.

This document is the operator entry point: how the fabric works and how to run
it. For the exact contracts and comprehensive detail, see:

- **INSTALL.md** — installation, models, and one-time storage setup.
- **PROTOCOL.md** — the HTTP contract of record (request/response shapes, status
  codes, error envelopes).
- **QUICKSTART.md** — condensed install/operation command reference.
- **SPEC-SERVER.md**, **SPEC-CLIENT.md**, **ARCHITECTURE.md** — the comprehensive
  design references.

The service is first run at commissioning. The behavior described here is stated
from the code as built; it is not a warranty of runtime-verified behavior.

## How the fabric works

The pipeline is autonomous. Nothing in routine operation requires an operator
call — you supply a corpus and the scheduler drives the rest on an adaptive
cadence:

```
  corpus files
       │
       ▼
  ┌──────────┐   filesystem connector: full scan, never follows symlinks,
  │  DETECT  │   (mtime, size) prescreen against known state, atomic staging
  └────┬─────┘
       ▼
  ┌──────────┐   importer writes canonical AcquisitionRecords + SourceLocations;
  │ ACQUIRE  │   raw bytes land in the content-addressed artifact store first
  └────┬─────┘
       ▼
  ┌──────────┐   EPUB: in-process EPUB worker; plain text: text worker.
  │  PARSE   │   Imported bundles become canonical units; invalid bundles fail.
  └────┬─────┘
       ▼
  ┌──────────────┐   chunks, lexical index, passage/section dense vectors,
  │ PROJECTIONS  │   persisted ColBERT token matrices, and derived views
  └────┬─────────┘
       ▼
  ┌──────────────┐   one active parse per source; dominance checks can hold
  │ GATE/ACTIVATE│   a valid candidate for disposition instead of activating it
  └────┬─────────┘
       ├──────────► RETRIEVE: POST /query serves the active parse
       ▼
  ┌──────────────┐   a separate worker runs entity, relation, and summary chains;
  │  ANNOTATE    │   completed outputs commit durably
  └────┬─────────┘
       └──────────► projection worker publishes graph, summary, dense + ColBERT
```

Only one parse is ever active per source: unit reads honor the active-parse gate,
so a non-active parse's units are never served.

Retrieval combines source-dense, lexical, and grouped graph/semantic annotation
rankings. ColBERT evaluates source and matched annotation representations; final
reranking receives canonical passages plus separately labeled matched context.
Publication and retrieval do not wait for all annotation types to finish.

### Annotation request chains

Each evidence-bearing unit is split into excerpts bounded by `max_input_chars`,
without dropping oversized-unit tails. Entity, relation, and summary work use
separate chains. Within a chain, model requests run sequentially; the server
validates each response before passing its output to the next request.

- **Entities:** extract candidate names from the excerpt, then send the same
  excerpt and bounded batches of those names for classification or rejection.
  Typing returns `entities` entries with `name`/`entityType` and `rejected`
  entries with `name`/`reason`. Together, the lists must account for every
  supplied name occurrence exactly once. Unknown names, omissions, excess
  duplicates, empty required text, and accepted `NOT_AN_ENTITY` types fail
  validation. Only accepted entries become entity annotations.
- **Relations:** select source statements, form relationship triples for each
  selected statement, then request supporting quotations for bounded batches of
  those relationships. Later calls include the original excerpt. Statements and
  quotations use a shared fuzzy matcher: Unicode lowercasing and alphanumeric
  filtering ignore case, punctuation, whitespace, and word boundaries; separate
  limits allow text repairs and source omissions. The model's cleaned text is
  retained. Matching does not establish semantic correctness. Nonempty-text
  validation and complete receipt-index accounting remain; empty quotation lists
  are permitted.
- **Summaries:** request one summary per excerpt and validate its response shape.

Empty name extraction skips typing. An entirely rejected candidate set also
produces no entities. Both are successful empty results: the worker records fresh
coverage so the excerpt is not retried merely because it has no annotations.
Rejection reasons remain in `logs/annotator.log`; rejected candidates produce no
entity rows. Model judgments are not independently verified for semantic accuracy.

Intermediate results stay in memory. A completed chain commits its annotation set
and any memo entry together; a failed chain restarts without intermediate
checkpoints, under the retry policy below. Prompt/schema changes alter memo
identity for new or unfinished work, while completed coverage remains fresh.
Existing annotations are not automatically reclassified or removed.

## Operating model

**Start the service.** The server binary is `data-store-service`. After inference
initialization, it serves HTTP during the configured
`server.startup_delay_seconds` window before initializing corpus storage or
starting workers. The supplied configuration uses 10 seconds; `0` disables the
wait. Send `data-store --config config.toml --rebuild-all` during this window to
clear the old corpus before ordinary startup can modify it. Health, Operation
polling, rebuild-all, and shutdown remain available; other storage-dependent
requests return `503` and readiness stays false. Rebuild-all ends the countdown
immediately; after successful clearing, normal startup continues without waiting
out the remaining delay. Failed rebuilds keep storage paused. Startup logging
and admin-token publication remain active.

**Set up storage once.** Run the server with `--setup-storage` to build the
fabric hot plane's SQLite schema. This is one deliberate operator
action. The runtime NEVER creates or migrates schema — all schema arrives through
this explicit setup step. See **INSTALL.md** for the full procedure and the
model artifacts required before first start.

```sh
data-store-service --config config.toml --setup-storage
# → fabric storage schema ready at <index_root>/fabric/...
```

If the fabric plane is missing or invalid at startup, the service still serves,
but reports `ready=false` and health explains why (run `--setup-storage`).

**Admin token handoff.** On every start the service generates an admin bearer
token, prints it in the startup handoff, and publishes it to an owner-only token
file (`admin.token_file_path`, required; `.data-store-admin-token` as shipped in
`config.example.toml`). The bundled
CLI reads this file to authenticate protected calls; a raw `curl` needs the token
as `Authorization: Bearer <token>`. The token file is one of several owner-only
secret files; **INSTALL.md** lists the full set (annotator and HTTP-backend
API-key files).

**Readiness.** The top-level `ready` flag is the conjunction of exactly two
readiness-critical components: `inference` and `sync`. Everything else reported by
health is diagnostic-only and never makes a running service report unavailable.

**Annotation work.** The worker runs the [annotation request chains](#annotation-request-chains)
over bounded excerpts. Each call enables thinking, requests structured JSON
without streaming, and allows up to 150,000 output tokens including reasoning.

**Projection work.** A separate synchronous worker publishes graph and summary
inputs independently, and builds dense/ColBERT representations for individual
annotations, combined annotations per excerpt, and canonical source windows.
It discovers existing committed annotations without regenerating them. Embedding
failures retain annotation results and the previous valid publication.

**Operator policies.** The entity-match document controls graph-entry fuzzy
matching. Inspect vocabulary with `--vocabulary <entity|relation>` before editing
it. Both policy documents remain required, validated, content-hashed, and
versioned at startup. The annotator-naming document is not applied to the current
single-goal prompts; editing it does not change producer memo identity.

**Annotation dry run.** `data-store-service --annotation-dry-run <N>` parses the
corpus and samples the first `<N>` excerpts per source per type (entity and
relation; no summaries or embeddings). Inspect them with
`--vocabulary <entity|relation> all`: sampled parses are not active. Normal
startup adopts those parses without re-converting them and completes ingestion.

## The HTTP surface at a glance

PROTOCOL.md is the contract of record. This is the map.

**Public** (no bearer token):

| Method & path | Purpose |
| --- | --- |
| `POST /query` | Synchronous retrieval; returns ranked passages and their canonical EvidencePack. |
| `GET /v1/health` | Readiness and per-component diagnostics. |
| `GET /v1/monitor` | Current ingestion, annotation, publication, and model-call observations. |
| `GET /units/{unitId}` | One canonical unit (served only if its parse is active). |
| `GET /units/{unitId}/relationships` | Unit relationships (direction/type filters). |
| `GET /sources/{sourceId}` | Source locations and freshness. |
| `GET /sync/status` | Published sync/scheduler health. |

**Protected** (bearer token required):

| Method & path | Purpose |
| --- | --- |
| `POST /sources` | Register/ingest a source (async Operation). |
| `POST /sources/{sourceId}/parses` | Force a parse run (async Operation). |
| `POST /sources/{sourceId}/parses/{parseId}/activate` | Activate a parse (async Operation). |
| `POST /parses/{parseId}/accept` | Accept a held parse (async Operation). |
| `POST /parses/{parseId}/discard` | Discard a held parse (async Operation). |
| `POST /snapshots` | Create a forensic snapshot (async Operation). |
| `POST /restore` | Restore a source from a snapshot (async Operation). |
| `POST /rebuild-all` | Clear indexed corpus state and schedule automatic rebuilding (async Operation). |
| `POST /clear-failures` | Unblock failed background work while preserving successful work and failure history (async Operation). |
| `POST /shutdown` | Graceful shutdown (immediate confirmation, then signal). |
| `GET /parses?status=held` | List parses awaiting disposition. |
| `GET /operations/{operationId}` | Poll an async Operation's status. |
| `GET /annotations/vocabulary?annotationType=…&scope=…` | Inspect the grouped entity/relation annotation vocabulary. |

## The polling admin model

The service's client-facing API uses JSON responses and Operation polling.
Internal annotator model calls return complete responses without streaming.

Every mutating admin route runs its work asynchronously. The route returns
`202 Accepted` with an operation id:

```json
{ "operationId": "op_..." }
```

Poll `GET /operations/{operationId}` until the Operation reaches a terminal
status (`succeeded` or `failed`). `POST /shutdown` is the one exception: it is a
control action, not async work, so it confirms immediately and is not an
Operation row.

**Operation-succeeded is not the same as a parse outcome.** An Operation reaching
`succeeded` means the pipeline lifecycle completed — the work ran to its end
without an internal error. It does NOT tell you the domain verdict. Whether a
parse was **activated**, **held**, or recorded a **failed parse** lives in the
parse run, not the Operation. To learn the verdict, consult the parse run
directly or list held candidates with `GET /parses?status=held`.

## Health

`GET /v1/health` returns:

```json
{
  "service": "data-store",
  "ready": true,
  "components": [
    {
      "name": "sync",
      "ready": true,
      "details": ["role: readiness-critical", "..."],
      "counts": []
    },
    {
      "name": "fabric",
      "ready": true,
      "details": ["role: diagnostic-only", "..."],
      "counts": [
        { "label": "held", "source_system": "filesystem", "value": 0, "as_of": "2026-..." }
      ]
    }
  ]
}
```

Each component carries typed `details` and a typed `counts` array. Every count
carries its own `as_of` label — a count is never presented as current without
saying when it was measured — and fabric counts are keyed by `source_system`.
`/v1/health` and `/v1/monitor` use `snake_case` response fields.
Components that publish no counters serialize an empty array.

- Readiness-gating components: `inference`, `sync`. Their combined readiness is
  the top-level `ready`.
- Diagnostic-only components: `logging`, and the fabric diagnostics `fabric`,
  `annotation`, `projections`, and `search_admission`. These are visible for operators but never
  gate readiness. The `fabric` and `annotation` counters (held, serving-stale,
  stuck-building, access-lost, unparseable-mime, verification-halted, annotation
  freshness, retry exhaustion, and so on) are surfaced for observation only.

`--health` presents a compact operational report, with attention items and
unreported measurements first. Corpus exceptions appear once per source system;
zero-valued categories collapse into one line. `--health-details` retains every
component detail, startup smoke result, and diagnostic counter. Both commands
read the same endpoint; summaries come from the server's component snapshots.

Both health commands show each document in the worker's measured inventory:
`completed / total (percentage)`, entity/relation/summary breakdowns, pending,
running, failed, retry-waiting, and exhausted work, and activity including commit
and storage waits. Source/parse/plan identity and measurement times identify the
captured work. Unknown totals and no required work are explicit. Completion
counts committed fresh coverage, including empty results and memo reuse; 100%
does not assert retrieval projection publication. The worker updates progress
during processing; newly active or changed sources appear on subsequent discovery.

Projection health separately reports published, pending, and failed graph,
summary, and embedding cohorts per source/parse, with activity and measurement
times. CLI and web health displays retain this distinction from annotation completion.

Last-cycle counts remain historical: eligible missing work excludes retry-waiting
and exhausted items, so zero does not establish completion. Parked workers retain
their reason. `--sync-status` reports ingestion. Model initialization does not
establish live endpoint health.
For current model activity, read the file configured by `logging.file_path`
(`logs/data-store.log` as shipped): stage starts, completions, measured
usage, failures, retry delays, and exhaustion are recorded there. Missing token
usage remains unknown; character counts are not token estimates. Existing
annotation log entries include `annotation_progress="completed / total (percentage)"`.

Annotator requests omit `response_format` and `stream` from the transcript while
retaining temperature. Responses show only content, reasoning, completion tokens,
reasoning tokens, prompt tokens, and total tokens; missing fields are unavailable.
Content and reasoning are retained in full. The transcript is written to
`logs/annotator.log`, relative to the config directory. It appends
one readable group per call as calls finish, for both normal work and dry runs,
independently of the service-log level. Match `call_id` across both logs. Each
group contains REQUEST, RESPONSE, and RESULT together, including structural
validation or the failure/cancellation reason. Unfinished groups are buffered in
memory and can be lost on process termination; call starts remain in the service
log. Calls use `stream: false`; no chunk or generation-
progress records are written. A timeout or cancellation before body receipt is
reported without a partial response. Database commits remain
separate service-log events. Transcript open/write failures appear in the service
log; authentication credentials are excluded from the transcript.

The transcript shows document progress immediately before `END CALL`. Its
pre-persistence timing is preserved: calls in a wave may repeat the count, and
the final transcript entry may remain below 100%. Health and service-log commit
entries show subsequent completion. Unmeasured progress, including dry runs,
is `unavailable`; neither log adds entries for progress.

## Known deviations (stated where an operator meets them)

These are recorded MVP narrowings, not defects:

- **`POST /query` omits `queryExecutionRecordId`.** The QueryExecutionRecord
  audit tier is deferred post-MVP, so no QER is written and the response has no
  `queryExecutionRecordId`. Any per-query id on the pack is
  a correlation handle, not a QER id.
- **The `multi_vector` retrieval channel is deferred post-MVP.** The active
  retrieval channels are **lexical**, **dense**, **graph**, and **semantic**;
  graph and semantic matches share one outer fusion contribution. (ColBERT MaxSim is
  used internally as a reranking stage, not as a retrieval channel.)
- **Replay claims at MVP:** evidence replay is `bit_exact`; retrieval and
  generation replay are `not_supported`. The system does not claim a replay
  fidelity it cannot demonstrate.
- **The query envelope is narrowed.** The request DTO rejects unknown fields, so
  the deferred spec query fields (`contentTypes`, `timeRange`, `metadataFilters`,
  `sourceSystems`, `freshness`, `channels`, `rerank`, `includeContradictions`,
  `includeFreshnessMetadata`) are rejected if named. They can be added back
  additively.

## Getting started

The bundled `data-store` CLI wraps the HTTP surface, reads the admin token file
for protected calls, and renders results for operators. Its verbs map directly to
the routes above. Run `data-store --help` for the full list; the interactive REPL
is documented in **SPEC-CLIENT.md** §1.2.

Operator verbs (CLI flag / REPL name):

| Verb | Arguments | What it does |
| --- | --- | --- |
| `--health` | — | Show compact operational health, prioritizing attention items. |
| `--health-details` | — | Show every component detail and diagnostic counter. |
| `--query` | `<queryText...>` | Run a query; remaining args join into the query text. |
| `--query-raw` | `<queryText...>` | Run the same query and print the complete response JSON. |
| `--ingest` | `<sourceSystem> <nativeUri>` | Register/ingest a source. |
| `--reparse` | `<sourceId> <sourceSystem> <nativeUri>` | Force a parse run. |
| `--activate` | `<sourceId> <parseId>` | Activate a parse. |
| `--accept` | `<parseId>` | Accept a held parse. |
| `--discard` | `<parseId>` | Discard a held parse. |
| `--snapshot` | `[requestJson]` | Create a snapshot. |
| `--restore` | `<sourceId> <parseId>` | Restore from a snapshot. |
| `--rebuild-all` | — | Clear indexed state and artifacts, then automatically reingest the corpus. Available during the startup delay. |
| `--clear-failures` | — | Requeue failed work and reset annotation retry budgets without rebuilding successful work. |
| `--held-parses` | — | List held parses awaiting disposition. |
| `--operation` | `<operationId>` | Read an Operation once. |
| `--vocabulary` (`--vocab`) | `<entity\|relation> [active\|all]` | Inspect the annotation vocabulary (scope defaults to `active`). |
| `--unit` | `<unitId>` | Read one unit. |
| `--relationships` | `<unitId> [direction] [relationshipType]` | Read unit relationships. |
| `--source` | `<sourceId>` | Read source locations and freshness. |
| `--sync-status` | — | Read sync/scheduler health. |
| `--shutdown` | — | Request graceful shutdown. |

For admin verbs the CLI submits the request, then polls the returned Operation
until it terminates, and points you at the parse run / held-parses for the domain
verdict.

### Example: query the fabric

`queryText` is the only required field. `retrieval.default_results` and
`retrieval.max_results` configure the default and maximum passage counts
(shipped values: 10 and 100). `maxFinalEvidenceUnits` selects a count within that range. Raw unit
locators default on; relationships, annotations, and `debug` default off.

```sh
curl -s http://127.0.0.1:8091/query \
  -H 'Content-Type: application/json' \
  -d '{
        "queryText": "how does activation gating work",
        "constraints": { "governanceDomains": ["local-corpus"] },
        "retrievalPolicy": { "maxFinalEvidenceUnits": 5 },
        "evidencePolicy": { "includeRelationships": true, "includeAnnotations": true },
        "debug": false
      }'
```

The response carries ranked `results` with passage text, source locations,
and section headings, alongside the complete
canonical constituents in `evidencePack`. Oversized single-unit excerpts are
labeled `truncated`; their full bodies remain in the pack. Per-stage retrieval
`diagnostics` are attached only when `debug` is `true`.

Via the CLI, pass bare query text — the client builds the `{"queryText": ...}`
body itself, so the remaining arguments are the text (quoted or unquoted):

```sh
data-store --config config.toml --query how does activation gating work
```

The CLI and web console show each passage's matching search channels and
annotation contribution, including annotation representations, source ranges,
matched entities, and directed relationships.
This attribution is available without `debug`; it describes candidate matches,
not a measured improvement in retrieval quality. Web retrieval details retain
unit mappings and supporting references.

Dense retrieval combines passage, section, and canonical source-window matches.
Individual and combined annotations are searched separately and share a grouped
annotation ranking with graph matches. Exact source excerpts remain identifiable
through ColBERT scoring, passage construction, reranking, and citations.

Query scoring streams persisted vectors with bounded buffers; operating-system
file caching can use spare RAM without requiring resident vector planes. Chunk
mappings and canonical metadata still scale with the captured corpus. Existing
annotations are embedded during normal discovery. Parses lacking required section
embeddings still need `data-store --config config.toml --rebuild-all` from the
project root, which clears indexed state and reingests the corpus. Snapshots
lacking the required passage/section representations are rejected before restore.

`indexing.chunk_max_tokens` bounds normalized retrieval chunks; annotation and
section windows have separate indexing limits. Displayed passages use
`retrieval.passage_max_tokens`. Final reranking uses a configured candidate depth
(shipped value: 100), raised for larger valid result requests. Matched graph
context shares the reranker's total input capacity without a separate token cutoff.

The CLI prints each passage once with its citation. Use `--query-raw` (REPL:
`query-raw`) with the same query text to print the complete response JSON. Raw
output does not automatically enable request diagnostics.

### Example: check readiness

```sh
curl -s http://127.0.0.1:8091/v1/health
```

```sh
data-store --config config.toml --health
```

### Ingestion monitor

```sh
data-store --config config.toml --monitor
```

The read-only terminal dashboard refreshes every 200 ms on one screen: ingestion
stages and queues, committed annotation coverage, retrieval publication, grouped
model calls, timings, reported token usage, waits, failures, and recent outcomes.
Progress bars use measured totals; files and deduplicated active known sources
remain separate. Blue, amber, and magenta accompany text state labels. Stale
connections and display overflow are explicit; resize for more space. Q, Esc,
or Ctrl-C exits. There are no monitor configuration settings or submenus.
See `SPEC-CLIENT.md` §1.7 for polling and terminal behavior.

The headline distinguishes service readiness from ingestion completion and
explains blocked work. Annotation and publication percentages cover active
documents only. Persisted parse and queue failures remain visible after restart;
routine checks leave idle panels unchanged.

To unblock failed work after addressing its cause:

```sh
data-store --config config.toml --clear-failures
```

This preserves successful work and failure history, resets retry eligibility,
and resumes background processing. Command success does not mean ingestion is
complete. Held parses and validation checks remain in force; workers terminated
by panic or failed startup still require a restart.

### Web UI

`--serve <host>:<port>` is a startup mode of the same binary, not a verb: it runs
a local HTTP server in the foreground until the process is terminated, printing
the bound URL. `--config` still applies — serve mode reuses the client's service
base URL and admin token file.

```sh
data-store --config config.toml --serve 127.0.0.1:8092
```

The UI offers a query console (constraints, passage limit, evidence toggles,
debug) showing cited passages with raw evidence and diagnostics in details panels, a unit explorer
with relationship filters, a source view, a health dashboard, sync status, the
held-parses list, an operation viewer, and the vocabulary explorer.

v1 is read-only. The browser reaches the service only through a fixed allowlist
of proxy routes under `/api/*`, which exposes no mutating route; admin reads use
the client's admin token file, and the UI itself carries no authentication.

## Configuration

Configuration is a single TOML file (`config.toml`, from `config.example.toml`).
Missing required settings and unknown keys are fatal startup errors. Configuration
comments specify units, enforcement scope, and excess-input behavior. The sections:

| Section | Purpose |
| --- | --- |
| `[server]` | HTTP bind address, required nonnegative integer `startup_delay_seconds` (`0` disables the wait), and request-shape limits. |
| `[logging]` | File-backed service logging. |
| `[admin]` | Admin token file location. |
| `[client]` | Client deadlines, polling cadence, and provenance previews. |
| `[retrieval]`, `[retrieval.entity_matching]` | Result counts, candidate depths, graph matching, passage/evidence budgets, and query admission. |
| `[indexing]` | Independent chunk, annotation-window, and section construction limits. |
| `[resources]` | Read, allocation, and inventory admission guards. |
| `[workers]` | Annotation/projection batching, concurrency, polling, and publication allowances. |
| `[sqlite]` | Lock-wait timeout, cooperative SQL execution timeout, and progress-callback cadence. |
| `[parsing]` | Candidate unit, relationship, and warning caps and the unit-body byte limit. |
| `[scheduling]` | Sync cadence, backoff, and maintenance polling. |
| `[diagnostics]` | Operational error, identifier, and progress-summary bounds. |
| `[inference]` | Accelerator selection for local retrieval models. Required but unused when dense, ColBERT, and the reranker all use HTTP; no local accelerator is initialized in that mode. |
| `[storage]` | Corpus root and service-owned index root. |
| `[connectors.filesystem]` | Governance domain stamped on acquired sources. |
| `[epub]` | EPUB admission budgets, all required: archive member count, per-member and total decompressed bytes, XML document bytes, image bytes, and element nesting depth. |
| `[models]` | Dense, ColBERT, and reranker each select an exclusive `local` or `http` backend. Remote ColBERT uses vLLM `/pooling` token inference, a matching local tokenizer, persisted document matrices, and CPU MaxSim; no local ColBERT weights are loaded. The annotator uses an external chat-completions endpoint. See **INSTALL.md** for backend fields. |
| `[policies]` | Paths to entity-match enable flags and annotator naming rules. Numeric entity-matching limits live in `config.toml`. |

See **INSTALL.md** for the annotated example and the required absolute paths.

Model serving capacity is separate from retrieval window size. The configured
HTTP capacities are 32,768 for `Qwen/Qwen3-Embedding-8B` and
`Qwen/Qwen3-Reranker-0.6B`, and 518 for `lightonai/ColBERT-Zero`; ColBERT query
and document inputs remain 512. Startup requires `/v1/models` to advertise the
configured capacity. Server-side truncation is disabled; oversized HTTP inputs
fail visibly. Existing application-side ColBERT token-ID and local-model prefix
handling remains in place; annotation windows preserve complete input through splitting.

Construction settings are recorded with new projections. Restore validates their
recorded settings or explicit legacy formats without inference. Current resource
guards may refuse large historical artifacts without declaring them corrupt.
`[parsing]` and `[epub]` admission limits do not change parser identities.

The EPUB worker parses `.epub` files (EPUB 2 and EPUB 3) in-process with no
external tool. It records only structure the source declares: sections from
the navigation document or NCX and from headings, print pages from declared
page-break markers, lists, tables decomposed to cells, figures with their
image bytes archived by hash, captions, code blocks from `pre`, asides, and
footnote and cross-reference links. No structure is inferred from text
content; a source that declares no semantics for a feature gets no such
feature. DRM-protected archives are recorded parse failures. Other formats are
converted to plain text outside this service; only `text/plain` and EPUB are
routed.

### Annotation settings

These `[models.annotator]` settings are required; the shipped values are:

| Setting | Value | Meaning |
| --- | ---: | --- |
| `max_input_chars` | 2000 | Source text per excerpt, in Unicode characters; prompts and prior-stage output are additional. |
| `timeout_seconds` | 120 | Deadline for each model call, including thinking. |
| `max_completion_tokens` | 150000 | Completion-token allowance per call, including reasoning. |
| `annotation_max_retries` | 10 | Malformed-output retries after the initial attempt. |
| `annotation_retry_interval_seconds` | 5 | Fixed malformed-output retry interval, with no backoff ceiling. |
| `execution_max_retries` | 10 | Execution-failure retries after the initial attempt. |
| `execution_retry_initial_delay_seconds` | 5 | Initial execution-failure retry delay. |
| `execution_retry_max_delay_seconds` | 300 | Execution-failure backoff ceiling. |

The two failure counters are independent. Execution failures include timeouts,
HTTP/protocol errors, token-limit termination, and internal producer failures.
Their delay doubles up to the configured ceiling; it is independent of the
configured scheduler waits and dense HTTP retry backoff. Malformed outputs use their fixed interval.
Both limits allow `0` to disable that category's retries. Delays must be positive,
and the execution ceiling must be at least the initial delay.

Temperature starts at `0.0` and becomes
`min(malformed_output_failures / annotation_max_retries, 1.0)` on retries;
execution failures do not advance it. With a zero annotation retry allowance,
no temperature division or malformed-output retry occurs.

Once either counter exceeds its allowance, the annotation remains failed and
is skipped as exhausted. Cancellation and scheduling deferrals spend neither
budget. Counters and eligibility timers reset on restart or rebuild; failed
chains restart as a whole, without intermediate-stage checkpoints.
