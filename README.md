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
  ┌──────────┐   Docling conversion → canonical units; one parse run per attempt
  │  PARSE   │
  └────┬─────┘
       ▼
  ┌──────────────┐   the gate admits at most ONE active parse per source. A parse
  │ GATE/ACTIVATE│   is activated, HELD for disposition, or recorded a failed parse
  └────┬─────────┘
       ▼
  ┌──────────────┐   post-activation, the annotation worker builds semantic
  │  ANNOTATE    │   annotations on its own; annotation freshness is tracked
  └────┬─────────┘
       ▼
  ┌──────────┐   POST /query returns cited passages and canonical evidence from
  │ RETRIEVE │   the lexical, dense, and graph channels
  └──────────┘
```

Only one parse is ever active per source: unit reads honor the active-parse gate,
so a non-active parse's units are never served.

## Operating model

**Start the service.** The server binary is `data-store-service`; it binds HTTP
before lengthy dependency initialization, so it is reachable early and reports
its own readiness through `/v1/health`.

**Set up storage once.** Run the server with `--setup-storage` to build the
fabric hot plane (the only durable store). This is one deliberate operator
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

**Tuning the rulesets (inspect, then adjust).** Two policy documents shape
retrieval and annotation without changing code: the entity-match ruleset
(graph-entry fuzzy matching) and the annotator naming rules (composed into the
entity/relation producer prompts). A neutral posture — an empty naming-rule list,
both fuzzy-match classes disabled — is the valid default; the in-repo
annotator-naming document is authored for the commissioning corpus from its
observed vocabulary, while entity-match remains neutral. Author them from the
corpus's own observed vocabulary: read `GET /annotations/vocabulary`
(`--vocabulary <entity|relation>`) to see the grouped entity/relation
vocabulary, edit the documents under `policies/`, then restart the service —
config is startup-only, so edits take effect on the next start. The service
content-hashes each document (ignoring comments and whitespace) and appends a
system-assigned version to an internal registry on every change; editing the
naming document changes producer identity, which invalidates memoized producer
output and re-annotates the frontier.

On a **fresh corpus**, the intended first step is the **annotation dry-run
mode**: `data-store-service --annotation-dry-run <N>` parses the corpus and
sample-annotates only the first `<N>` section groups per source per type
(entity and relation; no summaries, no embeddings), then serves the vocabulary
route so you can author the rulesets from observed vocabulary **before**
committing to full annotation. Inspect with `--vocabulary <entity|relation>
all` (the sampled parses are not active, so scope `all` is required), edit the
documents, then start normally — the normal start adopts the dry-run's parses
without re-converting them and completes ingestion under the final rulesets.
See **INSTALL.md** section 5 for the full procedure.

## The HTTP surface at a glance

PROTOCOL.md is the contract of record. This is the map.

**Public** (no bearer token):

| Method & path | Purpose |
| --- | --- |
| `POST /query` | Synchronous retrieval; returns ranked passages and their canonical EvidencePack. |
| `GET /v1/health` | Readiness and per-component diagnostics. |
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
| `POST /shutdown` | Graceful shutdown (immediate confirmation, then signal). |
| `GET /parses?status=held` | List parses awaiting disposition. |
| `GET /operations/{operationId}` | Poll an async Operation's status. |
| `GET /annotations/vocabulary?annotationType=…&scope=…` | Inspect the grouped entity/relation annotation vocabulary. |

## The polling admin model

There is NO NDJSON and no streaming anywhere in this service.

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
`/v1/health` is the one plain-named (non-camelCase) response on the surface.
Components that publish no counters serialize an empty array.

- Readiness-gating components: `inference`, `sync`. Their combined readiness is
  the top-level `ready`.
- Diagnostic-only components: `logging`, and the fabric diagnostics `fabric`,
  `annotation`, and `search_admission`. These are visible for operators but never
  gate readiness. The `fabric` and `annotation` counters (held, serving-stale,
  stuck-building, access-lost, unparseable-mime, verification-halted, annotation
  freshness, retry exhaustion, and so on) are surfaced for observation only.

## Known deviations (stated where an operator meets them)

These are recorded MVP narrowings, not defects:

- **`POST /query` omits `queryExecutionRecordId`.** The QueryExecutionRecord
  audit tier is deferred post-MVP, so no QER is written and the response has no
  `queryExecutionRecordId`. Any per-query id on the pack is
  a correlation handle, not a QER id.
- **The `multi_vector` retrieval channel is deferred post-MVP.** The active
  retrieval channels are **lexical**, **dense**, and **graph**. (ColBERT MaxSim is
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
| `--health` | — | Read `/v1/health`. |
| `--query` | `<queryText...>` | Run a query; remaining args join into the query text. |
| `--query-raw` | `<queryText...>` | Run the same query and print the complete response JSON. |
| `--ingest` | `<sourceSystem> <nativeUri>` | Register/ingest a source. |
| `--reparse` | `<sourceId> <sourceSystem> <nativeUri>` | Force a parse run. |
| `--activate` | `<sourceId> <parseId>` | Activate a parse. |
| `--accept` | `<parseId>` | Accept a held parse. |
| `--discard` | `<parseId>` | Discard a held parse. |
| `--snapshot` | `[requestJson]` | Create a snapshot. |
| `--restore` | `<sourceId> <parseId>` | Restore from a snapshot. |
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

`queryText` is the only required field. Queries return up to ten passages by
default; `maxFinalEvidenceUnits` sets the passage limit from 1 to 100. Raw unit
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
section headings, and physical PDF page references, alongside the complete
canonical constituents in `evidencePack`. Oversized single-unit excerpts are
labeled `truncated`; their full bodies remain in the pack. Per-stage retrieval
`diagnostics` are attached only when `debug` is `true`.

Via the CLI, pass bare query text — the client builds the `{"queryText": ...}`
body itself, so the remaining arguments are the text (quoted or unquoted):

```sh
data-store --config config.toml --query how does activation gating work
```

The CLI and web console show each passage's matching search channels and
annotation contribution, including matched entities and directed relationships.
This attribution is available without `debug`; it describes candidate matches,
not a measured improvement in retrieval quality. Web retrieval details retain
unit mappings and supporting references.

Dense retrieval uses both passage vectors and section-heading/content vectors.
Results identify direct passage and section-guided matches. Existing indexes
require an explicit `data-store --config config.toml --rebuild-all` from the
project root: this clears indexed state and artifacts and reingests the corpus.
Until rebuilt, queries against old parses report that section embeddings are
missing. Pre-feature snapshots cannot restore the new dense representation.

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
Unknown keys anywhere in the file are fatal startup errors. The sections:

| Section | Purpose |
| --- | --- |
| `[server]` | HTTP bind address and request-shape limits. |
| `[logging]` | File-backed service logging. |
| `[admin]` | Admin token file location. |
| `[client]` | Bundled CLI settings (server validates, never reads at runtime). |
| `[inference]` | Accelerator selection for local retrieval models. Required but unused when dense, ColBERT, and the reranker all use HTTP; no local accelerator is initialized in that mode. |
| `[storage]` | Corpus root and service-owned index root. |
| `[connectors.filesystem]` | Governance domain stamped on acquired sources. |
| `[docling]` | Docling executable and PDF conversion controls. |
| `[models]` | Dense, ColBERT, and reranker each select an exclusive `local` or `http` backend. Remote ColBERT uses vLLM `/pooling` token inference, a matching local tokenizer, persisted document matrices, and CPU MaxSim; no local ColBERT weights are loaded. The annotator uses an external chat-completions endpoint. See **INSTALL.md** for backend fields. |
| `[policies]` | Paths to the two operator-editable policy documents (entity-match ruleset, annotator naming rules); config holds paths only, and edits require a restart. |

See **INSTALL.md** for the annotated example and the required absolute paths.
