# SPEC-CLIENT — the bundled `data-store` CLI

Operator reference for the `data-store` command-line client built into
`src/bin/data-store.rs`. It documents the client exactly as built: the
transport model, every command, the polling model for async admin
mutations, and how each response is rendered.

Cross-references:

- `PROTOCOL.md` — the wire routes each command targets.
- `INSTALL.md` — config file and admin-token file setup.

Verification status: this client surface is first runtime-exercised at
the C10f commissioning package; until then it is compile-checked only
(recorded residual, `PLAN-CANONICAL-FABRIC.md`, 2026-07-17 entry).

---

## 1. Client model

The client is a **`reqwest` blocking** HTTP client. There is no async
runtime and **no streaming**: every request is a single blocking
request/response, and async server work is followed by polling (§4).

### 1.1 Shared configuration

The client reads the **same config file as the server**. The transport
is **HTTP-only**: the base URL is always plain `http://` derived from
`[server].bind_address` (bind-all addresses are rewritten to loopback
so the client can dial them) — the client cannot dial TLS. The
admin-token file path comes from `[admin].token_file_path`, and the
request timeout from the `[client]` section:

- `[client].operation_timeout_seconds` — the timeout on the underlying
  `reqwest` HTTP client. It bounds **each individual HTTP request** the
  client makes, including **each individual poll** of the operation
  loop (§4) — and nothing more: the poll loop as a whole is
  **unbounded** and runs until a terminal status is observed (§4), so a
  never-terminal operation polls forever. It is **not** a stream
  timeout — the client does no streaming. Default `3600`; must be
  greater than zero.

Relative paths in the config (e.g. the token file) resolve against the
config file's directory, matching the server's resolution rule.

Select the config with `--config <path>`; it defaults to `config.toml`.

### 1.2 Two modes

- **One-shot** — invoke a single command via its `--flag` and exit.
  Example: `data-store --config config.toml --health`.
- **Interactive REPL** — invoke `data-store` with no command flag to
  enter the read-eval-print loop (`--config <path>` is honored). The
  prompt is `data-store> `; type `help` for the command list, `exit`
  (or `quit`), or EOF (Ctrl-D), to leave; Ctrl-C interrupts the current
  line and reminds you to use `exit`. Line editing and history are
  provided by rustyline; command history persists to `.data-store.history`
  (resolved against the config file's directory) across sessions.

Both modes dispatch through the same command table and the same
renderers; the only difference is how the command is entered.

### 1.3 One-shot argv grammar

- Exactly **one operation flag** per invocation: a second operation
  flag is rejected (*"only one operation flag may be provided"*).
- `--help` cannot be combined with an operation flag.
- Unknown arguments are rejected with a pointer to `--help`. This includes
  the service-binary flags (`--setup-storage`, `--foreground`, `--smoke-dense`,
  `--annotation-dry-run`): they are not client flags and are rejected as
  unknown arguments.
- Positional-argument collection for an operation flag stops at the
  **next recognized flag** (`--config` or any operation flag/alias):
  everything between the flag and the next recognized flag is taken as
  its positional arguments.
- `--config` requires a value; a missing value — or a recognized flag
  in the value position — is rejected.

### 1.4 One-shot exit-code contract (scripting)

A one-shot invocation exits **`0`** on success. Any error — config
load, transport/send failure, HTTP error status, response decode
failure — propagates out of `main` and terminates the process with a
**nonzero** exit code, printing the anyhow error report (message plus
cause chain) on stderr.

**Terminal `failed` is not an error exit.** An async admin operation
that reaches terminal status `failed` is still rendered normally and
the process exits `0` — the client succeeded at driving the operation
to a terminal state. Scripts must inspect the **rendered `status`**
(and, for parse-producing types, the parse run row — §5.2), not the
exit code, for the domain outcome.

### 1.5 Admin token (protected commands)

Protected commands (§3, marked **protected**) send the admin token as an
HTTP bearer credential. The token is **read fresh from the token file on
every protected request** — it is never cached. Each poll of a running
operation re-reads it as well. The file is read, trimmed, and validated
non-empty; a missing or empty token file fails the command with a clear
error. Public commands send no credential.

---

## 2. Command dispatch

Every command has a REPL name (with optional aliases), an equivalent
one-shot `--flag`, positional arguments, a target route, a
public/protected classification, and a rendering. `help` prints the REPL
usage lines; `data-store --help` (alias `-h`) prints the one-shot usage
lines without reading the config.

Bracketed arguments (`[...]`) are optional; angle-bracketed arguments
(`<...>`) are required. A `[requestJson]` argument (`snapshot` only) is a
raw JSON string passed straight to the request body; invalid JSON is
rejected locally before any request is sent. `query` takes bare query
text (`<queryText...>`): the trailing arguments are joined with single
spaces and the client constructs the `{"queryText": ...}` body itself
via serde_json, so the text is JSON-escaped correctly and never parsed
as JSON. Raw query envelopes are curl's job.

REPL lines are tokenized by a minimal splitter (`split_shell_like`) —
no shell is involved:

- Double quotes group a token (a quoted region may open and close
  mid-token).
- **Inside quotes only**, backslash escapes `"` and `\`; before any
  other character the backslash is kept literally.
- **Outside quotes**, backslash is an ordinary literal character.
- An unterminated quote is an error.
- There is **no** variable, tilde, or glob expansion of any kind.

---

## 3. Command table

The list below is the complete `COMMAND_SPECS` table as built. Routes
are cross-checked against the router in `src/http.rs`.

| REPL name (aliases) | One-shot flag | Arguments | Route | Access | Async? |
|---|---|---|---|---|---|
| `health` | `--health` | — | `GET /v1/health` | public | no |
| `query` | `--query` | `<queryText...>` | `POST /query` | public | no |
| `ingest` | `--ingest` | `<sourceSystem> <nativeUri>` | `POST /sources` | protected | yes (operation) |
| `reparse` | `--reparse` | `<sourceId> <sourceSystem> <nativeUri>` | `POST /sources/{sourceId}/parses` | protected | yes (operation) |
| `activate` | `--activate` | `<sourceId> <parseId>` | `POST /sources/{sourceId}/parses/{parseId}/activate` | protected | yes (operation) |
| `accept` | `--accept` | `<parseId>` | `POST /parses/{parseId}/accept` | protected | yes (operation) |
| `discard` | `--discard` | `<parseId>` | `POST /parses/{parseId}/discard` | protected | yes (operation) |
| `snapshot` | `--snapshot` | `[requestJson]` | `POST /snapshots` | protected | yes (operation) |
| `restore` | `--restore` | `<sourceId> <parseId>` | `POST /restore` | protected | yes (operation) |
| `shutdown` | `--shutdown` | — | `POST /shutdown` | protected | no (control action) |
| `held-parses` (`held`) | `--held-parses` | — | `GET /parses?status=held` | protected | no |
| `operation` | `--operation` | `<operationId>` | `GET /operations/{operationId}` | protected | no (single read) |
| `vocabulary` (`vocab`) | `--vocabulary` (`--vocab`) | `<entity\|relation> [active\|all]` | `GET /annotations/vocabulary` | protected | no |
| `unit` | `--unit` | `<unitId>` | `GET /units/{unitId}` | public | no |
| `relationships` | `--relationships` | `<unitId> [direction] [relationshipType]` | `GET /units/{unitId}/relationships` | public | no |
| `source` | `--source` | `<sourceId>` | `GET /sources/{sourceId}` | public | no |
| `sync-status` (`sync`) | `--sync-status` | — | `GET /sync/status` | public | no |
| `help` | `--help` (`-h`) | — | — (local) | — | no |
| `exit` (`quit`) | — | — | — (REPL only) | — | no |

Notes on individual commands:

- **`ingest <sourceSystem> <nativeUri>`** — POSTs an ingest request
  (`{sourceSystem, nativeUri}`) to `/sources`.
- **`reparse <sourceId> <sourceSystem> <nativeUri>`** — POSTs the same
  ingest request shape to that source's `/parses` sub-route.
- **`activate` / `accept` / `discard`** — POST with no body to the
  respective parse-lifecycle routes.
- **`snapshot [requestJson]`** — the JSON body is optional; when
  omitted, the empty JSON object body `{}` is sent.
- **`restore <sourceId> <parseId>`** — POSTs `{sourceId, parseId}` to
  `/restore`.
- **`operation <operationId>`** — a **single** protected read of the
  operation record; it does **not** poll. (The async admin commands poll
  internally; this command is the standalone snapshot read.)
- **`relationships`** — `direction` and `relationshipType` are optional
  positional filters, encoded as `direction=` / `relationshipType=`
  query parameters only when supplied.
- **`vocabulary <entity|relation> [active|all]`** (alias `vocab`) — the
  required first argument selects the vocabulary; the optional second argument
  is the scope, defaulting to `active`. Both are validated **locally** before
  any request (annotation type must be `entity` or `relation`, scope must be
  `active` or `all`) and sent as the `annotationType` / `scope` query
  parameters of `GET /annotations/vocabulary`.
- **`exit` / `quit`** — REPL-only; no one-shot flag.

---

## 4. Polling model for async admin mutations

The seven async admin commands (`ingest`, `reparse`, `activate`,
`accept`, `discard`, `snapshot`, `restore`) drive server-side work that
runs asynchronously. The client:

1. Prints a **progress line** `<METHOD> <url>` to stdout (visible as
   `POST http://…/sources` in the example below), then POSTs to the
   target route with the bearer token (and optional JSON body).
2. Receives `202` with an acceptance body `{operationId}` and prints
   `Accepted: operationId=<id>`.
3. Polls `GET /operations/{operationId}` every
   **`OPERATION_POLL_INTERVAL` = 1 second** (a code constant) until the
   operation reaches a terminal status (`succeeded` or `failed`).
4. Renders the terminal operation record (§5.2).

Each poll re-reads the admin token and is itself bounded by
`[client].operation_timeout_seconds` (the per-request timeout). The poll
loop otherwise runs until a terminal status is observed. There is **no
NDJSON, no streaming, and no stream-timeout** anywhere in the client.

**`shutdown` is not an operation.** It prints the same
`POST <url>` progress line, POSTs to `/shutdown`, receives a `202` with
**no body**, prints `Shutdown signalled (HTTP 202)`, and is **never
polled**.

The progress line is printed **only** by the async admin POSTs and
`shutdown`: read commands and `query` print no progress line.

Example (interactive):

```
data-store> ingest confluence https://wiki/pages/123
POST http://127.0.0.1:8080/sources
Accepted: operationId=op-7f3c…
Operation: op-7f3c…
  type: source_ingest
  status: succeeded
  target: source src-91a…
  createdAt: 2026-07-17T12:00:00Z
  note: the operation lifecycle completed, but this does NOT confirm the
        domain outcome. Check the parse run row for the domain verdict;
        for a held result run `held-parses`.
```

---

## 5. Output rendering

All response mirrors deserialize **leniently**: additive server fields
are tolerated (no `deny_unknown_fields`), so an unexpected new field does
not break decoding. Rich/nested shapes (a unit `body`, the assembly
trace, the conformance report, the fused candidate pool, relationship
provenance, deletion evidence, location metadata) are kept as raw JSON
and printed **in full** rather than narrowed. As a consequence,
**structural drift in those passthrough fields surfaces as raw JSON,
not as a decode failure.** The claim holds **only** for the
`serde_json::Value` passthroughs: drift in a typed **required** field
(e.g. the evidence pack's `queryId`) IS a decode failure, rendered as
an "unexpected response body" error (§5.10). Errors of every kind
render per §5.10.

### 5.1 `health`

Prints the service name, readiness (`yes`/`no`), and each component with
its readiness and detail lines. Components carry typed **`counts`**: each
count is printed as `count <label> [<sourceSystem>]: <value> (as of
<timestamp>)` — a count is never shown as current without its as-of
marker, and the optional `sourceSystem` scopes fabric counts to their
owner.

### 5.2 Operation records (`operation`, and every async admin command)

Prints the operation `id`, `type`, `status`, `target` (object type +
id), timestamps, and `createdAt`. On `failed`, the server's specific
error detail is printed (`error: …`).

**Operation-succeeded ≠ parse-outcome rule (implemented in the CLI).**
When an operation is **parse-producing** — `is_parse_producing_operation`
matches the operation types **`source_ingest`**, **`parser_execution`**,
and **`parse_activation`** — a `succeeded` status means only that the
pipeline **lifecycle** completed. It does **not** confirm the domain
outcome: a recorded parse failure, or a held disposition awaiting
operator action, lives in the **parse run row**, not in
`Operation.status`. So on `succeeded` for those types the renderer prints
an explicit `note:` directing the operator to check the parse run row
for the domain verdict, and to run **`held-parses`** for a held result.
For non-parse-producing operation types, `succeeded` prints without the
note.

### 5.3 `query`

Takes bare query text: the trailing arguments are joined with single
spaces and the client builds the `{"queryText": ...}` request body itself
via serde_json (never string-formatted). It does not accept a raw JSON
envelope; a full-envelope query is curl's job.

Renders the evidence pack: `queryId`, `queryText`, `assembledAt`, the
evidence-unit count, then each unit with its `unitId`, `contentType`,
`sourceId`/`parseId`, optional `score`, optional `reasons`, an
**excerpted `textProjection`** (up to **`TEXT_EXCERPT_CHARS` = 800**
characters, ellipsized when clipped), a `locators` count, and the full
`body` as pretty JSON. Pack-level `relationships`/`annotations` are
counted; the full **`assemblyTrace`** is printed as pretty JSON. The
excerpt is a summary only — the unit `body` is always rendered in full
alongside it, so nothing is hidden.

When the request set `debug`, a `diagnostics:` block follows: a
per-stage **latency table** (openTransaction, capture, queryEmbed,
denseLexicalFusion, graph, maxsim, rerank, assembly, snapshotHeld, in
ms), the **`fusedPool`** count **and** the full fused pool dumped as
pretty JSON, the `maxsim` ranked lines (`#rank unit … score …`), and the
`reranked` lines (same, plus `logit` and `tokens` when present).

### 5.4 `held-parses` (`held`)

Lists held parses (or `Held parses: none`). Per parse: `id`, `status`,
`sourceId`, parser name/version, optional `heldReason`, timestamps,
optional `error`, a `warnings` count, and `metrics` as pretty JSON. The
**conformance report has no single verdict field** — the renderer prints
the report's **`dimensions`** map (each dimension name → value) as the
regression cause, then keeps the full `conformanceReport` reachable as
pretty JSON.

### 5.5 `unit`

Renders one content unit: `id`, `sourceId`/`parseId`, `contentType`,
`bodyHash`, optional `textHash`/`structureHash`/`primaryParentId`/
`sequenceIndex`, a `locators` count with full locator detail, timestamps,
and the arbitrary `body` in full as pretty JSON.

A **`404`** on `unit` or `relationships` is annotated: *"a 404 means the
unit is absent OR belongs to a non-active parse — the service makes these
two cases indistinguishable by design"* (§14). Non-404 errors pass
through unchanged.

### 5.6 `relationships`

Lists relationships for a unit (or `Relationships: none`). Per
relationship: `relationshipType`, `from`→`to` unit ids, `id`,
`sourceId`/`parseId`, optional `role`/`sequenceIndex`/`confidence`,
`provenance` as pretty JSON, and timestamps. Same §14 404 annotation as
`unit`.

### 5.7 `source`

Renders one source: `id`, `activeParseId` (or `none (no active parse)`),
`mimeType`, optional `sizeBytes`, `sourceHash`, `storageUri`, optional
`eventTime`, `ingestTime`, `createdAt`, optional `deactivatedAt`, then
each **location** with its `sourceSystem:nativeUri [status]`, `id`,
optional `nativeId`, `governanceDomain`, `firstSeenAt`, and
**`lastSeenAt`** prominently. `deletionEvidence` and `metadata` are kept
as pretty JSON.

### 5.8 `sync-status` (`sync`)

Renders the sync scheduler snapshot: `fabricReady` (`yes`/`no`), optional
`detail`, and the `pending` / `inFlight` / `failed` / `coalescedTotal`
counters, plus optional `cadenceMs` and `lastSuccessAt`.

### 5.9 `vocabulary` (`vocab`)

Renders the annotation vocabulary in the server's served order. The response
shape follows the requested annotation type, and the renderer decodes into the
matching mirror (`EntityVocabularyView` / `RelationVocabularyView`).

A header line reports the annotation type, effective scope, and group count
(`… vocabulary (scope <scope>): N groups` for entities, `… N predicates` for
relations), followed by a shared completeness line — `rowsRead`, empty-`[]`
markers skipped (`skippedMarkerCount`), malformed rows counted
(`malformedRowCount`), and a **loud truncation warning** when the view is
partial (a row-read or group cap was hit), because authoring rulesets from a
truncated view would miss vocabulary. An empty result prints `(no entity
vocabulary)` / `(no relation vocabulary)`.

- **Entity** groups: per group, `normalizedName` with its `totalCount` and
  `sourceCount`, then the distinct `entityTypes`, the `rawForms` (each raw form
  with its count), and per-model counts.
- **Relation** groups: relations group by **normalized predicate**. Per group,
  the `predicate` with its `totalCount` and `sourceCount`, then the `rawForms`
  (each raw predicate form with its count), and per-model counts.

### 5.10 Error rendering (both modes)

Every failure renders as one of the following operator-facing errors:

- **Non-2xx, structured body.** A body that decodes as
  `{"error":{status,kind,message}}` renders
  `<METHOD> <url> HTTP <n>: status=<s> kind=<k> message=<m>`. In the
  decode, `status` and `kind` are **optional** — a degraded body that
  carries only `message` still renders its message (with the absent
  parts omitted) rather than being replaced with a generic error.
- **Non-2xx, empty body.** Renders
  `<METHOD> <url> HTTP <n> with empty response body`.
- **Non-2xx, non-JSON body.** Renders the trimmed body text:
  `<METHOD> <url> HTTP <n>: <trimmed body>`.
- **Transport/send failure.** Renders
  `<METHOD> <url> failed to send HTTP request`, with the underlying
  cause preserved in the error chain.
- **2xx body that fails the typed decode.** Renders an
  *"returned an unexpected response body"* error. At the dispatch
  decode seam (`decode_value`, used by `query` and `held-parses`) the
  error carries the route **plus the full payload**; at the transport
  decode (`decode_success`) it carries the method and URL only.

**Mode asymmetry.** In the REPL, an error prints as `error: <msg>`
followed by an indented `caused by: <cause>` line per link in the
chain, and the loop **continues**. In one-shot mode the same failure
terminates the process with a nonzero exit code (§1.4).

---

## 6. Recorded deviations the operator meets via the CLI

- **`POST /query` omits `queryExecutionRecordId`.** The query response
  carries no query-execution-record id; the CLI has no field for it and
  renders none.
- **Active retrieval channels are lexical, dense, and graph.** The
  `multi_vector` channel is deferred post-MVP, so debug diagnostics
  reflect only the lexical/dense/graph pipeline.
