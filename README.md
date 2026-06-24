# How The Data Store Works

The data store is a standalone service that turns source documents into
searchable retrieval units, then answers search queries by combining lexical
search, vector search, ColBERT reranking, and a final reranker.

Source documents do not get uploaded through the API. Instead, the service is
configured with a corpus directory, and ingest requests name a file inside that
corpus. The service owns the conversion, splitting, embedding, storage,
indexing, and search pipeline.

## Capabilities

- Axum/Tokio confined to the HTTP transport shell for health, limits, ingest,
  search, protected admin version controls, and protected shutdown.
- Synchronous operation pipelines behind the transport shell for storage,
  model calls, Docling process handling, admission, shutdown state, and CLI
  operation.
- Interactive `data-store` CLI client for operating the documented HTTP API.
- Explicit Metal/CUDA accelerator selection for local Candle inference with no
  CPU fallback.
- Local Qwen3 dense embedding and ColBERT runtimes through Candle, plus a
  config-selected final reranker backend: local Candle ModernBERT or an HTTP
  Cohere-compatible rerank endpoint.
- PDF ingest through Docling, deterministic unit splitting, dense and ColBERT
  embedding, SQLite persistence, and FTS5 population.
- Startup-loaded in-memory dense vector cache with exact cosine retrieval.
- SQLite FTS5 BM25, dense/BM25 over-fetch, RRF fusion, persisted ColBERT
  MaxSim reranking, and config-selected final reranking.
- Immutable source-document versions with active-version publish and protected
  rollback.
- Explicit `--setup-storage` schema setup; normal runtime validates existing
  storage without creating or migrating schema.
- Config-backed request limits, fail-fast admission gates, and file logging.

## Ingestion

Ingestion is the process of making a document searchable.

```text
+------------------------------------------------------------------------------+
| 1. Request ingest for a corpus-relative source file                          |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 2. Validate request, resolve source, enforce limits/admission                |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 3. Convert PDF to text with configured Docling settings                      |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 4. Split text into retrieval units with source/version/page metadata         |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 5. Generate dense embeddings for each unit for semantic vector search        |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 6. Generate ColBERT token vectors for late-interaction scoring               |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 7. Write SQLite transaction: version, units, vectors, and FTS index          |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 8. Publish active snapshot and swap in-memory dense cache                    |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 9. Searches can now see the new active document version                      |
+------------------------------------------------------------------------------+
```

1. **The user requests ingest**

   A client sends an ingest operation with a corpus-relative source path, for
   example:

   ```json
   {
     "operation": "ingest",
     "payload": {
       "source": "The_Elements_of_Style.pdf"
     }
   }
   ```

   The service resolves that path inside its configured corpus root. File bytes
   are never sent through the HTTP request.

2. **The service checks admission and source validity**

   The service validates the request shape and field limits, then checks whether
   ingest capacity is available. If too many ingests are already running, it
   fails clearly with `503 Service Unavailable` instead of hiding the work in an
   internal queue.

   It also checks whether the document already has an active version. By
   default, ingest refuses to overwrite an existing active document. A caller
   must explicitly use `force: true` to create and publish a replacement
   version.

3. **PDF conversion runs through Docling**

   For PDFs, the service launches the configured Docling executable as a
   separate process. Docling converts the PDF into markdown-like text.

   Conversion behavior is controlled by service config, not by the request. That
   includes PDF backend, OCR mode, Docling device, timeout, thread count, and
   page batch size. The service does not silently switch parser backends or OCR
   modes if conversion fails.

4. **The converted text is split into retrieval units**

   The service breaks the converted document into deterministic searchable
   units. These are the chunks that search can return later.

   Each unit keeps useful metadata such as source path, version label, sequence,
   headings, page numbers, token counts, and content.

5. **The service creates a new immutable document version**

   Every successful ingest creates a new version for that source document. Older
   versions are retained.

   This matters because search visibility is versioned. A new ingest does not
   gradually leak into search while it is being processed. The new version
   becomes searchable only after all required storage, indexes, and in-memory
   cache updates are ready.

   ```text
   Before ingest:           During ingest:             After publish:
   +------------------+     +------------------+       +------------------+
   | Search sees      |     | Search still sees|       | Search sees      |
   | active version A |     | active version A |       | active version B |
   +------------------+     +------------------+       +------------------+
                                            |
                                            v
                                   +------------------+
                                   | Build version B  |
                                   | off to the side  |
                                   +------------------+

   Older version A is retained for rollback after B becomes active.
   ```

6. **Dense vectors are generated**

   Each retrieval unit is embedded with the local dense embedding model. These
   dense vectors are used later for semantic similarity search.

   The service validates vector dimensions, values, and norms before storing or
   using them.

7. **ColBERT document vectors are generated**

   Each retrieval unit also gets ColBERT document-token vectors. These are
   persisted in SQLite and used during search for late-interaction scoring.

   Search does not recompute ColBERT document vectors as a fallback. If the
   stored vectors are missing or invalid, that is an explicit data/runtime
   problem.

8. **Durable storage and indexes are written**

   The service writes the new document version, units, dense vectors, ColBERT
   vectors, and FTS5 lexical index rows to SQLite.

   Publishing the active document version happens as part of the same durable
   operation. If the new version cannot be fully stored and prepared, it does
   not become searchable.

9. **The active search cache is swapped**

   The service keeps an in-memory dense-vector cache for active document
   versions. After the new version is durable and cache-ready, the active cache
   is swapped.

   From that point on, new searches can see the newly ingested version.

## Search

Search is the process of taking a user query and finding the best matching
retrieval units.

```text
+--------------+     +-----------------+     +-------------------------+
| Search query |---->| Capture active  |---->| Embed query once        |
|              |     | snapshot        |     | for retrieval pipeline  |
+--------------+     +-----------------+     +-----------+-------------+
                                                          |
                         +--------------------------------+----------------+
                         |                                                 |
                         v                                                 v
             +---------------------+     +---------------------+
             | Dense vector search |     | BM25 lexical search |
             | semantic matches    |     | keyword matches     |
             +----------+----------+     +----------+----------+
                         |                                                 |
                         +------------------------+------------------------+
                                                  |
                                                  v
+--------------+     +-----------------+     +-------------------------+
| Top-K result |<----| Final reranker  |<----| RRF fusion + ColBERT    |
| + raw diag   |     | local or HTTP   |     | MaxSim reranking        |
+--------------+     +-----------------+     +-------------------------+
```

1. **The user sends a query**

   A client sends a search operation:

   ```json
   {
     "operation": "search",
     "payload": {
       "query": "clear writing style rules",
       "topK": 3
     }
   }
   ```

   `topK` controls how many final results the user wants.

2. **The service captures one active snapshot**

   At request admission, the service captures the active document-version map
   and dense-vector cache.

   This snapshot is used for the entire search. Even if another ingest finishes
   while the search is running, this search continues using the same captured
   corpus view. That prevents mixed-version or timing-dependent results.

3. **The query is embedded with the dense model**

   The service embeds the query using the dense embedding model, then validates
   the query vector.

4. **Dense semantic retrieval runs**

   The query vector is compared against the active dense-vector cache using
   exact cosine similarity.

   This finds units that are semantically similar to the query, even if they do
   not share exact words.

5. **BM25 lexical retrieval runs**

   In parallel conceptually, the service also searches SQLite FTS5 using BM25.

   This finds units with strong keyword or phrase overlap.

6. **Dense and BM25 candidates are fused**

   The service combines dense results and BM25 results using Reciprocal Rank
   Fusion, or RRF.

   This gives the pipeline a broader candidate pool: semantic matches, lexical
   matches, and items that score well in both.

7. **ColBERT reranking runs**

   The service takes the fused candidate pool, embeds the query with ColBERT,
   loads the stored ColBERT document vectors for the candidate units, and
   computes MaxSim scores.

   ColBERT is more precise than the first-stage dense/BM25 retrieval because it
   compares query-token and document-token representations rather than relying
   on a single vector per unit.

8. **The final reranker scores candidates**

   The ColBERT-ranked candidates are passed to the final reranker.

   This reranker is config-selected:

   - `backend = "local"` uses the local Candle ModernBERT reranker.
   - `backend = "http"` sends candidates to a Cohere-compatible HTTP rerank
     endpoint, such as vLLM, Cohere, or Jina-style APIs.

   ```text
   +-------------------------+     +-------------------------+
   | ColBERT-ranked pool     |---->| models.reranker.backend |
   | candidate units         |     | selects one backend     |
   +-------------------------+     +------------+------------+
                                                |
                            +-------------------+-------------------+
                            |                                       |
                            v                                       v
                +----------------------+              +----------------------+
                | local                |              | http                 |
                | Candle ModernBERT    |              | Cohere-compatible    |
                +----------+-----------+              +----------+-----------+
                           |                                     |
                           +------------------+------------------+
                                              |
                                              v
                                 +-------------------------+
                                 | Final ranked top-K      |
                                 | no backend fallback     |
                                 +-------------------------+
   ```

   The reranker candidate pool is separate from `topK`. For example, the user
   may request 10 results, while the reranker evaluates 40 candidates and
   chooses the best 10. This gives the strongest ranking model room to improve
   the final result set.

9. **Final top-K results are returned**

   The public results are ordered by final reranker score. Each result includes
   the unit ID, score, content, heading path, source path, and page numbers.

   The response also includes raw diagnostic data for the retrieval stages. That
   raw data preserves how dense search, BM25, RRF, ColBERT, and the final
   reranker contributed to the result. For the HTTP reranker, fields like raw
   logits and token counts are omitted because the remote API does not provide
   them.

## Important Guarantees

- Runtime never creates or migrates the SQLite schema. Storage setup is
  explicit.
- Ingested versions are immutable.
- Search uses one consistent active snapshot for the whole request.
- The service does not silently fall back between models, devices, OCR modes,
  parser backends, or reranker backends.
- Long-running operations stream progress to clients and also write durable
  service logs.
- HTTP reranker failures are explicit; they do not silently fall back to the
  local reranker.
- Public search results are concise, while raw diagnostics remain available for
  debugging and evaluation.

## Using the Service

The service is two binaries: `data-store-service`, the server, and `data-store`,
the bundled CLI client. The server must be running before you send operations;
see `INSTALL.md` for first-time setup and startup. The sections below cover the
documented operation API and the `data-store` CLI client.

## Use The CLI Client

The `data-store` client supports both an interactive REPL and one-shot command
invocation over the same documented `POST /v1/operations` stream API. Start the
service first, then start the interactive client from this directory:

```bash
cargo run --bin data-store -- --config config.toml
```

For a release build, run:

```bash
./target/release/data-store --config config.toml
```

When no operation flag is supplied, the client opens the interactive REPL.
For non-interactive use, pass exactly one operation flag:

```bash
./target/release/data-store --config config.toml --help
./target/release/data-store --config config.toml --health
./target/release/data-store --config config.toml --limits
./target/release/data-store --config config.toml --sources
./target/release/data-store --config config.toml --ingest The_Elements_of_Style.pdf
./target/release/data-store --config config.toml --ingest The_Elements_of_Style.pdf --force
./target/release/data-store --config config.toml --search "clear writing style rules" 3
./target/release/data-store --config config.toml --search-full "clear writing style rules" 3
./target/release/data-store --config config.toml --versions
./target/release/data-store --config config.toml --rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
./target/release/data-store --config config.toml --shutdown
```

The same one-shot command surface is available during development:

```bash
cargo run --bin data-store -- --config config.toml --health
```

The client reads `server.bind_address`, `admin.token_file_path`, and
`client.operation_timeout_seconds` from the same config file. Public commands
send unauthenticated operations. Protected commands read the configured token
file immediately before sending the operation. If the token file is missing,
start or restart the service.

At the prompt, run `help` to show the available commands and syntax:

```text
data-store> help
```

Common commands:

```text
data-store> health
data-store> limits
data-store> sources
data-store> ingest The_Elements_of_Style.pdf
data-store> ingest The_Elements_of_Style.pdf --force
data-store> search "clear writing style rules" 3
data-store> search-full "clear writing style rules" 3
data-store> versions
data-store> rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
data-store> shutdown
data-store> help
data-store> exit
```

`search` prints excerpts. `search-full` prints the full matched unit content.
Both search commands print a server-authoritative `Benchmarks:` summary after
the rendered search results, computed by the server and rendered by the client
with no client-side timing. It includes `search_preparation`, the streamed
search stages with the nested `retrieving_candidates` substages, and a
server-reported `Total` (the whole-operation duration). Rows may not sum exactly
to `Total`; the small unattributed remainder is shown rather than hidden.
`ingest` prints a server-authoritative `Benchmarks:` summary after the ingest
result, computed by the server and rendered by the client with no client-side
timing. It includes `docling_converting`, `unit_splitting`, `dense_embedding`,
`colbert_embedding`, and `storage_publishing` with its nested
`vector_validation`, `document_persistence`, `cache_preparation`, and `commit`
substages, and a server-reported `Total` (the whole-operation duration). Rows
may not sum exactly to `Total`; the small unattributed remainder is shown rather
than hidden. Quote multi-word queries and any argument containing spaces. Long
operations stream status and counted progress while they run. The CLI updates
the current stage line in place and prints a newline when each stage completes.
The `shutdown` command sends the protected operation directly and prints the
server-authored `shutdown_complete` confirmation from the terminal result
event. Any shutdown stream error or non-completion status is reported as an
operator-visible error.

The client prints human-readable output and stores readline history in
`.data-store.history`.

## Operation API

The documented consumer API is one streamed operation endpoint:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Each request starts one operation. The response is newline-delimited JSON with
`status`, `progress`, `result`, and `error` events. The stream ends after one
terminal `result` or `error` event. See `PROTOCOL.md` for the complete event
contract.

Public operations do not require authentication. Protected operations require
the startup-scoped bearer token printed as `admin_shutdown_token=<token>` or
written to the configured admin token file.

`GET /v1/health` is a supported readiness and liveness route for operators,
startup handoff, and the CLI's post-stream-loss probe. All other consumer
operations use `POST /v1/operations`. Protected operations use the same
operation endpoint with bearer authorization.

## Health And Limits

Check readiness:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"health","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

The top-level `ready` flag is based on readiness-critical components:

- `inference`: accelerator, model artifacts, tokenizer/model load, configured
  reranker backend, and startup smoke checks, including ColBERT max-capacity
  document encoding and reranker backend smoke scoring
- `storage_cache`: SQLite schema validation and active dense-cache load

Diagnostic-only components remain visible without making the service unready:

- `admission`: current ingest/search in-flight counters
- `logging`: initialized file log path and level

Fetch request and retrieval limits before constructing ingest or search
requests:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"limits","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

Oversized bodies return `413 Payload Too Large`. Oversized fields, unknown JSON
fields, and invalid request values return `400 Bad Request`.

## Ingest A Source

Ingest uses a corpus-relative source reference. File bytes do not cross the
HTTP API; the service resolves the source inside its configured corpus root.

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"ingest","payload":{"source":"The_Elements_of_Style.pdf"}}' \
  http://127.0.0.1:8091/v1/operations
```

If the resolved source already has an active version, ingest aborts before
conversion unless the request includes `force: true`. The CLI and REPL expose
that override as `--force`.

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"ingest","payload":{"source":"The_Elements_of_Style.pdf","force":true}}' \
  http://127.0.0.1:8091/v1/operations
```

PDF conversion progress comes from Docling stderr progress lines when Docling
emits them. The service passes `[docling].document_timeout_seconds` to Docling
as `--document-timeout`, `[docling].pdf_backend` as `--pdf-backend`,
`[docling].device` as `--device`, `[docling].num_threads` as `--num-threads`,
and `[docling].page_batch_size` as `--page-batch-size`. OCR behavior is
explicitly config-backed: `ocr_mode = "on"` passes `--ocr`, `ocr_mode = "off"`
passes `--no-ocr`, and `ocr_mode = "auto"` leaves Docling's CLI default in
control. The interactive CLI uses `[client].operation_timeout_seconds` as the
HTTP stream timeout for one operation.

## Run a Search

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"search","payload":{"query":"clear writing style rules","topK":3}}' \
  http://127.0.0.1:8091/v1/operations
```

The terminal `result` event contains public results plus raw stage diagnostics.
Reranker raw diagnostics expose a backend-dependent `mode`:
`modernbert_sequence_classifier` for the local backend and `http_rerank` for the
HTTP backend. Local diagnostics
include raw `logit` and `tokenCount` values when available; HTTP diagnostics
omit those fields and use the provider `relevance_score` as the public score.

If the configured ingest/search admission gate is saturated, the endpoint
emits a terminal `error` event with status `503 Service Unavailable`. The
service does not hide work in an unbounded queue.

## List Ingested Sources

`sources` is a public operation that lists source documents with an active
version.

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"sources","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

The terminal `result` payload contains a `sources` array, one entry per source
document that currently has an active version, ordered by source path ascending.
Each entry reports `sourcePath` (corpus-relative reference), `activeVersionLabel`
(the active version), `documentId` (the active version's document identifier),
`unitsIngested` (retrieval units stored for the active version), `status`
(stored ingest status), and `createdAtMs`/`updatedAtMs` (millisecond timestamps
for the active-version row).

`sources` lists only active documents; it does not list retained non-active
versions — use `versions` for full version history. It consumes no ingest/search
admission permits and emits a single `sources_listing` status stage before the
terminal result.

## Version Administration

Protected operations require the startup-scoped bearer token printed as
`admin_shutdown_token=<token>` or written to the configured admin token file.

List retained document versions:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"operation":"versions","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

Rollback one source document to an already-retained version:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"operation":"rollback","payload":{"source":"The_Elements_of_Style.pdf","versionLabel":"2026-06-01T21:37:22.184Z"}}' \
  http://127.0.0.1:8091/v1/operations
```

Rollback repoints `active_document_versions` and publishes a new active search
snapshot. It does not delete versions, rebuild embeddings, or run automatic
cleanup.

## Shutdown

Use the startup token for graceful shutdown:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"operation":"shutdown","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

Shutdown requests drain through Axum graceful shutdown instead of requiring a
process signal. The operation stream's terminal result is the authoritative
shutdown confirmation and contains `status: "shutdown_complete"` plus a
server-authored message emitted as the final confirmation before process
termination. If shutdown cannot be requested, the stream emits a terminal
`error` with the reason.

The CLI `shutdown` command sends the same protected operation and requires the
`shutdown_complete` terminal result before displaying the confirmation.

## Common Operator Diagnostics

- Missing SQLite database: run `--setup-storage` deliberately, then restart the
  service.
- SQLite schema version or table mismatch: the runtime will not migrate; rebuild
  or set up the development database intentionally.
- Requested accelerator unavailable or not compiled: rebuild with the matching
  Cargo feature and verify the configured device.
- Missing model artifacts or tokenizer files: fix local model paths in
  `config.toml`; readiness must fail clearly rather than falling back.
- HTTP reranker startup or search failure: inspect the API error and service
  log for endpoint, status, score-count, elapsed-time, and bounded response-body
  context. The service does not silently retry through the local reranker.
- Docling conversion failure: inspect the API error and service log. The service
  does not silently switch PDF backends or OCR modes.
- `401 Unauthorized` on protected operations: use the current startup token
  from stdout or the configured admin token file; tokens do not survive restart.
- Missing admin token file for the CLI: start or restart the service with a
  config that includes `[admin].token_file_path`.
- `503 Service Unavailable` on ingest/search: the configured in-flight limit is
  saturated. Retry after active work finishes or change the config deliberately.
