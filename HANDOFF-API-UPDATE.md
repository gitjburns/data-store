# API Update Code Inspection Handoff

## Purpose

This file captures the code inspection already performed for the
operation-stream API migration. It is intended to prevent the next
implementation session from spending tokens re-reading the same source files.

All details needed from the code inspection are included in this file. No
source files, config files, Cargo files, or existing service docs were modified
as part of this inspection handoff. Only the root onboarding documents
`PLAN-API-UPDATE.md` and `HANDOFF-API-UPDATE.md` were created. Therefore, do
not perform broad source exploration again before implementing the first split,
unless an actual compile error or implementation mismatch proves that the
source has changed since this handoff was written.

## High-Level State

The service and CLI are currently route-specific.

The intended target protocol, documented in `PROTOCOL.md`, `SPEC-SERVER.md`,
`SPEC-CLIENT.md`, `PLAN-SERVER.md`, and `PLAN-CLIENT.md`, is the universal
operation stream:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

The current implementation has not yet added operation request DTOs, operation
event DTOs, NDJSON streaming, or the operation dispatcher.

## Server Routing

File: `service/data-store/src/http.rs`

Current router shape:

```rust
pub fn build_router(state: Arc<AppState>) -> Router {
    let max_request_body_bytes = state.config.server.max_request_body_bytes;

    Router::new()
        .route("/v1/health", get(get_health))
        .route("/v1/limits", get(get_limits))
        .route("/v1/ingest", post(post_ingest))
        .route("/v1/search", post(post_search))
        .route("/admin/shutdown", post(post_admin_shutdown))
        .route("/admin/document-versions", get(get_admin_document_versions))
        .route(
            "/admin/document-versions/rollback",
            post(post_admin_document_version_rollback),
        )
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}
```

Implications:

- `POST /v1/operations` does not exist.
- `POST /v1/operations/{operationId}/control` does not exist.
- Current routes should likely be preserved during the migration unless the user
  explicitly approves removing them.
- The request body limit is already centralized on the router through
  `DefaultBodyLimit::max(max_request_body_bytes)`.

## Current Server Handlers

File: `service/data-store/src/http.rs`

### Health

Current handler:

```rust
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(state.health())
}
```

This can be reused by an operation implementation conceptually, but the helper
is currently an Axum handler. Best implementation shape is to extract or add a
small plain function for the response body if needed.

### Limits

Current handler builds `LimitsResponse` inline from config:

```rust
async fn get_limits(State(state): State<Arc<AppState>>) -> Json<LimitsResponse> {
    Json(LimitsResponse {
        request: RequestLimitsResponse {
            max_request_body_bytes: state.config.server.max_request_body_bytes,
            max_ingest_source_chars: state.config.server.max_ingest_source_chars,
            max_search_query_chars: state.config.server.max_search_query_chars,
        },
        retrieval: RetrievalLimitsResponse {
            default_top_k: state.config.retrieval.default_top_k,
            max_top_k: state.config.retrieval.max_top_k,
        },
    })
}
```

This response construction should be shared by route and operation paths.

### Ingest

Current `post_ingest` is one monolithic async handler. Its sequence is:

1. Start timer.
2. Parse `Json<IngestRequest>`.
3. Validate `source` using `request.validate`.
4. Acquire ingest admission with `state.try_acquire_ingest_admission()`.
5. Log admission or rejection.
6. Check `state.inference()?` and `state.storage()?`.
7. Resolve source with `resolve_source_reference`.
8. Convert source to markdown with `convert_source_to_markdown(...).await`.
9. Split conversion into units with `split_conversion_into_units`.
10. Allocate version label with `allocate_version_label`.
11. Build versioned document ID with `build_versioned_document_id`.
12. Assign units to version with `assign_units_to_document_version`.
13. Get inference runtime.
14. Dense embed every unit:

    ```rust
    let vectors = units
        .iter()
        .map(|unit| {
            inference
                .dense
                .embed_passage_vector(&unit.content)
                .map(|vector| UnitDenseVector {
                    unit_id: unit.unit_id.clone(),
                    vector,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    ```

15. ColBERT embed every unit:

    ```rust
    let colbert_vectors = units
        .iter()
        .map(|unit| {
            inference
                .colbert
                .embed_document(&unit.unit_id, &unit.content)
                .map(|embedding| UnitColbertDocumentVector {
                    unit_id: embedding.unit_id,
                    token_count: embedding.token_count,
                    dimension: embedding.dimension,
                    vector: embedding.vector,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    ```

16. Persist and publish with `storage.ingest_document(...)`.
17. Log operational summary.
18. Return `IngestResponse`.

Implications:

- The server operation foundation can call the same ingest logic if it is
  extracted to a plain async function returning `IngestResponse`.
- Real counted dense and ColBERT ingest progress can be added at the loops in
  the handler without touching inference internals.
- Storage publish progress is currently one call into storage. Fine-grained
  storage progress would require changing `storage.ingest_document`, but a
  real `storage_publishing` status before and completion status after the call
  is straightforward.

### Search

Current `post_search` is one monolithic async handler. Its sequence is:

1. Start timer.
2. Parse `Json<SearchRequest>`.
3. Validate query and topK with `request.validate`.
4. Acquire search admission with `state.try_acquire_search_admission()`.
5. Log admission or rejection.
6. Resolve `top_k` from request or config default.
7. Get inference and storage runtimes.
8. Dense embed query:

    ```rust
    let query_vector = inference.dense.embed_query_vector(&request.query)?;
    ```

9. Build storage candidate pool:

    ```rust
    let storage_output = storage.build_search_candidate_pool(
        &request.query,
        query_vector,
        top_k,
        &state.config.retrieval,
    )?;
    ```

10. Convert storage candidates into `ColbertDocumentEmbedding` values.
11. Score ColBERT persisted candidates:

    ```rust
    let colbert_scores = inference
        .colbert
        .score_persisted_candidates(&request.query, &colbert_candidates)?;
    ```

12. Build reranker candidates with `build_reranker_candidates`.
13. Score reranker candidates:

    ```rust
    let reranker_scores = inference
        .reranker
        .score_candidates(&request.query, &reranker_candidates)?;
    ```

14. Build public results and final raw diagnostics with `build_reranker_results`.
15. Build `raw` search diagnostics.
16. Log operational summary.
17. Return `SearchResponse`.

Implications:

- The server operation foundation can call the same search logic if it is
  extracted to a plain async function returning `SearchResponse`.
- Query embedding and candidate retrieval status events are simple boundaries.
- Counted ColBERT and reranker progress require changing inference APIs or
  adding alternate callback-enabled methods.

### Admin Shutdown

Current handler:

1. Extract bearer token with `bearer_token_from_headers`.
2. Validate with `state.authorize_admin_token`.
3. Call `state.request_shutdown`.
4. Return `ShutdownResponse { status: "shutting_down" }`.

For operation dispatch, auth must be checked only for protected operation names.

### Admin Versions

Current handler:

1. Extract and authorize bearer token.
2. Call `state.storage()?`.
3. Return `storage.list_document_versions()?`.

Response type is `crate::storage::DocumentVersionListing`, not in `types.rs`.
It is already serializable.

### Admin Rollback

Current handler:

1. Extract and authorize bearer token.
2. Parse `Json<DocumentVersionRollbackRequest>`.
3. Validate with `request.validate`.
4. Call `storage.rollback_document_version`.
5. Map result to `DocumentVersionRollbackResponse`.

This can be extracted into a reusable operation helper.

## Error Handling

File: `service/data-store/src/error.rs`

Current `ApiError` variants:

- `ConfigRead`
- `ConfigParse`
- `InvalidConfig`
- `InvalidCli`
- `InferenceInit`
- `SourceResolution`
- `DoclingUnavailable`
- `DoclingConversion`
- `InternalIo`
- `UnitSplitting`
- `StorageInit`
- `StorageOperation`
- `BadRequest`
- `PayloadTooLarge`
- `Unauthorized`
- `ServiceUnavailable`

Important current private methods:

```rust
fn status_code(&self) -> StatusCode
fn error_kind(&self) -> &'static str
```

Current status mapping:

- `BadRequest` and `SourceResolution` -> 400
- `PayloadTooLarge` -> 413
- `Unauthorized` -> 401
- `ServiceUnavailable` -> 503
- `DoclingConversion` -> 422
- config/init/internal/storage/unit/docling-unavailable style failures -> 500

Current kind mapping includes stable strings such as:

- `config_read`
- `config_parse`
- `invalid_config`
- `invalid_cli`
- `inference_init`
- `source_resolution`
- `docling_unavailable`
- `docling_conversion`
- `internal_io`
- `unit_splitting`
- `storage_init`
- `storage_operation`
- `bad_request`
- `payload_too_large`
- `unauthorized`
- `service_unavailable`

Current HTTP error body is:

```json
{
  "error": {
    "message": "..."
  }
}
```

The operation protocol needs structured error details:

```json
{
  "status": 422,
  "kind": "docling_conversion",
  "message": "failed to convert source document"
}
```

Implementation note:

- Make `ApiError::status_code` and `ApiError::error_kind` public or expose a
  public `to_error_detail`/`to_operation_error` method.
- Preserve existing logging behavior in `IntoResponse`.
- If pre-stream HTTP errors are updated to include `status`, `kind`, and
  `message`, update the CLI error parser later.

## DTOs

File: `service/data-store/src/types.rs`

Current DTOs are route-specific:

- `HealthResponse`
- `HealthComponent`
- `LimitsResponse`
- `RequestLimitsResponse`
- `RetrievalLimitsResponse`
- `IngestRequest`
- `IngestResponse`
- `SearchRequest`
- `SearchResponse`
- `SearchResult`
- `ShutdownResponse`
- `DocumentVersionRollbackRequest`
- `DocumentVersionRollbackResponse`

Request DTOs use `#[serde(deny_unknown_fields)]`.

Operation DTOs do not exist. Needed DTOs:

- Operation request envelope:

  ```rust
  #[derive(Debug, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct OperationRequest {
      #[serde(rename = "operationId")]
      pub operation_id: Option<String>,
      pub operation: String,
      pub payload: serde_json::Value,
  }
  ```

- Event model for `status`, `progress`, `result`, and `error`.
- Structured error detail with `status`, `kind`, and `message`.
- Control request envelope for `{ "type": "cancel" }` or an explicit reserved
  shape.

Recommended event serialization approach:

- Use `#[serde(tag = "type")]` for event variants, or explicit structs if
  Axum/body code is simpler.
- Keep payload as `serde_json::Value` at the stream event boundary so each
  operation can serialize existing response structs into payload values.
- Enforce monotonic `sequence` in one small event writer/emitter helper rather
  than in each operation branch.

## Storage Boundaries

File: `service/data-store/src/storage.rs`

Important types:

- `StorageRuntime`
- `UnitDenseVector`
- `UnitColbertDocumentVector`
- `SearchCandidatePoolOutput`
- `SearchCandidate`
- `DocumentVersionListing`
- `DocumentVersionRollbackResult`

### `ingest_document`

Signature:

```rust
pub fn ingest_document(
    &self,
    conversion: &DoclingConversionResult,
    version_label: &str,
    units: &[RetrievalUnit],
    vectors: Vec<UnitDenseVector>,
    colbert_vectors: Vec<UnitColbertDocumentVector>,
    dense: &DenseModelConfig,
    colbert: &ColbertModelConfig,
) -> Result<(), ApiError>
```

Current behavior:

- Validates dense vector count matches unit count.
- Validates ColBERT vector count matches unit count.
- Validates dense vector dimensions.
- Validates ColBERT document vector dimensions.
- Reads source bytes for checksum.
- Computes source and markdown SHA256.
- Builds diagnostics.
- Opens SQLite connection.
- Starts explicit transaction.
- Inserts document row.
- Loops over units and vectors:
  - `insert_unit`
  - `insert_dense_vector`
  - `insert_colbert_document_vector`
- Commits transaction.
- Publishes active document version and swaps cache.
- Logs `storage.ingest_version.published`.

Progress implication:

- A simple real `storage_publishing` status before/after this call is enough
  for the first progress pass.
- Counted per-unit storage progress would require adding callback plumbing
  inside this function.

### `build_search_candidate_pool`

Signature:

```rust
pub fn build_search_candidate_pool(
    &self,
    query: &str,
    query_vector: Vec<f32>,
    top_k: u32,
    retrieval: &RetrievalConfig,
) -> Result<SearchCandidatePoolOutput, ApiError>
```

Current behavior:

- Validates query vector.
- Computes candidate limit.
- Captures dense cache snapshot under lock, then releases lock.
- Runs exact dense search on snapshot.
- Builds FTS query.
- Runs BM25 filtered to active versions.
- Fuses dense and BM25 matches with RRF.
- Loads unit rows for fused matches.
- Builds `SearchCandidate` values including persisted ColBERT vectors.
- Builds raw retrieval diagnostics.

Progress implication:

- Candidate retrieval is one good status boundary.
- Dense/BM25 sub-progress would require changing storage internals, but this is
  not needed for the first operation foundation.

## Inference Boundaries

### Dense

File: `service/data-store/src/inference/dense.rs`

Public methods:

```rust
pub fn embed_passage_vector(&self, text: &str) -> Result<Vec<f32>, ApiError>
pub fn embed_query_vector(&self, text: &str) -> Result<Vec<f32>, ApiError>
```

Dense ingest progress can be emitted by the caller loop in `http.rs`, so dense
runtime changes are not required for counted dense ingest progress.

### ColBERT

File: `service/data-store/src/inference/colbert.rs`

Public methods:

```rust
pub fn embed_document(
    &self,
    unit_id: &str,
    document: &str,
) -> Result<ColbertDocumentEmbedding, ApiError>

pub fn score_persisted_candidates(
    &self,
    query: &str,
    candidates: &[ColbertDocumentEmbedding],
) -> Result<Vec<ColbertCandidateScore>, ApiError>
```

`embed_document` is called from an outer loop in `http.rs`, so counted ingest
ColBERT progress can be emitted without changing this method.

`score_persisted_candidates` contains an internal loop over candidates. Counted
search ColBERT progress requires one of these approaches:

- Add a callback parameter to `score_persisted_candidates`.
- Add a new callback-enabled method and keep the old method as a wrapper.

Prefer the second approach if keeping route compatibility simple.

### Reranker

File: `service/data-store/src/inference/reranker.rs`

Public method:

```rust
pub fn score_candidates(
    &self,
    query: &str,
    candidates: &[RerankerCandidateInput],
) -> Result<Vec<RerankerCandidateScore>, ApiError>
```

This method contains an internal loop over candidates. Counted reranker
progress requires one of these approaches:

- Add a callback parameter to `score_candidates`.
- Add a new callback-enabled method and keep the old method as a wrapper.

Prefer the second approach if keeping route compatibility simple.

## App State And Auth

File: `service/data-store/src/state.rs`

Important methods:

- `inference(&self) -> Result<&InferenceRuntime, ApiError>`
- `storage(&self) -> Result<&StorageRuntime, ApiError>`
- `try_acquire_ingest_admission`
- `try_acquire_search_admission`
- `ingest_admission_snapshot`
- `search_admission_snapshot`
- `authorize_admin_token(&self, candidate: &str) -> Result<(), ApiError>`
- `request_shutdown(&self) -> Result<(), ApiError>`
- `health(&self) -> HealthResponse`

Auth note:

- `authorize_admin_token` compares submitted token with the startup token using
  a constant-time helper.
- Operation dispatch should call auth only for protected operation names:
  `versions`, `rollback`, and `shutdown`.
- Public operations must not require an Authorization header.

## Startup Output

File: `service/data-store/src/main.rs`

Current startup ready line includes route-specific health URL:

```rust
reporter.report(format!(
    "data-store startup ready={ready} inference={inference_ready} storage_cache={storage_ready} health_url=http://{bind_address}/v1/health"
))?;
```

This is another known route-specific artifact. It does not need to be changed
in the server operation foundation unless the user approves including startup
output cleanup in that scope.

## Streaming Implementation Notes

Current `Cargo.toml` dependencies include:

- `axum = "0.8.9"`
- `tokio = { version = "1.52.3", features = ["full"] }`
- `serde`
- `serde_json`
- `reqwest` with blocking/json for the CLI

No stream helper dependency such as `tokio-stream`, `async-stream`, or
`futures-util` is currently present.

Implementation choices:

1. Add a small explicit stream helper dependency.
2. Hand-roll Axum body streaming with existing dependencies.

Recommendation:

- Prefer a small explicit dependency if needed for clean NDJSON streaming, but
  this is a `Cargo.toml` and `Cargo.lock` change, so get explicit config/build
  file approval before editing.
- If avoiding dependency changes, inspect Axum 0.8 body APIs narrowly during
  implementation only as needed.

Possible server design:

- `OperationEmitter` owns `operation_id` and `sequence`.
- It serializes each event to one JSON line plus `\n`.
- The stream response content type is `application/x-ndjson`.
- Accepted operation failures are emitted as terminal `error` events.
- Envelope parse errors, oversized request bodies, malformed JSON, invalid
  protected auth before stream open, and similar pre-stream failures return
  structured HTTP errors.

Potential issue:

- Some operation work is synchronous and CPU/GPU heavy. If the stream uses an
  async channel, operation execution may need to run in an async task and send
  events through the channel. Avoid holding global locks while sending events.

## CLI State

File: `service/data-store/src/bin/data-store.rs`

The CLI is compact and route helpers are isolated. It should not need broad
rewrites.

Current config context:

```rust
struct ClientContext {
    base_url: String,
    token_file_path: PathBuf,
    http: Client,
}
```

Current base URL construction:

```rust
base_url: format!("http://{}", config.server.bind_address)
```

This does not currently map unspecified bind addresses to loopback.

Current command enum:

- `Health`
- `Limits`
- `Ingest { source }`
- `Search { query, top_k }`
- `SearchFull { query, top_k }`
- `Versions`
- `Rollback { source, version_label }`
- `Shutdown`
- `Help`
- `Exit`

Current response DTOs are local to the CLI and mostly mirror server DTOs. They
can be reused for terminal `result.payload` deserialization.

## CLI Current Transport

File: `service/data-store/src/bin/data-store.rs`

Current `execute_command` routes directly:

```rust
Command::Health => render_health(public_get(context, "/v1/health")?),
Command::Limits => render_limits(public_get(context, "/v1/limits")?),
Command::Ingest { source } => {
    let request = IngestRequest { source };
    render_ingest(public_post_json(context, "/v1/ingest", &request)?);
}
Command::Search { query, top_k } => {
    let request = SearchRequest { query, top_k };
    render_search(public_post_json(context, "/v1/search", &request)?, false);
}
Command::SearchFull { query, top_k } => {
    let request = SearchRequest { query, top_k };
    render_search(public_post_json(context, "/v1/search", &request)?, true);
}
Command::Versions => render_versions(admin_get(context, "/admin/document-versions")?),
Command::Rollback { source, version_label } => {
    let request = DocumentVersionRollbackRequest { source, version_label };
    render_rollback(admin_post_json(
        context,
        "/admin/document-versions/rollback",
        &request,
    )?);
}
Command::Shutdown => {
    if confirm_shutdown()? {
        render_shutdown(admin_post_empty(context, "/admin/shutdown")?);
    } else {
        println!("shutdown cancelled");
    }
}
```

Current helpers:

- `public_get`
- `public_post_json`
- `admin_get`
- `admin_post_json`
- `admin_post_empty`
- `read_admin_token`
- `url`
- `send_json`
- `service_error`

Migration shape:

- Replace route helpers with one `send_operation` helper.
- Keep command parsing unchanged.
- Keep renderers unchanged except for adding event rendering around them.
- Keep `read_admin_token` behavior: read token immediately before protected
  operation.

## CLI Rendering

Existing renderers are usable:

- `render_health`
- `render_limits`
- `render_ingest`
- `render_search`
- `render_versions`
- `render_rollback`
- `render_shutdown`
- `render_help`

Search and search-full already differ only by renderer flag.

Needed additions:

- Operation start output with operation name and target URL.
- Status event rendering as newline-terminated lines.
- Counted progress event rendering with carriage return overwrite.
- Finalize active progress line before printing the next status, result, or
  error.
- Elapsed time after terminal result or error.
- Terminal error rendering with operation, stage, status, kind, and message.
- Transport and stream error formatting with method, URL, and cause chain.

## CLI NDJSON Parsing

The CLI currently uses blocking `reqwest`.

Recommended approach:

- Continue using blocking `reqwest` unless implementation proves it is
  unsuitable.
- Send:

  ```http
  POST /v1/operations
  Accept: application/x-ndjson
  Content-Type: application/json
  ```

- For protected operations, add `Authorization: Bearer <token>`.
- Read the response as a stream/reader and parse line by line.
- Treat non-success HTTP status before stream parsing as transport/service
  failure.
- Require exactly one terminal event, either `result` or `error`.
- If stream ends before terminal event, report a clear client-side stream
  error.
- If a result payload fails to deserialize into the command-specific DTO,
  report result payload deserialization failure.

Potential DTO shape:

```rust
#[derive(Debug, Serialize)]
struct OperationRequest {
    #[serde(rename = "operationId", skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    operation: &'static str,
    payload: serde_json::Value,
}
```

For events, a simple local CLI enum can use `serde_json::Value` for result
payload:

```rust
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OperationEvent {
    #[serde(rename = "status")]
    Status { operationId: String, sequence: u64, stage: Option<String>, message: Option<String> },
    #[serde(rename = "progress")]
    Progress { operationId: String, sequence: u64, stage: Option<String>, message: Option<String>, current: Option<u64>, total: Option<u64> },
    #[serde(rename = "result")]
    Result { operationId: String, sequence: u64, payload: serde_json::Value },
    #[serde(rename = "error")]
    Error { operationId: String, sequence: u64, stage: Option<String>, error: OperationErrorDetail },
}
```

Adjust exact optional fields to match the server DTOs chosen during
implementation.

## Unspecified Bind Address Mapping

Current client uses the configured `SocketAddr` directly. If config binds the
service to `0.0.0.0:<port>` or `[::]:<port>`, the client should connect to
loopback instead.

Recommended behavior:

- IPv4 unspecified `0.0.0.0:<port>` -> `127.0.0.1:<port>`.
- IPv6 unspecified `[::]:<port>` -> `[::1]:<port>`.
- Specific configured addresses remain unchanged.

Implement this in base URL construction rather than per request.

## Recommended First Implementation Session

Scope:

Server operation foundation only.

Files likely involved:

- `service/data-store/src/types.rs`
- `service/data-store/src/error.rs`
- `service/data-store/src/http.rs`
- Possibly `service/data-store/Cargo.toml` and `Cargo.lock` only if a stream
  helper dependency is approved.

Do not include:

- CLI migration.
- Deep counted progress callbacks.
- README/architecture cleanup.
- Startup output cleanup.

Reason:

- This establishes the protocol shape without coupling it to the CLI migration.
- Existing route handlers can remain working.
- Progress instrumentation can be added after the client can consume the stream.

Estimated effort: 14k-22k tokens.

Confidence: 90%.

## Recommended Second Implementation Session

Scope:

Client operation migration.

Files likely involved:

- `service/data-store/src/bin/data-store.rs`

Possibly no Cargo changes if blocking reqwest stream parsing is sufficient.

Estimated effort: 12k-20k tokens.

Confidence: 92%.

## Recommended Third Implementation Session

Scope:

Server real progress instrumentation.

Files likely involved:

- `service/data-store/src/http.rs`
- `service/data-store/src/inference/colbert.rs`
- `service/data-store/src/inference/reranker.rs`
- Possibly `service/data-store/src/storage.rs` if counted storage publish
  progress is desired.

Estimated effort: 10k-18k tokens.

Confidence: 85%.

## Verification Commands

Run from `service/data-store/` after Rust changes:

```bash
cargo fmt
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual runtime verification is still required for actual service startup,
model loading, ingest, search, protected operations, streaming progress, and
shutdown. Those require local config, model artifacts, Docling, and an
operator-controlled running service.
