# Data Store Service

Standalone HTTP service for document ingestion and retrieval. This service is
intentionally independent from the existing Node backend and frontend. It owns
its own config, corpus, Docling conversion, SQLite storage, dense vector cache,
and retrieval pipeline, and it currently runs alongside the existing
Node-backed Data Store.

Backend/frontend integration is later scope. Do not expect this service to be
started or managed by the Node app yet.

Run commands in this document from `service/data-store/` unless stated
otherwise.

## Capabilities

- Axum/Tokio HTTP API for health, limits, ingest, search, protected admin
  version controls, and protected shutdown.
- Interactive `data-store` CLI client for operating the documented HTTP API.
- Explicit Metal/CUDA accelerator selection with no CPU fallback.
- Local Qwen3 dense embedding, ColBERT, and Qwen3 reranker runtimes through
  Candle.
- PDF ingest through Docling, deterministic unit splitting, dense and ColBERT
  embedding, SQLite persistence, and FTS5 population.
- Startup-loaded in-memory dense vector cache with exact cosine retrieval.
- SQLite FTS5 BM25, dense/BM25 over-fetch, RRF fusion, persisted ColBERT
  MaxSim reranking, and Qwen3 yes/no reranking.
- Immutable source-document versions with active-version publish and protected
  rollback.
- Explicit `--setup-storage` schema setup; normal runtime validates existing
  storage without creating or migrating schema.
- Config-backed request limits, fail-fast admission gates, and file logging.

## Prerequisites

Create a local `config.toml` from `config.example.toml`. The local config is
machine-specific and should point at:

- local model directories for Qwen3 embedding, ColBERT-Zero, and Qwen3 reranker
- a service-owned corpus root for source files
- a service-owned index root for SQLite and Docling conversion artifacts
- the Python executable for environment diagnostics and the directly launched Docling executable
- the selected accelerator backend and device index
- required request, retrieval, admission, logging, and admin token-file settings

The service has no CPU inference fallback. If `inference.device = "metal"`, run
with `--features metal`. CUDA support is separate operational verification
work; this runbook documents the locally used Metal path.

## First-Time Storage Setup

Run schema setup deliberately before normal service startup:

```bash
cargo run -- --config config.toml --setup-storage
```

Normal runtime never creates tables, runs migrations, or repairs stale schemas.
If startup health reports a missing database, missing table, or schema-version
error, stop the service and run the setup command intentionally.

## Start The Service

```bash
cargo run --features metal -- --config config.toml
```

Normal startup forks a detached background service after printing bootstrap
handoff details to stdout. Use `--foreground` to keep the service attached to
the current terminal while debugging startup:

```bash
cargo run --features metal -- --config config.toml --foreground
```

## Build A Binary

`cargo run` is convenient during development because it builds and starts the
service in one command. For sustained manual operation, build a reusable release
binary and run it directly. By default, the release binary backgrounds itself
after the startup handoff completes:

```bash
cargo build --release --features metal
./target/release/data-store-service --config config.toml
```

Run the release binary in the foreground when needed:

```bash
./target/release/data-store-service --config config.toml --foreground
```

The compiled binary uses the same command-line flags as `cargo run`. Run
first-time storage setup with the release binary when needed:

```bash
./target/release/data-store-service --config config.toml --setup-storage
```

Startup prints bootstrap and readiness details to stdout before the invoking
parent process exits:

- config path
- configured and resolved log path
- log level
- file logging initialization
- bind address
- `admin_shutdown_token=<token>`
- configured admin token-file path and write status
- background child PID, unless `--foreground` is used
- inference initialization start and ready/not-ready state
- storage/cache initialization start and ready/not-ready state
- HTTP bind/listening state
- final readiness summary and `/v1/health` URL

Capture the admin shutdown token from stdout when using curl directly. The same
startup-scoped token is also written to the configured
`[admin].token_file_path` for the local CLI client. The token is not written to
config, SQLite, or the service log.

The default example config uses:

```toml
[admin]
token_file_path = ".data-store-admin-token"
```

Relative admin token-file paths resolve from the Rust service root. The service
replaces stale token files on startup, creates the token file with owner-only
permissions, fails startup if the file cannot be written securely, and removes
the file on graceful shutdown when it still contains the current token.

Operational events after file logging initialization are written to the
configured `[logging].file_path`. Relative log paths resolve from the Rust
service root, so `logs/data-store.log` resolves to `logs/data-store.log` inside
this directory.

## Use The CLI Client

The `data-store` client is an interactive REPL over the same HTTP API shown in
this runbook. Start the service first, then start the client from this
directory:

```bash
cargo run --bin data-store -- --config config.toml
```

For a release build, run:

```bash
./target/release/data-store --config config.toml
```

The client reads `server.bind_address` and `admin.token_file_path` from the
same config file. Public commands call `/v1` endpoints without authentication.
Admin commands read the current token file immediately before sending the
request and use the same bearer-token authentication as curl.

The service must be running with a config that includes:

```toml
[admin]
token_file_path = ".data-store-admin-token"
```

If the configured token file is missing, start or restart the service. The
client does not ask for or store admin tokens.

At the prompt, use:

```text
data-store> health
data-store> limits
data-store> ingest The_Elements_of_Style.pdf
data-store> search "clear writing style rules" 3
data-store> search-full "clear writing style rules" 3
data-store> versions
data-store> rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
data-store> shutdown
data-store> help
data-store> exit
```

Command reference:

```text
health
limits
ingest <source>
search <query> [topK]
search-full <query> [topK]
versions
rollback <source> <versionLabel>
shutdown
help
exit
```

`search` prints excerpts. `search-full` prints the full matched unit content.
Quote multi-word queries and any argument containing spaces. The `shutdown`
command asks for typed confirmation before it sends the protected shutdown
request.

The client prints human-readable output only. It stores readline history in
`.data-store.history`; both the history file and `.data-store-admin-token` are
local ignored files.

## Health And Limits

Check readiness:

```bash
curl http://127.0.0.1:8091/v1/health
```

The top-level `ready` flag is based on readiness-critical components:

- `inference`: accelerator, model artifacts, tokenizer/model load, and startup
  smoke checks
- `storage_cache`: SQLite schema validation and active dense-cache load

Diagnostic-only components remain visible without making the service unready:

- `admission`: current ingest/search in-flight counters
- `logging`: initialized file log path and level

Fetch request and retrieval limits before constructing ingest or search
requests:

```bash
curl http://127.0.0.1:8091/v1/limits
```

Oversized bodies return `413 Payload Too Large`. Oversized fields, unknown JSON
fields, and invalid request values return `400 Bad Request`.

## Ingest A Source

Ingest uses a corpus-relative source reference. File bytes do not cross the
HTTP API; the service resolves the source inside its configured corpus root.

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"source":"The_Elements_of_Style.pdf"}' \
  http://127.0.0.1:8091/v1/ingest
```

A successful ingest creates a new immutable source-document version, persists
units and vectors, updates the active-version map, and publishes a new active
search snapshot only after the new version is durable and cache-ready.

## Search

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"query":"clear writing style rules","topK":3}' \
  http://127.0.0.1:8091/v1/search
```

Search is synchronous. It captures the active document-version snapshot at
request admission and uses that same snapshot through dense retrieval, BM25,
RRF, ColBERT MaxSim, Qwen3 reranking, and result materialization. The response
contains public results plus raw stage diagnostics.

If the configured ingest/search admission gate is saturated, the endpoint
returns `503 Service Unavailable` immediately. The service does not hide work
in an unbounded queue.

## Version Administration

Admin endpoints require the startup-scoped bearer token printed as
`admin_shutdown_token=<token>` or written to the configured admin token file.

List retained document versions:

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8091/admin/document-versions
```

Rollback one source document to an already-retained version:

```bash
curl -X POST \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"source":"The_Elements_of_Style.pdf","versionLabel":"2026-06-01T21:37:22.184Z"}' \
  http://127.0.0.1:8091/admin/document-versions/rollback
```

Rollback repoints `active_document_versions` and publishes a new active search
snapshot. It does not delete versions, rebuild embeddings, or run automatic
cleanup.

## Shutdown

Use the startup token for graceful shutdown:

```bash
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:8091/admin/shutdown
```

Accepted shutdown requests drain through Axum graceful shutdown instead of
requiring a process signal.

The CLI `shutdown` command asks for typed confirmation before sending the same
protected admin request.

## Verify

```bash
cargo fmt
cargo check
cargo check --features metal
```

## Common Operator Diagnostics

- Missing SQLite database: run `--setup-storage` deliberately, then restart the
  service.
- SQLite schema version or table mismatch: the runtime will not migrate; rebuild
  or set up the development database intentionally.
- Requested accelerator unavailable or not compiled: rebuild with the matching
  Cargo feature and verify the configured device.
- Missing model artifacts or tokenizer files: fix local model paths in
  `config.toml`; readiness must fail clearly rather than falling back.
- Docling conversion failure: inspect the API error and service log. The service
  does not silently switch PDF backends or OCR modes.
- `401 Unauthorized` on admin endpoints: use the current startup token from
  stdout or the configured admin token file; tokens do not survive restart.
- Missing admin token file for the CLI: start or restart the service with a
  config that includes `[admin].token_file_path`.
- `503 Service Unavailable` on ingest/search: the configured in-flight limit is
  saturated. Retry after active work finishes or change the config deliberately.
