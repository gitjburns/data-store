# Data Store Service

Standalone HTTP service for document ingestion and retrieval.

This service is intentionally independent from the existing Node backend. The
current implementation owns its own config, corpus, Docling conversion,
SQLite storage, dense vector cache, and retrieval pipeline. It runs alongside
the existing Node-backed Data Store; backend/frontend integration is later
scope.

Implemented capabilities:

- Axum/Tokio HTTP API for health, ingest, search, and protected shutdown.
- Explicit Metal/CUDA accelerator selection with no CPU fallback.
- Local Qwen3 dense embedding runtime through Candle.
- PDF ingest through Docling, deterministic unit splitting, dense embedding,
  SQLite persistence, and FTS5 population.
- Startup-loaded in-memory dense vector cache with exact cosine retrieval.
- SQLite FTS5 BM25, dense/BM25 over-fetch, and RRF fusion.
- Explicit `--setup-storage` schema setup; normal runtime validates existing
  storage without creating or migrating schema.

## Run

```bash
cargo run --manifest-path service/data-store/Cargo.toml --features metal -- --config service/data-store/config.toml
```

The service prints `admin_shutdown_token=<token>` at startup. Use that token for
graceful shutdown:

```bash
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:8091/admin/shutdown
```

## Setup Storage

```bash
cargo run --manifest-path service/data-store/Cargo.toml -- --config service/data-store/config.toml --setup-storage
```

## Verify

```bash
cargo fmt --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml --features metal
```

## Current API

- `GET /v1/health` reports inference and storage/cache readiness diagnostics.
- `POST /v1/ingest` synchronously converts, splits, embeds, persists, and makes
  one corpus-relative source searchable.
- `POST /v1/search` returns dense/BM25/RRF fused results with raw diagnostics.
- `POST /admin/shutdown` gracefully stops the service when called with the
  startup-scoped bearer token.
