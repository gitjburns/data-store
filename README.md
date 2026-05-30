# Data Store Service

Standalone HTTP service for document ingestion and retrieval.

This service is intentionally independent from the existing Node backend. The
current milestone establishes the Rust application shell and HTTP contract only.
Model loading, Docling execution, LanceDB persistence, and retrieval are later
implementation work.

## Run

```bash
cargo run -- --config config.example.toml
```

## Verify

```bash
cargo fmt
cargo check
```

## Current API

- `GET /v1/health` reports service readiness. This scaffold reports
  `ready: false` until model and index runtime is implemented.
- `POST /v1/ingest` validates the request shape and returns `501`.
- `POST /v1/search` validates the request shape and returns `501`.

