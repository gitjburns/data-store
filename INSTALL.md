# Installing The Data Store Service

First-time setup for the standalone Data Store service. This service is
intentionally independent from the existing Node backend and frontend. It owns
its own config, corpus, Docling conversion, SQLite storage, dense vector cache,
and retrieval pipeline, and it currently runs alongside the existing
Node-backed Data Store.

For what the service does and how to operate it once running, see `README.md`.

Backend/frontend integration is later scope. Do not expect this service to be
started or managed by the Node app yet.

Run commands in this document from `service/data-store/` unless stated
otherwise.

## Prerequisites

Create a local `config.toml` from `config.example.toml`. The local config is
machine-specific and should point at:

- local model directories for Qwen3 embedding and ColBERT-Zero
- either a local ModernBERT reranker model directory or a remote
  Cohere-compatible rerank endpoint with model name, timeout, and optional API
  key file
- a service-owned corpus root for source files
- a service-owned index root for SQLite and Docling conversion artifacts
- the Python executable for environment diagnostics and the directly launched Docling executable
- the CLI operation timeout and explicit Docling document timeout, PDF backend,
  OCR mode, Docling device, thread count, and page batch size
- the selected Rust inference accelerator backend and device index for local
  Candle inference
- required request, retrieval, admission, logging, and admin token-file settings

The service has no CPU fallback for local Candle inference. If
`inference.device = "metal"`, run with `--features metal`. CUDA support is
separate operational verification work; this guide documents the locally used
Metal path.

Docling's `[docling].device` setting is passed to the Python Docling CLI as
`--device` and is separate from `[inference].device`, which controls the
Rust/Candle dense embedding, ColBERT, and local reranker runtimes.

`[models.reranker].backend` selects exactly one final reranker backend. Use
`backend = "local"` with `path` and `max_tokens` for the in-process Candle
ModernBERT runtime, or `backend = "http"` with `endpoint`, `model`,
`timeout_seconds`, and optional `api_key_file_path` for a remote endpoint
speaking the Cohere-compatible rerank contract. The service never falls back
between reranker backends; local misconfiguration or an unreachable HTTP
backend fails readiness or search explicitly.

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

## Build The Service Binary

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
parent process exits. It includes the current `admin_shutdown_token=<token>` for
manual protected operations and a `/v1/health` compatibility URL for readiness
checks.

The default example config uses:

```toml
[admin]
token_file_path = ".data-store-admin-token"
```

The CLI client reads the same startup-scoped admin token from this file.
Relative admin token-file paths resolve from the Rust service root.

Operational events after file logging initialization are written to the
configured `[logging].file_path`. Relative log paths resolve from the Rust
service root, so `logs/data-store.log` resolves to `logs/data-store.log` inside
this directory.

## Verify

```bash
cargo fmt
cargo check
cargo check --features metal
cargo clippy
```

