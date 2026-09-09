# Data Store — Quickstart

Condensed command reference for install and operation. Detail lives in
**INSTALL.md** (setup), **README.md** (operating model), **PROTOCOL.md**
(HTTP contract), and **SPEC-CLIENT.md** (CLI/REPL reference). Commands below assume the
repo root as working directory; the built binaries are
`./target/release/data-store-service` (the service) and
`./target/release/data-store` (the CLI client), shown here by bare name.

## 1. Build

```sh
cargo build --release                     # All retrieval models use HTTP
cargo build --release --features metal    # Apple accelerator
cargo build --release --features cuda     # NVIDIA accelerator
```

Choose one build. When dense, ColBERT, and the reranker all use `backend = "http"`,
retrieval requires no local accelerator; ColBERT MaxSim runs on the CPU.
`[inference]` remains required but its device selection is unused. Any local
retrieval backend requires the feature matching `[inference].device`; local
models have no CPU fallback.

## 2. Configure

```sh
cp config.example.toml config.toml
```

Fill the placeholder paths (`[storage].corpus_root`, `[storage].index_root`,
`[docling]`, `[models.*]` — all documented inline in the example). Unknown or
misspelled keys anywhere in `config.toml` are fatal at startup. Relative paths
resolve against the config file's directory. Config is startup-only: any edit
requires a service restart.

For remote ColBERT, select `backend = "http"`, remove `path`, and configure the
full `/pooling` endpoint, served model, timeout, and absolute local
`tokenizer_file_path` matching the served checkpoint. See **INSTALL.md**, Remote
ColBERT, for the vLLM serving command and configuration requirements.

API-key files for HTTP backends (dense, ColBERT, reranker, annotator) are owner-only
secret files named in config, e.g.:

```sh
printf '%s' "$API_KEY" > .annotator-api-key         && chmod 600 .annotator-api-key
printf '%s' "$API_KEY" > .data-store-dense-api-key  && chmod 600 .data-store-dense-api-key
```

## 3. Create storage (one-time)

```sh
data-store-service --config config.toml --setup-storage
# → fabric storage schema ready at {index_root}/fabric/fabric.sqlite3
```

The only path that ever creates schema; the running service never migrates.
Re-run only against a new or clean `index_root`.

## 4. Fresh corpus? Author the rulesets first (optional but intended)

Sample-annotate before paying for full annotation, then author the two policy
documents from observed vocabulary:

```sh
data-store-service --config config.toml --annotation-dry-run 5   # parses corpus, samples 5 groups/source/type, serves for inspection
data-store --config config.toml --vocabulary entity all          # inspect observed entity vocabulary
data-store --config config.toml --vocabulary relation all        # inspect observed predicates
$EDITOR policies/annotator-naming.toml policies/entity-match.toml
data-store --config config.toml --shutdown                       # end dry-run mode
```

`ready=false` health is expected during a dry-run (no inference runtime).
Scope `all` is required — sampled parses are never active. Editing the naming
document changes producer identity; the entity-match document is
query-time-only. The next normal start adopts the dry-run's parses without
re-running Docling.

## 5. Start and verify

```sh
data-store-service --config config.toml               # daemonized (default); ./start.sh is equivalent
data-store-service --config config.toml --foreground  # stay attached to this shell
```

Startup binds HTTP first, prints one `admin_shutdown_token=<token>` line on the
operator channel, and writes the token to the admin token file (0600) that the
CLI reads automatically. Do not pipe startup through short-lived readers —
SIGPIPE kills the relay chain.

```sh
data-store --config config.toml --health              # or: curl http://127.0.0.1:8091/v1/health
data-store --config config.toml --sync-status
```

`Ready: yes` = inference + sync healthy. `sync: no` usually means a missing
fabric plane — run `--setup-storage`. Ingestion then runs autonomously: the
scheduler detects, acquires, parses, and activates corpus files on its own
cadence; the annotation worker enriches after activation. Watch progress in
the service log (`[logging].file_path`, `logs/data-store.log` as shipped).

## 6. Query

The CLI takes bare query text; the client builds the `{"queryText": ...}` body
itself:

```sh
data-store --config config.toml --query how does activation gating work
```

For the full request envelope (extra fields, debug diagnostics), use curl:

```sh
curl -s http://127.0.0.1:8091/query -H 'Content-Type: application/json' \
  -d '{"queryText":"...","retrievalPolicy":{"maxFinalEvidenceUnits":5},
       "evidencePolicy":{"includeRelationships":true,"includeAnnotations":true},
       "debug":true}'
```

In that envelope `queryText` is the only required field; `debug:true` attaches
per-stage retrieval diagnostics (the per-channel operator window).

## 7. Operate

Public reads:

```sh
data-store --config config.toml --unit <unitId>
data-store --config config.toml --relationships <unitId> [direction] [relationshipType]
data-store --config config.toml --source <sourceId>
```

Admin verbs (CLI reads the token file; each submits an async Operation and
polls it to a terminal state — Operation `succeeded` is pipeline completion,
NOT the parse verdict, which lives in the parse run / held-parses):

```sh
data-store --config config.toml --ingest <sourceSystem> <nativeUri>
data-store --config config.toml --reparse <sourceId> <sourceSystem> <nativeUri>
data-store --config config.toml --held-parses
data-store --config config.toml --accept <parseId>
data-store --config config.toml --discard <parseId>
data-store --config config.toml --activate <sourceId> <parseId>
data-store --config config.toml --snapshot [requestJson]
data-store --config config.toml --restore <sourceId> <parseId>
data-store --config config.toml --operation <operationId>
data-store --config config.toml --vocabulary <entity|relation> [active|all]
```

Raw HTTP instead of the CLI: send the token file's contents as
`Authorization: Bearer <token>`.

### Interactive REPL

Run `data-store --config config.toml` with no operation flag to enter the REPL:
same verbs without leading dashes (e.g. `query how does activation gating work`).
`help` lists commands; `exit` leaves.

## 8. Stop

```sh
data-store --config config.toml --shutdown
```

Graceful: confirms immediately, then the service joins its worker threads and
cleans up the token file. There is no OS-signal handling; use the route.

## 9. Where things live

| Path | What |
| --- | --- |
| `config.toml` | All operational configuration (startup-only). |
| `{index_root}/fabric/fabric.sqlite3` | The hot plane — the only durable store. |
| `{index_root}/fabric/artifacts/` | Content-addressed, write-once artifact store. |
| `policies/*.toml` | Operator-editable rulesets (restart to apply). |
| `logs/data-store.log` | The durable service log (as shipped). |
| `.data-store-admin-token` | Startup-scoped admin token (written per start). |
