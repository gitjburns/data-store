# Installing the Data Store service

This guide covers first-time setup of the Data Store service: configuring it,
creating its storage, starting it, and verifying the install. It describes the
service as built. The service is first exercised at C10f commissioning; nothing
in this repository has been production-run before then, so the steps below state
what the code does at startup, not runtime-observed behavior.

The service has two primary binaries. `data-store-service` is the
service itself (`start.sh` runs `./target/release/data-store-service`);
`data-store` is the separate CLI client, which also provides the interactive
REPL (see `SPEC-CLIENT.md`).

## 1. Prerequisites and configuration

Parsing needs no external tool: EPUB (`.epub`, EPUB 2 and EPUB 3) and plain
text are parsed in-process by the service binary. Other formats are converted
to plain text outside this service before they enter the corpus.

### Copy the example configuration

The service reads `config.toml` from the working directory by default, or from
the path passed with `--config <path>`. Start from the shipped example:

```sh
cp config.example.toml config.toml
```

Every key is validated at startup with `deny_unknown_fields`. An unknown or
misspelled key anywhere in the file is a fatal startup error, not a silently
ignored value. Comments in `config.example.toml` document each field.

### Path resolution

Relative paths in the configuration resolve against the **parent directory of
the config file**, not the process working directory. Absolute paths are used
verbatim. This rule applies to log paths, the admin token file, API-key files,
and the client history file.

### Required sections to fill

The example ships with placeholder paths (`/absolute/path/to/...`). Before first
start, fill in at least the following:

- **`[server].bind_address`** — the socket address the HTTP service binds.
  Required, with no built-in default. The bundled client dials this same
  address; a bind-all address (`0.0.0.0`/`::`) is converted to loopback for
  the client.

- **`[admin].token_file_path`** — the file the service writes the startup admin
  token to, and the file the client reads it back from. Owner-only; see the
  token handoff below. Required, with no built-in default;
  `config.example.toml` ships `.data-store-admin-token`.

- **`[client].operation_timeout_seconds`** — used only by the bundled client,
  not by the service. It bounds each individual HTTP request; the operation
  poll loop itself is not bounded and runs until the operation reaches a
  terminal state. The server validates this section but never reads it.

- **`[inference].device`** / **`device_index`** — the accelerator backend for the
  retrieval models configured with `backend = "local"`. Local inference requires
  the matching compiled accelerator feature and has no CPU fallback. When dense,
  ColBERT, and the reranker all use HTTP, accelerator initialization is skipped;
  `[inference]` remains required but its device selection is unused.

- **`[storage].corpus_root`** — the root that corpus-relative ingest references
  address.

- **`[storage].index_root`** — the service-owned root for the fabric hot plane
  and the artifact store (see storage setup below).

- **`[epub]`** — six required admission budgets for EPUB archives:
  `max_members` (archive members accepted before the parse fails as a recorded
  outcome), `max_member_bytes` (decompressed bytes accepted for any single
  member), `max_total_member_bytes` (decompressed bytes accepted across all
  members read), `max_document_bytes` (decoded bytes accepted for any XML
  member), `max_image_bytes` (bytes accepted for one image; larger images are
  not archived and produce a warning), and `max_element_depth` (element
  nesting depth accepted in any XML member). Exceeding a budget other than
  `max_image_bytes` is a recorded parse failure naming the budget. The budgets
  are not part of parser identity.

- **`[indexing]`** — the retrieval grains, all required: ColBERT-token caps
  for fine chunks (`fine_max_tokens`), ColBERT windows (`colbert_max_tokens`,
  which must equal the ColBERT model's document limit), and context windows
  (`context_max_tokens`); the number of context windows per annotation excerpt
  (`excerpt_windows`); and the minimum fill of any run as a fraction of its cap
  (`min_fill_ratio`). Startup rejects a fine cap that could force the higher
  grains to split a chunk and a context cap that, with its section-path
  prefix, does not fit the dense and reranker capacities. See README,
  "Retrieval grains".

- **`[models.colbert].backend`** — required, selecting `local` or `http` without
  fallback. Local requires an absolute `path` to ColBERT-Zero artifacts. HTTP
  requires `endpoint`, `model`, an absolute `tokenizer_file_path`, and positive
  `timeout_seconds`, with optional `api_key_file_path`. Fields belonging to the
  unused backend must be absent. Both require `dimension = 128` and positive
  `query_max_tokens` / `document_max_tokens` no greater than 518.

- **`[models.dense].backend`** — required, selecting the dense embedding backend.
  The `local` backend requires `path` + `max_tokens`; the `http` backend requires
  `endpoint`, `model`, `timeout_seconds`, with an optional `api_key_file_path`;
  the two are exclusive and the unused backend's fields must not be set.

- **`[models.reranker].backend`** — required, selecting the reranker backend. The
  `local` backend requires `path` + `max_tokens`; the `http` backend requires
  `endpoint`, `model`, `timeout_seconds`, with an optional `api_key_file_path`;
  the two are exclusive and the unused backend's fields must not be set.

- **`[models.annotator].endpoint`** and **`model`** — the external
  OpenAI-compatible chat-completions endpoint used by the annotation producers.
  This is an exclusive dependency: there is no fallback endpoint. Set
  **`api_key_file_path`** to an owner-only file holding the bearer API key if the
  endpoint requires authentication.

- **`[policies].entity_match_file_path`** and
  **`annotator_naming_file_path`** — paths to the two operator-editable policy
  documents (both required; relative paths resolve against the config
  directory). `config.example.toml` ships `policies/entity-match.toml` and
  `policies/annotator-naming.toml`. A neutral posture — an empty naming-rule
  list, both fuzzy-match classes disabled — is a valid default that is
  byte-identical to no policy; `policies/entity-match.toml` ships neutral, but
  this repo's `policies/annotator-naming.toml` is authored for the commissioning
  corpus (it carries authored naming rules, not an empty list). Editing them is
  optional and best done after inspecting the corpus vocabulary
  (`GET /annotations/vocabulary`); note that editing the naming document is
  **producer-identity-bearing** — it changes the entity/relation producer prompt
  hash, invalidating memoized producer output and re-annotating the frontier.
  The documents carry **no version field**: the service assigns versions itself
  by content-hashing each document, so do not add one. Both documents are loaded
  once at startup and are strictly validated (unknown keys or invalid values are
  fatal); an edit takes effect only after a service restart.

### Remote ColBERT

Serve `lightonai/ColBERT-Zero` on the inference host with vLLM 0.28.0:

```sh
vllm serve lightonai/ColBERT-Zero --runner pooling --hf-overrides '{"architectures":["ColBERTModernBertModel"]}' --pooler-config.task token_embed
```

Set `[models.colbert].backend = "http"`, remove `path`, and set `endpoint` to
the full HTTP(S) URL ending in `/pooling`. The URL must not contain credentials,
query parameters, or a fragment. Set `model = "lightonai/ColBERT-Zero"` and
`tokenizer_file_path` to an absolute local path to that checkpoint's
`tokenizer.json`. The existing local tokenizer can be reused when it matches
the served checkpoint and revision; local model weights are not needed.

The app formats and tokenizes inputs locally, sends token IDs for document and
query inference, and retains document token matrices for CPU MaxSim scoring.
Startup validates remote document/query inference and CPU scoring before
reporting inference readiness; an unreachable endpoint or invalid matrix fails
startup.

### Owner-only secret files

The admin token file, the annotator API-key file (`.annotator-api-key`), the
dense HTTP-backend API-key file (`.data-store-dense-api-key`), ColBERT HTTP-backend
API-key file (`.data-store-colbert-api-key`), and any reranker API-key file hold
secrets. The service writes the admin token file with `0600`
(owner read/write only); create the API-key files with the same restriction
yourself (config-dir-relative, like the admin token file). `.gitignore` already
excludes `config.toml`, `.data-store-admin-token`, `.annotator-api-key`,
`.data-store-dense-api-key`, and the client history file, so these are not
committed.

## 2. Storage setup

Storage does not exist until you create it. Run the service binary once with
`--setup-storage`:

```sh
data-store-service --setup-storage            # uses ./config.toml
data-store-service --config /path/to/config.toml --setup-storage
```

(`--setup-storage` belongs to the service binary; the `data-store` client
rejects it.)

This action is **fabric-only**. It creates the `{index_root}/fabric` directory
and the fabric hot plane at `{index_root}/fabric/fabric.sqlite3`. The
content-addressed artifact store tree under `{index_root}/fabric/artifacts` is
not created by setup; the running service creates it lazily on first use.

`--setup-storage` is the **only** path that installs schema. The running service
never creates or migrates schema; that is a fixed process rule. Re-running
setup against an existing fabric plane is safe when the plane is compatible
(same schema version, every table contract valid): it validates the database
and leaves it untouched. An incompatible plane (schema version differs, or a
table contract fails) is never migrated: setup logs the reason and the row
counts of `content_units`, `semantic_annotations`, and `annotation_memo` that
are about to be lost, deletes the whole `{index_root}/fabric` directory
(hot plane and artifact store), and creates a fresh database. There is no
confirmation prompt; if that data matters, back up `{index_root}/fabric`
before re-running setup after a schema change.

The command prints `fabric storage schema ready at <path>` and exits on success.

### Rebuild the current corpus

With the service running, use the client from the project root:

```sh
data-store --config config.toml --rebuild-all
```

The REPL command is `rebuild-all`; neither form takes arguments. It clears all
stored corpus data, snapshots, annotation caches, and prior operation history,
then resumes automatic ingestion with fresh parses, embeddings, and annotations.
The database schema, original corpus files, models, configuration, and service
logs remain intact. No storage setup or migration is required.

Rebuild closes storage admission and cancels annotation HTTP requests, stops
further annotation dispatch, and discards unfinished results. The scheduler
finishes its admitted cycle. The command waits for storage leases to be released
before receiving an Operation ID; cancellation does not guarantee the inference
server immediately stops its own work. This initial request uses
`[client].operation_timeout_seconds`;
a timeout or disconnect does not cancel server work. If acceptance was not
received, consult health and the service log for the last known state.

Storage-dependent requests return `503` while existing work drains and storage
is cleared. Health, Operation polling, and shutdown remain available. Command
success means automatic rebuilding resumed; queries then see the progressively
rebuilt corpus. If clearing fails or is interrupted, storage remains paused,
including after restart. After durable acceptance, shutdown before resumption
also leaves an incomplete rebuild; annotation dry-run mode refuses that state.
Resolve the reported error and rerun `--rebuild-all` against the normally started
service.

## 3. Startup and admin-token handoff

Start the service:

```sh
data-store-service                            # background service (default)
data-store-service --foreground               # run in the foreground of this shell
```

(`--foreground`, like `--setup-storage`, is a service-binary flag; the
`data-store` client rejects it.)

Without `--foreground`, the process detaches a service child and the launching
process relays startup status until the child is up. With `--foreground`, the
service runs directly in the current shell and writes startup status to stdout.

Startup order, as built:

1. The service **binds the HTTP listener first**, before any lengthy dependency
   initialization, so it is reachable early.
2. It **generates a fresh admin token** for this startup and **reports it on the
   operator channel** (the startup status line `admin_shutdown_token=<token>`).
   The durable log records only that a token was reported, not the token itself.
3. It **writes the token to the admin token file** at the configured
   `[admin].token_file_path`, with `0600` permissions, replacing any stale file.
   The client reads this file freshly for every protected request; the service
   itself validates the presented bearer against its in-memory startup token
   and does not read the file per request.
4. It then initializes inference and the acquisition/sync machinery.

With `[models.dense].backend = "http"`, inference init loads no local dense
artifacts; instead it runs an HTTP embedding smoke round-trip against the
configured endpoint (progress stages `dense_http_smoke_embedding` /
`dense_http_smoke_ready`). Dense embedding is readiness-critical, so an
unreachable or misconfigured endpoint is a fatal startup error.

### Readiness

Top-level readiness is the conjunction of two components: **inference** and
**sync**. If the fabric hot plane is absent or invalid, startup is **not** fatal:
the service still binds and serves, but reports `ready=false`, and `/v1/health`
explains why. The remedy in that case is to run `--setup-storage`.

## 4. Verifying the install

Query health over HTTP:

```sh
curl http://127.0.0.1:8091/v1/health
```

or use the bundled client (this invokes the `data-store` client binary, not
the service binary):

```sh
data-store --health
```

`Ready: yes` with both the inference and sync components healthy indicates a
complete install. `Ready: no` reports which component is not yet healthy; a
missing or invalid fabric plane shows as the `sync` component not ready (the
client renders `sync: no`) — run `--setup-storage`. (`fabricReady` is a field
of the `sync-status` command / `GET /sync/status` output only, not of health.)

Because the service is first exercised at C10f commissioning, treat this health
check as the first live confirmation of the install rather than a re-run of
previously verified behavior.

For interactive use of the service, see `SPEC-CLIENT.md`.

## 5. Annotation dry-run mode (authoring rulesets on a fresh corpus)

The policy documents in section 1 are corpus-dependent: sensible entity-match
and naming rules come from the corpus's own observed vocabulary, not from
guessing. The annotation dry-run mode exists so you can observe that vocabulary
**before** paying for full annotation and embedding. On a fresh corpus, the
intended procedure is:

1. **Start from a fresh plane.** Point `[storage].index_root` at a new or clean
   location and run `--setup-storage` (section 2). Unlike a normal start, the
   dry-run mode **requires** a valid fabric plane and fails fatally without
   one.

2. **Run the dry-run pass:**

   ```sh
   data-store-service --config config.toml --annotation-dry-run <N>
   ```

   `<N>` is a positive integer: the pass parses the corpus, then is meant to
   sample-annotate the first `<N>` excerpts per source per type with the
   entity and relation producers (summaries are excluded). The mode currently
   cannot sample: excerpts are runs of context windows, which the dry-run pass
   does not build, so its annotation plans are empty (SPEC-SERVER.md §4.7).
   The mode always runs in the foreground and serves only health, the
   vocabulary route, operation reads, and shutdown. (`--annotation-dry-run`,
   like `--setup-storage`, is a service-binary flag; the `data-store` client
   rejects it.)

   **`Ready: no` in health is expected here**: no inference runtime is
   started, and the inference component reports
   `annotation dry-run mode: inference not initialized` by design.

3. **Inspect the observed vocabulary** while the mode serves:

   ```sh
   data-store --vocabulary entity all
   data-store --vocabulary relation all
   ```

   Use scope `all`: the dry-run parses are never activated, so the default
   `active` scope would show nothing.

4. **Author the rulesets.** Edit `policies/entity-match.toml` and
   `policies/annotator-naming.toml` against what you observed (section 1 —
   remember the naming document is producer-identity-bearing).

5. **Optionally run a second dry-run** with the edited documents and compare
   the resulting vocabulary before committing to the rulesets.

6. **Stop the mode** (`data-store --shutdown`) and **start the service
   normally.** With the same parser identity, the normal start adopts the
   dry-run's imported parses without repeating extraction, builds projections,
   gates and activates, and completes ingestion under the final rulesets.
