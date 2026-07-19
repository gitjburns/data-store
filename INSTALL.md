# Installing the Data Store service

This guide covers first-time setup of the Data Store service: configuring it,
creating its storage, starting it, and verifying the install. It describes the
service as built. The service is first exercised at C10f commissioning; nothing
in this repository has been production-run before then, so the steps below state
what the code does at startup, not runtime-observed behavior.

The build produces two operator-facing binaries. `data-store-service` is the
service itself (`start.sh` runs `./target/release/data-store-service`);
`data-store` is the separate CLI client, which also provides the interactive
REPL (see `INTERACTIVE.md`).

## 1. Prerequisites and configuration

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
  Required, with no built-in default; `config.example.toml` ships
  `127.0.0.1:8091`. The bundled client dials this same address; a
  bind-all address (`0.0.0.0`/`::`) is converted to loopback for the client.

- **`[admin].token_file_path`** — the file the service writes the startup admin
  token to, and the file the client reads it back from. Owner-only; see the
  token handoff below. Required, with no built-in default;
  `config.example.toml` ships `.data-store-admin-token`.

- **`[client].operation_timeout_seconds`** — used only by the bundled client,
  not by the service. It bounds each individual HTTP request; the operation
  poll loop itself is not bounded and runs until the operation reaches a
  terminal state. The server validates this section but never reads it.

- **`[inference].device`** / **`device_index`** — the accelerator backend for the
  in-process models (ColBERT always, plus dense and the reranker when their
  `backend = "local"`). There is no CPU fallback.

- **`[storage].corpus_root`** — the root that corpus-relative ingest references
  address.

- **`[storage].index_root`** — the service-owned root for the fabric hot plane
  and the artifact store (see storage setup below).

- **`[docling].python_path`** and **`docling_path`** — the Python environment and
  the Docling executable launched for PDF-to-markdown conversion.

- **`[models.colbert].path`** — the local ColBERT-Zero model artifact directory.

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

### Owner-only secret files

The admin token file, the annotator API-key file (`.annotator-api-key`), the
dense HTTP-backend API-key file (`.data-store-dense-api-key`), and any reranker
API-key file hold secrets. The service writes the admin token file with `0600`
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
never creates or migrates schema; that is a fixed process rule. Treat setup as a
deliberate, one-time operator action. Re-run it only when pointing at a new or
clean `index_root`; when run against an existing fabric plane, it validates the
existing schema rather than recreating it.

The command prints `fabric storage schema ready at <path>` and exits on success.

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

For interactive use of the service, see `INTERACTIVE.md`.
